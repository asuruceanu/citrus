/*
 * Copyright the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.citrusframework.distribution.worker;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionSettings.WorkerCwdCheckMode;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.engine.CancellationToken;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrokerBlobClientCacheTest {

    private static final String BLOB_PATH = "/api/v1/cache/blobs/";

    @TempDir
    Path tempDir;

    private HttpServer server;
    private ExecutorService serverExecutor;
    private final AtomicInteger blobGets = new AtomicInteger();
    private volatile BlobHandler blobHandler = exchange -> respond(exchange, 404, new byte[0]);

    @FunctionalInterface
    interface BlobHandler {
        void handle(HttpExchange exchange) throws IOException;
    }

    /** Runs in the partition class loader. */
    public static class TrivialFixture {
        @Test
        void passes() {
        }
    }

    /** Runs in the partition class loader and reads a resource of a partition jar like libraries do. */
    public static class JarResourceFixture {
        @Test
        void readsJarResource() throws IOException {
            URL resource = JarResourceFixture.class.getClassLoader().getResource("cache-probe/resource.txt");
            if (resource == null) {
                throw new AssertionError("partition jar resource not found");
            }
            try (InputStream in = resource.openStream()) {
                String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                if (!"probe".equals(content)) {
                    throw new AssertionError("unexpected resource content: " + content);
                }
            }
        }
    }

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        serverExecutor = Executors.newCachedThreadPool();
        server.setExecutor(serverExecutor);
        server.createContext(BLOB_PATH, exchange -> {
            try (exchange) {
                blobGets.incrementAndGet();
                blobHandler.handle(exchange);
            } catch (IOException | RuntimeException e) {
                // the client may abort a download mid-stream
            }
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
        serverExecutor.shutdownNow();
    }

    @Test
    void shouldServeWarmBlobFromStampWithoutHttpRequestAndReDownloadAfterExternalCorruption() throws Exception {
        byte[] content = "warm blob content".getBytes(StandardCharsets.UTF_8);
        ArtifactRef ref = ref(content);
        blobHandler = exchange -> respond(exchange, 200, content);

        try (WorkerFileCache cache = WorkerFileCache.open(tempDir.resolve("cache"), true, 0L);
             BrokerBlobClient blobs = new BrokerBlobClient(config(tempDir.resolve("cache")), cache)) {
            Path first = blobs.ensureCached(ref);
            assertThat(blobGets).hasValue(1);
            assertThat(first).hasBinaryContent(content);

            Path second = blobs.ensureCached(ref);
            assertThat(second).isEqualTo(first);
            assertThat(blobGets).as("warm blob is served from its verification stamp").hasValue(1);

            Files.writeString(first, "corrupted!");
            Path third = blobs.ensureCached(ref);
            assertThat(blobGets).as("corrupted blob is downloaded again").hasValue(2);
            assertThat(third).hasBinaryContent(content);
            assertThat(tmpFiles(cache)).isEmpty();
        }
    }

    @Test
    void shouldTriggerPreDownloadEvictionFromContentLengthWhenArtifactSizeIsZero() throws Exception {
        Path cacheDir = tempDir.resolve("cache");
        byte[] seeded = "unpinned older blob".getBytes(StandardCharsets.UTF_8);
        String seededSha = ClasspathPackager.sha256Hex(seeded);
        Path seededBlob = new BlobCacheDirectory(cacheDir).put(seededSha, seeded);

        byte[] content = new byte[64 * 1024];
        Arrays.fill(content, (byte) 7);
        ArtifactRef unsized = new ArtifactRef("unsized.bin", ClasspathPackager.sha256Hex(content), ArtifactKind.REGULAR_FILE, 0L);
        AtomicBoolean evictedBeforeCompletion = new AtomicBoolean();
        blobHandler = exchange -> {
            exchange.sendResponseHeaders(200, content.length);
            OutputStream out = exchange.getResponseBody();
            out.write(content, 0, 1024);
            out.flush();
            long deadline = System.currentTimeMillis() + 5_000;
            while (Files.exists(seededBlob) && System.currentTimeMillis() < deadline) {
                sleep(10);
            }
            evictedBeforeCompletion.set(!Files.exists(seededBlob));
            out.write(content, 1024, content.length - 1024);
        };

        // Buffer exactly met while the seeded blob exists: only the Content-Length can create a deficit.
        WorkerFileCache.DiskSpaceProbe probe = dir -> Files.exists(seededBlob) ? 1_000L : Long.MAX_VALUE / 4;
        try (WorkerFileCache cache = WorkerFileCache.open(cacheDir, true, 1_000L, probe, Clock.systemUTC());
             BrokerBlobClient blobs = new BrokerBlobClient(config(cacheDir), cache)) {
            assertThat(seededBlob).as("not evicted at startup").exists();

            Path stored = blobs.ensureCached(unsized);

            assertThat(stored).hasBinaryContent(content);
            assertThat(evictedBeforeCompletion).as("unpinned blob evicted before the stream completed").isTrue();
            assertThat(seededBlob).doesNotExist();
            assertThat(tmpFiles(cache)).isEmpty();
        }
    }

    @Test
    void shouldAbortOversizedDownloadAndHttpErrorsLeavingZeroTempFiles() throws Exception {
        Path cacheDir = tempDir.resolve("cache");
        byte[] content = "the real content".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(content);
        byte[] corrupt = "the fake content".getBytes(StandardCharsets.UTF_8);

        try (WorkerFileCache cache = WorkerFileCache.open(cacheDir, true, 0L);
             BrokerBlobClient production = new BrokerBlobClient(config(cacheDir), cache);
             BrokerBlobClient blobs = new BrokerBlobClient(config(cacheDir), cache, 4 * 1024L)) {
            // The production cap is enforced before sending a request.
            ArtifactRef tooLarge = new ArtifactRef("big.bin", sha, ArtifactKind.REGULAR_FILE,
                    ClasspathPackager.MAX_BLOB_BYTES + 1);
            assertThatThrownBy(() -> production.ensureCached(tooLarge)).isInstanceOf(IOException.class)
                    .hasMessageEndingWith("more than the maximum of " + ClasspathPackager.MAX_BLOB_BYTES);
            ArtifactRef overTestCap = new ArtifactRef("big.bin", sha, ArtifactKind.REGULAR_FILE, 4 * 1024L + 1);
            assertThatThrownBy(() -> blobs.ensureCached(overTestCap)).isInstanceOf(IOException.class)
                    .hasMessageContaining("maximum of 4096");
            assertThat(blobGets).hasValue(0);

            // Chunked body exceeding the cap: aborted mid-stream.
            String oversizedSha = "ab".repeat(32);
            byte[] oversizedBody = new byte[4 * 1024 + 1];
            blobHandler = exchange -> {
                exchange.sendResponseHeaders(200, 0);
                exchange.getResponseBody().write(oversizedBody);
            };
            assertThatThrownBy(() -> blobs.ensureCached(oversizedSha)).isInstanceOf(IOException.class)
                    .hasMessageContaining("exceeds the maximum of 4096");
            assertNothingStored(cache);

            // Declared Content-Length above the cap: aborted before streaming.
            blobHandler = exchange -> respond(exchange, 200, oversizedBody);
            assertThatThrownBy(() -> blobs.ensureCached(oversizedSha)).isInstanceOf(IOException.class)
                    .hasMessageContaining("Content-Length of 4097");
            assertNothingStored(cache);

            for (int status : List.of(404, 500)) {
                blobHandler = exchange -> respond(exchange, status, "error".getBytes(StandardCharsets.UTF_8));
                assertThatThrownBy(() -> blobs.ensureCached(sha)).isInstanceOf(IOException.class)
                        .hasMessageContaining(String.valueOf(status));
                assertNothingStored(cache);
            }

            // Two corrupt responses: fails after one retry.
            blobGets.set(0);
            blobHandler = exchange -> respond(exchange, 200, corrupt);
            assertThatThrownBy(() -> blobs.ensureCached(sha)).isInstanceOf(IOException.class);
            assertThat(blobGets).hasValue(2);
            assertNothingStored(cache);

            // One corrupt response, then the right content: the retry succeeds.
            blobGets.set(0);
            AtomicInteger calls = new AtomicInteger();
            blobHandler = exchange -> respond(exchange, 200, calls.getAndIncrement() == 0 ? corrupt : content);
            assertThat(blobs.ensureCached(sha)).hasBinaryContent(content);
            assertThat(blobGets).hasValue(2);
            assertThat(tmpFiles(cache)).isEmpty();
        }

        // Severed connection mid-stream, then recovery on the same client.
        byte[] large = new byte[256 * 1024];
        Arrays.fill(large, (byte) 3);
        String largeSha = ClasspathPackager.sha256Hex(large);
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir.resolve("cache-severed"), true, 0L);
             BrokerBlobClient blobs = new BrokerBlobClient(config(tempDir.resolve("cache-severed")), cache)) {
            blobHandler = exchange -> {
                exchange.sendResponseHeaders(200, large.length);
                OutputStream out = exchange.getResponseBody();
                out.write(large, 0, large.length / 2);
                out.flush();
                // closing the exchange with missing bytes drops the connection
            };
            assertThatThrownBy(() -> blobs.ensureCached(largeSha)).isInstanceOf(IOException.class);
            assertNothingStored(cache);

            blobHandler = exchange -> respond(exchange, 200, large);
            assertThat(blobs.ensureCached(largeSha)).hasBinaryContent(large);
            assertThat(tmpFiles(cache)).isEmpty();
        }

        // Physical write failure fails only that partition; the next partition on the same cache succeeds.
        Path testClasses = Paths.get(TrivialFixture.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(testClasses);
        byte[] classes = packaged.bytes();
        blobHandler = exchange -> respond(exchange, 200, classes);
        Path partitionCache = tempDir.resolve("cache-enospc");
        DistributionWorkerConfig config = config(partitionCache);
        try (WorkerFileCache cache = WorkerFileCache.open(partitionCache, true, 0L);
             BrokerBlobClient blobs = new BrokerBlobClient(config, cache)) {
            AtomicBoolean diskFull = new AtomicBoolean(true);
            blobs.tempFileOpener(tmp -> {
                OutputStream out = Files.newOutputStream(tmp);
                if (!diskFull.get()) {
                    return out;
                }
                return new FilterOutputStream(out) {
                    private long written;

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        written += len;
                        if (written > 1024) {
                            throw new IOException("No space left on device");
                        }
                        out.write(b, off, len);
                    }
                };
            });
            IsolatedPartitionRunner runner = new IsolatedPartitionRunner(config, cache, blobs);
            PartitionAssignPayload assignment = new PartitionAssignPayload("p-" + UUID.randomUUID(), "s1", 1,
                    List.of(packaged.ref()), Map.of(), List.of(unit(TrivialFixture.class, "passes")));

            IsolatedPartitionRunner.PartitionExecutionResult failed = runner.execute(assignment, CancellationToken.create(), e -> { });
            assertThat(failed.success()).isFalse();
            assertThat(failed.errorMessage()).contains("No space left on device");
            assertNothingStored(cache);

            diskFull.set(false);
            List<WorkerTestEventPayload> events = new ArrayList<>();
            IsolatedPartitionRunner.PartitionExecutionResult recovered = runner.execute(assignment, CancellationToken.create(), events::add);
            assertThat(recovered.success()).as(recovered.errorMessage()).isTrue();
            assertThat(events).anySatisfy(e -> assertThat(e.status()).isEqualTo(TestOutcomeStatus.PASSED));
            assertThat(new BlobCacheDirectory(cache.rootDir()).resolveBlobPath(packaged.ref().sha256())).exists();
            assertThat(tmpFiles(cache)).isEmpty();
            assertThat(cache.isPinned(packaged.ref().sha256())).as("pins released after the partition").isFalse();
        }
    }

    @Test
    void shouldFailAStalledPartitionAndDownloadSuccessfullyOnTheNextPartition() throws Exception {
        ClasspathPackager.PackagedArtifact classes = ClasspathPackager.packagePath(
                Paths.get(TrivialFixture.class.getProtectionDomain().getCodeSource().getLocation().toURI()));
        CountDownLatch releaseBody = new CountDownLatch(1);
        blobHandler = exchange -> {
            exchange.sendResponseHeaders(200, classes.bytes().length);
            try (OutputStream body = exchange.getResponseBody()) {
                body.write(classes.bytes(), 0, 128);
                body.flush();
                try {
                    releaseBody.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        Path cacheDir = tempDir.resolve("cache-stall");
        DistributionWorkerConfig config = configBuilder(cacheDir).blobReadIdleTimeout(Duration.ofMillis(400)).build();
        try (WorkerFileCache cache = WorkerFileCache.open(cacheDir, true, 0L);
             BrokerBlobClient blobs = new BrokerBlobClient(config, cache);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            IsolatedPartitionRunner runner = new IsolatedPartitionRunner(config, cache, blobs);
            PartitionAssignPayload assignment = new PartitionAssignPayload("p-stall", "s1", 1,
                    List.of(classes.ref()), Map.of(), List.of(unit(TrivialFixture.class, "passes")));
            var execution = executor.submit(() -> runner.execute(assignment, CancellationToken.create(), e -> { }));
            try {
                var failed = execution.get(5, TimeUnit.SECONDS);
                assertThat(failed.success()).isFalse();
                assertThat(failed.errorMessage()).contains("Blob download stalled", classes.ref().sha256());
                assertNothingStored(cache);
            } finally {
                releaseBody.countDown();
                execution.cancel(true);
            }

            serve(classes);
            var recovered = runner.execute(assignment, CancellationToken.create(), e -> { });
            assertThat(recovered.success()).as(recovered.errorMessage()).isTrue();
            assertThat(tmpFiles(cache)).isEmpty();
        }
    }

    @Test
    void shouldKeepCacheOwnershipUntilAnInFlightStagingWriteAndCleanupFinish() throws Exception {
        byte[] content = "broker payload".getBytes(StandardCharsets.UTF_8);
        blobHandler = exchange -> respond(exchange, 200, content);
        Path cacheDir = tempDir.resolve("closing-staging-cache");
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);

        try (WorkerFileCache cache = WorkerFileCache.open(cacheDir, false, 0L);
             BrokerBlobClient blobs = new BrokerBlobClient(config(cacheDir), cache);
             ExecutorService executor = Executors.newFixedThreadPool(2)) {
            blobs.tempFileOpener(tmp -> new FilterOutputStream(Files.newOutputStream(tmp)) {
                @Override
                public void write(byte[] bytes, int offset, int length) throws IOException {
                    writing.countDown();
                    try {
                        if (!releaseWrite.await(10, TimeUnit.SECONDS)) {
                            throw new IOException("Test did not release staging write");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    out.write(bytes, offset, length);
                }
            });
            var download = executor.submit(() -> blobs.ensureCached(ref(content)));
            try {
                assertThat(writing.await(5, TimeUnit.SECONDS)).isTrue();
                blobs.close();
                var closing = executor.submit(cache::close);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!closing.isDone() && !cache.hasThreadsWaitingForEvictionLock() && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                assertThat(cache.hasThreadsWaitingForEvictionLock()).as("cache close waits for the staging write").isTrue();
                assertThatThrownBy(() -> WorkerFileCache.open(cacheDir, false, 0L))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("--cache-dir");

                releaseWrite.countDown();
                closing.get(5, TimeUnit.SECONDS);
                assertThat(tmpFiles(cache)).as("old owner cleaned staging before releasing its lock").isEmpty();
                try (WorkerFileCache successor = WorkerFileCache.open(cacheDir, false, 0L)) {
                    assertThatThrownBy(() -> download.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
                    assertNothingStored(successor);
                }
            } finally {
                releaseWrite.countDown();
            }
        }
    }

    @Test
    void shouldDisposeOutstandingStagingBeforeAnotherWorkerOwnsTheCache() throws IOException {
        Path cacheDir = tempDir.resolve("outstanding-staging-cache");
        AtomicReference<Path> stagedFile = new AtomicReference<>();
        AtomicBoolean outputClosed = new AtomicBoolean();
        try (WorkerFileCache cache = WorkerFileCache.open(cacheDir, false, 0L);
             WorkerFileCache.DownloadStaging staging = cache.openDownloadStaging(file -> {
                 stagedFile.set(file);
                 return new FilterOutputStream(Files.newOutputStream(file)) {
                     @Override
                     public void close() throws IOException {
                         super.close();
                         outputClosed.set(true);
                     }
                 };
             })) {
            staging.write(new byte[] {1}, 1);
            cache.close();

            assertThat(outputClosed).isTrue();
            assertThat(stagedFile.get()).doesNotExist();
            try (WorkerFileCache successor = WorkerFileCache.open(cacheDir, false, 0L)) {
                Files.writeString(stagedFile.get(), "new owner staging");
                assertThatThrownBy(() -> staging.write(new byte[] {2}, 1)).isInstanceOf(IllegalStateException.class);
                staging.close();
                assertThat(stagedFile.get()).hasContent("new owner staging");
            }
        }
    }

    @Test
    void shouldRecoverAfterFailureOpeningTheDownloadTempFile() throws Exception {
        ClasspathPackager.PackagedArtifact classes = ClasspathPackager.packagePath(
                Paths.get(TrivialFixture.class.getProtectionDomain().getCodeSource().getLocation().toURI()));
        serve(classes);
        Path cacheDir = tempDir.resolve("cache-open-enospc");
        DistributionWorkerConfig config = config(cacheDir);
        try (WorkerFileCache cache = WorkerFileCache.open(cacheDir, true, 0L);
             BrokerBlobClient blobs = new BrokerBlobClient(config, cache)) {
            blobs.tempFileOpener(tmp -> {
                assertThat(tmp).exists();
                throw new IOException("No space left on device");
            });
            assertThatThrownBy(() -> blobs.ensureCached(classes.ref())).isInstanceOf(IOException.class)
                    .hasMessageContaining("No space left on device");
            assertNothingStored(cache);

            IsolatedPartitionRunner runner = new IsolatedPartitionRunner(config, cache, blobs);
            PartitionAssignPayload assignment = new PartitionAssignPayload("p-enospc", "s1", 1,
                    List.of(classes.ref()), Map.of(), List.of(unit(TrivialFixture.class, "passes")));
            var failed = runner.execute(assignment, CancellationToken.create(), e -> { });
            assertThat(failed.success()).isFalse();
            assertThat(failed.errorMessage()).contains("No space left on device");
            assertNothingStored(cache);

            blobs.tempFileOpener(Files::newOutputStream);
            var recovered = runner.execute(assignment, CancellationToken.create(), e -> { });
            assertThat(recovered.success()).as(recovered.errorMessage()).isTrue();
            assertThat(tmpFiles(cache)).isEmpty();
        }
    }

    @Test
    void shouldRunAWarmInProcessPartitionWithoutRehashingItsCachedBlobs() throws Exception {
        ClasspathPackager.PackagedArtifact classes = ClasspathPackager.packagePath(
                Paths.get(TrivialFixture.class.getProtectionDomain().getCodeSource().getLocation().toURI()));
        ClasspathPackager.PackagedArtifact jar = ClasspathPackager.packagePath(resourceJar(tempDir.resolve("probe.jar")));
        assertThat(classes.ref().kind()).isEqualTo(ArtifactKind.DIRECTORY_ZIP);
        assertThat(jar.ref().kind()).isEqualTo(ArtifactKind.JAR);
        serve(classes, jar);

        Path cacheDir = tempDir.resolve("cache-warm");
        DistributionWorkerConfig config = config(cacheDir);
        AtomicInteger hashes = new AtomicInteger();
        try (WorkerFileCache cache = WorkerFileCache.open(cacheDir, true, 0L, null, Clock.systemUTC(), FileChannel::tryLock,
                root -> new BlobCacheDirectory(root) {
                    @Override
                    public boolean isIntact(String sha256) throws IOException {
                        hashes.incrementAndGet();
                        return super.isIntact(sha256);
                    }

                    @Override
                    public Path getVerifiedPath(String sha256) {
                        throw new AssertionError("blobs are verified through WorkerFileCache only");
                    }
                });
             BrokerBlobClient blobs = new BrokerBlobClient(config, cache)) {
            IsolatedPartitionRunner runner = new IsolatedPartitionRunner(config, cache, blobs);
            for (int run = 0; run < 2; run++) {
                PartitionAssignPayload assignment = new PartitionAssignPayload("p-" + UUID.randomUUID(), "s1", 1,
                        List.of(classes.ref(), jar.ref()), Map.of(), List.of(unit(JarResourceFixture.class, "readsJarResource")));
                IsolatedPartitionRunner.PartitionExecutionResult result = runner.execute(assignment, CancellationToken.create(), e -> { });
                assertThat(result.success()).as(result.errorMessage()).isTrue();
            }
            assertThat(blobGets).as("downloaded once").hasValue(2);
            assertThat(hashes).as("downloads are verified while stored, warm blobs are never re-hashed").hasValue(0);
        }
    }

    @Test
    void shouldReleaseNamedJarFileHandlesAfterAnInProcessPartition() throws Exception {
        ClasspathPackager.PackagedArtifact classes = ClasspathPackager.packagePath(
                Paths.get(TrivialFixture.class.getProtectionDomain().getCodeSource().getLocation().toURI()));
        ClasspathPackager.PackagedArtifact jar = ClasspathPackager.packagePath(resourceJar(tempDir.resolve("handles.jar")));
        serve(classes, jar);

        Path cacheDir = tempDir.resolve("cache-handles");
        DistributionWorkerConfig config = config(cacheDir);
        AtomicLong usable = new AtomicLong(Long.MAX_VALUE);
        try (WorkerFileCache cache = WorkerFileCache.open(cacheDir, true, 1L, p -> usable.get(), Clock.systemUTC());
             BrokerBlobClient blobs = new BrokerBlobClient(config, cache)) {
            PartitionAssignPayload assignment = new PartitionAssignPayload("p-" + UUID.randomUUID(), "s1", 1,
                    List.of(classes.ref(), jar.ref()), Map.of(), List.of(unit(JarResourceFixture.class, "readsJarResource")));
            IsolatedPartitionRunner.PartitionExecutionResult result = new IsolatedPartitionRunner(config, cache, blobs)
                    .execute(assignment, CancellationToken.create(), e -> { });
            assertThat(result.success()).as(result.errorMessage()).isTrue();

            Path jarBlob = new BlobCacheDirectory(cache.rootDir()).resolveBlobPath(jar.ref().sha256());
            Path namedJarDir = cache.rootDir().resolve("classpath").resolve(jar.ref().sha256());
            Path namedJar = namedJarDir.resolve(jar.ref().name());
            assertThat(namedJar).exists();

            usable.set(0L);
            cache.evictAfterPartition();

            assertThat(cache.evictionDeleteFailureCount()).isZero();
            assertThat(jarBlob).doesNotExist();
            assertThat(namedJarDir).doesNotExist();
            Path fdDir = Path.of("/proc/self/fd");
            if (Files.isDirectory(fdDir)) {
                try (Stream<Path> fds = Files.list(fdDir)) {
                    List<String> openJar = fds.map(BrokerBlobClientCacheTest::linkTarget)
                            .filter(target -> target.startsWith(namedJar.toString()) || target.startsWith(jarBlob.toString()))
                            .toList();
                    assertThat(openJar).as("no open handle keeps the evicted jar's inode alive").isEmpty();
                }
            }
        }
    }

    private void serve(ClasspathPackager.PackagedArtifact... artifacts) {
        Map<String, byte[]> blobsBySha = new java.util.HashMap<>();
        for (ClasspathPackager.PackagedArtifact artifact : artifacts) {
            blobsBySha.put(artifact.ref().sha256(), artifact.bytes());
        }
        blobHandler = exchange -> {
            String path = exchange.getRequestURI().getPath();
            byte[] body = blobsBySha.get(path.substring(path.lastIndexOf('/') + 1));
            respond(exchange, body != null ? 200 : 404, body != null ? body : new byte[0]);
        };
    }

    private static Path resourceJar(Path jar) throws IOException {
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
            out.putNextEntry(new JarEntry("cache-probe/resource.txt"));
            out.write("probe".getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
        return jar;
    }

    private static String linkTarget(Path fd) {
        try {
            return Files.readSymbolicLink(fd).toString();
        } catch (IOException | UnsupportedOperationException e) {
            return "";
        }
    }

    private DistributionWorkerConfig config(Path cacheDir) {
        return configBuilder(cacheDir).build();
    }

    private DistributionWorkerConfig.Builder configBuilder(Path cacheDir) {
        return DistributionWorkerConfig.builder()
                .workerId("blob-" + UUID.randomUUID().toString().substring(0, 6))
                .brokerUrl("http://127.0.0.1:" + server.getAddress().getPort())
                .cacheDir(cacheDir)
                .cacheCleanup(true)
                .cacheFreeSpaceBufferBytes(0L)
                .cwdCheckMode(WorkerCwdCheckMode.OFF);
    }

    private static ArtifactRef ref(byte[] content) {
        return new ArtifactRef("blob.bin", ClasspathPackager.sha256Hex(content), ArtifactKind.REGULAR_FILE, content.length);
    }

    private static WorkUnitSpec unit(Class<?> fixture, String method) {
        return new WorkUnitSpec(fixture.getName() + "#" + method, fixture.getName(), method, null, Map.of(), 0L);
    }

    private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            exchange.getResponseBody().write(body);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void assertNothingStored(WorkerFileCache cache) throws IOException {
        assertThat(tmpFiles(cache)).as("temporary files").isEmpty();
        assertThat(new BlobCacheDirectory(cache.rootDir()).listBlobs()).as("stored blobs").isEmpty();
    }

    private static List<Path> tmpFiles(WorkerFileCache cache) throws IOException {
        Path tmp = cache.rootDir().resolve("tmp");
        if (!Files.isDirectory(tmp)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(tmp)) {
            return files.toList();
        }
    }
}
