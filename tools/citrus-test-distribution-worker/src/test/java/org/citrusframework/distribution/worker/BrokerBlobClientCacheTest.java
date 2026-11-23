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
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
             BrokerBlobClient blobs = new BrokerBlobClient(config(cacheDir), cache)) {
            // Declared size above the cap: rejected before any request.
            ArtifactRef tooLarge = new ArtifactRef("big.bin", sha, ArtifactKind.REGULAR_FILE, ClasspathPackager.MAX_BLOB_BYTES + 1);
            assertThatThrownBy(() -> blobs.ensureCached(tooLarge)).isInstanceOf(IOException.class);
            assertThat(blobGets).hasValue(0);

            // Chunked body exceeding the cap: aborted mid-stream.
            String oversizedSha = "ab".repeat(32);
            blobHandler = exchange -> {
                exchange.sendResponseHeaders(200, 0);
                streamZeros(exchange.getResponseBody(), ClasspathPackager.MAX_BLOB_BYTES + 1);
            };
            assertThatThrownBy(() -> blobs.ensureCached(oversizedSha)).isInstanceOf(IOException.class);
            assertNothingStored(cache);

            // Declared Content-Length above the cap: aborted before streaming.
            blobHandler = exchange -> {
                exchange.sendResponseHeaders(200, ClasspathPackager.MAX_BLOB_BYTES + 1);
                streamZeros(exchange.getResponseBody(), ClasspathPackager.MAX_BLOB_BYTES + 1);
            };
            assertThatThrownBy(() -> blobs.ensureCached(oversizedSha)).isInstanceOf(IOException.class);
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
            assertThat(cache.blobCache().resolveBlobPath(packaged.ref().sha256())).exists();
            assertThat(tmpFiles(cache)).isEmpty();
            assertThat(cache.isPinned(packaged.ref().sha256())).as("pins released after the partition").isFalse();
        }
    }

    private DistributionWorkerConfig config(Path cacheDir) {
        return DistributionWorkerConfig.builder()
                .workerId("blob-" + UUID.randomUUID().toString().substring(0, 6))
                .brokerUrl("http://127.0.0.1:" + server.getAddress().getPort())
                .cacheDir(cacheDir)
                .cacheCleanup(true)
                .cacheFreeSpaceBufferBytes(0L)
                .cwdCheckMode(WorkerCwdCheckMode.OFF)
                .build();
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

    private static void streamZeros(OutputStream out, long total) throws IOException {
        byte[] chunk = new byte[64 * 1024];
        long sent = 0;
        while (sent < total) {
            int len = (int) Math.min(chunk.length, total - sent);
            out.write(chunk, 0, len);
            sent += len;
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
        assertThat(cache.blobCache().listBlobs()).as("stored blobs").isEmpty();
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
