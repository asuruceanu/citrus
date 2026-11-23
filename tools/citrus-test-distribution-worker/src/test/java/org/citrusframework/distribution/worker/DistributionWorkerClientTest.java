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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.SessionDetailResponse;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DistributionWorkerClientTest {

    @TempDir
    Path tempDir;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public static class SampleRemoteTest {
        @Test
        void shouldPassOnWorker() {
            assertEquals("worker", System.getProperty("citrus.test.distribution.role"));
            assertEquals("hello-remote", System.getProperty("sample.custom.prop", "hello-remote"));
        }
    }

    public static class AlphaTenantRemoteTest {
        @Test
        void shouldIsolateAlphaTenantProperty() throws InterruptedException {
            assertEquals("worker", System.getProperty("citrus.test.distribution.role"));
            assertEquals("alpha", System.getProperty("custom.tenant.id"));
            AtomicReference<String> inherited = new AtomicReference<>();
            Thread child = new Thread(() -> inherited.set(System.getProperty("custom.tenant.id")));
            child.start();
            child.join(2_000L);
            assertEquals("alpha", inherited.get());
            Thread.sleep(250L);
            assertEquals("alpha", System.getProperty("custom.tenant.id"));
        }
    }

    public static class BetaTenantRemoteTest {
        @Test
        void shouldIsolateBetaTenantProperty() throws InterruptedException {
            assertEquals("worker", System.getProperty("citrus.test.distribution.role"));
            assertEquals("beta", System.getProperty("custom.tenant.id"));
            Thread.sleep(250L);
            assertEquals("beta", System.getProperty("custom.tenant.id"));
        }
    }

    @Test
    void shouldDownloadClasspathBlobExecuteJUnitTestAndStreamResultsToBroker() throws Exception {
        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .authToken("worker-secret")
                .cacheDir(tempDir.resolve("broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:worker_it_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();

            DistributionWorkerConfig workerConfig = DistributionWorkerConfig.builder()
                    .workerId("worker-1")
                    .brokerUrl(broker.baseUrl())
                    .authToken("worker-secret")
                    .cacheDir(tempDir.resolve("worker-cache"))
                    .forkMode(DistributionSettings.WorkerForkMode.AUTO) // exercises the shared worker JVM
                    .addCapabilitiesCsv("zone=eu-central")
                    .build();

            try (DistributionWorkerClient worker = new DistributionWorkerClient(workerConfig)) {
                worker.start();
                assertThat(worker.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();

                // Package test-classes directory as DIRECTORY_ZIP and upload to broker cache
                Path testClassesDir = Paths.get(SampleRemoteTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(testClassesDir);

                HttpResponse<String> uploadResp = httpClient.send(
                        HttpRequest.newBuilder(URI.create(broker.baseUrl() + "/api/v1/cache/blobs/" + packaged.ref().sha256()))
                                .header("Authorization", "Bearer worker-secret")
                                .PUT(HttpRequest.BodyPublishers.ofByteArray(packaged.bytes()))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(uploadResp.statusCode()).isEqualTo(201);

                HttpResponse<String> createResp = postJson(
                        broker.baseUrl() + "/api/v1/sessions",
                        "worker-secret",
                        new CreateSessionRequest("citrus", "main", "1234567", List.of(), 4, 1));
                BuildSessionResponse session = DistributionJson.fromJson(createResp.body(), BuildSessionResponse.class);

                WorkUnitSpec unit = new WorkUnitSpec(
                        SampleRemoteTest.class.getName() + "#shouldPassOnWorker",
                        SampleRemoteTest.class.getName(),
                        "shouldPassOnWorker",
                        null,
                        Map.of("zone", "eu-central"),
                        500L);

                SubmitWorkRequest submitReq = new SubmitWorkRequest(
                        "sub-worker-1",
                        "worker-test",
                        List.of(packaged.ref()),
                        Map.of("sample.custom.prop", "hello-remote"),
                        List.of(unit),
                        0,
                        1);
                HttpResponse<String> submitResp = postJson(
                        broker.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/work",
                        "worker-secret",
                        submitReq);
                assertThat(submitResp.statusCode()).isEqualTo(200);

                // Poll session detail until partition completes
                SessionDetailResponse detail = awaitPartitionCompletion(broker.baseUrl(), "worker-secret", session.sessionId());
                assertThat(detail.partitions()).hasSize(1);
                assertThat(detail.partitions().get(0).state()).isEqualTo(PartitionState.COMPLETED);
                assertThat(detail.tests()).anySatisfy(rec -> {
                    assertThat(rec.className()).isEqualTo(SampleRemoteTest.class.getName());
                    assertThat(rec.status()).isEqualTo(TestOutcomeStatus.PASSED);
                });

                postJson(
                        broker.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/complete",
                        "worker-secret",
                        new CompleteSessionRequest(0, 0, 0, 0L));

                SessionDetailResponse finalDetail = fetchDetail(broker.baseUrl(), "worker-secret", session.sessionId());
                assertThat(finalDetail.session().status()).isEqualTo(SessionStatus.PASSED);
                assertThat(finalDetail.session().passedTests()).isEqualTo(1);

                worker.drain(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void shouldIsolatePartitionSystemPropertiesAcrossConcurrentWorkersInSameJvm() throws Exception {
        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .authToken("worker-secret")
                .cacheDir(tempDir.resolve("broker-concurrent-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:worker_concurrent_" + System.nanoTime())
                .defaultMaxPartitionSize(1)
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();

            DistributionWorkerConfig alphaConfig = DistributionWorkerConfig.builder()
                    .workerId("worker-alpha")
                    .brokerUrl(broker.baseUrl())
                    .authToken("worker-secret")
                    .cacheDir(tempDir.resolve("worker-alpha-cache"))
                    .forkMode(DistributionSettings.WorkerForkMode.AUTO) // exercises the shared worker JVM
                    .build();
            DistributionWorkerConfig betaConfig = DistributionWorkerConfig.builder()
                    .workerId("worker-beta")
                    .brokerUrl(broker.baseUrl())
                    .authToken("worker-secret")
                    .cacheDir(tempDir.resolve("worker-beta-cache"))
                    .forkMode(DistributionSettings.WorkerForkMode.AUTO) // exercises the shared worker JVM
                    .build();

            try (DistributionWorkerClient workerAlpha = new DistributionWorkerClient(alphaConfig);
                 DistributionWorkerClient workerBeta = new DistributionWorkerClient(betaConfig)) {
                workerAlpha.start();
                workerBeta.start();
                assertThat(workerAlpha.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();
                assertThat(workerBeta.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();

                Path testClassesDir = Paths.get(AlphaTenantRemoteTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(testClassesDir);

                HttpResponse<String> uploadResp = httpClient.send(
                        HttpRequest.newBuilder(URI.create(broker.baseUrl() + "/api/v1/cache/blobs/" + packaged.ref().sha256()))
                                .header("Authorization", "Bearer worker-secret")
                                .PUT(HttpRequest.BodyPublishers.ofByteArray(packaged.bytes()))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(uploadResp.statusCode()).isEqualTo(201);

                HttpResponse<String> createResp = postJson(
                        broker.baseUrl() + "/api/v1/sessions",
                        "worker-secret",
                        new CreateSessionRequest("citrus", "main", "1234567", List.of(), 1, 0));
                BuildSessionResponse session = DistributionJson.fromJson(createResp.body(), BuildSessionResponse.class);

                WorkUnitSpec alphaUnit = new WorkUnitSpec(
                        AlphaTenantRemoteTest.class.getName() + "#shouldIsolateAlphaTenantProperty",
                        AlphaTenantRemoteTest.class.getName(),
                        "shouldIsolateAlphaTenantProperty",
                        null,
                        Map.of(),
                        500L);
                WorkUnitSpec betaUnit = new WorkUnitSpec(
                        BetaTenantRemoteTest.class.getName() + "#shouldIsolateBetaTenantProperty",
                        BetaTenantRemoteTest.class.getName(),
                        "shouldIsolateBetaTenantProperty",
                        null,
                        Map.of(),
                        500L);

                HttpResponse<String> submitAlpha = postJson(
                        broker.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/work",
                        "worker-secret",
                        new SubmitWorkRequest(
                                "sub-alpha",
                                "module-alpha",
                                List.of(packaged.ref()),
                                Map.of("custom.tenant.id", "alpha"),
                                List.of(alphaUnit),
                                0,
                                1));
                HttpResponse<String> submitBeta = postJson(
                        broker.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/work",
                        "worker-secret",
                        new SubmitWorkRequest(
                                "sub-beta",
                                "module-beta",
                                List.of(packaged.ref()),
                                Map.of("custom.tenant.id", "beta"),
                                List.of(betaUnit),
                                1,
                                0));
                assertThat(submitAlpha.statusCode()).isEqualTo(200);
                assertThat(submitBeta.statusCode()).isEqualTo(200);

                SessionDetailResponse detail = awaitPartitionsCompletion(
                        broker.baseUrl(), "worker-secret", session.sessionId(), 2);
                assertThat(detail.partitions()).hasSize(2);
                assertThat(detail.partitions()).allSatisfy(p ->
                        assertThat(p.state()).isEqualTo(PartitionState.COMPLETED));
                assertThat(detail.tests()).hasSize(2);
                assertThat(detail.tests()).allSatisfy(rec ->
                        assertThat(rec.status()).as("Test %s failed: %s", rec.className(), rec.errorMessage())
                                .isEqualTo(TestOutcomeStatus.PASSED));
                assertThat(System.getProperty("custom.tenant.id")).isNull();

                workerAlpha.drain(5, TimeUnit.SECONDS);
                workerBeta.drain(5, TimeUnit.SECONDS);
            }
        }
    }

    /** Runs in a forked partition JVM; plain JUnit so the child needs no assertion library. */
    public static class PinnedForkFixture {
        @Test
        void readInputAndWriteOutput() throws Exception {
            String greeting = Files.readString(Path.of("src/test/resources/greeting.txt"));
            if (!"hello".equals(greeting.trim())) {
                throw new AssertionError("input not materialized: " + greeting);
            }
            Files.createDirectories(Path.of("target/docs"));
            Files.writeString(Path.of("target/docs/report.html"), "<html>report</html>");
        }
    }

    public static class BlockingPartitionFixture {
        private static final CountDownLatch STARTED = new CountDownLatch(1);
        private static final CountDownLatch RELEASE = new CountDownLatch(1);

        @Test
        void shouldWaitOnWorker() throws Exception {
            // The fixture runs in a child-first loader; use the host copy to share the test's latches.
            ClassLoader.getSystemClassLoader().loadClass(getClass().getName()).getMethod("awaitRelease").invoke(null);
        }

        public static void awaitRelease() throws InterruptedException {
            STARTED.countDown();
            if (!RELEASE.await(15, TimeUnit.SECONDS)) {
                throw new AssertionError("test did not release the partition");
            }
        }
    }

    @Test
    void shouldFailFastWhenSecondWorkerClientSharesCacheDirAndAllowReuseAfterClose() throws Exception {
        try (DistributionBrokerServer broker = startBroker("worker_shared_cache_");
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Path sharedCache = tempDir.resolve("shared-worker-cache");
            ClasspathPackager.PackagedArtifact packaged = upload(broker, packageClassesOf(BlockingPartitionFixture.class));
            WorkUnitSpec blockedUnit = unit(BlockingPartitionFixture.class, "shouldWaitOnWorker");

            try (DistributionWorkerClient worker2 = new DistributionWorkerClient(workerConfig("worker-two", broker, sharedCache).build())) {
                try (DistributionWorkerClient worker1 = new DistributionWorkerClient(workerConfig("worker-one", broker, sharedCache).build())) {
                    worker1.start();
                    assertThat(worker1.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();
                    var partition = executor.submit(() -> runPartition(broker, List.of(packaged.ref()), blockedUnit,
                            List.of(), List.of()));
                    try {
                        assertThat(BlockingPartitionFixture.STARTED.await(10, TimeUnit.SECONDS)).isTrue();
                        assertThat(partition.isDone()).isFalse();
                        assertThat(worker1.blobClient().downloadRequests()).isEqualTo(1);
                        assertThatThrownBy(worker2::start)
                                .isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining("--cache-dir");
                        assertThat(worker2.isConnected()).isFalse();
                        assertThat(worker1.isConnected()).isTrue();
                    } finally {
                        BlockingPartitionFixture.RELEASE.countDown();
                    }
                    assertCompleted(partition.get(10, TimeUnit.SECONDS));
                }

                worker2.start();
                assertThat(worker2.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();
                WorkUnitSpec warmUnit = unit(SampleRemoteTest.class, "shouldPassOnWorker");
                assertCompleted(runPartition(broker, List.of(packaged.ref()), warmUnit, List.of(), List.of()));
                assertThat(worker2.blobClient().downloadRequests())
                        .as("warm blob reused, no blob GET issued to the coordinator").isZero();
            }
        }
    }

    @Test
    void shouldClosePromptlyWhileAPartitionDownloadIsStalled() throws Exception {
        CountDownLatch bodyStarted = new CountDownLatch(1);
        byte[] expected = new byte[1024];
        ArtifactRef artifact = new ArtifactRef("stalled.jar", ClasspathPackager.sha256Hex(expected),
                ArtifactKind.REGULAR_FILE, expected.length);
        PartitionAssignPayload assignment = new PartitionAssignPayload("stalled-partition", "s1", 1,
                List.of(artifact), Map.of(), List.of(unit(SampleRemoteTest.class, "shouldPassOnWorker")));
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = DistributionJson.convert(assignment, Map.class);
        String assignmentMessage = DistributionJson.toJson(new WorkerWsMessage(WorkerWsMessage.PARTITION_ASSIGN, payload));

        Vertx vertx = Vertx.vertx();
        try {
            HttpServer broker = vertx.createHttpServer()
                    .requestHandler(request -> request.response().setChunked(true).write(Buffer.buffer(new byte[128]))
                            .onSuccess(ignored -> bodyStarted.countDown()))
                    .webSocketHandler(socket -> socket.textMessageHandler(message -> {
                        if (WorkerWsMessage.WORKER_REGISTER.equals(DistributionJson.fromJson(message, WorkerWsMessage.class).type())) {
                            socket.writeTextMessage(assignmentMessage);
                        }
                    }))
                    .listen(0, "127.0.0.1").toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            Path cacheDir = tempDir.resolve("stalled-worker-cache");
            DistributionWorkerConfig config = DistributionWorkerConfig.builder()
                    .brokerUrl("http://127.0.0.1:" + broker.actualPort())
                    .cacheDir(cacheDir)
                    .cacheCleanup(false)
                    .forkMode(DistributionSettings.WorkerForkMode.AUTO)
                    .build();
            try (DistributionWorkerClient worker = new DistributionWorkerClient(config)) {
                worker.start();
                assertThat(bodyStarted.await(5, TimeUnit.SECONDS)).isTrue();
                CompletableFuture.runAsync(worker::close).get(4, TimeUnit.SECONDS);
                try (Stream<Path> tmpFiles = Files.list(cacheDir.resolve("tmp"));
                     WorkerFileCache reopened = WorkerFileCache.open(cacheDir, false, 0L)) {
                    assertThat(tmpFiles).isEmpty();
                    assertThat(reopened.statsSnapshot().blobCount()).isZero();
                }
            }
        } finally {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldKeepClasspathInputsAndOutputsPinnedUnderZeroFreeSpaceAndAllowCacheInsideUserDir() throws Exception {
        try (DistributionBrokerServer broker = startBroker("worker_pinned_")) {
            Path cacheDir = tempDir.resolve("pressure-cache");
            BlobCacheDirectory seededStore = new BlobCacheDirectory(cacheDir);
            List<Path> seeded = new ArrayList<>();
            for (String content : List.of("older unpinned blob one", "older unpinned blob two")) {
                byte[] bytes = content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                seeded.add(seededStore.put(ClasspathPackager.sha256Hex(bytes), bytes));
            }

            // Test classes (archive) and the JUnit Jupiter jars (named jars): without pins, each download would evict
            // the previously published classpath entries.
            List<ArtifactRef> classpath = new ArrayList<>();
            classpath.add(upload(broker, packageClassesOf(DistributionWorkerClientTest.class)).ref());
            classpath.add(upload(broker, packageClassesOf(Test.class)).ref());
            classpath.add(upload(broker, packageClassesOf(Class.forName("org.junit.jupiter.engine.JupiterTestEngine"))).ref());
            assertThat(classpath).extracting(ArtifactRef::kind).contains(ArtifactKind.JAR);

            BlobCacheDirectory hostCache = new BlobCacheDirectory(tempDir.resolve("host-cache"));
            Path module = Files.createDirectories(tempDir.resolve("module/src/test/resources")).getParent().getParent().getParent();
            Files.writeString(module.resolve("src/test/resources/greeting.txt"), "hello");
            WorkspaceFileSet input = WorkspacePackager.packageInputToCache(hostCache, module, "src/test/resources", false).orElseThrow();
            uploadBytes(broker, input.artifact().sha256(), hostCache.readBytes(input.artifact().sha256()));

            DistributionWorkerConfig config = workerConfig("worker-pressure", broker, cacheDir)
                    .cacheFreeSpaceBufferBytes(Long.MAX_VALUE / 2)
                    .forkJvmArgs(List.of("-Xmx256m"))
                    .build();
            try (DistributionWorkerClient worker = new DistributionWorkerClient(config)) {
                worker.start();
                assertThat(worker.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();
                assertThat(seeded).as("unpinned blobs evicted under disk pressure").allSatisfy(p -> assertThat(p).doesNotExist());

                assertCompleted(runPartition(broker, classpath, unit(SampleRemoteTest.class, "shouldPassOnWorker"),
                        List.of(), List.of()));
                assertCompleted(runPartition(broker, classpath, unit(PinnedForkFixture.class, "readInputAndWriteOutput"),
                        List.of(input), List.of("target/docs")));

                long deadline = System.currentTimeMillis() + 10_000L;
                while (!seededStore.listBlobs().isEmpty() && System.currentTimeMillis() < deadline) {
                    Thread.sleep(50L);
                }
                assertThat(seededStore.listBlobs()).as("pins released after the partitions, blobs evicted").isEmpty();
                worker.drain(5, TimeUnit.SECONDS);
            }
        }

        // A cache inside the worker's working directory, directly or through a link, is not a CWD violation.
        Path cwd = Files.createDirectories(tempDir.resolve("worker-cwd"));
        Path realCache = Files.createDirectories(cwd.resolve("cache"));
        Path link = tempDir.resolve("cache-link");
        Files.createSymbolicLink(link, realCache);
        Path plainCacheDir = cwd.resolve("plain-cache");
        for (Path configured : List.of(link, plainCacheDir)) {
            try (WorkerFileCache cache = WorkerFileCache.open(configured, true, 0L)) {
                Files.setLastModifiedTime(cache.rootDir(), FileTime.fromMillis(System.currentTimeMillis() - 60_000L));
                IsolatedPartitionRunner.WorkingDirectorySnapshot before =
                        IsolatedPartitionRunner.WorkingDirectorySnapshot.take(cwd, configured, cache.rootDir());
                IsolatedPartitionRunner.WorkingDirectorySnapshot unexcluded =
                        IsolatedPartitionRunner.WorkingDirectorySnapshot.take(cwd);

                byte[] jar = ("jar for " + configured).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                String sha = ClasspathPackager.sha256Hex(jar);
                Path tmp = cache.newDownloadTempFile();
                Files.write(tmp, jar);
                Path stored = cache.promoteDownload(sha, tmp);
                cache.materializeForClasspath(new ArtifactRef("lib.jar", sha, ArtifactKind.JAR, jar.length), stored,
                        tempDir.resolve("partition"));

                assertThat(unexcluded.changedEntries(IsolatedPartitionRunner.WorkingDirectorySnapshot.take(cwd)))
                        .as("the cache write is visible without exclusion").isNotEmpty();
                assertThat(before.changedEntries(
                        IsolatedPartitionRunner.WorkingDirectorySnapshot.take(cwd, configured, cache.rootDir())))
                        .as("cache writes are excluded for %s", configured).isEmpty();
            }
        }
    }

    @Test
    void shouldNotHoldTheClientMonitorWhileCloseWaitsForTheCache() throws Exception {
        DistributionWorkerConfig config = DistributionWorkerConfig.builder()
                .workerId("worker-close-latency")
                .brokerUrl("http://127.0.0.1:9")
                .cacheDir(tempDir.resolve("close-latency-cache"))
                .cacheCleanup(true)
                .cacheFreeSpaceBufferBytes(Long.MAX_VALUE / 2)
                .cwdCheckMode(DistributionSettings.WorkerCwdCheckMode.OFF)
                .build();
        DistributionWorkerClient client = new DistributionWorkerClient(config);
        CountDownLatch evicting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            client.start();
            WorkerFileCache cache = client.workerCache();
            cache.blobIndex(new WorkerFileCache.BlobIndex() {
                @Override
                public List<BlobCacheDirectory.BlobEntry> listBlobs() {
                    evicting.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return List.of();
                }

                @Override
                public boolean delete(String sha256) {
                    return false;
                }
            });
            CompletableFuture<Void> eviction = CompletableFuture.runAsync(cache::evictAfterPartition);
            assertThat(evicting.await(10, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<Void> closing = CompletableFuture.runAsync(client::close);
            long deadline = System.currentTimeMillis() + 15_000L;
            while (!cache.hasThreadsWaitingForEvictionLock() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20L);
            }
            assertThat(cache.hasThreadsWaitingForEvictionLock()).as("close waits for the eviction").isTrue();
            CompletableFuture<Void> monitor = CompletableFuture.runAsync(() -> {
                synchronized (client) {
                    assertThat(closing).isNotDone();
                }
            });
            monitor.get(2, TimeUnit.SECONDS);

            release.countDown();
            eviction.get(10, TimeUnit.SECONDS);
            closing.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            client.close();
        }
    }

    private DistributionBrokerServer startBroker(String dbPrefix) {
        DistributionBrokerServer broker = new DistributionBrokerServer(DistributionBrokerConfig.builder()
                .port(0)
                .authToken("worker-secret")
                .cacheDir(tempDir.resolve("broker-" + dbPrefix + "cache"))
                .jdbcUrl("jdbc:hsqldb:mem:" + dbPrefix + System.nanoTime())
                .build());
        broker.start();
        return broker;
    }

    private static DistributionWorkerConfig.Builder workerConfig(String workerId, DistributionBrokerServer broker, Path cacheDir) {
        return DistributionWorkerConfig.builder()
                .workerId(workerId)
                .brokerUrl(broker.baseUrl())
                .authToken("worker-secret")
                .cacheDir(cacheDir)
                .cacheCleanup(true)
                .cacheFreeSpaceBufferBytes(0L)
                .cwdCheckMode(DistributionSettings.WorkerCwdCheckMode.OFF)
                .forkMode(DistributionSettings.WorkerForkMode.AUTO);
    }

    private static ClasspathPackager.PackagedArtifact packageClassesOf(Class<?> anchor) throws Exception {
        return ClasspathPackager.packagePath(Paths.get(anchor.getProtectionDomain().getCodeSource().getLocation().toURI()));
    }

    private ClasspathPackager.PackagedArtifact upload(DistributionBrokerServer broker, ClasspathPackager.PackagedArtifact packaged)
            throws Exception {
        uploadBytes(broker, packaged.ref().sha256(), packaged.bytes());
        return packaged;
    }

    private void uploadBytes(DistributionBrokerServer broker, String sha256, byte[] bytes) throws Exception {
        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder(URI.create(broker.baseUrl() + "/api/v1/cache/blobs/" + sha256))
                        .header("Authorization", "Bearer worker-secret")
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(bytes))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isIn(200, 201);
    }

    private SessionDetailResponse runPartition(DistributionBrokerServer broker, List<ArtifactRef> classpath, WorkUnitSpec unit,
                                               List<WorkspaceFileSet> inputs, List<String> outputs) throws Exception {
        BuildSessionResponse session = DistributionJson.fromJson(postJson(broker.baseUrl() + "/api/v1/sessions",
                "worker-secret", new CreateSessionRequest("citrus", "main", "1234567", List.of(), 4, 1)).body(),
                BuildSessionResponse.class);
        HttpResponse<String> submit = postJson(broker.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/work",
                "worker-secret", new SubmitWorkRequest("sub-" + System.nanoTime(), "cache-module", classpath, Map.of(),
                        List.of(unit), 0, classpath.size(), inputs, outputs, !inputs.isEmpty() || !outputs.isEmpty(),
                        PartitionPolicy.LPT));
        assertThat(submit.statusCode()).isEqualTo(200);
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            SessionDetailResponse detail = fetchDetail(broker.baseUrl(), "worker-secret", session.sessionId());
            if (!detail.partitions().isEmpty() && detail.partitions().stream().allMatch(p ->
                    p.state() == PartitionState.COMPLETED || p.state() == PartitionState.FAILED)) {
                return detail;
            }
            Thread.sleep(100L);
        }
        return fetchDetail(broker.baseUrl(), "worker-secret", session.sessionId());
    }

    private static void assertCompleted(SessionDetailResponse detail) {
        assertThat(detail.partitions()).allSatisfy(p -> assertThat(p.state()).as(p.errorMessage())
                .isEqualTo(PartitionState.COMPLETED));
        assertThat(detail.tests()).isNotEmpty().allSatisfy(rec -> assertThat(rec.status())
                .as("Test %s failed: %s", rec.className(), rec.errorMessage()).isEqualTo(TestOutcomeStatus.PASSED));
    }

    private static WorkUnitSpec unit(Class<?> fixture, String method) {
        return new WorkUnitSpec(fixture.getName() + "#" + method, fixture.getName(), method, null, Map.of(), 500L);
    }

    @Test
    void shouldNormalizeWebSocketBrokerUrlsToHttpBaseUrl() {
        assertThat(DistributionWorkerConfig.builder()
                .brokerUrl("ws://127.0.0.1:18080/ws/v1/worker")
                .build()
                .brokerUrl()).isEqualTo("http://127.0.0.1:18080");
        assertThat(DistributionWorkerConfig.builder()
                .brokerUrl("wss://broker.example.org/ws/v1/worker/")
                .build()
                .brokerUrl()).isEqualTo("https://broker.example.org");
    }

    private SessionDetailResponse awaitPartitionsCompletion(String baseUrl, String token, String sessionId, int expectedPartitions) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            SessionDetailResponse detail = fetchDetail(baseUrl, token, sessionId);
            if (detail.partitions().size() >= expectedPartitions
                    && detail.partitions().stream().allMatch(p ->
                    p.state() == PartitionState.COMPLETED || p.state() == PartitionState.FAILED || p.state() == PartitionState.LOST)) {
                return detail;
            }
            Thread.sleep(50L);
        }
        return fetchDetail(baseUrl, token, sessionId);
    }

    private SessionDetailResponse awaitPartitionCompletion(String baseUrl, String token, String sessionId) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            SessionDetailResponse detail = fetchDetail(baseUrl, token, sessionId);
            if (!detail.partitions().isEmpty()
                    && detail.partitions().get(0).state() == PartitionState.COMPLETED) {
                return detail;
            }
            Thread.sleep(100L);
        }
        return fetchDetail(baseUrl, token, sessionId);
    }

    private SessionDetailResponse fetchDetail(String baseUrl, String token, String sessionId) throws Exception {
        HttpResponse<String> resp = httpClient.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/sessions/" + sessionId))
                        .header("Authorization", "Bearer " + token)
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        return DistributionJson.fromJson(resp.body(), SessionDetailResponse.class);
    }

    private HttpResponse<String> postJson(String url, String token, Object payload) throws Exception {
        return httpClient.send(
                HttpRequest.newBuilder(URI.create(url))
                        .header("Authorization", "Bearer " + token)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(DistributionJson.toJson(payload)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
