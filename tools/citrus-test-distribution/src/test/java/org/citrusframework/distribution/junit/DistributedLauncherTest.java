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

package org.citrusframework.distribution.junit;

import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.citrusframework.distribution.annotations.LocalOnly;
import org.citrusframework.distribution.annotations.RemoteOnly;
import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.worker.DistributionWorkerClient;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DistributedLauncherTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void resetProps() {
        System.clearProperty(DistributionSettings.ENABLED_PROPERTY);
        System.clearProperty(DistributionSettings.BROKER_URL_PROPERTY);
        System.clearProperty(DistributionSettings.FALLBACK_LOCAL_PROPERTY);
        System.clearProperty(DistributionSettings.REPORT_DIR_PROPERTY);
        System.clearProperty(DistributionSettings.CONNECT_TIMEOUT_MS_PROPERTY);
        System.clearProperty(DistributionSettings.SESSION_ID_PROPERTY);
        System.clearProperty(DistributionSettings.BASEDIR_PROPERTY);
        System.clearProperty(DistributionSettings.INPUTS_PROPERTY);
        System.clearProperty(DistributionSettings.OUTPUTS_PROPERTY);
        System.clearProperty(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY);
        System.clearProperty("custom.data.dir");
    }

    @Test
    void shouldExecuteEndToEndWithDeclaredInputsOutputsBasedirRewriteAndSiblingSessionIsolation() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            Path module = Files.createDirectories(tempDir.resolve("module"));
            Files.createDirectories(module.resolve("src/test/resources"));
            Files.writeString(module.resolve("src/test/resources/data.txt"), "remote-data");

            String sessionId = cluster.createSession();
            // A sibling module on the same reactor session: a failing in-process test and a crashing forked partition.
            cluster.client.submitWork(sessionId, new SubmitWorkRequest("sibling-1", "sibling", List.of(), Map.of(),
                    List.of(unit(SiblingFailingFixture.class)), 0, 0));
            cluster.client.submitWork(sessionId, new SubmitWorkRequest("sibling-2", "sibling", List.of(), Map.of(),
                    List.of(new WorkUnitSpec("missing", "org.example.Missing", null, null, Map.of(), 0L)), 0, 0,
                    List.of(), List.of("target/citrus-docs"), true, PartitionPolicy.LPT));
            // A succeeding sibling partition that writes a different file under the same output path.
            cluster.client.submitWork(sessionId, cluster.siblingWithOutputs(sessionId, SiblingOutputFixture.class));

            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, cluster.broker.baseUrl());
            System.setProperty(DistributionSettings.SESSION_ID_PROPERTY, sessionId);
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, module.toString());
            System.setProperty(DistributionSettings.INPUTS_PROPERTY, "src/test/resources");
            System.setProperty(DistributionSettings.OUTPUTS_PROPERTY, "target/citrus-docs");
            System.setProperty("custom.data.dir", module.resolve("src/test/resources").toString());

            Outcome outcome = run(RemoteWorkspaceFixture.class);

            assertThat(outcome.failed()).as("sibling module events are ignored").isZero();
            assertThat(outcome.passed()).isEqualTo(1);
            assertThat(module.resolve("target/citrus-docs/snippet.adoc")).hasContent("snippet from remote-data");
            try (Stream<Path> root = Files.list(module)) {
                assertThat(root.map(p -> p.getFileName().toString())).containsExactlyInAnyOrder("src", "target");
            }
            assertThat(module.resolve("target/citrus-docs/sibling.txt")).as("sibling outputs are not collected").doesNotExist();
            try (Stream<Path> outputs = Files.list(module.resolve("target/.citrus-dist-outputs"))) {
                assertThat(outputs.map(p -> p.getFileName().toString())).noneMatch(name -> name.startsWith("tx-"));
            }
        }
    }

    @Test
    void shouldFailRunWhenRemotePartitionCrashesWithoutTestEvents() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            Path module = Files.createDirectories(tempDir.resolve("module-crash"));
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, cluster.broker.baseUrl());
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, module.toString());
            System.setProperty(DistributionSettings.OUTPUTS_PROPERTY, "target/citrus-docs");

            assertThatThrownBy(() -> run(RemoteCrashingFixture.class))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("failed")
                    .hasMessageContaining("exited with code 7");
        }
    }

    @Test
    void shouldFailRunAndWriteNothingWhenPartitionsProduceConflictingOutputs() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            Path module = Files.createDirectories(tempDir.resolve("module-conflict"));
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, cluster.broker.baseUrl());
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, module.toString());
            System.setProperty(DistributionSettings.OUTPUTS_PROPERTY, "target/citrus-docs");
            System.setProperty(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY, "1");

            assertThatThrownBy(() -> run(ConflictingOutputFixtureA.class, ConflictingOutputFixtureB.class))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("target/citrus-docs/conflict.txt");
            assertThat(module.resolve("target/citrus-docs/conflict.txt")).doesNotExist();
        }
    }

    @Test
    void shouldFailBeforeSubmittingWhenRequiredInputIsMissing() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            Path module = Files.createDirectories(tempDir.resolve("module-missing"));
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, cluster.broker.baseUrl());
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, module.toString());
            System.setProperty(DistributionSettings.INPUTS_PROPERTY, "src/test/resources,?optional/dir");

            assertThatThrownBy(() -> run(RemoteWorkspaceFixture.class))
                    .hasMessageContaining("src/test/resources");
            HttpResponse<String> sessions = java.net.http.HttpClient.newHttpClient().send(
                    java.net.http.HttpRequest.newBuilder(java.net.URI.create(cluster.broker.baseUrl() + "/api/v1/sessions")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(sessions.body()).as("nothing reached the coordinator").isEqualTo("[]");
        }
    }

    @Test
    void shouldTreatBlankAndPlaceholderSettingsAsUnsetAndNoneAsClear() {
        System.setProperty(DistributionSettings.INPUTS_PROPERTY, "");
        assertThat(DistributedLauncher.WorkspaceSettings.setting(DistributionSettings.INPUTS_PROPERTY, null))
                .as("the root POM passes undefined settings through as empty values").isNull();
        System.setProperty(DistributionSettings.INPUTS_PROPERTY, "${citrus.test.distribution.inputs}");
        assertThat(DistributedLauncher.WorkspaceSettings.setting(DistributionSettings.INPUTS_PROPERTY, null)).isNull();
        assertThat(DistributedLauncher.WorkspaceSettings.setting(DistributionSettings.INPUTS_PROPERTY, "PATH"))
                .as("a blank property falls back to the environment variable").isEqualTo(System.getenv("PATH"));
        System.setProperty(DistributionSettings.INPUTS_PROPERTY, "none");
        assertThat(DistributionSettings.parseDeclaredPaths(
                DistributedLauncher.WorkspaceSettings.setting(DistributionSettings.INPUTS_PROPERTY, null))).isEmpty();
    }

    @Test
    void shouldSendTheWorkingDirectoryRelativeToTheModule() {
        Path module = tempDir.resolve("mod");
        assertThat(DistributedLauncher.WorkspaceSettings.relativeWorkingDirectory(module, module.resolve("target")))
                .as("Surefire runs in target/ in this repository").isEqualTo("target");
        assertThat(DistributedLauncher.WorkspaceSettings.relativeWorkingDirectory(module, module)).as("Failsafe: module root").isEmpty();
        assertThat(DistributedLauncher.WorkspaceSettings.relativeWorkingDirectory(module, tempDir.resolve("elsewhere")))
                .as("outside the module: workspace root").isEmpty();
        assertThat(DistributedLauncher.WorkspaceSettings.relativeWorkingDirectory(module, module.resolve("target/sub")))
                .isEqualTo("target/sub");
    }

    private record Outcome(int passed, int failed) {
    }

    private static Outcome run(Class<?>... fixtures) {
        AtomicInteger passed = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        DistributedLauncher distributed = new DistributedLauncher(LauncherFactory.create());
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(java.util.Arrays.stream(fixtures).map(DiscoverySelectors::selectClass).toList())
                .build();
        distributed.execute(request, new TestExecutionListener() {
            @Override
            public void executionFinished(TestIdentifier id, TestExecutionResult result) {
                if (id.isTest()) {
                    (result.getStatus() == TestExecutionResult.Status.SUCCESSFUL ? passed : failed).incrementAndGet();
                }
            }
        });
        return new Outcome(passed.get(), failed.get());
    }

    private static WorkUnitSpec unit(Class<?> fixture) {
        return new WorkUnitSpec(fixture.getName(), fixture.getName(), null, null, Map.of(), 0L);
    }

    /** An embedded broker with two workers. */
    private record Cluster(DistributionBrokerServer broker, List<DistributionWorkerClient> workers, BrokerHttpClient client)
            implements AutoCloseable {

        static Cluster start(Path tempDir) throws Exception {
            DistributionBrokerServer broker = new DistributionBrokerServer(DistributionBrokerConfig.builder()
                    .port(0)
                    .cacheDir(tempDir.resolve("broker-cache"))
                    .jdbcUrl("jdbc:hsqldb:mem:launcher_ws_" + System.nanoTime())
                    .build());
            broker.start();
            List<DistributionWorkerClient> workers = new java.util.ArrayList<>();
            for (int i = 0; i < 2; i++) {
                DistributionWorkerClient worker = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                        .workerId("ws-worker-" + i)
                        .brokerUrl(broker.baseUrl())
                        .cacheDir(tempDir.resolve("worker-cache-" + i))
                        .forkJvmArgs(List.of("-Xmx384m"))
                        .build());
                worker.start();
                assertThat(worker.awaitRegistration(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                workers.add(worker);
            }
            return new Cluster(broker, workers, new BrokerHttpClient(broker.baseUrl(), "", 3_000L));
        }

        /**
         * A sibling submission running {@code fixture} in a forked JVM from this test JVM's classpath, uploaded first.
         */
        SubmitWorkRequest siblingWithOutputs(String sessionId, Class<?> fixture) throws Exception {
            List<org.citrusframework.distribution.model.ArtifactRef> classpath = new java.util.ArrayList<>();
            for (String entry : System.getProperty("java.class.path").split(java.io.File.pathSeparator)) {
                Path path = Path.of(entry);
                if (!Files.exists(path)) {
                    continue;
                }
                org.citrusframework.distribution.cache.ClasspathPackager.PackagedArtifact packaged =
                        org.citrusframework.distribution.cache.ClasspathPackager.packagePath(path);
                client.uploadArtifact(packaged);
                classpath.add(packaged.ref());
            }
            return new SubmitWorkRequest("sibling-3", "sibling", classpath, Map.of(), List.of(unit(fixture)), 0, 0,
                    List.of(), List.of("target/citrus-docs"), true, PartitionPolicy.LPT);
        }

        String createSession() throws Exception {
            return client.createSession(new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 0)).sessionId();
        }

        @Override
        public void close() {
            workers.forEach(DistributionWorkerClient::close);
            broker.close();
        }
    }

    @Test
    void shouldDistributeRemoteTestsPinLocalOnlyAndWriteSurefireXml() throws Exception {
        LocalPinnedFixture.EXECUTED_IN_COORDINATOR.set(false);
        RemoteSampleFixture.EXECUTED_IN_COORDINATOR.set(false);

        DistributionBrokerConfig brokerCfg = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:junit_dist_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerCfg)) {
            broker.start();

            DistributionWorkerConfig workerCfg = DistributionWorkerConfig.builder()
                    .workerId("worker-1")
                    .brokerUrl(broker.baseUrl())
                    .cacheDir(tempDir.resolve("worker-cache"))
                    .capabilities(Map.of("os", "linux"))
                    .build();

            try (DistributionWorkerClient worker = new DistributionWorkerClient(workerCfg)) {
                worker.start();

                Path reportDir = tempDir.resolve("surefire-reports");
                System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
                System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, broker.baseUrl());
                System.setProperty(DistributionSettings.REPORT_DIR_PROPERTY, reportDir.toString());

                Launcher rawLauncher = LauncherFactory.create();
                DistributedLauncher distributed = new DistributedLauncher(rawLauncher);

                AtomicInteger passedCount = new AtomicInteger();
                LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                        .selectors(
                                DiscoverySelectors.selectClass(RemoteSampleFixture.class),
                                DiscoverySelectors.selectClass(LocalPinnedFixture.class))
                        .build();

                distributed.execute(request, new TestExecutionListener() {
                    @Override
                    public void executionFinished(TestIdentifier id, TestExecutionResult result) {
                        if (id.isTest() && result.getStatus() == TestExecutionResult.Status.SUCCESSFUL) {
                            passedCount.incrementAndGet();
                        }
                    }
                });

                assertThat(passedCount.get()).isEqualTo(2);
                assertThat(LocalPinnedFixture.EXECUTED_IN_COORDINATOR.get()).isTrue();
                // RemoteSampleFixture ran inside IsolatedPartitionRunner's child ClassLoader, NOT coordinator static!
                assertThat(RemoteSampleFixture.EXECUTED_IN_COORDINATOR.get()).isFalse();

                Path xmlReport = reportDir.resolve("TEST-" + RemoteSampleFixture.class.getName() + ".xml");
                assertThat(Files.exists(xmlReport)).isTrue();
                assertThat(Files.readString(xmlReport)).contains("shouldRunOnRemoteWorker");
            }
        }
    }

    @Test
    void shouldFallbackLocallyWhenBrokerUnreachableAndFailFastOnRemoteOnly() {
        RemoteSampleFixture.EXECUTED_IN_COORDINATOR.set(false);
        System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
        System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, "http://127.0.0.1:1");
        System.setProperty(DistributionSettings.CONNECT_TIMEOUT_MS_PROPERTY, "500");
        System.setProperty(DistributionSettings.FALLBACK_LOCAL_PROPERTY, "true");

        DistributedLauncher distributed = new DistributedLauncher(LauncherFactory.create());
        LauncherDiscoveryRequest fallbackReq = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(RemoteSampleFixture.class))
                .build();
        distributed.execute(fallbackReq);
        assertThat(RemoteSampleFixture.EXECUTED_IN_COORDINATOR.get()).isTrue();

        LauncherDiscoveryRequest remoteOnlyReq = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(StrictRemoteOnlyFixture.class))
                .build();
        assertThatThrownBy(() -> distributed.execute(remoteOnlyReq))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("broker unreachable");
    }

    @Test
    void shouldForwardCustomSurefireSystemPropertiesWhileExcludingHostAndDistributionInternals() {
        java.util.Properties source = new java.util.Properties();
        source.setProperty("citrus.default.timeout", "15000");
        source.setProperty("spring.profiles.active", "integration");
        source.setProperty("custom.service.port", "19090");
        source.setProperty("citrus.test.distribution.broker.url", "http://127.0.0.1:18080");
        source.setProperty("junit.platform.launcher.interceptors.enabled", "true");
        source.setProperty("java.home", "/host/jdk-21");
        source.setProperty("user.dir", "/host/workspace");
        source.setProperty("os.name", "Mac OS X");
        source.setProperty("surefire.real.class.path", "/host/target/surefirebooter.jar");
        source.setProperty("maven.multiModuleProjectDirectory", "/host/workspace");
        source.setProperty("basedir", "/host/workspace/module");
        source.setProperty("localRepository", "/host/.m2/repository");

        java.util.Map<String, String> forwarded = DistributedLauncher.collectPropagatedProperties(source);

        assertThat(forwarded)
                .containsEntry("citrus.default.timeout", "15000")
                .containsEntry("spring.profiles.active", "integration")
                .containsEntry("custom.service.port", "19090")
                .doesNotContainKeys(
                        "citrus.test.distribution.broker.url",
                        "junit.platform.launcher.interceptors.enabled",
                        "java.home",
                        "user.dir",
                        "os.name",
                        "surefire.real.class.path",
                        "maven.multiModuleProjectDirectory",
                        "basedir",
                        "localRepository");
    }

    /** Runs in a forked worker JVM with the module workspace as working directory. */
    public static class RemoteWorkspaceFixture {
        @Test
        void readInputWriteOutput() throws Exception {
            if (!DistributionSettings.isWorkerRole()) {
                throw new AssertionError("must run on a worker");
            }
            String data = Files.readString(Path.of("src/test/resources/data.txt"));
            String rewritten = System.getProperty("custom.data.dir");
            if (!Path.of(rewritten).toRealPath().equals(Path.of("src/test/resources").toRealPath())) {
                throw new AssertionError("host path not rewritten to the workspace: " + rewritten);
            }
            Files.createDirectories(Path.of("target/citrus-docs"));
            Files.writeString(Path.of("target/citrus-docs/snippet.adoc"), "snippet from " + data);
        }
    }

    public static class SiblingFailingFixture {
        @Test
        void failsInAnotherModule() {
            throw new AssertionError("sibling failure must not leak");
        }
    }

    public static class RemoteCrashingFixture {
        @Test
        void crash() {
            // Only ever exit inside a forked partition JVM (its working directory is the partition workspace).
            if (!"workspace".equals(Path.of("").toAbsolutePath().getFileName().toString())) {
                throw new AssertionError("expected to run in a forked partition JVM");
            }
            System.exit(7);
        }
    }

    public static class SiblingOutputFixture {
        @Test
        void writeSiblingOutput() throws Exception {
            Files.createDirectories(Path.of("target/citrus-docs"));
            Files.writeString(Path.of("target/citrus-docs/sibling.txt"), "from another module");
        }
    }

    public static class ConflictingOutputFixtureA {
        @Test
        void write() throws Exception {
            Files.createDirectories(Path.of("target/citrus-docs"));
            Files.writeString(Path.of("target/citrus-docs/conflict.txt"), "A");
        }
    }

    public static class ConflictingOutputFixtureB {
        @Test
        void write() throws Exception {
            Files.createDirectories(Path.of("target/citrus-docs"));
            Files.writeString(Path.of("target/citrus-docs/conflict.txt"), "B");
        }
    }

    public static class RemoteSampleFixture {
        static final AtomicBoolean EXECUTED_IN_COORDINATOR = new AtomicBoolean(false);

        @Test
        void shouldRunOnRemoteWorker() {
            if (!DistributionSettings.isWorkerRole()) {
                EXECUTED_IN_COORDINATOR.set(true);
            }
        }
    }

    @LocalOnly
    public static class LocalPinnedFixture {
        static final AtomicBoolean EXECUTED_IN_COORDINATOR = new AtomicBoolean(false);

        @Test
        void shouldStayOnCoordinator() {
            if (!DistributionSettings.isWorkerRole()) {
                EXECUTED_IN_COORDINATOR.set(true);
            }
        }
    }

    @RemoteOnly
    public static class StrictRemoteOnlyFixture {
        @Test
        void mustRunRemotely() {
        }
    }
}
