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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.citrusframework.distribution.annotations.LocalOnly;
import org.citrusframework.distribution.annotations.RemoteOnly;
import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerCacheStats;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
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
import org.junit.platform.launcher.TestPlan;
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
    void shouldReplayRemoteResultsAgainstTheTestPlanInsideStartedClassContainers() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, cluster.broker.baseUrl());

            SurefireLikeListener listener = new SurefireLikeListener();
            new DistributedLauncher(LauncherFactory.create()).execute(LauncherDiscoveryRequestBuilder.request()
                    .selectors(DiscoverySelectors.selectClass(RemoteMixedFixture.class))
                    .build(), listener);

            assertThat(listener.problems).as("events Surefire could not attribute to a test set").isEmpty();
            assertThat(listener.finishedTests).containsOnlyKeys(
                    planMethodId(listener.plan, RemoteMixedFixture.class, "passes"),
                    planMethodId(listener.plan, RemoteMixedFixture.class, "fails"));
            assertThat(listener.finishedTests.get(planMethodId(listener.plan, RemoteMixedFixture.class, "fails")))
                    .isEqualTo(TestExecutionResult.Status.FAILED);
            assertThat(listener.finishedContainers).as("class test set completed")
                    .contains(RemoteMixedFixture.class.getName());
        }
    }

    @Test
    void shouldReportAClassSplitBetweenLocalAndRemoteUnitsAsOneTestSet() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, cluster.broker.baseUrl());

            SurefireLikeListener listener = new SurefireLikeListener();
            new DistributedLauncher(LauncherFactory.create()).execute(LauncherDiscoveryRequestBuilder.request()
                    .selectors(DiscoverySelectors.selectClass(SplitFixture.class))
                    .build(), listener);

            assertThat(listener.classStarts).containsEntry(SplitFixture.class.getName(), 1);
            assertThat(listener.problems).isEmpty();
            assertThat(listener.finishedTests).containsOnlyKeys(
                    planMethodId(listener.plan, SplitFixture.class, "onHost"),
                    planMethodId(listener.plan, SplitFixture.class, "onWorker"));
        }
    }

    @Test
    void shouldReportEachTestOnceToListenersRegisteredOnTheLauncher() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, cluster.broker.baseUrl());

            List<String> finished = new CopyOnWriteArrayList<>();
            DistributedLauncher launcher = new DistributedLauncher(LauncherFactory.create());
            launcher.registerTestExecutionListeners(new TestExecutionListener() {
                @Override
                public void executionFinished(TestIdentifier id, TestExecutionResult result) {
                    if (id.isTest()) {
                        finished.add(id.getDisplayName());
                    }
                }
            });
            launcher.execute(LauncherDiscoveryRequestBuilder.request()
                    .selectors(DiscoverySelectors.selectClass(SplitFixture.class))
                    .build());

            assertThat(finished).containsExactlyInAnyOrder("onHost()", "onWorker()");
        }
    }

    @Test
    void shouldReportLocallyRunTestsLiveWithTheirRealDurationAndOutcome() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, cluster.broker.baseUrl());

            Map<String, Long> startedAt = new java.util.concurrent.ConcurrentHashMap<>();
            Map<String, Long> durations = new java.util.concurrent.ConcurrentHashMap<>();
            Map<String, TestExecutionResult.Status> outcomes = new java.util.concurrent.ConcurrentHashMap<>();
            new DistributedLauncher(LauncherFactory.create()).execute(LauncherDiscoveryRequestBuilder.request()
                    .selectors(DiscoverySelectors.selectClass(LocalTimingFixture.class))
                    .build(), new TestExecutionListener() {
                @Override
                public void executionStarted(TestIdentifier id) {
                    startedAt.put(id.getDisplayName(), System.nanoTime());
                }

                @Override
                public void executionFinished(TestIdentifier id, TestExecutionResult result) {
                    if (id.isTest()) {
                        durations.put(id.getDisplayName(), (System.nanoTime() - startedAt.get(id.getDisplayName())) / 1_000_000);
                        outcomes.put(id.getDisplayName(), result.getStatus());
                    }
                }
            });

            assertThat(durations.get("slow()")).as("listeners see the test while it runs").isGreaterThanOrEqualTo(250L);
            assertThat(outcomes).containsEntry("assumed()", TestExecutionResult.Status.ABORTED)
                    .containsEntry("remote()", TestExecutionResult.Status.SUCCESSFUL);
        }
    }

    private static String planMethodId(TestPlan plan, Class<?> type, String method) {
        java.util.List<TestIdentifier> found = new ArrayList<>();
        java.util.ArrayDeque<TestIdentifier> queue = new java.util.ArrayDeque<>(plan.getRoots());
        while (!queue.isEmpty()) {
            TestIdentifier id = queue.poll();
            queue.addAll(plan.getChildren(id));
            if (id.getSource().orElse(null) instanceof org.junit.platform.engine.support.descriptor.MethodSource ms
                    && ms.getClassName().equals(type.getName()) && ms.getMethodName().equals(method)) {
                found.add(id);
            }
        }
        assertThat(found).hasSize(1);
        return found.getFirst().getUniqueId();
    }

    /** Mirrors what Surefire's RunListenerAdapter needs: tests from the plan, nested in a started class container. */
    private static final class SurefireLikeListener implements TestExecutionListener {
        private TestPlan plan;
        private final Set<String> startedContainers = new HashSet<>();
        private final Map<String, Integer> classStarts = new java.util.concurrent.ConcurrentHashMap<>();
        private final Map<String, TestExecutionResult.Status> finishedTests = new java.util.concurrent.ConcurrentHashMap<>();
        private final Set<String> finishedContainers = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final List<String> problems = new CopyOnWriteArrayList<>();

        @Override
        public void testPlanExecutionStarted(TestPlan testPlan) {
            plan = testPlan;
        }

        @Override
        public void executionStarted(TestIdentifier id) {
            if (id.isContainer()) {
                startedContainers.add(id.getUniqueId());
                if (id.getSource().orElse(null) instanceof org.junit.platform.engine.support.descriptor.ClassSource cs) {
                    classStarts.merge(cs.getClassName(), 1, Integer::sum);
                }
            } else {
                checkAttributable(id);
            }
        }

        @Override
        public void executionFinished(TestIdentifier id, TestExecutionResult result) {
            if (id.isContainer()) {
                if (id.getSource().orElse(null) instanceof org.junit.platform.engine.support.descriptor.ClassSource cs) {
                    finishedContainers.add(cs.getClassName());
                }
                return;
            }
            checkAttributable(id);
            finishedTests.put(id.getUniqueId(), result.getStatus());
        }

        private void checkAttributable(TestIdentifier id) {
            String parent = id.getParentId().orElse(null);
            if (parent == null) {
                problems.add(id.getUniqueId() + " has no parent");
                return;
            }
            try {
                plan.getTestIdentifier(org.junit.platform.engine.UniqueId.parse(parent));
            } catch (RuntimeException e) {
                problems.add(id.getUniqueId() + " has a parent outside the test plan");
            }
            if (!startedContainers.contains(parent)) {
                problems.add(id.getUniqueId() + " reported before its container was started");
            }
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
            assertThat(cluster.broker.store().listSessions(1).getFirst().status())
                    .isEqualTo(org.citrusframework.distribution.model.SessionStatus.FAILED);
            assertThat(cluster.broker.store().listSessions(1).getFirst().failureReason())
                    .as("the launcher reports why its own session failed")
                    .contains("target/citrus-docs/conflict.txt");
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
            HttpResponse<String> sessions = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(cluster.broker.baseUrl() + "/api/v1/sessions")).GET().build(),
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

    @Test
    void shouldResolveTheWorkingDirectoryThroughSymlinks() throws Exception {
        Path real = Files.createDirectories(tempDir.resolve("real-module/target"));
        Path link = Files.createSymbolicLink(tempDir.resolve("linked-module"), real.getParent());
        assertThat(DistributedLauncher.WorkspaceSettings.relativeWorkingDirectory(link, real)).isEqualTo("target");
    }

    @Test
    void shouldMarkTheHostCacheAsLauncherOwnedWhenItIsUsed() throws Exception {
        Path module = tempDir.resolve("module-owner");
        Files.createDirectories(module.resolve("src/test/resources"));
        Files.writeString(module.resolve("src/test/resources/data.txt"), "data");
        System.setProperty(DistributionSettings.BASEDIR_PROPERTY, module.toString());
        System.setProperty(DistributionSettings.INPUTS_PROPERTY, "src/test/resources");

        DistributedLauncher.WorkspaceSettings settings = DistributedLauncher.WorkspaceSettings.resolve();

        assertThat(BlobCacheDirectory.readOwner(settings.cache().getRootDir()))
                .contains(BlobCacheDirectory.OWNER_LAUNCHER);
    }

    @Test
    void shouldForkWhenOnlyMissingOptionalInputsAreDeclared() {
        Path module = tempDir.resolve("module-optional");
        System.setProperty(DistributionSettings.BASEDIR_PROPERTY, module.toString());
        System.setProperty(DistributionSettings.INPUTS_PROPERTY, "?not/there");
        DistributedLauncher.WorkspaceSettings settings = DistributedLauncher.WorkspaceSettings.resolve();
        assertThat(settings.inputArtifacts()).isEmpty();
        assertThat(settings.cache().getRootDir()).doesNotExist();
        assertThat(settings.forkWorkspace()).as("declaring inputs implies a forked JVM").isTrue();
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
            List<DistributionWorkerClient> workers = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                DistributionWorkerClient worker = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                        .workerId("ws-worker-" + i)
                        .brokerUrl(broker.baseUrl())
                        .cacheDir(tempDir.resolve("worker-cache-" + i))
                        .forkJvmArgs(List.of("-Xmx384m"))
                        .build());
                worker.start();
                assertThat(worker.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();
                workers.add(worker);
            }
            return new Cluster(broker, workers, new BrokerHttpClient(broker.baseUrl(), "", 3_000L));
        }

        /**
         * A sibling submission running {@code fixture} in a forked JVM from this test JVM's classpath, uploaded first.
         */
        SubmitWorkRequest siblingWithOutputs(String sessionId, Class<?> fixture) throws Exception {
            List<ArtifactRef> classpath = new ArrayList<>();
            for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
                Path path = Path.of(entry);
                if (!Files.exists(path)) {
                    continue;
                }
                ClasspathPackager.PackagedArtifact packaged =
                        ClasspathPackager.packagePath(path);
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

                String sid = broker.store().listSessions(1).getFirst().sessionId();
                assertThat(broker.store().listTestExecutions(sid))
                        .extracting(e -> e.className() + "#" + e.testName() + "@" + e.partitionId())
                        .anyMatch(s -> s.startsWith(RemoteSampleFixture.class.getName() + "#shouldRunOnRemoteWorker"))
                        .anyMatch(s -> s.startsWith(LocalPinnedFixture.class.getName() + "#shouldStayOnCoordinator") && s.endsWith("@local"));
            }
        }
    }

    @Test
    void shouldReportLocalOnlyModuleExecutionsInSharedReactorSession() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            String sessionId = cluster.createSession();
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, cluster.broker.baseUrl());
            System.setProperty(DistributionSettings.SESSION_ID_PROPERTY, sessionId);

            Outcome outcome = run(LocalPinnedFixture.class);
            assertThat(outcome.passed()).isEqualTo(1);

            cluster.client.completeSession(sessionId,
                    new org.citrusframework.distribution.model.CompleteSessionRequest(0, 0, 0, 0L));

            var session = cluster.broker.store().findSession(sessionId).orElseThrow();
            assertThat(session.status()).isEqualTo(org.citrusframework.distribution.model.SessionStatus.PASSED);
            assertThat(session.totalTests()).isEqualTo(1);
            assertThat(session.passedTests()).isEqualTo(1);
            assertThat(cluster.broker.store().listTestExecutions(sessionId))
                    .singleElement()
                    .satisfies(exec -> {
                        assertThat(exec.className()).isEqualTo(LocalPinnedFixture.class.getName());
                        assertThat(exec.testName()).isEqualTo("shouldStayOnCoordinator()");
                        assertThat(exec.partitionId()).isEqualTo("local");
                    });
        }
    }

    @Test
    void shouldCaptureBeforeAllFailureDisabledClassAndOverloadedMethodKeysInLocalFallback() {
        Launcher launcher = LauncherFactory.create();
        WorkUnitSpec disabledUnit = new WorkUnitSpec(
                DisabledLocalClassFixture.class.getName(), DisabledLocalClassFixture.class.getName(), null, null, Map.of(), 0L);
        WorkUnitSpec beforeAllUnit = new WorkUnitSpec(
                BeforeAllFailingLocalFixture.class.getName(), BeforeAllFailingLocalFixture.class.getName(), null, null, Map.of(), 0L);
        WorkUnitSpec noArgUnit = new WorkUnitSpec(
                "key-no-arg", OverloadedLocalFixture.class.getName(), "verify()", null, Map.of(), 0L);
        WorkUnitSpec parameterizedUnit = new WorkUnitSpec(
                "key-with-info", OverloadedLocalFixture.class.getName(),
                "verify(org.junit.jupiter.api.TestInfo)", null, Map.of(), 0L);

        LocalFallbackExecutor.LocalExecutionStats stats = LocalFallbackExecutor.executeLocally(
                launcher,
                List.of(disabledUnit, beforeAllUnit, noArgUnit, parameterizedUnit),
                new TestExecutionListener[0]);

        assertThat(stats.passed()).isEqualTo(2);
        assertThat(stats.failed()).isEqualTo(1);
        assertThat(stats.skipped()).isEqualTo(1);
        assertThat(stats.executions())
                .extracting(e -> e.workUnitKey() + ":" + e.status())
                .containsExactlyInAnyOrder(
                        DisabledLocalClassFixture.class.getName() + ":SKIPPED",
                        BeforeAllFailingLocalFixture.class.getName() + ":FAILED",
                        "key-no-arg:PASSED",
                        "key-with-info:PASSED");
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

    @Test
    void shouldReuseWorkerLocalFileCacheAcrossSessionsAndPersistAfterWorkerRestart() throws Exception {
        Path brokerCacheDir = tempDir.resolve("broker-cache");
        Path workerCacheDir = tempDir.resolve("worker-file-cache");
        Path module = Files.createDirectories(tempDir.resolve("module-warm"));
        Files.createDirectories(module.resolve("src/test/resources"));
        Files.writeString(module.resolve("src/test/resources/data.txt"), WarmCacheFixture.EXPECTED_DATA);

        DistributionBrokerServer broker = new DistributionBrokerServer(DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(brokerCacheDir)
                .jdbcUrl("jdbc:hsqldb:mem:launcher_warm_" + System.nanoTime())
                .build());
        broker.start();
        try {
            BrokerHttpClient client = new BrokerHttpClient(broker.baseUrl(), "", 3_000L);
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, broker.baseUrl());
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, module.toString());
            System.setProperty(DistributionSettings.INPUTS_PROPERTY, "src/test/resources");

            // Session 1: a cold worker cache, filled from the broker.
            try (DistributionWorkerClient worker = startCachingWorker(broker, "warm-worker-1", workerCacheDir)) {
                Outcome outcome = run(WarmCacheFixture.class);
                assertThat(outcome.failed()).isZero();
                assertThat(outcome.passed()).isEqualTo(1);

                assertThat(workerCacheDir.resolve("blobs")).isDirectory();
                assertThat(workerCacheDir.resolve(".lock")).isRegularFile();
                assertThat(filesUnder(workerCacheDir.resolve("tmp"))).as("no download leftovers").isEmpty();

                WorkerCacheStats stats = awaitCacheStats(broker.baseUrl(), "warm-worker-1", s -> s.blobCount() > 0);
                assertThat(stats.cleanupEnabled()).isTrue();
                assertThat(stats.freeSpaceBufferBytes()).isZero();
                assertThat(stats.blobCount()).isPositive();
                assertThat(stats.cachedBytes()).isPositive();
                assertThat(stats.usableDiskBytes()).isPositive();
                assertThat(stats.lowDiskSpace()).isFalse();
            }

            // The broker loses every blob: session 2 can only run from the worker's local file cache.
            DistributedLauncher.WorkspaceSettings workspace = DistributedLauncher.WorkspaceSettings.resolve();
            assertThat(workspace.inputArtifacts()).hasSize(1);
            Path brokerInputBlob = new BlobCacheDirectory(brokerCacheDir)
                    .resolveBlobPath(workspace.inputArtifacts().get(0).artifact().sha256());
            assertThat(brokerInputBlob).isRegularFile();
            deleteRecursively(brokerCacheDir.resolve("blobs"));
            assertThat(brokerInputBlob).doesNotExist();

            // Session 2: a restarted worker on the same cache directory. Submitted directly, without the launcher's
            // upload step, so nothing repopulates the broker's blob cache.
            try (DistributionWorkerClient worker = startCachingWorker(broker, "warm-worker-2", workerCacheDir)) {
                WorkerCacheStats restarted = awaitCacheStats(broker.baseUrl(), "warm-worker-2", s -> s.blobCount() > 0);
                assertThat(restarted.cachedBytes()).as("cache survives the worker restart").isPositive();

                List<ArtifactRef> classpath = new ArrayList<>();
                for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
                    Path path = Path.of(entry);
                    if (!entry.isBlank() && Files.exists(path)) {
                        classpath.add(ClasspathPackager.packagePath(path).ref());
                    }
                }
                String sessionId = client.createSession(
                        new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 0)).sessionId();
                Set<String> partitions = Set.copyOf(client.submitWork(sessionId, new SubmitWorkRequest(
                        "warm-2", "default", classpath, Map.of(), List.of(unit(WarmCacheFixture.class)),
                        classpath.size() + 1, 0, workspace.inputArtifacts(), List.of(), true, PartitionPolicy.LPT,
                        workspace.workingDirectory())).partitionIds());

                List<String> testStatuses = new CopyOnWriteArrayList<>();
                List<String> partitionStates = new CopyOnWriteArrayList<>();
                Set<String> pending = new HashSet<>(partitions);
                client.streamSessionEvents(sessionId, 0L, 60_000L, event -> {
                    if (event.partitionId() == null || !partitions.contains(event.partitionId())) {
                        return false;
                    }
                    switch (event.type()) {
                        case "test.event" -> {
                            WorkerTestEventPayload payload = DistributionJson.convert(event.data(),
                                    WorkerTestEventPayload.class);
                            if (payload.status() != TestOutcomeStatus.STARTED) {
                                testStatuses.add(payload.testName() + ":" + payload.status()
                                        + (payload.errorMessage() != null ? " " + payload.errorMessage() : ""));
                            }
                        }
                        case "partition.completed", "partition.lost" -> {
                            partitionStates.add(event.type() + ":" + event.data().get("state")
                                    + (event.data().get("errorMessage") != null
                                    ? " " + event.data().get("errorMessage") : ""));
                            pending.remove(event.partitionId());
                        }
                        default -> {
                            // other events are irrelevant here
                        }
                    }
                    return pending.isEmpty();
                });

                assertThat(partitionStates).containsExactly("partition.completed:COMPLETED");
                assertThat(testStatuses).containsExactly("readInputFromWarmCache():PASSED");
                assertThat(brokerInputBlob).as("nothing re-uploaded the input to the broker").doesNotExist();
            }
        } finally {
            broker.close();
        }
    }

    @Test
    void shouldNotCreateDefaultWorkerCacheWhenDistributionDisabled() throws Exception {
        Path isolatedUserHome = Files.createDirectories(tempDir.resolve("isolated-user-home"));
        String originalUserHome = System.getProperty("user.home");
        LocalDisabledFixture.EXECUTED.set(false);
        try {
            System.setProperty("user.home", isolatedUserHome.toString());
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "false");
            System.setProperty("junit.platform.launcher.interceptors.enabled", "true");

            Launcher launcher = LauncherFactory.create();
            launcher.execute(LauncherDiscoveryRequestBuilder.request()
                    .selectors(DiscoverySelectors.selectClass(LocalDisabledFixture.class))
                    .build());

            assertThat(LocalDisabledFixture.EXECUTED.get()).isTrue();
            assertThat(isolatedUserHome.resolve(".citrus-test-distribution-agent")).doesNotExist();
        } finally {
            System.clearProperty("junit.platform.launcher.interceptors.enabled");
            System.setProperty("user.home", originalUserHome);
        }
    }

    private static DistributionWorkerClient startCachingWorker(DistributionBrokerServer broker, String workerId,
                                                               Path cacheDir) throws Exception {
        DistributionWorkerClient worker = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                .workerId(workerId)
                .brokerUrl(broker.baseUrl())
                .cacheDir(cacheDir)
                .cacheCleanup(true)
                .cacheFreeSpaceBufferBytes(0L)
                .forkJvmArgs(List.of("-Xmx384m"))
                .build());
        worker.start();
        assertThat(worker.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();
        return worker;
    }

    /** Polls {@code GET /api/v1/workers} until the worker reports cache statistics matching {@code condition}. */
    private static WorkerCacheStats awaitCacheStats(String brokerUrl, String workerId,
                                                    Predicate<WorkerCacheStats> condition)
            throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        long deadline = System.currentTimeMillis() + 10_000L;
        WorkerCacheStats last = null;
        while (System.currentTimeMillis() < deadline) {
            HttpResponse<String> resp = http.send(HttpRequest.newBuilder(
                            URI.create(brokerUrl + "/api/v1/workers")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(resp.statusCode()).isEqualTo(200);
            for (WorkerRecord worker : DistributionJson.fromJson(resp.body(), WorkerRecord[].class)) {
                if (workerId.equals(worker.workerId())) {
                    last = worker.cacheStats();
                }
            }
            if (last != null && condition.test(last)) {
                return last;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("Worker " + workerId + " did not report matching cache statistics; last: " + last);
    }

    private static List<Path> filesUnder(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.walk(dir)) {
            return files.filter(Files::isRegularFile).toList();
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    /** Runs in a forked worker JVM; its only input comes from the declared {@code src/test/resources}. */
    public static class WarmCacheFixture {
        static final String EXPECTED_DATA = "warm-cache-data";

        @Test
        void readInputFromWarmCache() throws Exception {
            if (!DistributionSettings.isWorkerRole()) {
                throw new AssertionError("must run on a worker");
            }
            String data = Files.readString(Path.of("src/test/resources/data.txt"));
            if (!EXPECTED_DATA.equals(data)) {
                throw new AssertionError("unexpected input content: " + data);
            }
        }
    }

    public static class LocalDisabledFixture {
        static final AtomicBoolean EXECUTED = new AtomicBoolean(false);

        @Test
        void runsLocally() {
            EXECUTED.set(true);
        }
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

    public static class LocalTimingFixture {
        @Test
        @LocalOnly
        void slow() throws InterruptedException {
            Thread.sleep(300);
        }

        @Test
        @LocalOnly
        void assumed() {
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "not on this host");
        }

        @Test
        void remote() {
        }
    }

    public static class SplitFixture {
        @Test
        @LocalOnly
        void onHost() {
        }

        @Test
        void onWorker() {
        }
    }

    public static class RemoteMixedFixture {
        @Test
        void passes() {
        }

        @Test
        void fails() {
            throw new AssertionError("expected remote failure");
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

    @LocalOnly
    @org.junit.jupiter.api.Disabled("class-level disabled")
    public static class DisabledLocalClassFixture {
        @Test
        void neverRuns() {
        }
    }

    @LocalOnly
    public static class BeforeAllFailingLocalFixture {
        @org.junit.jupiter.api.BeforeAll
        static void failBeforeAll() {
            if ("true".equals(System.getProperty("citrus.test.local.beforeall.arm", "true"))) {
                throw new IllegalStateException("beforeAll failure");
            }
        }

        @Test
        void neverReached() {
        }
    }

    @LocalOnly
    public static class OverloadedLocalFixture {
        @Test
        void verify() {
        }

        @Test
        void verify(org.junit.jupiter.api.TestInfo info) {
            assertThat(info.getDisplayName()).isNotBlank();
        }
    }
}
