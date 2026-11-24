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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.sun.net.httpserver.HttpServer;
import org.citrusframework.distribution.annotations.LocalOnly;
import org.citrusframework.distribution.annotations.RemoteOnly;
import org.citrusframework.distribution.coordinator.DistributionCoordinatorConfig;
import org.citrusframework.distribution.coordinator.DistributionCoordinatorServer;
import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.workspace.WorkspaceLease;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
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

    private String outerRole;

    @BeforeEach
    void enterLauncherRole() {
        // Each test acts as the launcher of its own distribution session.
        outerRole = System.getProperty(DistributionSettings.ROLE_PROPERTY);
        System.clearProperty(DistributionSettings.ROLE_PROPERTY);
    }

    @AfterEach
    void resetProps() {
        if (outerRole != null) {
            System.setProperty(DistributionSettings.ROLE_PROPERTY, outerRole);
        }
        System.clearProperty(DistributionSettings.ENABLED_PROPERTY);
        System.clearProperty(DistributionSettings.COORDINATOR_URL_PROPERTY);
        System.clearProperty(DistributionSettings.FALLBACK_LOCAL_PROPERTY);
        System.clearProperty(DistributionSettings.REPORT_DIR_PROPERTY);
        System.clearProperty(DistributionSettings.CONNECT_TIMEOUT_MS_PROPERTY);
        System.clearProperty(DistributionSettings.SESSION_ID_PROPERTY);
        System.clearProperty(DistributionSettings.BASEDIR_PROPERTY);
        System.clearProperty(DistributionSettings.INPUTS_PROPERTY);
        System.clearProperty(DistributionSettings.OUTPUTS_PROPERTY);
        System.clearProperty(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY);
        System.clearProperty(DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY);
        System.clearProperty(DistributionSettings.MIN_REMOTE_COLD_UNITS_PROPERTY);
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
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());
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
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());
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
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());

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
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());

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
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());

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
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());

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
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, module.toString());
            System.setProperty(DistributionSettings.OUTPUTS_PROPERTY, "target/citrus-docs");
            System.setProperty(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY, "1");

            assertThatThrownBy(() -> run(ConflictingOutputFixtureA.class, ConflictingOutputFixtureB.class))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("target/citrus-docs/conflict.txt");
            assertThat(module.resolve("target/citrus-docs/conflict.txt")).doesNotExist();
            assertThat(cluster.coordinator.store().listSessions(1).getFirst().status())
                    .isEqualTo(org.citrusframework.distribution.model.SessionStatus.FAILED);
            assertThat(cluster.coordinator.store().listSessions(1).getFirst().failureReason())
                    .as("the launcher reports why its own session failed")
                    .contains("target/citrus-docs/conflict.txt");
        }
    }

    @Test
    void shouldFailBeforeSubmittingWhenRequiredInputIsMissing() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            Path module = Files.createDirectories(tempDir.resolve("module-missing"));
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, module.toString());
            System.setProperty(DistributionSettings.INPUTS_PROPERTY, "src/test/resources,?optional/dir");

            assertThatThrownBy(() -> run(RemoteWorkspaceFixture.class))
                    .hasMessageContaining("src/test/resources");
            HttpResponse<String> sessions = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(cluster.coordinator.baseUrl() + "/api/v1/sessions")).GET().build(),
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

    /** An embedded coordinator with two workers. */
    private record Cluster(DistributionCoordinatorServer coordinator, List<DistributionWorkerClient> workers, CoordinatorHttpClient client)
            implements AutoCloseable {

        static Cluster start(Path tempDir) throws Exception {
            DistributionCoordinatorServer coordinator = new DistributionCoordinatorServer(DistributionCoordinatorConfig.builder()
                    .port(0)
                    .cacheDir(tempDir.resolve("coordinator-cache"))
                    .jdbcUrl("jdbc:hsqldb:mem:launcher_ws_" + System.nanoTime())
                    .build());
            coordinator.start();
            List<DistributionWorkerClient> workers = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                DistributionWorkerClient worker = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                        .workerId("ws-worker-" + i)
                        .coordinatorUrl(coordinator.baseUrl())
                        .cacheDir(tempDir.resolve("worker-cache-" + i))
                        .forkJvmArgs(List.of("-Xmx384m"))
                        .build());
                worker.start();
                assertThat(worker.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();
                workers.add(worker);
            }
            return new Cluster(coordinator, workers, new CoordinatorHttpClient(coordinator.baseUrl(), "", 3_000L));
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
            coordinator.close();
        }
    }

    @Test
    void shouldDistributeRemoteTestsPinLocalOnlyAndWriteSurefireXml() throws Exception {
        LocalPinnedFixture.EXECUTED_ON_LAUNCHER_HOST.set(false);
        RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.set(false);

        DistributionCoordinatorConfig coordinatorCfg = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("coordinator-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:junit_dist_" + System.nanoTime())
                .build();

        try (DistributionCoordinatorServer coordinator = new DistributionCoordinatorServer(coordinatorCfg)) {
            coordinator.start();

            DistributionWorkerConfig workerCfg = DistributionWorkerConfig.builder()
                    .workerId("worker-1")
                    .coordinatorUrl(coordinator.baseUrl())
                    .cacheDir(tempDir.resolve("worker-cache"))
                    .capabilities(Map.of("os", "linux"))
                    .build();

            try (DistributionWorkerClient worker = new DistributionWorkerClient(workerCfg)) {
                worker.start();

                Path reportDir = tempDir.resolve("surefire-reports");
                System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
                System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, coordinator.baseUrl());
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
                assertThat(LocalPinnedFixture.EXECUTED_ON_LAUNCHER_HOST.get()).isTrue();
                // RemoteSampleFixture ran inside IsolatedPartitionRunner's child ClassLoader, NOT coordinator static!
                assertThat(RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.get()).isFalse();

                Path xmlReport = reportDir.resolve("TEST-" + RemoteSampleFixture.class.getName() + ".xml");
                assertThat(Files.exists(xmlReport)).isTrue();
                assertThat(Files.readString(xmlReport)).contains("shouldRunOnRemoteWorker");

                String sid = coordinator.store().listSessions(1).getFirst().sessionId();
                assertThat(coordinator.store().listTestExecutions(sid))
                        .extracting(e -> e.className() + "#" + e.testName() + "@" + e.partitionId())
                        .anyMatch(s -> s.startsWith(RemoteSampleFixture.class.getName() + "#shouldRunOnRemoteWorker"))
                        .anyMatch(s -> s.startsWith(LocalPinnedFixture.class.getName() + "#shouldStayOnLauncherHost") && s.endsWith("@local"));
            }
        }
    }

    @Test
    void shouldRunSmallModuleLocallyOnWarmHistorySkipRemoteDispatchAndReportLocalTests() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());
            System.setProperty(DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY, "2000");
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, tempDir.toString());
            RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.set(false);

            assertThat(run(RemoteSampleFixture.class)).isEqualTo(new Outcome(1, 0));
            assertThat(RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.get()).isFalse();
            String key = RemoteSampleFixture.class.getName();
            assertThat(cluster.coordinator.store().findHistoricalDurationsMs(List.of(key), 10).get(key))
                    .isNotNull().isLessThan(2_000L);

            String sessionId = cluster.createSession();
            System.setProperty(DistributionSettings.SESSION_ID_PROPERTY, sessionId);
            Path reportDir = tempDir.resolve("warm-reports");
            System.setProperty(DistributionSettings.REPORT_DIR_PROPERTY, reportDir.toString());
            RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.set(false);

            assertThat(run(RemoteSampleFixture.class)).isEqualTo(new Outcome(1, 0));
            assertThat(RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.get()).isTrue();
            assertThat(cluster.coordinator.store().listPartitions(sessionId)).isEmpty();
            assertThat(cluster.coordinator.store().listTestExecutions(sessionId)).singleElement().satisfies(execution -> {
                assertThat(execution.partitionId()).isEqualTo("local");
                assertThat(execution.workUnitKey()).isEqualTo(key);
            });
            assertThat(Files.readString(reportDir.resolve("TEST-" + key + ".xml")))
                    .contains("shouldRunOnRemoteWorker");
        }
    }

    @Test
    void shouldKeepDistributingWhenRemoteOnlyPresentOrHistoryEndpointUnavailable() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());
            System.setProperty(DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY, "2000");
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, tempDir.toString());
            assertThat(run(StrictRemoteOnlyFixture.class)).isEqualTo(new Outcome(1, 0));

            String seededSession = cluster.coordinator.store().listSessions(1).getFirst().sessionId();
            String key = cluster.coordinator.store().listPartitions(seededSession).getFirst().workUnits().getFirst().key();
            assertThat(cluster.coordinator.store().findHistoricalDurationsMs(List.of(key), 10).get(key))
                    .isNotNull().isLessThan(2_000L);
            String sessionId = cluster.createSession();
            System.setProperty(DistributionSettings.SESSION_ID_PROPERTY, sessionId);
            assertThat(run(StrictRemoteOnlyFixture.class)).isEqualTo(new Outcome(1, 0));
            assertThat(cluster.coordinator.store().listPartitions(sessionId)).hasSize(1);

            AtomicInteger historyRequests = new AtomicInteger();
            AtomicReference<SubmitWorkRequest> submission = new AtomicReference<>();
            HttpServer oldCoordinator = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            oldCoordinator.createContext("/api/v1/history/durations", exchange -> {
                try (exchange) {
                    historyRequests.incrementAndGet();
                    exchange.sendResponseHeaders(404, -1);
                }
            });
            oldCoordinator.createContext("/api/v1/cache/missing", exchange -> {
                try (exchange) {
                    byte[] body = "{\"missing\":[]}".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                }
            });
            oldCoordinator.createContext("/api/v1/sessions/" + sessionId + "/work", exchange -> {
                try (exchange) {
                    submission.set(DistributionJson.fromJson(
                            new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8),
                            SubmitWorkRequest.class));
                    byte[] body = "{\"partitionIds\":[]}".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                }
            });
            oldCoordinator.start();
            try {
                String oldCoordinatorUrl = "http://127.0.0.1:" + oldCoordinator.getAddress().getPort();
                System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, oldCoordinatorUrl);
                assertThat(new CoordinatorHttpClient(oldCoordinatorUrl, "", 500).findHistoricalDurations(List.of(key), 10)).isEmpty();
                RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.set(false);
                run(RemoteSampleFixture.class);
                assertThat(RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.get()).isFalse();
                assertThat(historyRequests.get()).isEqualTo(2);
                assertThat(submission.get()).isNotNull();
                assertThat(submission.get().workUnits()).extracting(WorkUnitSpec::key)
                        .containsExactly(RemoteSampleFixture.class.getName());
            } finally {
                oldCoordinator.stop(0);
            }
        }
    }

    @Test
    void shouldBypassOnColdHistoryWhenMinRemoteColdUnitsThresholdMatches() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());
            System.setProperty(DistributionSettings.MIN_REMOTE_COLD_UNITS_PROPERTY, "2");
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, tempDir.toString());
            RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.set(false);

            assertThat(run(RemoteSampleFixture.class)).isEqualTo(new Outcome(1, 0));
            assertThat(RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.get()).isTrue();
            assertThat(cluster.coordinator.store().listSessions(10)).isEmpty();
            assertThat(tempDir.resolve("target/.citrus-dist")).doesNotExist();
        }
    }

    @Test
    void shouldIgnoreMalformedHistoryAndTimeOutEvenAfterResponseHeadersArrive() throws Exception {
        AtomicBoolean stall = new AtomicBoolean();
        CountDownLatch releaseBody = new CountDownLatch(1);
        HttpServer coordinator = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        coordinator.createContext("/api/v1/history/durations", exchange -> {
            try (exchange) {
                if (stall.get()) {
                    exchange.sendResponseHeaders(200, 0);
                    exchange.getResponseBody().write('{');
                    exchange.getResponseBody().flush();
                    releaseBody.await(5, TimeUnit.SECONDS);
                    exchange.getResponseBody().write('}');
                } else {
                    byte[] body = "invalid JSON".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        coordinator.start();
        try {
            CoordinatorHttpClient client = new CoordinatorHttpClient("http://127.0.0.1:" + coordinator.getAddress().getPort(), "", 500);
            assertThat(client.findHistoricalDurations(List.of("test"), 10)).isEmpty();
            stall.set(true);
            long start = System.nanoTime();
            assertThat(client.findHistoricalDurations(List.of("test"), 10)).isEmpty();
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(4));
        } finally {
            releaseBody.countDown();
            coordinator.stop(0);
        }
    }

    @Test
    void shouldFinishWarmLocalRunWhenSharedReportResponseBodyStalls() throws Exception {
        String key = RemoteSampleFixture.class.getName();
        CountDownLatch reportStarted = new CountDownLatch(1);
        CountDownLatch releaseBody = new CountDownLatch(1);
        HttpServer coordinator = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        coordinator.createContext("/api/v1/history/durations", exchange -> {
            try (exchange) {
                byte[] body = DistributionJson.toJson(Map.of("durationsMs", Map.of(key, 1L)))
                        .getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
        });
        coordinator.createContext("/api/v1/sessions/reactor/local-tests", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(200, 0);
                exchange.getResponseBody().write('{');
                exchange.getResponseBody().flush();
                reportStarted.countDown();
                releaseBody.await(10, TimeUnit.SECONDS);
                exchange.getResponseBody().write('}');
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        coordinator.start();
        System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, "http://127.0.0.1:" + coordinator.getAddress().getPort());
        System.setProperty(DistributionSettings.CONNECT_TIMEOUT_MS_PROPERTY, "500");
        System.setProperty(DistributionSettings.SESSION_ID_PROPERTY, "reactor");
        System.setProperty(DistributionSettings.BASEDIR_PROPERTY, tempDir.toString());
        RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.set(false);
        var execution = new FutureTask<>(() -> run(RemoteSampleFixture.class));
        Thread launch = Thread.ofVirtual().start(execution);
        try {
            assertThat(reportStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(execution.get(4, TimeUnit.SECONDS)).isEqualTo(new Outcome(1, 0));
            assertThat(RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.get()).isTrue();
        } finally {
            releaseBody.countDown();
            coordinator.stop(0);
            launch.join(Duration.ofSeconds(5));
        }
    }

    @Test
    void shouldAbortBeforeDispatchWhenHistoryLookupIsInterrupted() throws Exception {
        assertAbortsWhenHistoryLookupIsInterrupted(0);
    }

    @Test
    void shouldAbortBeforeColdBypassWhenHistoryLookupIsInterrupted() throws Exception {
        assertAbortsWhenHistoryLookupIsInterrupted(1);
    }

    private void assertAbortsWhenHistoryLookupIsInterrupted(int coldThreshold) throws Exception {
        CountDownLatch historyStarted = new CountDownLatch(1);
        CountDownLatch releaseHistory = new CountDownLatch(1);
        AtomicInteger sessionRequests = new AtomicInteger();
        HttpServer coordinator = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        coordinator.createContext("/api/v1/history/durations", exchange -> {
            try (exchange) {
                historyStarted.countDown();
                releaseHistory.await(10, TimeUnit.SECONDS);
                exchange.sendResponseHeaders(503, -1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        coordinator.createContext("/api/v1/sessions", exchange -> {
            try (exchange) {
                sessionRequests.incrementAndGet();
                exchange.sendResponseHeaders(503, -1);
            }
        });
        coordinator.start();
        System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, "http://127.0.0.1:" + coordinator.getAddress().getPort());
        System.setProperty(DistributionSettings.CONNECT_TIMEOUT_MS_PROPERTY, "10000");
        System.setProperty(DistributionSettings.MIN_REMOTE_COLD_UNITS_PROPERTY, Integer.toString(coldThreshold));
        System.setProperty(DistributionSettings.BASEDIR_PROPERTY, tempDir.toString());
        RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.set(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptPreserved = new AtomicBoolean();
        Thread launch = Thread.ofVirtual().start(() -> {
            try {
                run(RemoteSampleFixture.class);
            } catch (Throwable e) {
                failure.set(e);
            } finally {
                interruptPreserved.set(Thread.currentThread().isInterrupted());
            }
        });
        try {
            assertThat(historyStarted.await(5, TimeUnit.SECONDS)).isTrue();
            launch.interrupt();
            assertThat(launch.join(Duration.ofSeconds(4))).isTrue();
            assertThat(failure.get()).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("interrupted before test execution");
            assertThat(interruptPreserved.get()).isTrue();
            assertThat(RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.get()).isFalse();
            assertThat(sessionRequests.get()).isZero();
        } finally {
            releaseHistory.countDown();
            coordinator.stop(0);
            launch.join(Duration.ofSeconds(5));
        }
    }

    @Test
    void shouldResolveTestPlanWorkUnitsFromFactoryAndNestedClassAncestry() {
        Launcher launcher = LauncherFactory.create();
        TestPlan plan = launcher.discover(LauncherDiscoveryRequestBuilder.request().selectors(
                DiscoverySelectors.selectClass(PlanMappingFixture.class),
                DiscoverySelectors.selectClass(FirstDerivedPlanFixture.class),
                DiscoverySelectors.selectClass(SecondDerivedPlanFixture.class),
                DiscoverySelectors.selectClass(LocalPinnedFixture.class)).build());
        Class<?> factory = PlanMappingFixture.class;
        Class<?> nested = BasePlanFixture.NestedPlanFixture.class;
        List<WorkUnitSpec> units = List.of(
                new WorkUnitSpec("first-source", factory.getName(), "firstFactory", "source", Map.of(), 0L),
                new WorkUnitSpec("second-source", factory.getName(), "secondFactory", "source", Map.of(), 0L),
                new WorkUnitSpec("first-nested", nested.getName(), null, null, Map.of(), 0L,
                        List.of(FirstDerivedPlanFixture.class.getName())),
                new WorkUnitSpec("second-nested", nested.getName(), null, null, Map.of(), 0L,
                        List.of(SecondDerivedPlanFixture.class.getName())));
        List<TestPlan> runningPlans = new ArrayList<>();
        var stats = LocalFallbackExecutor.executeTestPlanLocally(launcher, plan, units, new TestExecutionListener[] {
                new TestExecutionListener() {
                    @Override
                    public void testPlanExecutionStarted(TestPlan runningPlan) {
                        runningPlans.add(runningPlan);
                    }
                }
        });

        assertThat(runningPlans).singleElement().satisfies(running -> assertThat(running.getRoots()).isEqualTo(plan.getRoots()));
        assertThat(stats.passed()).isEqualTo(6);
        assertThat(stats.executions()).extracting(WorkerTestEventPayload::workUnitKey)
                .containsExactlyInAnyOrder("first-source", "second-source", "first-nested", "second-nested", null, null);
        assertThat(stats.executions())
                .filteredOn(execution -> execution.testName().equals("source-more")
                        || execution.className().equals(LocalPinnedFixture.class.getName()))
                .hasSize(2).allSatisfy(execution -> assertThat(execution.workUnitKey()).isNull());
    }

    @Test
    void shouldReportLocalOnlyModuleExecutionsInSharedReactorSession() throws Exception {
        try (Cluster cluster = Cluster.start(tempDir)) {
            String sessionId = cluster.createSession();
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, cluster.coordinator.baseUrl());
            System.setProperty(DistributionSettings.SESSION_ID_PROPERTY, sessionId);

            Outcome outcome = run(LocalPinnedFixture.class);
            assertThat(outcome.passed()).isEqualTo(1);

            cluster.client.completeSession(sessionId,
                    new org.citrusframework.distribution.model.CompleteSessionRequest(0, 0, 0, 0L));

            var session = cluster.coordinator.store().findSession(sessionId).orElseThrow();
            assertThat(session.status()).isEqualTo(org.citrusframework.distribution.model.SessionStatus.PASSED);
            assertThat(session.totalTests()).isEqualTo(1);
            assertThat(session.passedTests()).isEqualTo(1);
            assertThat(cluster.coordinator.store().listTestExecutions(sessionId))
                    .singleElement()
                    .satisfies(exec -> {
                        assertThat(exec.className()).isEqualTo(LocalPinnedFixture.class.getName());
                        assertThat(exec.testName()).isEqualTo("shouldStayOnLauncherHost()");
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
    void shouldFallbackLocallyWhenCoordinatorUnreachableAndFailFastOnRemoteOnly() {
        RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.set(false);
        System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
        System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, "http://127.0.0.1:1");
        System.setProperty(DistributionSettings.CONNECT_TIMEOUT_MS_PROPERTY, "500");
        System.setProperty(DistributionSettings.FALLBACK_LOCAL_PROPERTY, "true");

        DistributedLauncher distributed = new DistributedLauncher(LauncherFactory.create());
        LauncherDiscoveryRequest fallbackReq = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(RemoteSampleFixture.class))
                .build();
        distributed.execute(fallbackReq);
        assertThat(RemoteSampleFixture.EXECUTED_ON_LAUNCHER_HOST.get()).isTrue();

        LauncherDiscoveryRequest remoteOnlyReq = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(StrictRemoteOnlyFixture.class))
                .build();
        assertThatThrownBy(() -> distributed.execute(remoteOnlyReq))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("coordinator unreachable");
    }

    @Test
    void shouldForwardCustomSurefireSystemPropertiesWhileExcludingHostAndDistributionInternals() {
        java.util.Properties source = new java.util.Properties();
        source.setProperty("citrus.default.timeout", "15000");
        source.setProperty("spring.profiles.active", "integration");
        source.setProperty("custom.service.port", "19090");
        source.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, "http://127.0.0.1:18080");
        source.setProperty("junit.platform.launcher.interceptors.enabled", "true");
        source.setProperty("java.home", "/host/jdk-21");
        source.setProperty("user.dir", "/host/workspace");
        source.setProperty("os.name", "Mac OS X");
        source.setProperty("surefire.real.class.path", "/host/target/surefirebooter.jar");
        source.setProperty("maven.multiModuleProjectDirectory", "/host/workspace");
        source.setProperty("basedir", "/host/workspace/module");
        source.setProperty(WorkspaceLease.ENABLED_PROPERTY, "true");
        source.setProperty(WorkspaceLease.TICKET_PROPERTY, "/host/local-ticket");
        source.setProperty("localRepository", "/host/.m2/repository");

        java.util.Map<String, String> forwarded = DistributedLauncher.collectPropagatedProperties(source);

        assertThat(forwarded)
                .containsEntry("citrus.default.timeout", "15000")
                .containsEntry("spring.profiles.active", "integration")
                .containsEntry("custom.service.port", "19090")
                .containsEntry(WorkspaceLease.ENABLED_PROPERTY, "true")
                .doesNotContainKeys(
                        WorkspaceLease.TICKET_PROPERTY,
                        DistributionSettings.COORDINATOR_URL_PROPERTY,
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
        Path coordinatorCacheDir = tempDir.resolve("coordinator-cache");
        Path workerCacheDir = tempDir.resolve("worker-file-cache");
        Path module = Files.createDirectories(tempDir.resolve("module-warm"));
        Files.createDirectories(module.resolve("src/test/resources"));
        Files.writeString(module.resolve("src/test/resources/data.txt"), WarmCacheFixture.EXPECTED_DATA);

        DistributionCoordinatorServer coordinator = new DistributionCoordinatorServer(DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(coordinatorCacheDir)
                .jdbcUrl("jdbc:hsqldb:mem:launcher_warm_" + System.nanoTime())
                .build());
        coordinator.start();
        try {
            CoordinatorHttpClient client = new CoordinatorHttpClient(coordinator.baseUrl(), "", 3_000L);
            System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            System.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, coordinator.baseUrl());
            System.setProperty(DistributionSettings.BASEDIR_PROPERTY, module.toString());
            System.setProperty(DistributionSettings.INPUTS_PROPERTY, "src/test/resources");

            // Session 1: a cold worker cache, filled from the coordinator.
            try (DistributionWorkerClient worker = startCachingWorker(coordinator, "warm-worker-1", workerCacheDir)) {
                Outcome outcome = run(WarmCacheFixture.class);
                assertThat(outcome.failed()).isZero();
                assertThat(outcome.passed()).isEqualTo(1);

                assertThat(workerCacheDir.resolve("blobs")).isDirectory();
                assertThat(workerCacheDir.resolve(".lock")).isRegularFile();
                assertThat(filesUnder(workerCacheDir.resolve("tmp"))).as("no download leftovers").isEmpty();

                WorkerCacheStats stats = awaitCacheStats(coordinator.baseUrl(), "warm-worker-1", s -> s.blobCount() > 0);
                assertThat(stats.cleanupEnabled()).isTrue();
                assertThat(stats.freeSpaceBufferBytes()).isZero();
                assertThat(stats.blobCount()).isPositive();
                assertThat(stats.cachedBytes()).isPositive();
                assertThat(stats.usableDiskBytes()).isPositive();
                assertThat(stats.lowDiskSpace()).isFalse();
            }

            // The coordinator loses every blob: session 2 can only run from the worker's local file cache.
            DistributedLauncher.WorkspaceSettings workspace = DistributedLauncher.WorkspaceSettings.resolve();
            assertThat(workspace.inputArtifacts()).hasSize(1);
            Path coordinatorInputBlob = new BlobCacheDirectory(coordinatorCacheDir)
                    .resolveBlobPath(workspace.inputArtifacts().get(0).artifact().sha256());
            assertThat(coordinatorInputBlob).isRegularFile();
            deleteRecursively(coordinatorCacheDir.resolve("blobs"));
            assertThat(coordinatorInputBlob).doesNotExist();

            // Session 2: a restarted worker on the same cache directory. Submitted directly, without the launcher's
            // upload step, so nothing repopulates the coordinator's blob cache.
            try (DistributionWorkerClient worker = startCachingWorker(coordinator, "warm-worker-2", workerCacheDir)) {
                WorkerCacheStats restarted = awaitCacheStats(coordinator.baseUrl(), "warm-worker-2", s -> s.blobCount() > 0);
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
                assertThat(coordinatorInputBlob).as("nothing re-uploaded the input to the coordinator").doesNotExist();
            }
        } finally {
            coordinator.close();
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

    private static DistributionWorkerClient startCachingWorker(DistributionCoordinatorServer coordinator, String workerId,
                                                               Path cacheDir) throws Exception {
        DistributionWorkerClient worker = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                .workerId(workerId)
                .coordinatorUrl(coordinator.baseUrl())
                .cacheDir(cacheDir)
                .cacheCleanup(true)
                .cacheFreeSpaceBufferBytes(0L)
                .forkJvmArgs(List.of("-Xmx384m"))
                .build());
        try {
            worker.start();
            assertThat(worker.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();
            return worker;
        } catch (Exception | Error failure) {
            try {
                worker.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /** Polls {@code GET /api/v1/workers} until the worker reports cache statistics matching {@code condition}. */
    private static WorkerCacheStats awaitCacheStats(String coordinatorUrl, String workerId,
                                                    Predicate<WorkerCacheStats> condition)
            throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(coordinatorUrl + "/api/v1/workers")).GET().build();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        WorkerCacheStats last = null;
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            for (long remaining = deadline - System.nanoTime(); remaining > 0; remaining = deadline - System.nanoTime()) {
                var requestResult = http.sendAsync(request, HttpResponse.BodyHandlers.ofString());
                HttpResponse<String> response;
                try {
                    response = requestResult.get(remaining, TimeUnit.NANOSECONDS);
                } finally {
                    requestResult.cancel(true);
                }
                assertThat(response.statusCode()).isEqualTo(200);
                for (WorkerRecord worker : DistributionJson.fromJson(response.body(), WorkerRecord[].class)) {
                    if (workerId.equals(worker.workerId())) {
                        last = worker.cacheStats();
                    }
                }
                if (last != null && condition.test(last)) {
                    return last;
                }
                TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.MILLISECONDS.toNanos(100),
                        Math.max(0, deadline - System.nanoTime())));
            }
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

    public static class PlanMappingFixture {
        @TestFactory
        DynamicContainer firstFactory() {
            return DynamicContainer.dynamicContainer("factory group", List.of(
                    DynamicTest.dynamicTest("source", () -> {}),
                    DynamicTest.dynamicTest("source-more", () -> {})));
        }

        @TestFactory
        DynamicTest secondFactory() {
            return DynamicTest.dynamicTest("source", () -> {});
        }
    }

    abstract static class BasePlanFixture {
        @Nested
        class NestedPlanFixture {
            @Test
            void runsInEachSubclass() {
            }
        }
    }

    static class FirstDerivedPlanFixture extends BasePlanFixture {
    }

    static class SecondDerivedPlanFixture extends BasePlanFixture {
    }

    public static class RemoteSampleFixture {
        static final AtomicBoolean EXECUTED_ON_LAUNCHER_HOST = new AtomicBoolean(false);

        @Test
        void shouldRunOnRemoteWorker() {
            if (!DistributionSettings.isWorkerRole()) {
                EXECUTED_ON_LAUNCHER_HOST.set(true);
            }
        }
    }

    @LocalOnly
    public static class LocalPinnedFixture {
        static final AtomicBoolean EXECUTED_ON_LAUNCHER_HOST = new AtomicBoolean(false);

        @Test
        void shouldStayOnLauncherHost() {
            if (!DistributionSettings.isWorkerRole()) {
                EXECUTED_ON_LAUNCHER_HOST.set(true);
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
