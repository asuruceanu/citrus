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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.citrusframework.TestActionRunner;
import org.citrusframework.annotations.CitrusEndpoint;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.annotations.CitrusTest;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.config.DistributionSettings.WorkerCwdCheckMode;
import org.citrusframework.distribution.config.DistributionSettings.WorkerForkMode;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.dsl.TestActionSupport;
import org.citrusframework.endpoint.Endpoint;
import org.citrusframework.endpoint.direct.annotation.DirectEndpointConfig;
import org.citrusframework.junit.jupiter.CitrusSupport;
import org.citrusframework.message.DefaultMessageQueue;
import org.citrusframework.message.MessageQueue;
import org.citrusframework.spi.BindToRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.engine.CancellationToken;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionJUnitLauncherAndCwdGuardTest {

    private static final String PROBE_FILE_PROPERTY = "cwd.probe.file";

    @TempDir
    Path tempDir;

    /** Not run by Surefire directly (nested class); executed through the launcher. */
    public static class OverloadedFixture {
        @Test
        void named() {
        }

        @Test
        void named(TestInfo info) {
            assertThat(info.getTestMethod()).isPresent();
        }
    }

    /** Fails before any test starts. */
    public static class FailingSetupFixture {
        @org.junit.jupiter.api.BeforeAll
        static void setUp() {
            throw new IllegalStateException("setup exploded");
        }

        @Test
        void neverRuns() {
        }
    }

    /** Declares the no-argument overload; the subclass adds another. */
    public static class InheritedBaseFixture {
        @Test
        void named() {
        }
    }

    public static class InheritedOverloadFixture extends InheritedBaseFixture {
        @Test
        void named(TestInfo info) {
        }
    }

    /** Has no tests at all. */
    public static class NoTestsFixture {
        void helper() {
        }
    }

    /** Skipped as a whole. */
    @org.junit.jupiter.api.Disabled("not today")
    public static class DisabledFixture {
        @Test
        void skipped() {
        }
    }

    /** Writes a file into the worker's CWD when asked to, and checks the per-partition property redirection. */
    public static class WorkspaceFixture {
        @Test
        void checkRedirectedProperties() {
            String basedir = System.getProperty("basedir");
            assertThat(basedir).endsWith("workspace");
            assertThat(Path.of(basedir)).isDirectory();
            assertThat(Path.of(basedir, "target")).isDirectory();
            assertThat(Path.of(basedir).getParent().getFileName().toString()).startsWith("cdist-");
            assertThat(System.getProperty("citrus.report.directory")).isEqualTo(Path.of(basedir, "target/citrus-reports").toString());
            assertThat(System.getProperty("citrus.message.trace.directory"))
                    .isEqualTo(Path.of(basedir, "custom/traces").toString());
            assertThat(System.getProperty("custom.dir")).isEqualTo(basedir + java.io.File.separator + "conf");
        }

        @Test
        void writeProbeIntoWorkingDirectory() throws Exception {
            String probe = System.getProperty(PROBE_FILE_PROPERTY);
            if (probe != null) {
                Files.writeString(Path.of("").toAbsolutePath().resolve(probe), "written by a test");
            }
        }
    }

    @Test
    void shouldSelectOverloadedMethodByParameterTypes() {
        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
        PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 3,
                List.of(unit(OverloadedFixture.class, "named(org.junit.jupiter.api.TestInfo)")),
                getClass().getClassLoader(), CancellationToken.create(), events::add);

        assertThat(summary.success()).isTrue();
        assertThat(finished(events)).singleElement().satisfies(e -> {
            assertThat(e.testName()).isEqualTo("named(TestInfo)");
            assertThat(e.status()).isEqualTo(TestOutcomeStatus.PASSED);
            assertThat(e.attempt()).isEqualTo(3);
            assertThat(e.sessionId()).isEqualTo("s1");
        });

        events.clear();
        PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1, List.of(unit(OverloadedFixture.class, "named()")),
                getClass().getClassLoader(), CancellationToken.create(), events::add);
        assertThat(finished(events)).singleElement().satisfies(e -> {
            assertThat(e.testName()).isEqualTo("named()");
            assertThat(e.uniqueId()).as("JUnit unique id lets the host replay into its own test plan")
                    .startsWith("[engine:junit-jupiter]/[class:" + OverloadedFixture.class.getName() + "]")
                    .endsWith("[method:named()]");
        });
    }

    @Test
    void shouldAttributeOverloadsInOnePartitionToTheirOwnUnits() {
        // The host keys a no-argument method by its bare name, so the bare key must not fall to another overload.
        WorkUnitSpec withInfo = unit(OverloadedFixture.class, "named(org.junit.jupiter.api.TestInfo)");
        WorkUnitSpec noArgs = unit(OverloadedFixture.class, "named");
        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
        PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1,
                List.of(withInfo, noArgs), getClass().getClassLoader(), CancellationToken.create(), events::add);

        assertThat(summary.success()).as(summary.errorMessage()).isTrue();
        assertThat(finished(events)).extracting(WorkerTestEventPayload::testName, WorkerTestEventPayload::workUnitKey)
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple("named(TestInfo)", withInfo.key()),
                        org.assertj.core.groups.Tuple.tuple("named()", noArgs.key()));
    }

    @Test
    void shouldResolveBareNameToTheNoArgumentOverload() {
        // The host sends a bare name exactly for methods without parameters.
        for (int i = 0; i < 5; i++) {
            List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
            PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1, List.of(unit(OverloadedFixture.class, "named")),
                    getClass().getClassLoader(), CancellationToken.create(), events::add);
            assertThat(finished(events)).singleElement().satisfies(e -> assertThat(e.testName()).isEqualTo("named()"));
        }
    }

    @Test
    void shouldResolveBareNameToAnInheritedNoArgumentOverload() {
        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
        PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1, List.of(unit(InheritedOverloadFixture.class, "named")),
                getClass().getClassLoader(), CancellationToken.create(), events::add);
        assertThat(finished(events)).singleElement().satisfies(e -> assertThat(e.testName()).isEqualTo("named()"));
    }

    @Test
    void shouldReportContainerFailureBeforeAnyTestAsFailedEvent() {
        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
        PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1,
                List.of(unit(FailingSetupFixture.class, null)), getClass().getClassLoader(), CancellationToken.create(), events::add);

        assertThat(summary.success()).isTrue();
        assertThat(finished(events)).singleElement().satisfies(e -> {
            assertThat(e.status()).isEqualTo(TestOutcomeStatus.FAILED);
            assertThat(e.className()).isEqualTo(FailingSetupFixture.class.getName());
            assertThat(e.errorMessage()).contains("setup exploded");
            assertThat(e.stackTrace()).contains("IllegalStateException");
        });
    }

    @Test
    void shouldFailWorkUnitThatMatchesNoTestsButAcceptSkippedContainers() {
        PartitionJUnitLauncher.ExecutionSummary empty = PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1,
                List.of(unit(NoTestsFixture.class, null)), getClass().getClassLoader(), CancellationToken.create(), e -> { });
        assertThat(empty.success()).isFalse();
        assertThat(empty.errorMessage()).contains(NoTestsFixture.class.getName()).contains("no tests");

        List<WorkerTestEventPayload> unresolved = new CopyOnWriteArrayList<>();
        PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1,
                List.of(unit(OverloadedFixture.class, "named(java.lang.String)")),
                getClass().getClassLoader(), CancellationToken.create(), unresolved::add);
        assertThat(finished(unresolved)).as("an unresolvable selector surfaces as the engine's discovery failure")
                .singleElement().satisfies(e -> {
                    assertThat(e.status()).isEqualTo(TestOutcomeStatus.FAILED);
                    assertThat(e.errorMessage()).contains("discovery");
                });

        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
        PartitionJUnitLauncher.ExecutionSummary disabled = PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1,
                List.of(unit(DisabledFixture.class, null)), getClass().getClassLoader(), CancellationToken.create(), events::add);
        assertThat(disabled.success()).as(disabled.errorMessage()).isTrue();
        assertThat(finished(events)).singleElement().satisfies(e -> {
            assertThat(e.status()).isEqualTo(TestOutcomeStatus.SKIPPED);
            assertThat(e.errorMessage()).contains("not today");
        });
    }

    @Test
    void shouldKeepReportDirectoriesInsideTheWorkspace() {
        Path workspace = tempDir.resolve("cdist-x/workspace");
        Map<String, String> escaping = IsolatedPartitionRunner.partitionOverrides(
                Map.of("citrus.report.directory", "../../outside"), workspace);
        assertThat(escaping.get("citrus.report.directory"))
                .as("a relative path escaping the workspace falls back to the default")
                .isEqualTo(workspace.resolve("target/citrus-reports").toString());

        Map<String, String> absolute = IsolatedPartitionRunner.partitionOverrides(
                Map.of("citrus.report.directory", tempDir.resolve("abs").toString()), workspace);
        assertThat(absolute.get("citrus.report.directory")).isEqualTo(tempDir.resolve("abs").toString());

        Map<String, String> nested = IsolatedPartitionRunner.partitionOverrides(
                Map.of("citrus.message.trace.directory", "logs/../traces"), workspace);
        assertThat(nested.get("citrus.message.trace.directory")).isEqualTo(workspace.resolve("traces").toString());
    }

    @Test
    void shouldFailFastOnInvalidWorkerSetting() {
        String previous = System.getProperty(DistributionSettings.WORKER_CWD_CHECK_PROPERTY);
        System.setProperty(DistributionSettings.WORKER_CWD_CHECK_PROPERTY, "sometimes");
        try {
            org.assertj.core.api.Assertions.assertThatThrownBy(DistributionWorkerConfig::builder)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(DistributionSettings.WORKER_CWD_CHECK_PROPERTY)
                    .hasMessageContaining("sometimes");
        } finally {
            if (previous == null) {
                System.clearProperty(DistributionSettings.WORKER_CWD_CHECK_PROPERTY);
            } else {
                System.setProperty(DistributionSettings.WORKER_CWD_CHECK_PROPERTY, previous);
            }
        }
    }

    @Test
    void shouldReportMissingTestClassAsFailedExecution() {
        PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1,
                List.of(new WorkUnitSpec("missing", "org.example.DoesNotExist", null, null, Map.of(), 0L)),
                getClass().getClassLoader(), CancellationToken.create(), e -> { });
        assertThat(summary.success()).isFalse();
        assertThat(summary.errorMessage()).contains("org.example.DoesNotExist");
    }

    @Test
    void shouldStillRunThePartitionsOtherUnitsWhenOneTestClassIsMissing() {
        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
        PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1,
                List.of(new WorkUnitSpec("missing", "org.example.DoesNotExist", null, null, Map.of(), 0L),
                        unit(OverloadedFixture.class, "named()")),
                getClass().getClassLoader(), CancellationToken.create(), events::add);

        assertThat(finished(events)).singleElement().satisfies(e -> {
            assertThat(e.testName()).isEqualTo("named()");
            assertThat(e.status()).isEqualTo(TestOutcomeStatus.PASSED);
        });
        assertThat(summary.success()).isFalse();
        assertThat(summary.errorMessage()).isEqualTo("Test class not found: org.example.DoesNotExist");
    }

    @Test
    void shouldRedirectBasedirReportsAndExpandWorkspaceTokenPerPartition() {
        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
        IsolatedPartitionRunner.PartitionExecutionResult result = runner(WorkerCwdCheckMode.FAIL).execute(
                assignment(Map.of(
                        "custom.dir", DistributionSettings.WORKSPACE_TOKEN + "/conf",
                        "citrus.message.trace.directory", "custom/traces"),
                        unit(WorkspaceFixture.class, "checkRedirectedProperties")),
                CancellationToken.create(), events::add);

        assertThat(result.success()).as(result.errorMessage()).isTrue();
        assertThat(result.outputArtifacts()).isEmpty();
        assertThat(finished(events)).singleElement()
                .satisfies(e -> assertThat(e.status()).as(e.errorMessage()).isEqualTo(TestOutcomeStatus.PASSED));
        assertThat(System.getProperty("basedir")).as("overrides never leak into the worker JVM")
                .satisfiesAnyOf(b -> assertThat(b).isNull(), b -> assertThat(b).doesNotContain("cdist-"));
        assertThat(System.getProperty("citrus.report.directory")).isNull();
        assertThat(System.getProperty("custom.dir")).isNull();
    }

    @Test
    void shouldDetectUndeclaredWorkerCwdWriteWithoutDeletingExistingOrCreatedFiles() throws Exception {
        Path cwd = Path.of("").toAbsolutePath();
        Path existing = Files.writeString(cwd.resolve("cdist-existing-" + UUID.randomUUID() + ".txt"), "pre-existing");
        String probeName = "cdist-probe-" + UUID.randomUUID() + ".txt";
        Path probe = cwd.resolve(probeName);
        try {
            IsolatedPartitionRunner.PartitionExecutionResult failed = runner(WorkerCwdCheckMode.FAIL).execute(
                    assignment(Map.of(PROBE_FILE_PROPERTY, probeName), unit(WorkspaceFixture.class, "writeProbeIntoWorkingDirectory")),
                    CancellationToken.create(), e -> { });

            assertThat(failed.success()).isFalse();
            assertThat(failed.errorMessage())
                    .contains(probeName)
                    .contains("citrus.test.distribution.fork=true")
                    .contains("citrus.test.distribution.outputs");
            assertThat(probe).as("detected file is never deleted").hasContent("written by a test");
            assertThat(existing).as("pre-existing file is never touched").hasContent("pre-existing");

            Files.delete(probe);
            IsolatedPartitionRunner.PartitionExecutionResult warned = runner(WorkerCwdCheckMode.WARN).execute(
                    assignment(Map.of(PROBE_FILE_PROPERTY, probeName), unit(WorkspaceFixture.class, "writeProbeIntoWorkingDirectory")),
                    CancellationToken.create(), e -> { });
            assertThat(warned.success()).isTrue();
            assertThat(probe).exists();

            Files.delete(probe);
            IsolatedPartitionRunner.PartitionExecutionResult unchecked = runner(WorkerCwdCheckMode.OFF).execute(
                    assignment(Map.of(PROBE_FILE_PROPERTY, probeName), unit(WorkspaceFixture.class, "writeProbeIntoWorkingDirectory")),
                    CancellationToken.create(), e -> { });
            assertThat(unchecked.success()).isTrue();

            Files.delete(probe);
            IsolatedPartitionRunner.PartitionExecutionResult clean = runner(WorkerCwdCheckMode.FAIL).execute(
                    assignment(Map.of(), unit(WorkspaceFixture.class, "writeProbeIntoWorkingDirectory")),
                    CancellationToken.create(), e -> { });
            assertThat(clean.success()).as(clean.errorMessage()).isTrue();
        } finally {
            Files.deleteIfExists(probe);
            Files.deleteIfExists(existing);
        }
    }

    @Test
    void shouldIgnoreChangesToPreExistingTopLevelFilesSuchAsTheWorkersOwnLog() throws Exception {
        Path root = Files.createDirectories(tempDir.resolve("worker-cwd"));
        Path log = Files.writeString(root.resolve("worker.log"), "start\n");
        Files.createDirectories(root.resolve("target"));
        IsolatedPartitionRunner.WorkingDirectorySnapshot before = IsolatedPartitionRunner.WorkingDirectorySnapshot.take(root, null);

        Files.writeString(log, "more log lines\n", java.nio.file.StandardOpenOption.APPEND);
        log.toFile().setLastModified(System.currentTimeMillis() + 5_000);
        Files.writeString(root.resolve("new-file.txt"), "x");
        Files.writeString(root.resolve("target/written.txt"), "x");

        assertThat(before.changedEntries(IsolatedPartitionRunner.WorkingDirectorySnapshot.take(root, null)))
                .containsExactly("new-file.txt", "target" + java.io.File.separator + "written.txt");
    }

    @Test
    void shouldStillFlagWritesIntoPreExistingTopLevelDirectories() throws Exception {
        Path root = Files.createDirectories(tempDir.resolve("worker-cwd-dirs"));
        Path scratch = Files.createDirectories(root.resolve(".citrus-jbang"));
        scratch.toFile().setLastModified(System.currentTimeMillis() - 60_000);
        IsolatedPartitionRunner.WorkingDirectorySnapshot before = IsolatedPartitionRunner.WorkingDirectorySnapshot.take(root, null);

        Files.writeString(scratch.resolve("run.log"), "x");

        assertThat(before.changedEntries(IsolatedPartitionRunner.WorkingDirectorySnapshot.take(root, null)))
                .containsExactly(".citrus-jbang");
    }

    @Test
    void shouldExposeForkAndCwdSettingsAndAdvertiseWorkspaceCapability() {
        DistributionWorkerConfig defaults = DistributionWorkerConfig.builder().cacheDir(tempDir).build();
        assertThat(defaults.forkMode()).isEqualTo(WorkerForkMode.ALWAYS);
        assertThat(defaults.cwdCheckMode()).isEqualTo(WorkerCwdCheckMode.FAIL);
        assertThat(defaults.forkJvmArgs()).containsExactly("-Xmx768m", "-XX:ActiveProcessorCount=2");
        assertThat(defaults.capabilities()).containsEntry(DistributionSettings.WORKSPACE_CAPABILITY_KEY, "true");

        DistributionWorkerConfig custom = DistributionWorkerConfig.builder()
                .cacheDir(tempDir)
                .forkMode(WorkerForkMode.AUTO)
                .cwdCheckMode(WorkerCwdCheckMode.WARN)
                .forkJvmArgs(List.of("-Xmx1g"))
                .addCapabilitiesCsv(DistributionSettings.WORKSPACE_CAPABILITY_KEY + "=false")
                .build();
        assertThat(custom.forkMode()).isEqualTo(WorkerForkMode.AUTO);
        assertThat(custom.cwdCheckMode()).isEqualTo(WorkerCwdCheckMode.WARN);
        assertThat(custom.forkJvmArgs()).containsExactly("-Xmx1g");
        assertThat(custom.capabilities()).as("explicit opt-out is kept").containsEntry(DistributionSettings.WORKSPACE_CAPABILITY_KEY, "false");
    }

    @CitrusSupport
    public static class CitrusEndpointsFixture implements TestActionSupport {

        @CitrusEndpoint(name = "ordersHttp")
        @DirectEndpointConfig(queueName = "orders.queue")
        @BindToRegistry(name = "ordersHttp")
        private Endpoint ordersEndpoint;

        @CitrusEndpoint(name = "auditKafka")
        @DirectEndpointConfig(queueName = "audit.queue")
        @BindToRegistry(name = "auditKafka")
        private Endpoint auditEndpoint;

        @CitrusEndpoint(name = "billingSoap")
        @DirectEndpointConfig(queueName = "billing.queue")
        @BindToRegistry(name = "billingSoap")
        private Endpoint billingEndpoint;

        @BindToRegistry(name = "orders.queue")
        public MessageQueue ordersQueue() {
            return new DefaultMessageQueue("orders.queue");
        }

        @BindToRegistry(name = "audit.queue")
        public MessageQueue auditQueue() {
            return new DefaultMessageQueue("audit.queue");
        }

        @BindToRegistry(name = "billing.queue")
        public MessageQueue billingQueue() {
            return new DefaultMessageQueue("billing.queue");
        }

        @Test
        @CitrusTest
        void shouldProcessOrder(@CitrusResource TestActionRunner runner) {
            runner.run(send(ordersEndpoint).message().body("order-1"));
            runner.run(receive(ordersEndpoint).message().body("order-1"));
            runner.run(sequential().actions(
                    send("auditKafka").message().body("audit-1"),
                    receive("auditKafka").message().body("audit-1")
            ));
        }

        @Test
        @CitrusTest
        void shouldChargeBilling(@CitrusResource TestActionRunner runner) {
            runner.run(send(billingEndpoint).message().body("invoice-1"));
            runner.run(receive(billingEndpoint).message().body("invoice-1"));
        }

        @Test
        @CitrusTest
        void shouldNotCaptureUnexecutedTrailingActionInFailedContainer(@CitrusResource TestActionRunner runner) {
            runner.run(sequential().actions(
                    send(ordersEndpoint).message().body("before-boom"),
                    fail("boom"),
                    send("unexecutedQueue").message().body("never-reached")
            ));
        }
    }

    @Test
    void shouldCaptureCitrusEndpointsPerTestFromDirectAndContainerActions() {
        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
        PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits(
                "p-citrus",
                "s-citrus",
                1,
                List.of(
                        unit(CitrusEndpointsFixture.class, "shouldProcessOrder"),
                        unit(CitrusEndpointsFixture.class, "shouldChargeBilling"),
                        unit(CitrusEndpointsFixture.class, "shouldNotCaptureUnexecutedTrailingActionInFailedContainer"),
                        unit(OverloadedFixture.class, "named()")
                ),
                getClass().getClassLoader(),
                CancellationToken.create(),
                events::add
        );

        assertThat(summary.success()).as(summary.errorMessage()).isTrue();
        List<WorkerTestEventPayload> completed = finished(events);
        assertThat(completed).hasSize(4);
        assertThat(completed.get(0).testName()).startsWith("shouldProcessOrder");
        assertThat(completed.get(0).endpoints()).containsExactly("ordersHttp", "auditKafka");
        assertThat(completed.get(1).testName()).startsWith("shouldChargeBilling");
        assertThat(completed.get(1).endpoints()).containsExactly("billingSoap");
        assertThat(completed.get(2).testName()).startsWith("shouldNotCaptureUnexecutedTrailingActionInFailedContainer");
        assertThat(completed.get(2).status()).isEqualTo(TestOutcomeStatus.FAILED);
        assertThat(completed.get(2).endpoints()).containsExactly("ordersHttp");
        assertThat(completed.get(3).testName()).isEqualTo("named()");
        assertThat(completed.get(3).endpoints()).isEmpty();
    }

    private IsolatedPartitionRunner runner(WorkerCwdCheckMode mode) {
        return new IsolatedPartitionRunner(DistributionWorkerConfig.builder()
                .cacheDir(tempDir.resolve("cache"))
                .cwdCheckMode(mode)
                .build());
    }

    private static PartitionAssignPayload assignment(Map<String, String> systemProperties, WorkUnitSpec unit) {
        return new PartitionAssignPayload("p-" + UUID.randomUUID(), "s1", 1, List.of(), systemProperties, List.of(unit));
    }

    @Test
    void shouldRunThePartitionsClassesInOneJUnitRunSoTheirParallelConfigurationApplies() {
        BarrierFixtureA.BARRIER.reset();
        Map<String, String> parallel = Map.of(
                "junit.jupiter.execution.parallel.enabled", "true",
                "junit.jupiter.execution.parallel.mode.default", "same_thread",
                "junit.jupiter.execution.parallel.mode.classes.default", "concurrent");
        parallel.forEach(System::setProperty);
        try {
            List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
            PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1,
                    List.of(classUnit(BarrierFixtureA.class), classUnit(BarrierFixtureB.class)),
                    getClass().getClassLoader(), CancellationToken.create(), events::add);

            assertThat(summary.success()).isTrue();
            assertThat(finished(events)).as("both classes waited for each other at the barrier")
                    .extracting(WorkerTestEventPayload::workUnitKey, WorkerTestEventPayload::status)
                    .containsExactlyInAnyOrder(
                            org.assertj.core.groups.Tuple.tuple(BarrierFixtureA.class.getName(), TestOutcomeStatus.PASSED),
                            org.assertj.core.groups.Tuple.tuple(BarrierFixtureB.class.getName(), TestOutcomeStatus.PASSED));
        } finally {
            parallel.keySet().forEach(System::clearProperty);
        }
    }

    @Test
    void shouldHonourTheModulesLauncherInterceptorsLikeSurefireDoes() {
        // e.g. quarkus-junit5-config enables interceptors in its junit-platform.properties to load @QuarkusTest classes
        System.setProperty("junit.platform.launcher.interceptors.enabled", "true");
        CountingLauncherInterceptor.INVOCATIONS.set(0);
        try {
            List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
            PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1, List.of(classUnit(BarrierFixtureB.class)),
                    getClass().getClassLoader(), CancellationToken.create(), events::add);

            assertThat(CountingLauncherInterceptor.INVOCATIONS.get()).as("module's launcher interceptor ran").isPositive();
        } finally {
            System.clearProperty("junit.platform.launcher.interceptors.enabled");
        }
    }

    @Test
    void shouldSelectTestsByNameSoFrameworkInterceptorsCanLoadTheClasses() {
        // Quarkus' launcher interceptor loads @QuarkusTest classes through its own class loader; a selector that
        // already holds a Class loaded by the partition loader bypasses it.
        org.junit.platform.engine.DiscoverySelector classSelector =
                PartitionJUnitLauncher.selectorFor(classUnit(BarrierFixtureB.class));
        org.junit.platform.engine.DiscoverySelector methodSelector =
                PartitionJUnitLauncher.selectorFor(unit(OverloadedFixture.class, "named()"));

        assertThat(classSelector).isInstanceOfSatisfying(org.junit.platform.engine.discovery.ClassSelector.class,
                selector -> {
                    assertThat(selector.getClassName()).isEqualTo(BarrierFixtureB.class.getName());
                    assertThat(selector.getClassLoader()).as("resolved lazily, through the context loader").isNull();
                });
        assertThat(methodSelector).isInstanceOfSatisfying(org.junit.platform.engine.discovery.MethodSelector.class,
                selector -> {
                    assertThat(selector.getClassName()).isEqualTo(OverloadedFixture.class.getName());
                    assertThat(selector.getMethodName()).isEqualTo("named");
                    assertThat(selector.getClassLoader()).isNull();
                });
    }

    @Test
    void shouldNotSwitchOffTheModulesLauncherInterceptorsInForkedPartitions() {
        assertThat(ForkedPartitionRunner.CHILD_SYSTEM_PROPERTIES)
                .noneMatch(property -> property.startsWith("-Djunit.platform.launcher.interceptors.enabled"));
    }

    @Test
    void shouldRunAnInheritedNestedClassThroughTheSubclassItBelongsTo() {
        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
        WorkUnitSpec viaFirst = new WorkUnitSpec(FirstSub.class.getName() + "/" + Base.Inner.class.getName(),
                Base.Inner.class.getName(), null, null, Map.of(), 0L, List.of(FirstSub.class.getName()));
        WorkUnitSpec viaSecond = new WorkUnitSpec(SecondSub.class.getName() + "/" + Base.Inner.class.getName(),
                Base.Inner.class.getName(), null, null, Map.of(), 0L, List.of(SecondSub.class.getName()));

        PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1,
                List.of(viaFirst, viaSecond), getClass().getClassLoader(), CancellationToken.create(), events::add);

        assertThat(summary.success()).as(summary.errorMessage()).isTrue();
        assertThat(finished(events)).extracting(WorkerTestEventPayload::workUnitKey, WorkerTestEventPayload::uniqueId)
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple(viaFirst.key(), "[engine:junit-jupiter]/[class:"
                                + FirstSub.class.getName() + "]/[nested-class:Inner]/[method:inherited()]"),
                        org.assertj.core.groups.Tuple.tuple(viaSecond.key(), "[engine:junit-jupiter]/[class:"
                                + SecondSub.class.getName() + "]/[nested-class:Inner]/[method:inherited()]"));
    }

    @Test
    void shouldAccountForUnitsOfADisabledClassThatWasSplit() {
        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
        WorkUnitSpec method = unit(DisabledOuter.class, "outer");
        WorkUnitSpec nested = new WorkUnitSpec(DisabledOuter.class.getName() + "/" + DisabledOuter.Inner.class.getName(),
                DisabledOuter.Inner.class.getName(), null, null, Map.of(), 0L, List.of(DisabledOuter.class.getName()));

        PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits("p1", "s1", 1,
                List.of(method, nested), getClass().getClassLoader(), CancellationToken.create(), events::add);

        assertThat(summary.success()).as("a disabled class is reported as skipped, not as missing tests: %s",
                summary.errorMessage()).isTrue();
        assertThat(events).anyMatch(e -> e.status() == TestOutcomeStatus.SKIPPED);
    }

    public abstract static class Base {
        @org.junit.jupiter.api.Nested
        class Inner {
            @Test
            void inherited() {
            }
        }
    }

    public static class FirstSub extends Base {
    }

    public static class SecondSub extends Base {
    }

    @org.junit.jupiter.api.Disabled("not today")
    public static class DisabledOuter {
        @Test
        void outer() {
        }

        @org.junit.jupiter.api.Nested
        class Inner {
            @Test
            void inner() {
            }
        }
    }

    private static WorkUnitSpec classUnit(Class<?> fixture) {
        return new WorkUnitSpec(fixture.getName(), fixture.getName(), null, null, Map.of(), 0L);
    }

    public static class BarrierFixtureA {
        static final java.util.concurrent.CyclicBarrier BARRIER = new java.util.concurrent.CyclicBarrier(2);

        @Test
        void meet() throws Exception {
            BARRIER.await(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    public static class BarrierFixtureB {
        @Test
        void meet() throws Exception {
            BarrierFixtureA.BARRIER.await(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private static WorkUnitSpec unit(Class<?> fixture, String methodName) {
        return new WorkUnitSpec(fixture.getName() + "#" + methodName, fixture.getName(), methodName, null, Map.of(), 0L);
    }

    private static List<WorkerTestEventPayload> finished(List<WorkerTestEventPayload> events) {
        return events.stream().filter(e -> e.status() != TestOutcomeStatus.STARTED).toList();
    }
}
