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

package org.citrusframework.distribution.coordinator;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.InputPreparation;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CoordinatorEvent;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionCompletedPayload;
import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.ReportLocalTestsRequest;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SessionSummary;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.TestExecutionRecord;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerCacheStats;
import org.citrusframework.distribution.model.WorkerHeartbeatPayload;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerState;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkerWarningCode;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.citrusframework.distribution.store.JdbcDistributionStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SessionCoordinatorTest {

    private static final long LEASE_MS = 50L;
    private static final Map<String, String> WORKSPACE_CAPS = Map.of(DistributionSettings.WORKSPACE_CAPABILITY_KEY, "true");

    @TempDir
    Path tempDir;

    private final AtomicLong fixtureClock = new AtomicLong(System.currentTimeMillis());
    private JdbcDistributionStore store;
    private CoordinatorEventStreamHub hub;
    private SessionCoordinator coordinator;

    @BeforeEach
    void setUp() {
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .cacheDir(tempDir.resolve("cache"))
                .clock(fixtureClock::get)
                .leaseTimeoutMs(LEASE_MS)
                .build();
        store = new JdbcDistributionStore("jdbc:hsqldb:mem:coordinator_" + System.nanoTime(), "SA", "");
        store.initialize();
        hub = new CoordinatorEventStreamHub(store);
        coordinator = new SessionCoordinator(config, store, hub);
    }

    @Test
    void observedPeakCountsDistinctOverlappingUnitsInOneAttempt() {
        String session = newSession(1);
        recordTestEvent(session, "A", "first", "STARTED", 1);
        recordTestEvent(session, "A", "second", "STARTED", 1);
        recordTestEvent(session, "A", "first", "PASSED", 1);
        recordTestEvent(session, "B", "third", "STARTED", 1);
        recordTestEvent(session, "C", "fourth", "STARTED", 2);
        recordTestEvent(session, "A", "second", "PASSED", 1);
        recordTestEvent(session, "D", "fifth", "STARTED", 1);
        assertThat(coordinator.observedWorkUnitPeak(session, "part-test", 1)).isEqualTo(2);
        assertThat(coordinator.observedWorkUnitPeak(session, "part-test", 2)).isEqualTo(1);
        assertThat(coordinator.observedWorkUnitPeak(session, "part-test", 3)).isZero();
    }

    private void recordTestEvent(String session, String unit, String test, String status, int attempt) {
        store.appendEvent(new CoordinatorEvent(0, "test.event", session, "part-test", "worker", 0,
                Map.of("attempt", attempt, "workUnitKey", unit, "uniqueId", test, "status", status)),
                null, null, List.of());
    }

    @Test
    void shouldPackFastUnitsAdaptivelyWhenSessionMaxPartitionSizeIsDefaultZero() {
        assertFastUnitsScheduled(0, List.of(), 12, 1);
    }

    @Test
    void shouldHonorExplicitPositiveSessionMaxPartitionSizeWithoutAdaptivePacking() {
        assertFastUnitsScheduled(2, List.of(), 6, 3);
    }

    @Test
    void shouldHonorExplicitPartitionCapAfterRestart() {
        assertPartitionSizingAfterRestart(2, 3, 2);
    }

    @Test
    void shouldRetainAdaptivePartitionSizingAfterRestart() {
        assertPartitionSizingAfterRestart(0, 1, 6);
    }

    private void assertPartitionSizingAfterRestart(int requestedSize, int partitionCount, int largestPartition) {
        String jdbcUrl = "jdbc:hsqldb:file:" + tempDir.resolve("restart/session-store") + ";shutdown=true";
        String sessionId;
        try (JdbcDistributionStore before = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            before.initialize();
            SessionCoordinator running = new SessionCoordinator(
                    DistributionCoordinatorConfig.builder().defaultMaxPartitionSize(10).build(),
                    before, new CoordinatorEventStreamHub(before));
            sessionId = running.createSession(
                    new CreateSessionRequest("citrus", "main", "abc", List.of(), requestedSize, 1)).sessionId();
        }

        try (JdbcDistributionStore after = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            after.initialize();
            SessionCoordinator restarted = new SessionCoordinator(
                    DistributionCoordinatorConfig.builder().defaultMaxPartitionSize(4).minPartitionDurationMs(5_000L).build(),
                    after, new CoordinatorEventStreamHub(after));
            List<WorkUnitSpec> units = IntStream.range(0, 6).mapToObj(i -> unit("pkg.RestartTest" + i)).toList();
            restarted.submitWork(sessionId, new SubmitWorkRequest("sub-after-restart", "module", List.of(), Map.of(),
                    units, 0, 0));

            assertThat(after.listPartitions(sessionId)).hasSize(partitionCount)
                    .allSatisfy(partition -> assertThat(partition.workUnits()).hasSize(largestPartition));
        }
    }

    @Test
    void shouldRestoreAssignmentsAndDeduplicateReplayedWorkerResultsAfterRestart() {
        String jdbcUrl = "jdbc:hsqldb:file:" + tempDir.resolve("recovery/store") + ";shutdown=true";
        var config = DistributionCoordinatorConfig.builder().defaultMaxPartitionSize(1).build();
        String sessionId;
        PartitionAssignPayload interrupted;
        SubmitWorkRequest request = new SubmitWorkRequest("stable-submission", "module", List.of(),
                Map.of("test.setting", "durable"), List.of(unit("pkg.First"), unit("pkg.Second")), 0, 0);
        try (var running = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            running.initialize();
            coordinator = new SessionCoordinator(config, running, new CoordinatorEventStreamHub(running));
            sessionId = coordinator.createSession(new CreateSessionRequest("citrus", "main", "abc", List.of(), 1, 2))
                    .sessionId();
            coordinator.submitWork(sessionId, request);
            Worker worker = register("old-connection", "worker-a", Map.of());
            interrupted = worker.lastAssignment();
            assertThat(running.listPartitions(sessionId)).extracting(PartitionRecord::state)
                    .containsExactlyInAnyOrder(PartitionState.ASSIGNED, PartitionState.PENDING);
        }
        try (var persisted = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            persisted.initialize();
            coordinator = new SessionCoordinator(config, persisted, new CoordinatorEventStreamHub(persisted));
            coordinator.restorePendingWork();
            assertThat(coordinator.submitWork(sessionId, request).partitionIds()).hasSize(2);
            Worker recovered = register("new-connection", "worker-a", Map.of());
            var test = new WorkerTestEventPayload(interrupted.partitionId(), sessionId, 1, "key", "pkg.Test", "one",
                    TestOutcomeStatus.PASSED, 10L, null, null, List.of());
            Map<String, Object> delivery = new java.util.LinkedHashMap<>(DistributionJson.convert(test, Map.class));
            delivery.put(WorkerWsMessage.DELIVERY_ID, "0000000000000000001-00000000-0000-0000-0000-000000000001");
            recovered.send(WorkerWsMessage.TEST_EVENT, delivery);
            recovered.send(WorkerWsMessage.TEST_EVENT, delivery);
            assertThat(recovered.messagesOfType(WorkerWsMessage.RESULT_ACK)).hasSize(2);
            assertThat(persisted.listTestExecutions(sessionId)).hasSize(1);

            Worker another = register("another-connection", "worker-b", Map.of());
            var retried = List.of(recovered.lastAssignment(), another.lastAssignment()).stream()
                    .filter(a -> a.partitionId().equals(interrupted.partitionId())).findFirst().orElseThrow();
            assertThat(retried.attempt()).isEqualTo(2);
            assertThat(retried.systemProperties()).containsEntry("test.setting", "durable");
            for (Worker worker : List.of(recovered, another)) {
                var assignment = worker.lastAssignment();
                worker.testEvent(assignment, assignment.attempt(),
                        assignment.partitionId().equals(retried.partitionId()) ? "one" : "two");
                worker.complete(assignment, assignment.attempt(), List.of());
            }
            assertThat(coordinator.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L)).passedTests())
                    .isEqualTo(2);
            List<CoordinatorEvent> events = new java.util.ArrayList<>();
            persisted.replayEvents(sessionId, 0, 0, events::add);
            assertThat(events).extracting(CoordinatorEvent::type)
                    .contains("session.created", "work.submitted", "partition.requeued", "session.completed");
            assertThat(events).extracting(CoordinatorEvent::sequence).doesNotHaveDuplicates().isSorted();
        }
    }

    @Test
    void shouldStoreRepeatedLocalJournalDeliveryOnce() {
        String sessionId = coordinator.createSession(new CreateSessionRequest("citrus", "main", "abc", List.of(), 0, 1))
                .sessionId();
        var event = new WorkerTestEventPayload("local", sessionId, 1, "key", "pkg.Test", "test",
                TestOutcomeStatus.PASSED, 7L, null, null, List.of());
        var delivery = new ReportLocalTestsRequest("local-host", List.of(event),
                "0000000000000000001-00000000-0000-0000-0000-000000000002");
        assertThat(coordinator.reportLocalTests(sessionId, delivery)).isEqualTo(1);
        coordinator = new SessionCoordinator(DistributionCoordinatorConfig.builder().build(), store, new CoordinatorEventStreamHub(store));
        assertThat(coordinator.reportLocalTests(sessionId, delivery)).isZero();
        assertThat(store.listTestExecutions(sessionId)).hasSize(1);
        assertThat(coordinator.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L)).passedTests())
                .isEqualTo(1);
    }

    @Test
    void shouldPreservePartitionSizeCapForDeclaredOutputs() {
        assertFastUnitsScheduled(0, List.of("target/docs"), 12, 2);
    }

    @Test
    void shouldCapWavesByDistinctEligibleOnlineWorkersIncludingBusyWorkers() {
        coordinator = new SessionCoordinator(DistributionCoordinatorConfig.builder()
                .defaultMaxPartitionSize(2)
                .minPartitionDurationMs(0L)
                .maxWavesPerWorker(2)
                .build(), store, hub);
        Map<String, String> docker = Map.of("docker", "true");
        Worker busy = register("docker-one", "worker-one", docker);
        String busySession = newSession(0);
        submit(busySession, "sub-busy", List.of(), List.of(), false,
                new WorkUnitSpec("pkg.BusyTest", "pkg.BusyTest", null, null, docker, 1_000L));
        assertThat(busy.assignments()).hasSize(1);

        register("docker-two", "worker-two", docker);
        register("docker-duplicate", "worker-two", docker);
        register("docker-draining", "worker-three", docker).send(WorkerWsMessage.WORKER_DRAINING, Map.of());
        register("no-docker", "worker-four", Map.of());

        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 0, 0)).sessionId();
        WorkUnitSpec[] units = IntStream.range(0, 12).mapToObj(i -> {
            String key = "pkg.DockerTest" + i;
            return new WorkUnitSpec(key, key, null, null, docker, 1_000L);
        }).toArray(WorkUnitSpec[]::new);
        submit(sessionId, "sub-docker", List.of(), List.of(), false, units);

        assertThat(store.listPartitions(sessionId)).hasSize(4)
                .allSatisfy(partition -> assertThat(partition.workUnits()).hasSize(3));
    }

    @Test
    void shouldResolveAdaptiveSettingsThroughCoordinatorConfigBuilder() {
        String property = DistributionCoordinatorConfig.MIN_PARTITION_DURATION_MS_PROPERTY;
        String previous = System.getProperty(property);
        try {
            System.setProperty(property, "4500");
            assertThat(DistributionCoordinatorConfig.builder().build().minPartitionDurationMs()).isEqualTo(4_500L);
            assertThat(DistributionCoordinatorConfig.fromSettings().minPartitionDurationMs()).isEqualTo(4_500L);
            DistributionCoordinatorConfig disabled = DistributionCoordinatorConfig.builder()
                    .minPartitionDurationMs(-1L).maxWavesPerWorker(-1).build();
            assertThat(disabled.minPartitionDurationMs()).isZero();
            assertThat(disabled.maxWavesPerWorker()).isZero();
        } finally {
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    private void assertFastUnitsScheduled(int requestedSize, List<String> outputs, int unitCount, int expectedPartitions) {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), requestedSize, 1)).sessionId();
        register("one", "worker-one", WORKSPACE_CAPS);
        register("two", "worker-two", WORKSPACE_CAPS);

        WorkUnitSpec[] units = IntStream.range(0, unitCount).mapToObj(i -> {
            WorkUnitSpec unit = unit("pkg.FastTest" + i);
            store.saveTestExecution(new TestExecutionRecord("historical-" + i, "history", "part", "worker",
                    unit.key(), unit.className(), "test", TestOutcomeStatus.PASSED, 200L, 1,
                    false, null, null, List.of(), 1L));
            return unit;
        }).toArray(WorkUnitSpec[]::new);
        submit(sessionId, "sub-fast", List.of(), outputs, false, units);

        assertThat(store.listPartitions(sessionId)).hasSize(expectedPartitions);
    }

    @Test
    void shouldGateWorkspacePartitionsOnCapabilityAndForwardWorkspaceFields() {
        String sessionId = newSession(1);
        Worker legacy = register("legacy", "worker-legacy", Map.of());

        ArtifactRef blob = new ArtifactRef("resources.zip", "d".repeat(64), ArtifactKind.DIRECTORY_ZIP, 10L);
        List<WorkspaceFileSet> inputs = List.of(new WorkspaceFileSet("src/test/resources", blob));
        submit(sessionId, "sub-ws", inputs, List.of("target/docs"), false, unit("ws.A"));
        assertThat(legacy.assignments()).as("pre-workspace worker must not receive workspace partitions").isEmpty();
        assertThat(store.listPartitions(sessionId)).singleElement()
                .satisfies(p -> assertThat(p.requiredCapabilities()).containsEntry(DistributionSettings.WORKSPACE_CAPABILITY_KEY, "true"));

        Worker modern = register("modern", "worker-modern", WORKSPACE_CAPS);
        PartitionAssignPayload assign = modern.lastAssignment();
        assertThat(assign.attempt()).isEqualTo(1);
        assertThat(assign.inputArtifacts()).isEqualTo(inputs);
        assertThat(assign.outputPaths()).containsExactly("target/docs");
        assertThat(assign.forkWorkspace()).isFalse();
        assertThat(assign.workingDirectory()).isEqualTo("target");

        submit(sessionId, "sub-fork-only", List.of(), List.of(), true, unit("fork.B"));
        assertThat(legacy.assignments()).as("fork=true alone also requires a workspace worker").isEmpty();

        submit(sessionId, "sub-plain", List.of(), List.of(), false, unit("plain.C"));
        assertThat(legacy.assignments()).singleElement()
                .satisfies(a -> assertThat(a.workUnits()).extracting(WorkUnitSpec::key).containsExactly("plain.C"));
    }

    @Test
    void shouldAcceptCompletionWhilePartitionIsRunningAndPublishOutputArtifacts() {
        String sessionId = newSession(1);
        Worker worker = register("conn-1", "worker-1", WORKSPACE_CAPS);
        submit(sessionId, "sub-1", List.of(), List.of("target/docs"), false, unit("a.A"));
        PartitionAssignPayload assign = worker.lastAssignment();

        worker.send(WorkerWsMessage.PARTITION_STARTED, Map.of("partitionId", assign.partitionId(), "sessionId", sessionId));
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.RUNNING);

        WorkspaceFileSet docs = new WorkspaceFileSet("target/docs",
                new ArtifactRef("docs.zip", "e".repeat(64), ArtifactKind.DIRECTORY_ZIP, 20L));
        worker.complete(assign, assign.attempt(), List.of(docs));

        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.COMPLETED);
        CoordinatorEvent completed = lastEvent(sessionId, "partition.completed");
        assertThat(completed.data()).containsEntry("attempt", 1).containsKey("outputArtifacts")
                .as("lets the launcher close each class test set once all its units reported")
                .containsEntry("workUnits", List.of("a.A"));
        assertThat(DistributionJson.toJson(completed.data().get("outputArtifacts"))).contains("target/docs", "e".repeat(64));
    }

    @Test
    void shouldCancelExpiredLeaseAndReassignOnlyAfterWorkerConfirmsItStopped() {
        AtomicLong clock = useControlledClock(false, LEASE_MS);
        String sessionId = newSession(2);
        Worker worker = register("conn-1", "worker-1", WORKSPACE_CAPS);
        submit(sessionId, "sub-1", List.of(), List.of("target/docs"), true, unit("a.A"));
        PartitionAssignPayload first = worker.lastAssignment();
        assertThat(first.attempt()).isEqualTo(1);

        clock.addAndGet(LEASE_MS * 2);
        coordinator.sweepExpiredLeases();

        assertThat(worker.messagesOfType(WorkerWsMessage.PARTITION_CANCEL)).singleElement()
                .satisfies(cancel -> assertThat(cancel.payload())
                        .containsEntry("partitionId", first.partitionId())
                        .containsEntry("attempt", 1));
        assertThat(worker.assignments()).as("a worker still running the cancelled attempt gets no new work").hasSize(1);
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.PENDING);

        worker.heartbeat(first.partitionId());
        assertThat(worker.assignments()).as("heartbeat still naming the cancelled partition").hasSize(1);

        WorkspaceFileSet stale = new WorkspaceFileSet("target/docs",
                new ArtifactRef("stale.zip", "1".repeat(64), ArtifactKind.DIRECTORY_ZIP, 1L));
        worker.testEvent(first, 1, "staleTest");
        worker.complete(first, 1, List.of(stale));
        assertThat(store.listTestExecutions(sessionId)).as("late attempt-1 test event is discarded").isEmpty();

        PartitionAssignPayload second = worker.lastAssignment();
        assertThat(second.partitionId()).isEqualTo(first.partitionId());
        assertThat(second.attempt()).as("the late completion confirms the worker stopped; it now gets the retry").isEqualTo(2);
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.ASSIGNED);

        worker.complete(first, 1, List.of(stale));
        assertThat(partition(sessionId).state()).as("a replayed attempt-1 completion is still discarded").isEqualTo(PartitionState.ASSIGNED);

        WorkspaceFileSet fresh = new WorkspaceFileSet("target/docs",
                new ArtifactRef("fresh.zip", "2".repeat(64), ArtifactKind.DIRECTORY_ZIP, 1L));
        worker.testEvent(second, 2, "freshTest");
        worker.complete(second, 2, List.of(fresh));
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.COMPLETED);
        assertThat(store.listTestExecutions(sessionId)).extracting(e -> e.testName()).containsExactly("freshTest");
        String published = DistributionJson.toJson(lastEvent(sessionId, "partition.completed").data());
        assertThat(published).contains("2".repeat(64)).doesNotContain("1".repeat(64));
    }

    @Test
    void shouldKeepFrozenWorkerOutOfDispatchUntilItReportsNoActivePartition() throws Exception {
        String sessionId = newSession(2);
        Worker frozen = register("conn-a", "worker-a", WORKSPACE_CAPS);
        submit(sessionId, "sub-1", List.of(), List.of(), true, unit("a.A"));
        PartitionAssignPayload first = frozen.lastAssignment();
        Worker healthy = register("conn-b", "worker-b", WORKSPACE_CAPS);

        expireLeases(healthy);

        assertThat(frozen.messagesOfType(WorkerWsMessage.PARTITION_CANCEL)).hasSize(1);
        assertThat(healthy.lastAssignment().attempt()).as("the retry goes to the healthy worker").isEqualTo(2);

        submit(sessionId, "sub-2", List.of(), List.of(), true, unit("b.B"));
        assertThat(frozen.assignments()).as("frozen worker absorbs no further partitions").hasSize(1);

        frozen.heartbeat(null);
        assertThat(frozen.assignments()).hasSize(2);
        assertThat(frozen.lastAssignment().workUnits()).extracting(WorkUnitSpec::key).containsExactly("b.B");
        assertThat(first.partitionId()).isNotEqualTo(frozen.lastAssignment().partitionId());
    }

    @Test
    void shouldSkipWorkersSilentForLongerThanTheLease() throws Exception {
        String sessionId = newSession(1);
        Worker silent = register("conn-1", "worker-1", Map.of());
        fixtureClock.addAndGet(LEASE_MS * 2);
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("a.A"));
        assertThat(silent.assignments()).isEmpty();

        silent.heartbeat(null);
        assertThat(silent.assignments()).hasSize(1);
    }

    @Test
    void shouldIgnoreAllMessagesFromStalledConnectionAfterReassignment() throws Exception {
        String sessionId = newSession(2);
        Worker stalled = register("conn-a", "worker-a", WORKSPACE_CAPS);
        submit(sessionId, "sub-1", List.of(), List.of(), true, unit("a.A"));
        Worker healthy = register("conn-b", "worker-b", WORKSPACE_CAPS);
        assertThat(healthy.assignments()).isEmpty();

        expireLeases(healthy);

        assertThat(stalled.messagesOfType(WorkerWsMessage.PARTITION_CANCEL)).hasSize(1);
        PartitionAssignPayload second = healthy.lastAssignment();
        assertThat(second.attempt()).isEqualTo(2);

        // Forged current attempt from the stalled connection: wrong connection, so dropped.
        stalled.send(WorkerWsMessage.PARTITION_STARTED, Map.of("partitionId", second.partitionId(), "sessionId", sessionId,
                "attempt", 2));
        assertThat(partition(sessionId).workerId()).isEqualTo("worker-b");
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.ASSIGNED);
        stalled.testEvent(second, 2, "forged");
        stalled.complete(second, 2, List.of());
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.ASSIGNED);
        assertThat(store.listTestExecutions(sessionId)).isEmpty();

        // A stalled worker's heartbeat naming the partition must not keep the new attempt's lease alive.
        fixtureClock.addAndGet(LEASE_MS * 2);
        stalled.heartbeat(second.partitionId());
        coordinator.sweepExpiredLeases();
        assertThat(healthy.messagesOfType(WorkerWsMessage.PARTITION_CANCEL))
                .as("lease of attempt 2 expired despite the stalled worker's heartbeat")
                .hasSize(1);
    }

    @Test
    void shouldDropMessagesFromSameWorkerIdOnAnotherConnection() {
        String sessionId = newSession(1);
        Worker original = register("conn-1", "worker-1", Map.of());
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("a.A"));
        PartitionAssignPayload assign = original.lastAssignment();

        Worker reconnected = register("conn-2", "worker-1", Map.of());
        reconnected.complete(assign, 1, List.of());
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.ASSIGNED);

        original.complete(assign, 1, List.of());
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.COMPLETED);
    }

    @Test
    void shouldExposeInputPreparationOnlyForTheActiveAttemptAndKeepItOnCompletion() {
        String sessionId = newSession(1);
        Worker worker = register("conn-1", "worker-1", Map.of());
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("pkg.BaseTest"));
        var assign = worker.lastAssignment();
        var prep = new InputPreparation(100L, 290L, 50L);
        worker.send(WorkerWsMessage.PARTITION_INPUTS_READY, Map.of(
                "partitionId", assign.partitionId(), "attempt", 2, "inputPreparation", prep));
        assertThat(partition(sessionId).inputPreparation()).isNull();
        worker.send(WorkerWsMessage.PARTITION_INPUTS_READY, Map.of(
                "partitionId", assign.partitionId(), "attempt", 1, "inputPreparation", prep));
        assertThat(partition(sessionId).inputPreparation()).isEqualTo(prep);
        worker.complete(assign, 1, List.of());
        assertThat(partition(sessionId).inputPreparation()).isEqualTo(prep);
    }

    @Test
    void shouldKeepAssignedLeaseAliveWithHeartbeatsBeforeStartedAndRejectStaleStarted() {
        AtomicLong clock = useControlledClock(false, LEASE_MS);
        String sessionId = newSession(1);
        Worker worker = register("conn-1", "worker-1", Map.of());
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("a.A"));
        PartitionAssignPayload assign = worker.lastAssignment();

        for (int i = 0; i < 3; i++) {
            clock.addAndGet(LEASE_MS / 2);
            worker.heartbeat(assign.partitionId());
        }
        coordinator.sweepExpiredLeases();
        assertThat(worker.messagesOfType(WorkerWsMessage.PARTITION_CANCEL)).isEmpty();
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.ASSIGNED);

        worker.send(WorkerWsMessage.PARTITION_STARTED, Map.of("partitionId", assign.partitionId(), "sessionId", sessionId,
                "attempt", 7));
        assertThat(partition(sessionId).state()).as("STARTED for another attempt is discarded").isEqualTo(PartitionState.ASSIGNED);
        worker.send(WorkerWsMessage.PARTITION_STARTED, Map.of("partitionId", assign.partitionId(), "sessionId", sessionId,
                "attempt", 1));
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.RUNNING);
    }

    @Test
    void shouldAcceptAttemptZeroOnlyFromWorkerThatHeldNoEarlierAttempt() throws Exception {
        String sessionId = newSession(2);
        Worker stalled = register("conn-a", "worker-a", Map.of());
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("a.A"));
        PartitionAssignPayload first = stalled.lastAssignment();
        Worker other = register("conn-b", "worker-b", Map.of());

        expireLeases(other);
        PartitionAssignPayload retry = other.lastAssignment();
        assertThat(retry.attempt()).isEqualTo(2);

        stalled.complete(first, 0, List.of());
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.ASSIGNED);

        other.testEvent(retry, 0, "legacyTest");
        other.complete(retry, 0, List.of());
        assertThat(partition(sessionId).state())
                .as("worker-b never held an earlier attempt, so its attempt 0 can only mean attempt 2")
                .isEqualTo(PartitionState.COMPLETED);
        assertThat(store.listTestExecutions(sessionId)).singleElement()
                .satisfies(e -> assertThat(e.attempt()).as("stored with the coordinator's attempt").isEqualTo(2));
    }

    @Test
    void shouldNotTakeStaleAttemptZeroCompletionThatConfirmsCancellationAsTheRetryResult() throws Exception {
        String sessionId = newSession(2);
        Worker legacy = register("conn-1", "worker-1", Map.of());
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("a.A"));
        PartitionAssignPayload first = legacy.lastAssignment();

        expireLeases();
        legacy.complete(first, 0, List.of());

        PartitionAssignPayload retry = legacy.lastAssignment();
        assertThat(retry.attempt()).as("the stale completion confirmed the cancel, so the retry was dispatched").isEqualTo(2);
        assertThat(partition(sessionId).state()).as("but it was not taken as attempt 2's result").isEqualTo(PartitionState.ASSIGNED);

        legacy.complete(retry, 0, List.of());
        assertThat(partition(sessionId).state())
                .as("attempt 0 from a worker that held attempt 1 stays ambiguous (known limitation for pre-attempt workers)")
                .isEqualTo(PartitionState.ASSIGNED);
        legacy.complete(retry, 2, List.of());
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.COMPLETED);
    }

    @Test
    void shouldDropAttemptZeroFromSameWorkerReconnectedOnNewConnection() {
        String sessionId = newSession(2);
        Worker original = register("conn-1", "worker-1", Map.of());
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("a.A"));
        original.lastAssignment();
        coordinator.handleWorkerDisconnect("conn-1");

        Worker reconnected = register("conn-2", "worker-1", Map.of());
        PartitionAssignPayload retry = reconnected.lastAssignment();
        assertThat(retry.attempt()).isEqualTo(2);

        reconnected.testEvent(retry, 0, "fromOldRun");
        reconnected.complete(retry, 0, List.of());
        assertThat(partition(sessionId).state()).as("old run's attempt-0 completion on the new connection").isEqualTo(PartitionState.ASSIGNED);
        assertThat(store.listTestExecutions(sessionId)).isEmpty();

        reconnected.complete(retry, 2, List.of());
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.COMPLETED);
    }

    @Test
    void shouldSpreadConsecutiveSinglePartitionsAcrossIdleWorkers() {
        String sessionId = newSession(1);
        List<Worker> pool = List.of(
                register("conn-a", "worker-a", WORKSPACE_CAPS),
                register("conn-b", "worker-b", WORKSPACE_CAPS),
                register("conn-c", "worker-c", WORKSPACE_CAPS));

        java.util.Set<String> used = new java.util.LinkedHashSet<>();
        for (int i = 0; i < 3; i++) {
            // Like a reactor of small modules: each submits one partition and waits for it.
            String key = "m" + i + ".T";
            submit(sessionId, "sub-" + i, List.of(), List.of(), false, unit(key));
            Worker assigned = pool.stream().filter(w -> !w.assignments().isEmpty()
                            && w.lastAssignment().workUnits().getFirst().key().equals(key))
                    .findFirst().orElseThrow();
            used.add(assigned.connectionId);
            PartitionAssignPayload assign = assigned.lastAssignment();
            assigned.complete(assign, assign.attempt(), List.of());
        }

        assertThat(used).as("least recently used idle worker gets the next partition").hasSize(3);
    }

    @Test
    void shouldUseIdleWorkersBeforeGrantingOneOverflowSlotPerWorker() {
        var a = register("a", "worker-a", Map.of());
        var spare = new Worker("spare");
        var spareRegistration = new WorkerRegisterPayload("worker-a", "host", null, null, null, Map.of(), null, true);
        coordinator.registerWorker("spare", spareRegistration, spare.received::add);
        var b = register("b", "worker-b", Map.of());
        var session = newSession(1);
        submit(session, "two", List.of(), List.of(), false, unit("A"), unit("B"));
        assertThat(a.assignments()).hasSize(1);
        assertThat(b.assignments()).hasSize(1);
        assertThat(spare.assignments()).isEmpty();

        submit(session, "more", List.of(), List.of(), false, unit("C"), unit("D"));
        assertThat(spare.assignments()).hasSize(1);
        var duplicate = new Worker("spare-reconnect");
        coordinator.registerWorker(duplicate.connectionId, spareRegistration, duplicate.received::add);
        assertThat(duplicate.assignments()).isEmpty();
        assertThat(store.listPartitions(session)).filteredOn(p -> p.state() == PartitionState.PENDING).hasSize(1);
        assertThat(coordinator.listWorkers()).filteredOn(w -> w.workerId().equals("worker-a")).singleElement()
                .satisfies(w -> {
                    assertThat(w.connectedSlots()).isEqualTo(2);
                    assertThat(w.activePartitionIds()).hasSize(2);
                });
        b.complete(b.lastAssignment(), 1, List.of());
        assertThat(b.assignments()).hasSize(2);
    }

    @Test
    void shouldPreferAnotherWorkerForRetry() {
        String sessionId = newSession(2);
        Worker original = register("conn-a1", "worker-a", Map.of());
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("a.A"));
        assertThat(original.lastAssignment().attempt()).isEqualTo(1);

        // Both an idle reconnection of worker-a and worker-b are eligible when the partition is requeued.
        Worker reconnected = register("conn-a2", "worker-a", Map.of());
        Worker other = register("conn-b", "worker-b", Map.of());
        coordinator.handleWorkerDisconnect("conn-a1");

        assertThat(other.assignments()).as("retry prefers a worker that did not run an earlier attempt").hasSize(1);
        assertThat(other.lastAssignment().attempt()).isEqualTo(2);
        assertThat(reconnected.assignments()).isEmpty();
    }

    @Test
    void shouldNotDispatchToSilentWorkerWhoseFirstMessageIsDraining() throws Exception {
        String sessionId = newSession(1);
        Worker worker = register("conn-1", "worker-1", Map.of());
        fixtureClock.addAndGet(LEASE_MS * 2);
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("a.A"));

        worker.send(WorkerWsMessage.WORKER_DRAINING, Map.of());
        assertThat(worker.assignments()).isEmpty();
    }

    @Test
    void shouldGateSubmissionsCarryingTheWorkspaceTokenAwayFromLegacyWorkers() {
        String sessionId = newSession(1);
        Worker legacy = register("legacy", "worker-legacy", Map.of());
        coordinator.submitWork(sessionId, new SubmitWorkRequest("sub-token", "module", List.of(),
                Map.of("project.build.directory", DistributionSettings.WORKSPACE_TOKEN + "/target"),
                List.of(unit("t.T")), 0, 0));
        assertThat(legacy.assignments()).as("an older worker cannot expand the token").isEmpty();

        Worker modern = register("modern", "worker-modern", WORKSPACE_CAPS);
        assertThat(modern.assignments()).hasSize(1);
    }

    @Test
    void shouldRecordTestEventsUnderThePartitionsOwnSession() {
        String sessionId = newSession(1);
        String otherSession = newSession(1);
        Worker worker = register("conn-1", "worker-1", Map.of());
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("a.A"));
        PartitionAssignPayload assign = worker.lastAssignment();

        worker.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(assign.partitionId(),
                otherSession, 1, "key", "pkg.Test", "spoofed", TestOutcomeStatus.PASSED, 5L, null, null, List.of()), Map.class));

        assertThat(store.listTestExecutions(otherSession)).isEmpty();
        assertThat(store.listTestExecutions(sessionId)).extracting(e -> e.testName()).containsExactly("spoofed");
    }

    @Test
    void shouldGateOutputsOnlySubmissionsAwayFromLegacyWorkers() {
        String sessionId = newSession(1);
        Worker legacy = register("legacy", "worker-legacy", Map.of());
        submit(sessionId, "sub-outputs", List.of(), List.of("target/docs"), false, unit("o.O"));
        assertThat(legacy.assignments()).isEmpty();
    }

    @Test
    void shouldScheduleSinglePolicyAndRejectConflictsWithoutSideEffects() {
        String sessionId = newSession(1);
        coordinator.submitWork(sessionId, new SubmitWorkRequest("sub-single", "m", List.of(), Map.of(),
                List.of(unit("a.A"), unit("b.B"), unit("c.C")), 0, 0, List.of(), List.of(), false, PartitionPolicy.SINGLE));
        assertThat(store.listPartitions(sessionId)).singleElement()
                .satisfies(p -> assertThat(p.workUnits()).hasSize(3));

        SubmitWorkRequest conflicting = new SubmitWorkRequest("sub-conflict", "m", List.of(), Map.of(),
                List.of(new WorkUnitSpec("x", "pkg.X", null, null, Map.of("os", "linux"), 0L),
                        new WorkUnitSpec("y", "pkg.Y", null, null, Map.of("os", "windows"), 0L)),
                3, 4, List.of(), List.of(), false, PartitionPolicy.SINGLE);
        assertThatThrownBy(() -> coordinator.submitWork(sessionId, conflicting))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("os");
        assertThat(store.listPartitions(sessionId)).hasSize(1);
        assertThat(store.findWorkSubmission(sessionId, "sub-conflict")).isEmpty();
    }

    @Test
    void shouldKeepFailureReasonReportedWithFailedBuildCompletion() {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).sessionId();

        BuildSessionRecord completed = coordinator.completeSession(sessionId, new CompleteSessionRequest(
                0, 0, 0, 0L, true, "citrus-api: NoClassDefFoundError: org/citrusframework/junit/jupiter/CitrusTestFactory"));

        assertThat(completed.status()).isEqualTo(SessionStatus.FAILED);
        assertThat(completed.failureReason())
                .isEqualTo("citrus-api: NoClassDefFoundError: org/citrusframework/junit/jupiter/CitrusTestFactory");
        assertThat(coordinator.getSessionDetail(sessionId).orElseThrow().session().failureReason())
                .isEqualTo(completed.failureReason());
    }

    @Test
    void shouldCountParameterizedInvocationsSharingADisplayNameSeparately() {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).sessionId();
        String template = "[engine:junit-jupiter]/[class:a.A]/[test-template:p(int)]";
        coordinator.reportLocalTests(sessionId, new org.citrusframework.distribution.model.ReportLocalTestsRequest(
                "local-host", List.of(
                        new WorkerTestEventPayload("local", sessionId, 1, "a.A", "a.A", "value", TestOutcomeStatus.PASSED,
                                1L, null, null, List.of(), template + "/[test-template-invocation:#1]"),
                        new WorkerTestEventPayload("local", sessionId, 1, "a.A", "a.A", "value", TestOutcomeStatus.PASSED,
                                1L, null, null, List.of(), template + "/[test-template-invocation:#2]"))));

        BuildSessionRecord completed = coordinator.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L));

        assertThat(completed.totalTests()).isEqualTo(2);
    }

    @Test
    void shouldKeepCitrusFactorySourcesApartEvenThoughEachReportsTheSameUniqueId() {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).sessionId();
        // Each source runs in its own filtered JUnit run, so every source is dynamic-test #1.
        String sameId = "[engine:junit-jupiter]/[class:a.A]/[test-factory:f()]/[dynamic-test:#1]";
        coordinator.reportLocalTests(sessionId, new org.citrusframework.distribution.model.ReportLocalTestsRequest(
                "local-host", List.of(
                        new WorkerTestEventPayload("local", sessionId, 1, "a.A#f[s1]", "a.A", "s1", TestOutcomeStatus.FAILED,
                                1L, "boom", null, List.of(), sameId),
                        new WorkerTestEventPayload("local", sessionId, 1, "a.A#f[s2]", "a.A", "s2", TestOutcomeStatus.PASSED,
                                1L, null, null, List.of(), sameId))));

        BuildSessionRecord completed = coordinator.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L));

        assertThat(completed.totalTests()).isEqualTo(2);
        assertThat(completed.failedTests()).isEqualTo(1);
        assertThat(completed.status()).isEqualTo(SessionStatus.FAILED);
        assertThat(coordinator.getSessionDetail(sessionId).orElseThrow().tests())
                .extracting(TestExecutionRecord::attempt, TestExecutionRecord::flaky).containsOnly(tuple(1, false));
    }

    @Test
    void shouldTreatAnOldRecordWithoutUniqueIdAndItsRetryWithOneAsTheSameTest() {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).sessionId();
        coordinator.reportLocalTests(sessionId, new org.citrusframework.distribution.model.ReportLocalTestsRequest(
                "local-host", List.of(new WorkerTestEventPayload("local", sessionId, 1, "a.A", "a.A", "t()",
                        TestOutcomeStatus.FAILED, 1L, "boom", null, List.of()))));
        coordinator.reportLocalTests(sessionId, new org.citrusframework.distribution.model.ReportLocalTestsRequest(
                "local-host", List.of(new WorkerTestEventPayload("local", sessionId, 1, "a.A", "a.A", "t()",
                        TestOutcomeStatus.FAILED, 1L, "boom", null, List.of(), "[engine:junit-jupiter]/[class:a.A]/[method:t()]"))));
        coordinator.reportLocalTests(sessionId, new org.citrusframework.distribution.model.ReportLocalTestsRequest(
                "local-host", List.of(new WorkerTestEventPayload("local", sessionId, 1, "a.A", "a.A", "t()",
                        TestOutcomeStatus.PASSED, 1L, null, null, List.of(), "[engine:junit-jupiter]/[class:a.A]/[method:t()]"))));

        BuildSessionRecord completed = coordinator.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L));

        assertThat(completed.totalTests()).isEqualTo(1);
        assertThat(completed.passedTests()).isEqualTo(1);
        assertThat(coordinator.getSessionDetail(sessionId).orElseThrow().tests())
                .filteredOn(TestExecutionRecord::flaky).hasSize(1);
    }

    @Test
    void shouldLetOnlyOneInvocationTakeOverAnOldRecordWithoutUniqueId() {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).sessionId();
        String template = "[engine:junit-jupiter]/[class:a.A]/[test-template:p(int)]";
        coordinator.reportLocalTests(sessionId, new org.citrusframework.distribution.model.ReportLocalTestsRequest(
                "local-host", List.of(new WorkerTestEventPayload("local", sessionId, 1, "a.A", "a.A", "value",
                        TestOutcomeStatus.PASSED, 1L, null, null, List.of()))));
        coordinator.reportLocalTests(sessionId, new org.citrusframework.distribution.model.ReportLocalTestsRequest(
                "local-host", List.of(
                        new WorkerTestEventPayload("local", sessionId, 1, "a.A", "a.A", "value", TestOutcomeStatus.PASSED,
                                1L, null, null, List.of(), template + "/[test-template-invocation:#1]"),
                        new WorkerTestEventPayload("local", sessionId, 1, "a.A", "a.A", "value", TestOutcomeStatus.PASSED,
                                1L, null, null, List.of(), template + "/[test-template-invocation:#2]"))));

        BuildSessionRecord completed = coordinator.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L));

        assertThat(completed.totalTests()).as("#1 continues the old record, #2 is a test of its own").isEqualTo(2);
    }

    @Test
    void shouldKeepTheFailedTestCountNextToAReportedReason() {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).sessionId();

        BuildSessionRecord completed = coordinator.completeSession(sessionId, new CompleteSessionRequest(
                0, 2, 0, 0L, true, "Conflicting outputs for target/docs/a.txt"));

        assertThat(completed.failureReason()).isEqualTo("Conflicting outputs for target/docs/a.txt (2 test(s) failed)");
    }

    @Test
    void shouldExplainFailedBuildCompletionWithoutReportedReason() {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).sessionId();

        BuildSessionRecord completed = coordinator.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L, true));

        assertThat(completed.status()).isEqualTo(SessionStatus.FAILED);
        assertThat(completed.failureReason()).isNotBlank();
    }

    @Test
    void shouldNotRecordFailureReasonForPassedSession() {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).sessionId();

        BuildSessionRecord completed = coordinator.completeSession(sessionId, new CompleteSessionRequest(
                0, 0, 0, 0L, false, "ignored"));

        assertThat(completed.status()).isEqualTo(SessionStatus.PASSED);
        assertThat(completed.failureReason()).isNull();
    }

    @Test
    void shouldRecordAbortReasonOnSession() {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).sessionId();

        BuildSessionRecord aborted = coordinator.abortSession(sessionId, "Maven build interrupted");

        assertThat(aborted.status()).isEqualTo(SessionStatus.ABORTED);
        assertThat(aborted.failureReason()).isEqualTo("Maven build interrupted");
    }

    @Test
    void shouldPreserveEvictedAttemptRecordPersistWorkspaceMetadataAndComputeAccurateFlakyAndLiveTotals() {
        String sessionId = coordinator.createSession(
                new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).sessionId();
        Worker worker1 = register("conn-w1", "worker-1", WORKSPACE_CAPS);

        ArtifactRef inputBlob = new ArtifactRef("res.zip", "a".repeat(64), ArtifactKind.DIRECTORY_ZIP, 12L);
        List<WorkspaceFileSet> inputs = List.of(new WorkspaceFileSet("src/test/resources", inputBlob));
        coordinator.submitWork(sessionId, new SubmitWorkRequest(
                "sub-live",
                "citrus-http",
                List.of(),
                Map.of(),
                List.of(unit("pkg.A"), unit("pkg.B")),
                3,
                1,
                inputs,
                List.of("target/docs"),
                true,
                PartitionPolicy.LPT,
                "target"));

        BuildSessionRecord liveAfterSubmit = store.findSession(sessionId).orElseThrow();
        assertThat(liveAfterSubmit.status()).isEqualTo(SessionStatus.RUNNING);
        assertThat(liveAfterSubmit.totalBlobs()).isEqualTo(4);
        assertThat(liveAfterSubmit.cachedBlobs()).isEqualTo(3);
        assertThat(liveAfterSubmit.totalTests()).isEqualTo(2);

        PartitionAssignPayload assign1 = worker1.lastAssignment();
        worker1.send(WorkerWsMessage.PARTITION_STARTED,
                Map.of("partitionId", assign1.partitionId(), "sessionId", sessionId, "attempt", 1));
        worker1.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(
                assign1.partitionId(), sessionId, 1, "pkg.A", "pkg.A", "testStable",
                TestOutcomeStatus.PASSED, 100L, null, null, List.of("httpClient")), Map.class));
        worker1.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(
                assign1.partitionId(), sessionId, 1, "pkg.B", "pkg.B", "testFlaky",
                TestOutcomeStatus.STARTED, 0L, null, null, List.of()), Map.class));

        BuildSessionRecord liveAfterFirstPass = store.findSession(sessionId).orElseThrow();
        assertThat(liveAfterFirstPass.passedTests()).isEqualTo(1);
        assertThat(liveAfterFirstPass.totalTests()).isEqualTo(2);

        coordinator.handleWorkerDisconnect("conn-w1");

        List<PartitionRecord> attemptsDuringRetry = store.listPartitions(sessionId);
        assertThat(attemptsDuringRetry).hasSize(2);
        assertThat(attemptsDuringRetry.get(0).attempt()).isEqualTo(1);
        assertThat(attemptsDuringRetry.get(0).state()).isEqualTo(PartitionState.LOST);
        assertThat(attemptsDuringRetry.get(0).workerId()).isEqualTo("worker-1");
        assertThat(attemptsDuringRetry.get(0).errorMessage()).contains("Worker disconnected");
        assertThat(attemptsDuringRetry.get(1).attempt()).isEqualTo(2);
        assertThat(attemptsDuringRetry.get(1).state()).isEqualTo(PartitionState.PENDING);

        Worker worker2 = register("conn-w2", "worker-2", WORKSPACE_CAPS);
        PartitionAssignPayload assign2 = worker2.lastAssignment();
        assertThat(assign2.attempt()).isEqualTo(2);

        worker2.send(WorkerWsMessage.PARTITION_STARTED,
                Map.of("partitionId", assign2.partitionId(), "sessionId", sessionId, "attempt", 2));
        worker2.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(
                assign2.partitionId(), sessionId, 2, "pkg.A", "pkg.A", "testStable",
                TestOutcomeStatus.PASSED, 110L, null, null, List.of("httpClient")), Map.class));
        worker2.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(
                assign2.partitionId(), sessionId, 2, "pkg.B", "pkg.B", "testFlaky",
                TestOutcomeStatus.PASSED, 190L, null, null, List.of("kafkaEndpoint")), Map.class));

        WorkspaceFileSet producedDoc = new WorkspaceFileSet(
                "target/docs",
                new ArtifactRef("docs.zip", "b".repeat(64), ArtifactKind.DIRECTORY_ZIP, 24L));
        worker2.complete(assign2, 2, List.of(producedDoc));

        PartitionRecord completedAttempt2 = partition(sessionId);
        assertThat(completedAttempt2.attempt()).isEqualTo(2);
        assertThat(completedAttempt2.state()).isEqualTo(PartitionState.COMPLETED);
        assertThat(completedAttempt2.moduleName()).isEqualTo("citrus-http");
        assertThat(completedAttempt2.forkWorkspace()).isTrue();
        assertThat(completedAttempt2.workingDirectory()).isEqualTo("target");
        assertThat(completedAttempt2.inputPaths()).containsExactly("src/test/resources");
        assertThat(completedAttempt2.outputPaths()).containsExactly("target/docs");
        assertThat(completedAttempt2.producedOutputs()).containsExactly("target/docs");

        List<TestExecutionRecord> allExecs = store.listTestExecutions(sessionId);
        TestExecutionRecord stableAttempt2 = allExecs.stream()
                .filter(e -> e.attempt() == 2 && "testStable".equals(e.testName()))
                .findFirst().orElseThrow();
        TestExecutionRecord flakyAttempt2 = allExecs.stream()
                .filter(e -> e.attempt() == 2 && "testFlaky".equals(e.testName()))
                .findFirst().orElseThrow();
        assertThat(stableAttempt2.flaky()).as("stable test passed on attempt 1 too").isFalse();
        assertThat(flakyAttempt2.flaky()).as("flaky test was interrupted when worker-1 disconnected").isTrue();

        BuildSessionRecord completed = coordinator.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L));
        assertThat(completed.status()).isEqualTo(SessionStatus.PASSED);
        assertThat(completed.totalTests()).isEqualTo(2);
        assertThat(completed.passedTests()).isEqualTo(2);
        assertThat(completed.failedTests()).isZero();
        assertThat(completed.serialDurationMs()).isEqualTo(300L);
    }

    @Test
    void shouldPersistPartitionFailureReasonAndFailSessionWhenPartitionCrashesWithZeroTests() {
        String sessionId = newSession(0);
        Worker worker = register("conn-crash", "worker-crash", WORKSPACE_CAPS);
        submit(sessionId, "sub-crash", List.of(), List.of("target/docs"), false, unit("pkg.CrashIT"));
        PartitionAssignPayload assign = worker.lastAssignment();

        worker.send(WorkerWsMessage.PARTITION_COMPLETED, DistributionJson.convert(new PartitionCompletedPayload(
                assign.partitionId(),
                sessionId,
                1,
                false,
                "Undeclared write to shared working directory: dirty.tmp",
                List.of()), Map.class));

        PartitionRecord crashed = partition(sessionId);
        assertThat(crashed.state()).isEqualTo(PartitionState.FAILED);
        assertThat(crashed.errorMessage()).isEqualTo("Undeclared write to shared working directory: dirty.tmp");

        BuildSessionRecord completed = coordinator.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L));
        assertThat(completed.status()).isEqualTo(SessionStatus.FAILED);
    }

    @Test
    void shouldEstimateFullClassFromLocalFallbackWithDifferentPerTestAttempts() {
        String testClass = "pkg.FallbackIT";
        String sessionId = newSession(0);
        Worker worker = register("conn-fallback", "worker-fallback", Map.of());
        submit(sessionId, "sub-fallback", List.of(), List.of(), false, unit(testClass));
        PartitionAssignPayload assignment = worker.lastAssignment();
        worker.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(
                assignment.partitionId(), sessionId, 1, testClass, testClass, "alpha", TestOutcomeStatus.PASSED,
                100L, null, null, List.of(), "alpha-id"), Map.class));
        coordinator.handleWorkerDisconnect("conn-fallback");
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.LOST);

        coordinator.reportLocalTests(sessionId, new ReportLocalTestsRequest("local-host", List.of(
                new WorkerTestEventPayload("local", sessionId, 1, testClass, testClass, "alpha", TestOutcomeStatus.PASSED,
                        300L, null, null, List.of(), "alpha-id"),
                new WorkerTestEventPayload("local", sessionId, 1, testClass, testClass, "beta", TestOutcomeStatus.PASSED,
                        700L, null, null, List.of(), "beta-id"))));
        assertThat(store.listTestExecutions(sessionId)).filteredOn(test -> "local".equals(test.partitionId()))
                .extracting(TestExecutionRecord::testName, TestExecutionRecord::attempt)
                .containsExactlyInAnyOrder(tuple("alpha", 2), tuple("beta", 1));
        assertThat(coordinator.findHistoricalDurations(List.of(testClass), 1)).containsEntry(testClass, 1_000L);
    }

    @Test
    void shouldRemoveOnlyFinishedSessionsAndOfflineWorkers() {
        String session = newSession(1);
        String other = newSession(1);
        Worker worker = register("connection", "worker-1", Map.of());
        submit(session, "submission", List.of(), List.of(), false, unit("pkg.RemoteIT"));
        var assignment = worker.lastAssignment();
        worker.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(
                assignment.partitionId(), session, 1, "pkg.RemoteIT", "pkg.RemoteIT", "test",
                TestOutcomeStatus.PASSED, 5L, null, null, List.of()), Map.class));

        assertThat(coordinator.deleteSession(session)).isEqualTo(SessionCoordinator.Deletion.ACTIVE);
        assertThat(coordinator.deleteWorker("worker-1")).isEqualTo(SessionCoordinator.Deletion.ACTIVE);
        coordinator.abortSession(session, "finished");
        coordinator.handleWorkerDisconnect("connection");
        assertThat(coordinator.deleteWorker("worker-1")).isEqualTo(SessionCoordinator.Deletion.DELETED);
        assertThat(store.listTestExecutions(session)).isNotEmpty();
        assertThat(partition(session).workerId()).isEqualTo("worker-1");
        assertThat(coordinator.deleteSession(session)).isEqualTo(SessionCoordinator.Deletion.DELETED);
        assertThat(coordinator.getSessionDetail(session)).isEmpty();
        assertThat(coordinator.getSessionDetail(other)).isPresent();
        assertThat(store.listPartitions(session)).isEmpty();
        assertThat(store.listTestExecutions(session)).isEmpty();
        assertThat(store.findWorkSubmission(session, "submission")).isEmpty();
        store.replayEvents(session, 0, 0, event -> { throw new AssertionError(event); });
        assertThat(coordinator.deleteSession(session)).isEqualTo(SessionCoordinator.Deletion.MISSING);
        assertThat(coordinator.deleteWorker("worker-1")).isEqualTo(SessionCoordinator.Deletion.MISSING);
    }

    @Test
    void shouldRecordLocalTestExecutionsAndAbortRunningSession() {
        String sessionId = newSession(1);
        Worker worker = register("conn-abort", "worker-abort", Map.of());
        submit(sessionId, "sub-abort", List.of(), List.of(), false, unit("pkg.RemoteIT"));
        PartitionAssignPayload assign = worker.lastAssignment();
        worker.send(WorkerWsMessage.PARTITION_STARTED,
                Map.of("partitionId", assign.partitionId(), "sessionId", sessionId, "attempt", 1));

        int savedLocal = coordinator.reportLocalTests(sessionId, new ReportLocalTestsRequest(
                "local-maven-host",
                List.of(new WorkerTestEventPayload(
                        "local", sessionId, 1, "pkg.LocalOnlyIT#shouldRunLocally",
                        "pkg.LocalOnlyIT", "shouldRunLocally", TestOutcomeStatus.PASSED,
                        75L, null, null, List.of("localHttpServer")))));
        assertThat(savedLocal).isEqualTo(1);
        assertThat(store.findSession(sessionId).orElseThrow().passedTests()).isEqualTo(1);

        BuildSessionRecord aborted = coordinator.abortSession(sessionId, "Cancelled by user");
        assertThat(aborted.status()).isEqualTo(SessionStatus.ABORTED);
        assertThat(worker.messagesOfType(WorkerWsMessage.PARTITION_CANCEL)).hasSize(1);
        assertThat(partition(sessionId).state()).isEqualTo(PartitionState.LOST);
        assertThat(partition(sessionId).errorMessage()).isEqualTo("Cancelled by user");
        assertThat(lastEvent(sessionId, "session.aborted").data()).containsEntry("status", "ABORTED");
    }

    @Test
    void shouldMarkSessionFailedOnBuildExceptionWithoutInflatingFailedTestCount() {
        String sessionWithFailedTest = newSession(0);
        Worker worker = register("conn-bf", "worker-bf", Map.of());
        submit(sessionWithFailedTest, "sub-bf-1", List.of(), List.of(), false, unit("pkg.FailingIT"));
        PartitionAssignPayload assign1 = worker.lastAssignment();
        worker.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(
                assign1.partitionId(), sessionWithFailedTest, 1, "pkg.FailingIT",
                "pkg.FailingIT", "shouldFail", TestOutcomeStatus.FAILED, 90L, "boom", "trace", List.of()), Map.class));
        worker.send(WorkerWsMessage.PARTITION_COMPLETED, DistributionJson.convert(new PartitionCompletedPayload(
                assign1.partitionId(), sessionWithFailedTest, 1, true, null, List.of()), Map.class));

        BuildSessionRecord finished1 = coordinator.completeSession(
                sessionWithFailedTest, new CompleteSessionRequest(0, 0, 0, 0L, true));
        assertThat(finished1.status()).isEqualTo(SessionStatus.FAILED);
        assertThat(finished1.failedTests()).as("remote failure is not double-counted").isEqualTo(1);
        assertThat(finished1.totalTests()).isEqualTo(1);

        String sessionWithCompileError = newSession(0);
        BuildSessionRecord finished2 = coordinator.completeSession(
                sessionWithCompileError, new CompleteSessionRequest(0, 0, 0, 0L, true));
        assertThat(finished2.status()).isEqualTo(SessionStatus.FAILED);
        assertThat(finished2.failedTests()).as("no synthetic test failure invented").isZero();
    }

    @Test
    void shouldStoreWorkerCacheStatsOnRegisterAndKeepThemWhenAHeartbeatReportsNone() {
        WorkerCacheStats initial = new WorkerCacheStats(true, 100L, 2, 300L, 400L, 0L, false);
        WorkerCacheStats refreshed = new WorkerCacheStats(true, 100L, 3, 350L, 50L, 1L, true);
        Worker worker = new Worker("conn-cache");
        coordinator.registerWorker("conn-cache",
                new WorkerRegisterPayload("w-cache", "host", Map.of(), initial), worker.received::add);
        assertThat(storedStats("w-cache")).isEqualTo(initial);

        worker.heartbeat(null, refreshed);
        assertThat(storedStats("w-cache")).isEqualTo(refreshed);

        worker.heartbeat(null);
        assertThat(storedStats("w-cache")).as("heartbeat without stats keeps the previous ones").isEqualTo(refreshed);
        assertThat(coordinator.listWorkers()).singleElement().satisfies(w -> {
            assertThat(w.state()).isEqualTo(WorkerState.IDLE);
            assertThat(w.cacheStats()).isEqualTo(refreshed);
        });

        WorkerCacheStats recovered = new WorkerCacheStats(true, 100L, 3, 350L, 200L, 1L, false);
        worker.heartbeat(null, recovered);
        assertThat(coordinator.listWorkers()).singleElement().satisfies(w -> {
            assertThat(w.state()).isEqualTo(WorkerState.IDLE);
            assertThat(w.cacheStats()).isEqualTo(recovered);
        });

        worker.heartbeat(null, refreshed);
        coordinator.handleWorkerDisconnect("conn-cache");
        assertThat(coordinator.listWorkers()).singleElement().satisfies(w -> {
            assertThat(w.state()).isEqualTo(WorkerState.OFFLINE);
            assertThat(w.cacheStats()).isEqualTo(refreshed);
        });

        register("conn-legacy", "w-legacy", Map.of());
        assertThat(storedStats("w-legacy")).isNull();
    }

    @Test
    void shouldPreserveWorkerCacheStatsWhenAbortingASessionResetsAnOrphanedBusyWorker() {
        WorkerCacheStats stats = new WorkerCacheStats(true, 100L, 2, 300L, 400L, 6L, false);
        long now = fixtureClock.get();
        store.saveSession(new BuildSessionRecord(
                "sess-cache-abort", "citrus", "main", "deadbeef",
                SessionStatus.RUNNING, now - 5_000L, 0L,
                1, 0, 0, 0, 1, 1, 0L, 0L));
        store.upsertWorker(new WorkerRecord(
                "worker-cache-orphan", "host-1", "v1", "pool", "Original pool", Map.of(),
                WorkerState.BUSY, "part-cache-run", "Old > test", now, now, now - 10_000L, stats, List.of()));
        store.savePartition(new PartitionRecord(
                "part-cache-run", "sess-cache-abort", "worker-cache-orphan",
                PartitionState.RUNNING, 1, 2_000L, now - 3_000L, 0L,
                Map.of(), List.of(unit("pkg.CacheIT"))));

        coordinator.abortSession("sess-cache-abort", "Aborted from UI");

        assertThat(store.listWorkers()).singleElement().satisfies(w -> {
            assertThat(w.state()).isEqualTo(WorkerState.IDLE);
            assertThat(w.cacheStats()).isEqualTo(stats);
            assertThat(w.version()).isEqualTo("v1");
            assertThat(w.poolId()).isEqualTo("pool");
            assertThat(w.poolDisplayName()).isEqualTo("Original pool");
            assertThat(w.activeTestDisplay()).isNull();
        });
    }

    private WorkerCacheStats storedStats(String workerId) {
        return store.listWorkers().stream()
                .filter(w -> w.workerId().equals(workerId))
                .findFirst().orElseThrow().cacheStats();
    }

    @Test
    void shouldAbortPersistedRunningSessionAndMarkPersistedPartitionsLostAfterRehydration() {
        long now = fixtureClock.get();
        store.saveSession(new BuildSessionRecord(
                "sess-rehydrated-abort", "citrus", "main", "deadbeef",
                SessionStatus.RUNNING, now - 5_000L, 0L,
                2, 0, 0, 0, 10, 8, 0L, 0L));
        store.upsertWorker(new WorkerRecord(
                "worker-persisted-1", "host-1", Map.of(),
                WorkerState.BUSY, "part-persisted-run", now, now - 10_000L));
        store.savePartition(new PartitionRecord(
                "part-persisted-run", "sess-rehydrated-abort", "worker-persisted-1",
                PartitionState.RUNNING, 1, 2_000L, now - 3_000L, 0L,
                Map.of(), List.of(unit("pkg.PersistedIT"))));
        store.savePartition(new PartitionRecord(
                "part-persisted-pending", "sess-rehydrated-abort", null,
                PartitionState.PENDING, 1, 1_500L, 0L, 0L,
                Map.of(), List.of(unit("pkg.PendingIT"))));

        BuildSessionRecord aborted = coordinator.abortSession("sess-rehydrated-abort", "Aborted from UI");
        assertThat(aborted.status()).isEqualTo(SessionStatus.ABORTED);

        List<PartitionRecord> parts = store.listPartitions("sess-rehydrated-abort");
        assertThat(parts).hasSize(2);
        assertThat(parts).allSatisfy(p -> {
            assertThat(p.state()).isEqualTo(PartitionState.LOST);
            assertThat(p.errorMessage()).isEqualTo("Aborted from UI");
        });
        assertThat(store.listWorkers())
                .filteredOn(w -> "worker-persisted-1".equals(w.workerId()))
                .singleElement()
                .satisfies(w -> {
                    assertThat(w.state()).isEqualTo(WorkerState.IDLE);
                    assertThat(w.activePartitionId()).isNull();
                });
    }

    @Test
    void shouldSupersedeFailedRemoteAttemptOnLocalFallbackAcrossRehydrationAndReconcileMixedLocalCounts() {
        String sessionId = newSession(1);
        submit(sessionId, "sub-recover", List.of(), List.of(), false, unit("pkg.LostIT#testRecover"));

        Worker w1 = register("conn-rec-1", "w-rec-1", Map.of());
        PartitionAssignPayload a1 = w1.lastAssignment();
        w1.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(
                a1.partitionId(), sessionId, 1, "pkg.LostIT#testRecover", "pkg.LostIT", "testRecover",
                TestOutcomeStatus.FAILED, 120L, "Socket closed", "trace", List.of()), Map.class));
        coordinator.handleWorkerDisconnect("conn-rec-1");

        Worker w2 = register("conn-rec-2", "w-rec-2", Map.of());
        PartitionAssignPayload a2 = w2.lastAssignment();
        assertThat(a2.attempt()).isEqualTo(2);
        coordinator.handleWorkerDisconnect("conn-rec-2");

        coordinator.reportLocalTests(sessionId, new ReportLocalTestsRequest(
                "local-host",
                List.of(new WorkerTestEventPayload(
                        "local", sessionId, 1, "pkg.LostIT#testRecover", "pkg.LostIT", "testRecover",
                        TestOutcomeStatus.PASSED, 90L, null, null, List.of()))));

        SessionCoordinator rehydrated = new SessionCoordinator(
                DistributionCoordinatorConfig.builder().cacheDir(tempDir.resolve("cache-rehydrated")).leaseTimeoutMs(LEASE_MS).build(),
                store,
                new CoordinatorEventStreamHub(store));
        BuildSessionRecord completed = rehydrated.completeSession(
                sessionId, new CompleteSessionRequest(2, 0, 0, 250L));

        assertThat(completed.status()).isEqualTo(SessionStatus.PASSED);
        assertThat(completed.failedTests()).isZero();
        assertThat(completed.passedTests()).isEqualTo(2);
        assertThat(completed.totalTests()).isEqualTo(2);
    }

    private static final Map<String, String> JDK = Map.of("jdk", "21");
    private static final Map<String, String> DOCKER = Map.of("jdk", "21", "docker", "true");

    private AtomicLong useControlledClock(boolean requirePool) {
        return useControlledClock(requirePool, 100_000_000L);
    }

    private AtomicLong useControlledClock(boolean requirePool, long leaseMs) {
        AtomicLong clock = new AtomicLong(10_000_000L);
        coordinator = new SessionCoordinator(DistributionCoordinatorConfig.builder().clock(clock::get)
                .requireAgentPool(requirePool).leaseTimeoutMs(leaseMs).build(), store, hub);
        return clock;
    }

    private Worker join(String name, String poolId, String secret, Map<String, String> caps) {
        Worker worker = new Worker(name);
        coordinator.registerWorker(name, new WorkerRegisterPayload(name, "host", "5.0", poolId, secret, caps, null),
                worker.received::add, () -> worker.closed = true);
        return worker;
    }

    private WorkerRecord row(String id) {
        return store.listWorkers().stream().filter(w -> id.equals(w.workerId())).findFirst().orElseThrow();
    }

    private void renew(Worker worker, String pool, String key) {
        worker.send(WorkerWsMessage.WORKER_RENEW, Map.of("workerId", worker.connectionId, "poolId", pool, "registrationKey", key));
    }

    private void assertRejected(Worker worker, String code) {
        assertThat(worker.messagesOfType(WorkerWsMessage.WORKER_REJECTED)).singleElement()
                .satisfies(m -> assertThat(m.payload()).containsEntry("code", code));
        assertThat(worker.closed).isTrue();
    }

    private void demand(String session, String id, Map<String, String> caps) {
        submit(session, id, List.of(), List.of(), false, new WorkUnitSpec(id, id, null, null, caps, 100L));
    }

    @Test
    void rejectsUnusableRegistrationWithoutChangingKeyUsage() {
        useControlledClock(true);
        var a = coordinator.createAgentPool("A", JDK, true, "first");
        var b = coordinator.createAgentPool("B", JDK, false, null);
        String id = a.pool().poolId();
        String secret = a.generatedKey().secretKey();
        assertThat(id).matches("[a-z0-9]{8}");
        assertThat(secret).matches("ctrk_[a-z0-9]{8}_[a-z0-9]{32}");
        assertRejected(join("none", null, null, JDK), "POOL_REQUIRED");
        assertRejected(join("missing", "unknown", secret, JDK), "POOL_NOT_FOUND");
        assertRejected(join("wrong", b.pool().poolId(), secret, JDK), "INVALID_REGISTRATION_KEY");
        assertRejected(join("no-cap", id, secret, Map.of()), "MISSING_POOL_CAPABILITIES");
        assertRejected(join("x".repeat(129), id, secret, JDK), "INVALID_WORKER_METADATA");
        coordinator.revokeRegistrationKey(id, a.generatedKey().metadata().keyId());
        assertRejected(join("revoked", id, secret, JDK), "REVOKED_REGISTRATION_KEY");
        assertThat(store.listWorkers()).isEmpty();
        assertThat(coordinator.listRegistrationKeys(id).orElseThrow()).singleElement()
                .satisfies(key -> assertThat(key.lastUsedAt()).isNull());
        var events = new java.util.ArrayList<CoordinatorEvent>();
        store.replayEvents(null, 0, 2000, events::add);
        assertThat(events).extracting(CoordinatorEvent::type).contains("pool.updated", "worker.rejected");
        assertThat(DistributionJson.toJson(events)).doesNotContain(secret, store.listRegistrationKeys(id).getFirst().keyHash());
    }

    @Test
    void sendsWorkerRejectionAndClosesWhenEventPersistenceFails() {
        var database = "jdbc:hsqldb:mem:rejection_failure_" + System.nanoTime();
        try (var failing = new JdbcDistributionStore(database, "SA", "") {
            @Override
            public long appendEvent(CoordinatorEvent event, String deliveryId, TestExecutionRecord result,
                                    List<PartitionRecord> partitions) {
                if ("worker.rejected".equals(event.type())) {
                    throw new IllegalStateException("Simulated rejection event write failure");
                }
                return super.appendEvent(event, deliveryId, result, partitions);
            }
        }) {
            failing.initialize();
            var config = DistributionCoordinatorConfig.builder().requireAgentPool(true).build();
            var rejecting = new SessionCoordinator(config, failing, new CoordinatorEventStreamHub(failing));
            var worker = new Worker("rejected");
            assertThatThrownBy(() -> rejecting.registerWorker(worker.connectionId,
                    new WorkerRegisterPayload("worker", "host", JDK),
                    message -> {
                        assertThat(worker.closed).isFalse();
                        worker.received.add(message);
                    }, () -> worker.closed = true))
                    .isInstanceOf(IllegalStateException.class).hasMessage("Simulated rejection event write failure");
            assertRejected(worker, "POOL_REQUIRED");
        }
    }

    @Test
    void persistsWorkerRejectionWhenTheSocketWriteThrows() {
        useControlledClock(true);
        var worker = new Worker("rejected");
        assertThatThrownBy(() -> coordinator.registerWorker(worker.connectionId,
                new WorkerRegisterPayload("worker", "host", JDK),
                message -> { throw new IllegalStateException("Simulated socket write failure"); },
                () -> worker.closed = true))
                .isInstanceOf(IllegalStateException.class).hasMessage("Simulated socket write failure");
        assertThat(worker.closed).isTrue();
        var events = new java.util.ArrayList<CoordinatorEvent>();
        store.replayEvents(null, 0, 100, events::add);
        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.type()).isEqualTo("worker.rejected");
            assertThat(event.data()).containsEntry("code", "POOL_REQUIRED");
        });
    }

    @Test
    void honorsRevocationDeadlineAndReassignsActiveWorkOnRenewal() {
        var now = useControlledClock(false);
        var a = coordinator.createAgentPool("A", JDK, true, "key");
        String pool = a.pool().poolId();
        var generated = a.generatedKey();
        var worker = join("pooled", pool, generated.secretKey(), JDK);
        var second = join("second", pool, generated.secretKey(), JDK);
        var fallback = join("fallback", null, null, JDK);
        String session = newSession(2);
        demand(session, "first", JDK);
        now.incrementAndGet();
        coordinator.revokeRegistrationKey(pool, generated.metadata().keyId());
        long deadline = now.get() + 3_600_000L;
        assertThat(row("pooled").warnings()).singleElement().satisfies(w -> {
            assertThat(w.code()).isEqualTo(WorkerWarningCode.KEY_REVOKED);
            assertThat(w.disconnectAt()).isEqualTo(deadline);
        });
        coordinator.deleteAgentPool(pool);
        renew(worker, pool, generated.secretKey());
        assertRejected(worker, "REVOKED_REGISTRATION_KEY");
        assertThat(fallback.lastAssignment().attempt()).isEqualTo(2);
        now.set(deadline);
        coordinator.sweepExpiredLeases();
        assertRejected(second, "REVOKED_REGISTRATION_KEY");
        assertThat(row("second").state()).isEqualTo(WorkerState.OFFLINE);
    }

    @Test
    void preservesDriftDeadlineOnRenewalAndClearsOnlyWhenRequirementsRestored() {
        var now = useControlledClock(false);
        var a = coordinator.createAgentPool("A", JDK, true, null);
        String pool = a.pool().poolId();
        String key = a.generatedKey().secretKey();
        var worker = join("worker", pool, key, JDK);
        coordinator.updateAgentPool(pool, "renamed", DOCKER);
        var drift = row("worker").warnings().getFirst();
        assertThat(drift.code()).isEqualTo(WorkerWarningCode.POOL_CAPABILITY_MISMATCH);
        assertThat(drift.disconnectAt()).isEqualTo(now.get() + 14_400_000L);
        String session = newSession(1);
        demand(session, "docker", DOCKER);
        assertThat(worker.assignments()).isEmpty();
        demand(session, "java", JDK);
        assertThat(worker.assignments()).hasSize(1);
        now.set(drift.disconnectAt() - 1);
        renew(worker, pool, key);
        assertThat(worker.messagesOfType(WorkerWsMessage.WORKER_RENEWED)).hasSize(1);
        assertThat(row("worker").warnings()).containsExactly(drift);
        assertThat(coordinator.listRegistrationKeys(pool).orElseThrow().getFirst().lastUsedAt()).isEqualTo(now.get());
        coordinator.updateAgentPool(pool, "renamed again", DOCKER);
        assertThat(row("worker").warnings()).containsExactly(drift);
        coordinator.updateAgentPool(pool, "restored", JDK);
        assertThat(row("worker").warnings()).isEmpty();
        coordinator.updateAgentPool(pool, "drift", DOCKER);
        now.set(row("worker").warnings().getFirst().disconnectAt());
        renew(worker, pool, key);
        assertRejected(worker, "MISSING_POOL_CAPABILITIES");
    }

    @Test
    void keepsDeletedPoolWorkersOnExistingPartitionUntilDeadlineAndPreventsNewAssignments() {
        var now = useControlledClock(false);
        var a = coordinator.createAgentPool("A", JDK, true, null);
        String pool = a.pool().poolId();
        String key = a.generatedKey().secretKey();
        var worker = join("worker", pool, key, JDK);
        var other = join("other", pool, key, JDK);
        String session = newSession(1);
        demand(session, "first", JDK);
        var assigned = other.lastAssignment();
        coordinator.deleteAgentPool(pool);
        var warning = row("other").warnings().getFirst();
        assertThat(warning.code()).isEqualTo(WorkerWarningCode.POOL_DELETED);
        assertThat(warning.disconnectAt()).isEqualTo(now.get() + 14_400_000L);
        demand(session, "second", JDK);
        other.complete(assigned, assigned.attempt(), List.of());
        assertThat(other.assignments()).hasSize(1);
        assertThat(worker.assignments()).isEmpty();
        now.set(warning.disconnectAt() - 1);
        renew(other, pool, key);
        assertThat(other.messagesOfType(WorkerWsMessage.WORKER_RENEWED)).hasSize(1);
        assertThat(row("other").warnings()).containsExactly(warning);
        renew(worker, pool, "different-secret");
        assertRejected(worker, "INVALID_REGISTRATION_KEY");
        now.incrementAndGet();
        renew(other, pool, key);
        assertRejected(other, "POOL_NOT_FOUND");
        assertThat(lastEvent(null, "pool.deleted").data()).containsEntry("poolId", pool);
    }

    @Test
    void choosesByPoolPriorityThenFallsBackAndIgnoresWorkerOnlyCapabilities() {
        useControlledClock(false);
        var low = coordinator.createAgentPool("low", JDK, true, "");
        var high = coordinator.createAgentPool("high", JDK, true, "");
        coordinator.reorderAgentPools(List.of(high.pool().poolId(), low.pool().poolId()));
        var preferred = join("z", high.pool().poolId(), high.generatedKey().secretKey(), DOCKER);
        var next = join("y", low.pool().poolId(), low.generatedKey().secretKey(), JDK);
        var fallback = join("a", null, null, JDK);
        String session = newSession(1);
        demand(session, "docker", DOCKER);
        demand(session, "first", JDK);
        assertThat(preferred.lastAssignment().workUnits().getFirst().key()).isEqualTo("first");
        demand(session, "second", JDK);
        assertThat(next.lastAssignment().workUnits().getFirst().key()).isEqualTo("second");
        demand(session, "third", JDK);
        assertThat(fallback.lastAssignment().workUnits().getFirst().key()).isEqualTo("third");
        var sample = coordinator.sampleUsageNow();
        assertThat(sample.activeJobs()).hasSize(2).allSatisfy(j -> assertThat(j.activeSessionIds()).containsExactly(session));
        assertThat(coordinator.queryHistoricalUsage("1h", "").series().getLast().activeJobs()).isOne();
        assertThat(coordinator.queryHistoricalUsage("1h", "docker").series().getLast().connected()).isZero();
    }

    @Test
    void updatesLiveTestOnlyForCurrentAttemptAndPreservesMetadataOnRestart() {
        var now = useControlledClock(false);
        var pool = coordinator.createAgentPool("First", JDK, true, "");
        var worker = join("worker", pool.pool().poolId(), pool.generatedKey().secretKey(), JDK);
        String session = newSession(2);
        demand(session, "first", JDK);
        var assignment = worker.lastAssignment();
        var start = new WorkerTestEventPayload(assignment.partitionId(), session, 1, "key", "org.acme.OrderIT", "shouldCreate",
                TestOutcomeStatus.STARTED, 0L, null, null, List.of());
        now.incrementAndGet();
        worker.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(start, Map.class));
        assertThat(row("worker").activeTestDisplay()).isEqualTo("OrderIT > shouldCreate");
        assertThat(row("worker").stateUpdatedAt()).isEqualTo(now.get());
        now.incrementAndGet();
        worker.complete(assignment, assignment.attempt(), List.of());
        assertThat(row("worker").activeTestDisplay()).isNull();
        assertThat(row("worker").stateUpdatedAt()).isEqualTo(now.get());
        demand(session, "second", JDK);
        @SuppressWarnings("unchecked")
        Map<String, Object> replay = DistributionJson.convert(start, Map.class);
        replay.put(WorkerWsMessage.DELIVERY_ID, "0000000000000000001-00000000-0000-0000-0000-000000000001");
        worker.send(WorkerWsMessage.TEST_EVENT, replay);
        assertThat(row("worker").activeTestDisplay()).isNull();
        coordinator = new SessionCoordinator(DistributionCoordinatorConfig.builder().clock(now::get).build(), store, hub);
        coordinator.restorePendingWork();
        var offline = row("worker");
        assertThat(offline.state()).isEqualTo(WorkerState.OFFLINE);
        assertThat(offline.activePartitionId()).isNull();
        assertThat(offline.version()).isEqualTo("5.0");
        assertThat(offline.poolId()).isEqualTo(pool.pool().poolId());
        assertThat(offline.poolDisplayName()).isEqualTo("First");
        assertThat(coordinator.listAgentPools()).singleElement().satisfies(p -> assertThat(p.connectedWorkers()).isZero());
    }

    @Test
    void countsDeletedPoolCapacityDuringGraceWithoutAllocatingNewDemandToIt() {
        useControlledClock(false);
        var old = coordinator.createAgentPool("Retired", JDK, true, null);
        var active = coordinator.createAgentPool("Current", JDK, true, null);
        join("old-worker", old.pool().poolId(), old.generatedKey().secretKey(), DOCKER);
        join("new-worker", active.pool().poolId(), active.generatedKey().secretKey(), JDK);
        var session = newSession(0);
        demand(session, "one", JDK);
        coordinator.deleteAgentPool(old.pool().poolId());
        demand(session, "two", JDK);
        coordinator.sampleUsageNow();
        var usage = coordinator.queryHistoricalUsage("1h", "jdk=21");
        assertThat(usage.series().getLast().connected()).isEqualTo(2);
        assertThat(usage.series().getLast().utilized()).isEqualTo(2);
        assertThat(usage.pools()).extracting(p -> p.displayName(), p -> p.points().getLast().desired())
                .containsExactly(tuple("Retired", 0), tuple("Current", 2));
        assertThat(coordinator.queryHistoricalUsage("1h", "docker").pools()).isEmpty();
    }

    @Test
    void aRenewalCannotSwitchWorkerPoolOrKeyOrTouchAnotherKeysUsage() {
        var now = useControlledClock(false);
        var first = coordinator.createAgentPool("First", JDK, true, null);
        var second = coordinator.createAgentPool("Second", JDK, true, null);
        String pool = first.pool().poolId();
        var unused = coordinator.generateRegistrationKey(pool, "second key").orElseThrow();
        var registered = join("worker", pool, first.generatedKey().secretKey(), JDK);
        long registeredAt = now.getAndIncrement();
        renew(registered, pool, unused.secretKey());
        assertRejected(registered, "INVALID_REGISTRATION_KEY");
        assertThat(store.findRegistrationKey(unused.metadata().keyId()).orElseThrow().metadata().lastUsedAt()).isNull();
        assertThat(store.findRegistrationKey(first.generatedKey().metadata().keyId()).orElseThrow().metadata().lastUsedAt())
                .isEqualTo(registeredAt);
        var reconnected = join("other", pool, first.generatedKey().secretKey(), JDK);
        renew(reconnected, second.pool().poolId(), second.generatedKey().secretKey());
        assertRejected(reconnected, "INVALID_REGISTRATION_KEY");
        assertThat(store.findRegistrationKey(second.generatedKey().metadata().keyId()).orElseThrow().metadata().lastUsedAt())
                .isNull();
        var third = join("third", pool, first.generatedKey().secretKey(), JDK);
        third.send(WorkerWsMessage.WORKER_RENEW, Map.of("workerId", "other", "poolId", pool,
                "registrationKey", first.generatedKey().secretKey()));
        assertRejected(third, "INVALID_REGISTRATION_KEY");
    }

    @Test
    void adaptiveSizingUsesPoolCapabilitiesAndIncludesBusyWorkers() {
        var now = new AtomicLong(500L);
        coordinator = new SessionCoordinator(DistributionCoordinatorConfig.builder().clock(now::get)
                .defaultMaxPartitionSize(2).minPartitionDurationMs(0).maxWavesPerWorker(2).build(), store, hub);
        var java = coordinator.createAgentPool("Java", JDK, true, null);
        var containers = coordinator.createAgentPool("Containers", DOCKER, true, null);
        join("hidden-docker", java.pool().poolId(), java.generatedKey().secretKey(), DOCKER);
        join("busy-docker", containers.pool().poolId(), containers.generatedKey().secretKey(), DOCKER);
        demand(newSession(0), "busy", DOCKER);
        String session = coordinator.createSession(new CreateSessionRequest("p", "main", "sha", List.of(), 0, 0)).sessionId();
        WorkUnitSpec[] units = IntStream.range(0, 12)
                .mapToObj(i -> new WorkUnitSpec("T" + i, "T" + i, null, null, DOCKER, 100L)).toArray(WorkUnitSpec[]::new);
        submit(session, "adaptive", List.of(), List.of(), false, units);
        assertThat(store.listPartitions(session)).hasSize(2)
                .allSatisfy(partition -> assertThat(partition.workUnits()).hasSize(6));
    }

    @Test
    void earlierCompletionCannotConfirmCancellationOfALaterAttempt() {
        var now = useControlledClock(false);
        var worker = join("worker", null, null, JDK);
        String session = newSession(3);
        demand(session, "work", JDK);
        var first = worker.lastAssignment();
        startTest(worker, first, "First", "one");
        now.addAndGet(100_000_001L);
        coordinator.sweepExpiredLeases();
        assertThat(row("worker").activeTestDisplay()).isNull();
        worker.complete(first, 1, List.of());
        var second = worker.lastAssignment();
        assertThat(second.attempt()).isEqualTo(2);
        startTest(worker, second, "Second", "two");
        assertThat(row("worker").activeTestDisplay()).isEqualTo("Second > two");
        now.addAndGet(100_000_001L);
        coordinator.sweepExpiredLeases();
        worker.complete(first, 1, List.of());
        assertThat(worker.assignments()).hasSize(2);
        worker.complete(second, 2, List.of());
        var third = worker.lastAssignment();
        assertThat(third.attempt()).isEqualTo(3);
        startTest(worker, third, "Third", "three");
        coordinator.handleWorkerDisconnect("worker");
        assertThat(row("worker").activeTestDisplay()).isNull();
        assertThat(row("worker").state()).isEqualTo(WorkerState.OFFLINE);
    }

    private void startTest(Worker worker, PartitionAssignPayload assignment, String className, String testName) {
        worker.send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(assignment.partitionId(),
                assignment.sessionId(), assignment.attempt(), "key", className, testName, TestOutcomeStatus.STARTED,
                0L, null, null, List.of()), Map.class));
    }

    @Test
    void prefersTheHighestPriorityPoolEvenWhenItsWorkerAlreadyAttemptedThePartition() {
        useControlledClock(false);
        var pool = coordinator.createAgentPool("First", JDK, true, null);
        var original = join("worker", pool.pool().poolId(), pool.generatedKey().secretKey(), JDK);
        var session = newSession(2);
        demand(session, "first", JDK);
        var first = original.lastAssignment();
        startTest(original, first, "T", "one");
        var duplicate = new Worker("duplicate");
        coordinator.registerWorker(duplicate.connectionId, new WorkerRegisterPayload("worker", "host", "5.0",
                pool.pool().poolId(), pool.generatedKey().secretKey(), JDK, null), duplicate.received::add);
        var fallback = join("fallback", null, null, JDK);
        assertThat(row("worker").activeTestDisplay()).isEqualTo("T > one");
        assertThat(coordinator.getAgentPoolDetail(pool.pool().poolId()).orElseThrow().pool().connectedWorkers()).isOne();
        assertThat(coordinator.sampleUsageNow().pools().getFirst().connected()).isOne();
        coordinator.handleWorkerDisconnect("worker");
        assertThat(duplicate.lastAssignment().attempt()).isEqualTo(2);
        assertThat(fallback.assignments()).isEmpty();
        assertThat(row("worker").state()).isEqualTo(WorkerState.BUSY);
    }

    @Test
    void sweepRequeuesAnActiveWorkerAtTheOriginalRevocationDeadline() {
        var now = useControlledClock(false);
        var pool = coordinator.createAgentPool("A", JDK, true, null);
        var original = join("worker", pool.pool().poolId(), pool.generatedKey().secretKey(), JDK);
        var fallback = join("fallback", null, null, JDK);
        demand(newSession(2), "work", JDK);
        var assigned = original.lastAssignment();
        startTest(original, assigned, "T", "test");
        coordinator.revokeRegistrationKey(pool.pool().poolId(), pool.generatedKey().metadata().keyId());
        now.addAndGet(3_599_999);
        coordinator.revokeRegistrationKey(pool.pool().poolId(), pool.generatedKey().metadata().keyId());
        coordinator.sweepExpiredLeases();
        assertThat(original.closed).isFalse();
        now.incrementAndGet();
        coordinator.sweepExpiredLeases();
        assertRejected(original, "REVOKED_REGISTRATION_KEY");
        assertThat(row("worker").activeTestDisplay()).isNull();
        assertThat(row("worker").warnings()).isEmpty();
        assertThat(fallback.lastAssignment().attempt()).isEqualTo(2);
        assertThat(fallback.lastAssignment().partitionId()).isEqualTo(assigned.partitionId());
    }

    @Test
    void registersLegacyWorkersWithLargeCapabilityMapsAndMatchesTheirExactValues() {
        useControlledClock(false);
        var capabilities = Map.of("large", "x".repeat(5000), "variant", "exact");
        var worker = register("legacy", "legacy", capabilities);
        assertThat(worker.messagesOfType(WorkerWsMessage.WORKER_REGISTERED)).hasSize(1);
        assertThat(row("legacy").capabilities()).isEqualTo(capabilities);
        var session = newSession(0);
        demand(session, "wrong", Map.of("variant", "other"));
        demand(session, "right", Map.of("variant", "exact"));
        assertThat(worker.assignments()).singleElement().satisfies(
                assignment -> assertThat(assignment.workUnits().getFirst().key()).isEqualTo("right"));
        assertThat(coordinator.sampleUsageNow().unpooledWorkers().getFirst().capabilities()).isEqualTo(capabilities);
    }

    private static final class FailingWorkerProjectionStore extends JdbcDistributionStore {
        private boolean failWorkerRow;
        private boolean failWorkerEvent;

        FailingWorkerProjectionStore() {
            super("jdbc:hsqldb:mem:worker_projection_" + System.nanoTime(), "SA", "");
        }

        @Override
        public void upsertWorker(WorkerRecord worker) {
            if (failWorkerRow) {
                failWorkerRow = false;
                throw new IllegalStateException("Simulated worker row failure");
            }
            super.upsertWorker(worker);
        }

        @Override
        public long appendEvent(CoordinatorEvent event, String deliveryId, TestExecutionRecord result,
                                List<PartitionRecord> partitions) {
            if (failWorkerEvent && "worker.updated".equals(event.type())) {
                failWorkerEvent = false;
                throw new IllegalStateException("Simulated worker event failure");
            }
            return super.appendEvent(event, deliveryId, result, partitions);
        }
    }

    private AtomicLong useProjectionStore(FailingWorkerProjectionStore failing) {
        failing.initialize();
        store = failing;
        hub = new CoordinatorEventStreamHub(store);
        return useControlledClock(false);
    }

    @Test
    void preservesDeletedPoolGraceForEveryConnectionWhenWorkerProjectionFails() {
        try (var failing = new FailingWorkerProjectionStore()) {
            var clock = useProjectionStore(failing);
            var pool = coordinator.createAgentPool("Retiring", JDK, true, null);
            String id = pool.pool().poolId();
            String key = pool.generatedKey().secretKey();
            var first = join("first", id, key, JDK);
            var second = join("second", id, key, JDK);
            failing.failWorkerRow = true;
            assertThat(coordinator.deleteAgentPool(id)).isTrue();
            long deadline = clock.get() + 14_400_000L;
            clock.set(deadline - 1);
            demand(newSession(0), "unassigned", JDK);
            for (Worker worker : List.of(first, second)) {
                renew(worker, id, key);
                assertThat(worker.messagesOfType(WorkerWsMessage.WORKER_RENEWED)).hasSize(1);
                worker.heartbeat(null);
                assertThat(row(worker.connectionId).warnings()).singleElement().satisfies(warning -> {
                    assertThat(warning.code()).isEqualTo(WorkerWarningCode.POOL_DELETED);
                    assertThat(warning.disconnectAt()).isEqualTo(deadline);
                });
                assertThat(worker.assignments()).isEmpty();
            }
            clock.set(deadline);
            coordinator.sweepExpiredLeases();
            assertRejected(first, "POOL_NOT_FOUND");
            assertRejected(second, "POOL_NOT_FOUND");
        }
    }

    @Test
    void appliesPoolEditsAndRevocationsToAllWorkersWhenTheirUpdateEventFails() {
        try (var failing = new FailingWorkerProjectionStore()) {
            var clock = useProjectionStore(failing);
            var pool = coordinator.createAgentPool("Pool", JDK, true, null);
            String id = pool.pool().poolId();
            String key = pool.generatedKey().secretKey();
            var first = join("first", id, key, JDK);
            var second = join("second", id, key, JDK);
            failing.failWorkerEvent = true;
            coordinator.updateAgentPool(id, "Renamed", DOCKER);
            for (Worker worker : List.of(first, second)) {
                worker.heartbeat(null);
                assertThat(row(worker.connectionId).warnings()).singleElement().satisfies(warning -> {
                    assertThat(warning.code()).isEqualTo(WorkerWarningCode.POOL_CAPABILITY_MISMATCH);
                    assertThat(warning.detectedAt()).isEqualTo(clock.get());
                });
            }
            failing.failWorkerRow = true;
            coordinator.revokeRegistrationKey(id, pool.generatedKey().metadata().keyId());
            for (Worker worker : List.of(first, second)) {
                worker.heartbeat(null);
                assertThat(row(worker.connectionId).warnings()).anySatisfy(warning -> {
                    assertThat(warning.code()).isEqualTo(WorkerWarningCode.KEY_REVOKED);
                    assertThat(warning.disconnectAt()).isEqualTo(clock.get() + 3_600_000L);
                });
            }
            clock.addAndGet(3_600_000L);
            coordinator.sweepExpiredLeases();
            assertRejected(first, "REVOKED_REGISTRATION_KEY");
            assertRejected(second, "REVOKED_REGISTRATION_KEY");
        }
    }

    @Test
    void dispatchesCommittedAssignmentAndRetriesItsWorkerUpdateOnHeartbeat() {
        try (var failing = new FailingWorkerProjectionStore()) {
            var clock = useProjectionStore(failing);
            var worker = join("worker", null, null, JDK);
            var session = newSession(0);
            failing.failWorkerEvent = true;
            demand(session, "only-attempt", JDK);
            var assignment = worker.lastAssignment();
            assertThat(partition(session).state()).isEqualTo(PartitionState.ASSIGNED);
            long assignedAt = row("worker").stateUpdatedAt();
            assertThat(lastEvent(null, "worker.updated").data()).containsEntry("state", "IDLE");
            clock.incrementAndGet();
            worker.heartbeat(assignment.partitionId());
            assertThat(row("worker").stateUpdatedAt()).isEqualTo(assignedAt);
            assertThat(lastEvent(null, "worker.updated").data()).containsEntry("state", "BUSY");
            worker.complete(assignment, 1, List.of());
            assertThat(partition(session).state()).isEqualTo(PartitionState.COMPLETED);
            assertThat(worker.assignments()).hasSize(1);
        }
    }

    @Test
    void samplesLegacyUnpooledCapabilitiesContainingNullValues() {
        useControlledClock(false);
        var capabilities = new java.util.LinkedHashMap<String, String>();
        capabilities.put("jdk", "21");
        capabilities.put("gpu", null);
        register("legacy-connection", "legacy", capabilities);
        register("other-connection", "other", Map.of("gpu", "true"));

        var sample = coordinator.sampleUsageNow();
        assertThat(sample.unpooledWorkers()).hasSize(2);
        assertThat(sample.unpooledWorkers()).filteredOn(bucket -> bucket.capabilities().containsKey("jdk")).singleElement()
                .satisfies(bucket -> assertThat(bucket.capabilities()).containsEntry("gpu", null));
        assertThat(coordinator.queryHistoricalUsage("1h", "").series().getLast().connected()).isEqualTo(2);
        assertThat(coordinator.queryHistoricalUsage("1h", "jdk=21").series().getLast().connected()).isOne();
        assertThat(coordinator.queryHistoricalUsage("1h", "gpu").series().getLast().connected()).isOne();
    }

    @Test
    void keepsCommittedResultInSummaryWhenWorkerStatusWriteFailsBeforeAcknowledgement() {
        assertRecoveryAfter(FailedWrite.WORKER_STATUS);
    }

    @Test
    void heartbeatShowsRemainingTestAfterSummaryWriteFailureAndJournalReplay() {
        assertRecoveryAfter(FailedWrite.SESSION_SUMMARY);
    }

    private enum FailedWrite { WORKER_STATUS, SESSION_SUMMARY }

    private void assertRecoveryAfter(FailedWrite failure) {
        class FailingStore extends JdbcDistributionStore {
            FailedWrite failNext;

            FailingStore() {
                super("jdbc:hsqldb:mem:status_failure_" + System.nanoTime(), "SA", "");
            }

            @Override
            public void upsertWorker(WorkerRecord worker) {
                failOnce(FailedWrite.WORKER_STATUS);
                super.upsertWorker(worker);
            }

            @Override
            public void updateSessionStatus(String sessionId, SessionStatus status, long finishedAt,
                                            SessionSummary summary, String failureReason) {
                failOnce(FailedWrite.SESSION_SUMMARY);
                super.updateSessionStatus(sessionId, status, finishedAt, summary, failureReason);
            }

            private void failOnce(FailedWrite write) {
                if (failNext == write) {
                    failNext = null;
                    throw new IllegalStateException("Simulated write failure: " + write);
                }
            }
        }

        try (var failing = new FailingStore()) {
            failing.initialize();
            var config = DistributionCoordinatorConfig.builder().leaseTimeoutMs(60_000L).build();
            coordinator = new SessionCoordinator(config, failing, new CoordinatorEventStreamHub(failing));
            var worker = join("worker", null, null, JDK);
            String session = newSession(1);
            demand(session, "partition", JDK);
            var assignment = worker.lastAssignment();
            startTest(worker, assignment, "Test", "remaining");
            startTest(worker, assignment, "Test", "one");
            assertThat(failing.listWorkers().getFirst().activeTestDisplay()).isEqualTo("Test > one");
            var failed = new WorkerTestEventPayload(assignment.partitionId(), session, assignment.attempt(), "key",
                    "Test", "one", TestOutcomeStatus.FAILED, 22L, "assertion failed", null, List.of());
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = DistributionJson.convert(failed, Map.class);
            String delivery = "0000000000000000001-00000000-0000-0000-0000-000000000001";
            payload.put(WorkerWsMessage.DELIVERY_ID, delivery);
            failing.failNext = failure;
            assertThatThrownBy(() -> worker.send(WorkerWsMessage.TEST_EVENT, payload))
                    .isInstanceOf(IllegalStateException.class).hasMessage("Simulated write failure: " + failure);
            assertThat(failing.hasDelivery(delivery)).isTrue();
            assertThat(worker.messagesOfType(WorkerWsMessage.RESULT_ACK)).isEmpty();

            worker.send(WorkerWsMessage.TEST_EVENT, payload);
            assertThat(worker.messagesOfType(WorkerWsMessage.RESULT_ACK)).hasSize(1);
            assertThat(failing.listTestExecutions(session)).hasSize(1);
            var events = new java.util.ArrayList<CoordinatorEvent>();
            failing.replayEvents(null, 0L, 100, events::add);
            long beforeHeartbeat = events.getLast().sequence();
            worker.heartbeat(assignment.partitionId());
            events.clear();
            failing.replayEvents(null, beforeHeartbeat, 100, events::add);
            assertThat(events).extracting(CoordinatorEvent::type).contains("worker.updated");
            assertThat(failing.listWorkers().getFirst().activeTestDisplay()).isEqualTo("Test > remaining");
            worker.complete(assignment, assignment.attempt(), List.of());
            var summary = coordinator.completeSession(session, new CompleteSessionRequest(0, 0, 0, 0L));
            assertThat(summary.failedTests()).isOne();
            assertThat(summary.status()).isEqualTo(SessionStatus.FAILED);
        }
    }

    private String newSession(int maxRetries) {
        return coordinator.createSession(new CreateSessionRequest("citrus", "main", "abc", List.of(), 1, maxRetries)).sessionId();
    }

    private void submit(String sessionId, String submissionId, List<WorkspaceFileSet> inputs, List<String> outputs,
                        boolean fork, WorkUnitSpec... units) {
        coordinator.submitWork(sessionId, new SubmitWorkRequest(submissionId, "module", List.of(), Map.of(),
                List.of(units), 0, 0, inputs, outputs, fork, PartitionPolicy.LPT, "target"));
    }

    /**
     * Lets every lease expire, keeps {@code liveWorkers} alive with an idle heartbeat, then sweeps.
     */
    private void expireLeases(Worker... liveWorkers) throws InterruptedException {
        fixtureClock.addAndGet(LEASE_MS * 2);
        for (Worker live : liveWorkers) {
            live.heartbeat(null);
        }
        coordinator.sweepExpiredLeases();
    }

    private PartitionRecord partition(String sessionId) {
        return store.listPartitions(sessionId).getLast();
    }

    private CoordinatorEvent lastEvent(String sessionId, String type) {
        List<CoordinatorEvent> matching = new java.util.ArrayList<>();
        store.replayEvents(sessionId, 0, 2000, event -> {
            if (event.type().equals(type)) {
                matching.add(event);
            }
        });
        assertThat(matching).as("events of type %s", type).isNotEmpty();
        return matching.getLast();
    }

    private static WorkUnitSpec unit(String key) {
        return new WorkUnitSpec(key, key, null, null, Map.of(), 100L);
    }

    private Worker register(String connectionId, String workerId, Map<String, String> capabilities) {
        Worker worker = new Worker(connectionId);
        coordinator.registerWorker(connectionId, new WorkerRegisterPayload(workerId, "host", capabilities), worker.received::add);
        return worker;
    }

    private final class Worker {
        private boolean closed;
        private final String connectionId;
        private final List<WorkerWsMessage> received = new CopyOnWriteArrayList<>();

        private Worker(String connectionId) {
            this.connectionId = connectionId;
        }

        List<WorkerWsMessage> messagesOfType(String type) {
            return received.stream().filter(m -> m.type().equals(type)).toList();
        }

        List<PartitionAssignPayload> assignments() {
            return messagesOfType(WorkerWsMessage.PARTITION_ASSIGN).stream()
                    .map(m -> DistributionJson.convert(m.payload(), PartitionAssignPayload.class))
                    .toList();
        }

        PartitionAssignPayload lastAssignment() {
            List<PartitionAssignPayload> assignments = assignments();
            assertThat(assignments).as("assignments on %s", connectionId).isNotEmpty();
            return assignments.getLast();
        }

        @SuppressWarnings("unchecked")
        void send(String type, Map<?, ?> payload) {
            coordinator.handleWorkerMessage(connectionId, new WorkerWsMessage(type, (Map<String, Object>) payload));
        }

        void heartbeat(String activePartitionId) {
            heartbeat(activePartitionId, null);
        }

        void heartbeat(String activePartitionId, WorkerCacheStats cacheStats) {
            send(WorkerWsMessage.WORKER_HEARTBEAT, DistributionJson.convert(
                    new WorkerHeartbeatPayload(connectionId, activePartitionId, cacheStats), Map.class));
        }

        void testEvent(PartitionAssignPayload assign, int attempt, String testName) {
            send(WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(assign.partitionId(),
                    assign.sessionId(), attempt, "key", "pkg.Test", testName, TestOutcomeStatus.PASSED, 5L,
                    null, null, List.of()), Map.class));
        }

        void complete(PartitionAssignPayload assign, int attempt, List<WorkspaceFileSet> outputs) {
            send(WorkerWsMessage.PARTITION_COMPLETED, DistributionJson.convert(new PartitionCompletedPayload(
                    assign.partitionId(), assign.sessionId(), attempt, true, null, outputs), Map.class));
        }
    }
}
