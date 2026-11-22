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

package org.citrusframework.distribution.broker;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BrokerEvent;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionCompletedPayload;
import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.ReportLocalTestsRequest;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.TestExecutionRecord;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerHeartbeatPayload;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerState;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.citrusframework.distribution.store.JdbcDistributionStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrokerSessionCoordinatorTest {

    private static final long LEASE_MS = 50L;
    private static final Map<String, String> WORKSPACE_CAPS = Map.of(DistributionSettings.WORKSPACE_CAPABILITY_KEY, "true");

    @TempDir
    Path tempDir;

    private JdbcDistributionStore store;
    private BrokerEventStreamHub hub;
    private BrokerSessionCoordinator coordinator;

    @BeforeEach
    void setUp() {
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .cacheDir(tempDir.resolve("cache"))
                .leaseTimeoutMs(LEASE_MS)
                .build();
        store = new JdbcDistributionStore("jdbc:hsqldb:mem:coordinator_" + System.nanoTime(), "SA", "");
        store.initialize();
        hub = new BrokerEventStreamHub();
        coordinator = new BrokerSessionCoordinator(config, store, hub);
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
        BrokerEvent completed = lastEvent(sessionId, "partition.completed");
        assertThat(completed.data()).containsEntry("attempt", 1).containsKey("outputArtifacts");
        assertThat(DistributionJson.toJson(completed.data().get("outputArtifacts"))).contains("target/docs", "e".repeat(64));
    }

    @Test
    void shouldCancelExpiredLeaseAndReassignOnlyAfterWorkerConfirmsItStopped() throws Exception {
        String sessionId = newSession(2);
        Worker worker = register("conn-1", "worker-1", WORKSPACE_CAPS);
        submit(sessionId, "sub-1", List.of(), List.of("target/docs"), true, unit("a.A"));
        PartitionAssignPayload first = worker.lastAssignment();
        assertThat(first.attempt()).isEqualTo(1);

        expireLeases();

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
        Thread.sleep(LEASE_MS * 2);
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
        Thread.sleep(LEASE_MS * 2);
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
    void shouldKeepAssignedLeaseAliveWithHeartbeatsBeforeStartedAndRejectStaleStarted() throws Exception {
        String sessionId = newSession(1);
        Worker worker = register("conn-1", "worker-1", Map.of());
        submit(sessionId, "sub-1", List.of(), List.of(), false, unit("a.A"));
        PartitionAssignPayload assign = worker.lastAssignment();

        for (int i = 0; i < 3; i++) {
            Thread.sleep(LEASE_MS / 2);
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
        Thread.sleep(LEASE_MS * 2);
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
    void shouldAbortPersistedRunningSessionAndMarkPersistedPartitionsLostAfterRehydration() {
        long now = System.currentTimeMillis();
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

        BrokerSessionCoordinator rehydrated = new BrokerSessionCoordinator(
                DistributionBrokerConfig.builder().cacheDir(tempDir.resolve("cache-rehydrated")).leaseTimeoutMs(LEASE_MS).build(),
                store,
                new BrokerEventStreamHub());
        BuildSessionRecord completed = rehydrated.completeSession(
                sessionId, new CompleteSessionRequest(2, 0, 0, 250L));

        assertThat(completed.status()).isEqualTo(SessionStatus.PASSED);
        assertThat(completed.failedTests()).isZero();
        assertThat(completed.passedTests()).isEqualTo(2);
        assertThat(completed.totalTests()).isEqualTo(2);
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
        Thread.sleep(LEASE_MS * 2);
        for (Worker live : liveWorkers) {
            live.heartbeat(null);
        }
        coordinator.sweepExpiredLeases();
    }

    private PartitionRecord partition(String sessionId) {
        return store.listPartitions(sessionId).getLast();
    }

    private BrokerEvent lastEvent(String sessionId, String type) {
        List<BrokerEvent> matching = hub.sessionEvents(sessionId).stream().filter(e -> e.type().equals(type)).toList();
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
            send(WorkerWsMessage.WORKER_HEARTBEAT, DistributionJson.convert(
                    new WorkerHeartbeatPayload(connectionId, activePartitionId), Map.class));
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
