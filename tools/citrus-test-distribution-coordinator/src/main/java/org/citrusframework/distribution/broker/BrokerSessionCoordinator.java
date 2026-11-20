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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionCompletedPayload;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.SessionDetailResponse;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SessionSummary;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.TestExecutionRecord;
import org.citrusframework.distribution.model.TestHistorySummary;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerHeartbeatPayload;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerState;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.citrusframework.distribution.scheduler.CapabilityMatcher;
import org.citrusframework.distribution.scheduler.LptPartitionScheduler;
import org.citrusframework.distribution.store.DistributionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe coordinator for build sessions, LPT partition scheduling, worker matching, and fault recovery.
 */
public final class BrokerSessionCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerSessionCoordinator.class);

    private final DistributionBrokerConfig config;
    private final DistributionStore store;
    private final BrokerEventStreamHub eventHub;
    private final LptPartitionScheduler scheduler = new LptPartitionScheduler();

    private final Map<String, SessionRuntime> sessions = new ConcurrentHashMap<>();
    private final Map<String, SubmitWorkResponse> submissionResponses = new ConcurrentHashMap<>();
    private final Map<String, PartitionRuntime> partitions = new ConcurrentHashMap<>();
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();

    public BrokerSessionCoordinator(DistributionBrokerConfig config, DistributionStore store, BrokerEventStreamHub eventHub) {
        this.config = config;
        this.store = store;
        this.eventHub = eventHub;
    }

    public synchronized BuildSessionResponse createSession(CreateSessionRequest request) {
        String sessionId = "sess-" + UUID.randomUUID().toString().substring(0, 12);
        long now = System.currentTimeMillis();
        int maxPartSize = request.maxPartitionSize() > 0 ? request.maxPartitionSize() : config.defaultMaxPartitionSize();
        int maxRetries = request.maxRetries() >= 0 ? request.maxRetries() : config.defaultMaxRetries();

        BuildSessionRecord record = new BuildSessionRecord(
                sessionId,
                request.projectName() != null ? request.projectName() : "citrus-project",
                request.gitBranch(),
                request.gitCommit(),
                SessionStatus.RUNNING,
                now,
                0L,
                0, 0, 0, 0, 0, 0, 0L, 0L
        );
        store.saveSession(record);
        if (request.changedFiles() != null && !request.changedFiles().isEmpty()) {
            store.saveChangedFiles(sessionId, request.changedFiles());
        }

        sessions.put(sessionId, new SessionRuntime(record, maxPartSize, maxRetries));
        eventHub.publish("session.created", sessionId, null, null, Map.of(
                "projectName", record.projectName(),
                "status", record.status().name()));
        return new BuildSessionResponse(sessionId, SessionStatus.RUNNING);
    }

    public synchronized SubmitWorkResponse submitWork(String sessionId, SubmitWorkRequest request) {
        SessionRuntime session = getOrRehydrateSession(sessionId);
        if (session == null) {
            throw new IllegalArgumentException("Unknown sessionId: " + sessionId);
        }
        String dedupKey = sessionId + "::" + request.submissionId();
        SubmitWorkResponse existing = submissionResponses.get(dedupKey);
        if (existing != null) {
            return existing;
        }
        Optional<SubmitWorkResponse> persisted = store.findWorkSubmission(sessionId, request.submissionId());
        if (persisted.isPresent()) {
            submissionResponses.put(dedupKey, persisted.get());
            return persisted.get();
        }

        session.cachedBlobs += Math.max(0, request.cachedBlobs());
        session.uploadedBlobs += Math.max(0, request.uploadedBlobs());

        List<WorkUnitSpec> units = request.workUnits() != null ? request.workUnits() : List.of();
        List<String> keys = units.stream().map(WorkUnitSpec::key).toList();
        Map<String, Long> historicalDurations = store.findHistoricalDurationsMs(keys, 10);

        List<PartitionRecord> scheduled = scheduler.schedule(
                sessionId,
                units,
                historicalDurations,
                config.defaultDurationMs(),
                session.maxPartitionSize);

        List<String> partitionIds = new ArrayList<>(scheduled.size());
        for (PartitionRecord part : scheduled) {
            store.savePartition(part);
            PartitionRuntime runtime = new PartitionRuntime(
                    part,
                    request.moduleName(),
                    request.classpathArtifacts() != null ? List.copyOf(request.classpathArtifacts()) : List.of(),
                    request.systemProperties() != null ? Map.copyOf(request.systemProperties()) : Map.of(),
                    session.maxRetries
            );
            partitions.put(part.partitionId(), runtime);
            session.partitionIds.add(part.partitionId());
            partitionIds.add(part.partitionId());
        }

        SubmitWorkResponse response = new SubmitWorkResponse(
                request.submissionId(),
                scheduled.size(),
                partitionIds,
                request.cachedBlobs(),
                request.uploadedBlobs()
        );
        store.saveWorkSubmission(sessionId, response);
        submissionResponses.put(dedupKey, response);

        eventHub.publish("work.submitted", sessionId, null, null, Map.of(
                "moduleName", request.moduleName() != null ? request.moduleName() : "",
                "partitionsCreated", scheduled.size(),
                "workUnits", units.size(),
                "cachedBlobs", request.cachedBlobs(),
                "uploadedBlobs", request.uploadedBlobs()
        ));

        dispatchPendingPartitions();
        return response;
    }

    public synchronized BuildSessionRecord completeSession(String sessionId, CompleteSessionRequest request) {
        SessionRuntime session = sessions.get(sessionId);
        if (session == null) {
            return store.findSession(sessionId)
                    .orElseThrow(() -> new IllegalArgumentException("Unknown sessionId: " + sessionId));
        }

        List<TestExecutionRecord> executions = store.listTestExecutions(sessionId);
        int passed = (request != null ? request.localPassedTests() : 0);
        int failed = (request != null ? request.localFailedTests() : 0);
        int skipped = (request != null ? request.localSkippedTests() : 0);
        long serialMs = (request != null ? request.localSerialDurationMs() : 0L);

        for (TestExecutionRecord exec : executions) {
            serialMs += Math.max(0L, exec.durationMs());
            if (exec.status() == TestOutcomeStatus.PASSED) {
                passed++;
            } else if (exec.status() == TestOutcomeStatus.FAILED) {
                failed++;
            } else if (exec.status() == TestOutcomeStatus.SKIPPED) {
                skipped++;
            }
        }

        boolean anyLostOrFailedPartition = store.listPartitions(sessionId).stream()
                .anyMatch(p -> p.state() == PartitionState.FAILED || p.state() == PartitionState.LOST);

        SessionStatus finalStatus = (failed > 0 || anyLostOrFailedPartition) ? SessionStatus.FAILED : SessionStatus.PASSED;
        long finishedAt = System.currentTimeMillis();
        long wallMs = Math.max(0L, finishedAt - session.record.startedAt());
        int totalTests = passed + failed + skipped;
        int totalBlobs = session.cachedBlobs + session.uploadedBlobs;

        SessionSummary summary = new SessionSummary(
                totalTests,
                passed,
                failed,
                skipped,
                totalBlobs,
                session.cachedBlobs,
                serialMs,
                wallMs
        );
        store.updateSessionStatus(sessionId, finalStatus, finishedAt, summary);

        BuildSessionRecord updated = store.findSession(sessionId).orElse(session.record);
        session.record = updated;
        eventHub.publish("session.completed", sessionId, null, null, Map.of(
                "status", finalStatus.name(),
                "totalTests", totalTests,
                "passedTests", passed,
                "failedTests", failed,
                "skippedTests", skipped,
                "serialDurationMs", serialMs,
                "wallDurationMs", wallMs
        ));
        return updated;
    }

    public synchronized Optional<SessionDetailResponse> getSessionDetail(String sessionId) {
        return store.findSession(sessionId).map(session -> new SessionDetailResponse(
                session,
                store.findChangedFiles(sessionId),
                store.listPartitions(sessionId),
                store.listTestExecutions(sessionId)
        ));
    }

    public List<BuildSessionRecord> listSessions(int limit) {
        return store.listSessions(limit);
    }

    public List<WorkerRecord> listWorkers() {
        return store.listWorkers();
    }

    public List<TestHistorySummary> listTestHistory(int limit) {
        return store.listTestHistory(limit);
    }

    public int seedDemoData() {
        int count = DemoDataSeeder.seed(store);
        eventHub.publish("demo.seeded", null, null, null, Map.of("sessionsSeeded", count));
        return count;
    }

    public synchronized void registerWorker(String connectionId, WorkerRegisterPayload payload, Consumer<WorkerWsMessage> sender) {
        String workerId = payload.workerId() != null && !payload.workerId().isBlank()
                ? payload.workerId()
                : "worker-" + connectionId.substring(0, Math.min(8, connectionId.length()));
        Map<String, String> caps = payload.capabilities() != null ? Map.copyOf(payload.capabilities()) : Map.of();
        WorkerConnection conn = new WorkerConnection(
                connectionId,
                workerId,
                payload.host() != null ? payload.host() : "unknown",
                caps,
                sender
        );
        workers.put(connectionId, conn);
        persistWorker(conn);

        sender.accept(new WorkerWsMessage(WorkerWsMessage.WORKER_REGISTERED, Map.of("workerId", workerId)));
        eventHub.publish("worker.registered", null, null, workerId, Map.of(
                "host", conn.host,
                "capabilities", caps));

        dispatchPendingPartitions();
    }

    public synchronized void handleWorkerMessage(String connectionId, WorkerWsMessage message) {
        WorkerConnection conn = workers.get(connectionId);
        if (conn == null) {
            return;
        }
        conn.lastHeartbeatAt = System.currentTimeMillis();

        switch (message.type()) {
            case WorkerWsMessage.WORKER_HEARTBEAT -> {
                WorkerHeartbeatPayload hb = DistributionJson.convert(message.payload(), WorkerHeartbeatPayload.class);
                if (hb != null && hb.activePartitionId() != null) {
                    PartitionRuntime pr = partitions.get(hb.activePartitionId());
                    if (pr != null) {
                        pr.lastProgressAt = System.currentTimeMillis();
                    }
                }
                persistWorker(conn);
            }
            case WorkerWsMessage.WORKER_DRAINING -> {
                conn.state = WorkerState.DRAINING;
                persistWorker(conn);
                eventHub.publish("worker.draining", null, conn.activePartitionId, conn.workerId, Map.of());
            }
            case WorkerWsMessage.PARTITION_STARTED -> {
                String partitionId = stringField(message.payload(), "partitionId");
                PartitionRuntime pr = partitionId != null ? partitions.get(partitionId) : null;
                if (pr != null) {
                    long now = System.currentTimeMillis();
                    pr.lastProgressAt = now;
                    pr.record = new PartitionRecord(
                            pr.record.partitionId(),
                            pr.record.sessionId(),
                            conn.workerId,
                            PartitionState.RUNNING,
                            pr.record.attempt(),
                            pr.record.estimatedDurationMs(),
                            pr.record.startedAt() > 0L ? pr.record.startedAt() : now,
                            0L,
                            pr.record.requiredCapabilities(),
                            pr.record.workUnits()
                    );
                    store.savePartition(pr.record);
                    eventHub.publish("partition.started", pr.record.sessionId(), partitionId, conn.workerId, Map.of(
                            "attempt", pr.record.attempt()));
                }
            }
            case WorkerWsMessage.TEST_EVENT -> {
                WorkerTestEventPayload evt = DistributionJson.convert(message.payload(), WorkerTestEventPayload.class);
                if (evt != null) {
                    PartitionRuntime pr = partitions.get(evt.partitionId());
                    if (pr != null) {
                        pr.lastProgressAt = System.currentTimeMillis();
                    }
                    if (evt.status() != TestOutcomeStatus.STARTED) {
                        TestExecutionRecord execRecord = new TestExecutionRecord(
                                UUID.randomUUID().toString(),
                                evt.sessionId(),
                                evt.partitionId(),
                                conn.workerId,
                                evt.workUnitKey(),
                                evt.className(),
                                evt.testName(),
                                evt.status(),
                                evt.durationMs(),
                                evt.attempt(),
                                evt.attempt() > 1 && evt.status() == TestOutcomeStatus.PASSED,
                                evt.errorMessage(),
                                evt.stackTrace(),
                                evt.endpoints() != null ? evt.endpoints() : List.of(),
                                System.currentTimeMillis()
                        );
                        store.saveTestExecution(execRecord);
                    }
                    @SuppressWarnings("unchecked")
                    Map<String, Object> evtData = DistributionJson.convert(evt, Map.class);
                    eventHub.publish("test.event", evt.sessionId(), evt.partitionId(), conn.workerId, evtData);
                }
            }
            case WorkerWsMessage.PARTITION_COMPLETED -> {
                PartitionCompletedPayload done = DistributionJson.convert(message.payload(), PartitionCompletedPayload.class);
                if (done != null) {
                    completePartition(conn, done);
                }
            }
            default -> LOG.debug("Ignoring unknown worker message type {}", message.type());
        }
    }

    public synchronized void handleWorkerDisconnect(String connectionId) {
        WorkerConnection conn = workers.remove(connectionId);
        if (conn == null) {
            return;
        }
        conn.state = WorkerState.OFFLINE;
        String inFlightPartitionId = conn.activePartitionId;
        conn.activePartitionId = null;
        persistWorker(conn);

        eventHub.publish("worker.offline", null, inFlightPartitionId, conn.workerId, Map.of());

        if (inFlightPartitionId != null) {
            PartitionRuntime pr = partitions.get(inFlightPartitionId);
            if (pr != null && pr.record.state() != PartitionState.COMPLETED && pr.record.state() != PartitionState.LOST) {
                requeueOrLosePartition(pr, conn.workerId, "Worker disconnected during execution");
            }
        }
        dispatchPendingPartitions();
    }

    public synchronized void sweepExpiredLeases() {
        long cutoff = System.currentTimeMillis() - config.leaseTimeoutMs();
        for (PartitionRuntime pr : partitions.values()) {
            if ((pr.record.state() == PartitionState.ASSIGNED || pr.record.state() == PartitionState.RUNNING)
                    && pr.lastProgressAt < cutoff) {
                String stalledWorkerId = pr.record.workerId();
                requeueOrLosePartition(pr, stalledWorkerId, "Worker heartbeat lease expired");
            }
        }
        dispatchPendingPartitions();
    }

    private void completePartition(WorkerConnection conn, PartitionCompletedPayload done) {
        PartitionRuntime pr = partitions.get(done.partitionId());
        if (pr != null) {
            PartitionState endState = done.success() ? PartitionState.COMPLETED : PartitionState.FAILED;
            long now = System.currentTimeMillis();
            long started = pr.record.startedAt() > 0L ? pr.record.startedAt() : now;
            pr.record = new PartitionRecord(
                    pr.record.partitionId(),
                    pr.record.sessionId(),
                    conn.workerId,
                    endState,
                    pr.record.attempt(),
                    pr.record.estimatedDurationMs(),
                    started,
                    now,
                    pr.record.requiredCapabilities(),
                    pr.record.workUnits()
            );
            store.savePartition(pr.record);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("state", endState.name());
            data.put("attempt", pr.record.attempt());
            if (done.errorMessage() != null) {
                data.put("errorMessage", done.errorMessage());
            }
            eventHub.publish("partition.completed", pr.record.sessionId(), pr.record.partitionId(), conn.workerId, data);
        }

        if (done.partitionId().equals(conn.activePartitionId)) {
            conn.activePartitionId = null;
        }
        if (conn.state != WorkerState.DRAINING && conn.state != WorkerState.OFFLINE) {
            conn.state = WorkerState.IDLE;
        }
        persistWorker(conn);
        dispatchPendingPartitions();
    }

    private void requeueOrLosePartition(PartitionRuntime pr, String previousWorkerId, String reason) {
        if (pr.retriesUsed < pr.maxRetries) {
            pr.retriesUsed++;
            pr.record = new PartitionRecord(
                    pr.record.partitionId(),
                    pr.record.sessionId(),
                    null,
                    PartitionState.PENDING,
                    pr.record.attempt(),
                    pr.record.estimatedDurationMs(),
                    0L,
                    0L,
                    pr.record.requiredCapabilities(),
                    pr.record.workUnits()
            );
            store.savePartition(pr.record);
            eventHub.publish("partition.requeued", pr.record.sessionId(), pr.record.partitionId(), previousWorkerId, Map.of(
                    "reason", reason,
                    "retriesUsed", pr.retriesUsed,
                    "maxRetries", pr.maxRetries
            ));
        } else {
            pr.record = new PartitionRecord(
                    pr.record.partitionId(),
                    pr.record.sessionId(),
                    previousWorkerId,
                    PartitionState.LOST,
                    pr.record.attempt(),
                    pr.record.estimatedDurationMs(),
                    pr.record.startedAt(),
                    System.currentTimeMillis(),
                    pr.record.requiredCapabilities(),
                    pr.record.workUnits()
            );
            store.savePartition(pr.record);
            eventHub.publish("partition.lost", pr.record.sessionId(), pr.record.partitionId(), previousWorkerId, Map.of(
                    "reason", reason,
                    "workUnits", pr.record.workUnits().stream().map(WorkUnitSpec::key).toList()
            ));
        }
    }

    private void dispatchPendingPartitions() {
        List<PartitionRuntime> pending = partitions.values().stream()
                .filter(p -> p.record.state() == PartitionState.PENDING)
                .sorted((a, b) -> Long.compare(b.record.estimatedDurationMs(), a.record.estimatedDurationMs()))
                .toList();

        for (PartitionRuntime pr : pending) {
            WorkerConnection matchingWorker = findIdleMatchingWorker(pr.record.requiredCapabilities());
            if (matchingWorker == null) {
                continue;
            }
            int nextAttempt = pr.retriesUsed + 1;
            long now = System.currentTimeMillis();
            pr.lastProgressAt = now;
            pr.record = new PartitionRecord(
                    pr.record.partitionId(),
                    pr.record.sessionId(),
                    matchingWorker.workerId,
                    PartitionState.ASSIGNED,
                    nextAttempt,
                    pr.record.estimatedDurationMs(),
                    now,
                    0L,
                    pr.record.requiredCapabilities(),
                    pr.record.workUnits()
            );
            store.savePartition(pr.record);

            matchingWorker.state = WorkerState.BUSY;
            matchingWorker.activePartitionId = pr.record.partitionId();
            persistWorker(matchingWorker);

            PartitionAssignPayload assignPayload = new PartitionAssignPayload(
                    pr.record.partitionId(),
                    pr.record.sessionId(),
                    nextAttempt,
                    pr.classpathArtifacts,
                    pr.systemProperties,
                    pr.record.workUnits()
            );
            @SuppressWarnings("unchecked")
            Map<String, Object> payloadMap = DistributionJson.convert(assignPayload, Map.class);
            matchingWorker.sender.accept(new WorkerWsMessage(WorkerWsMessage.PARTITION_ASSIGN, payloadMap));

            eventHub.publish("partition.assigned", pr.record.sessionId(), pr.record.partitionId(), matchingWorker.workerId, Map.of(
                    "attempt", nextAttempt,
                    "workUnits", pr.record.workUnits().size()
            ));
        }
    }

    private WorkerConnection findIdleMatchingWorker(Map<String, String> requiredCapabilities) {
        for (WorkerConnection conn : workers.values()) {
            if (conn.state == WorkerState.IDLE && conn.activePartitionId == null
                    && CapabilityMatcher.matches(requiredCapabilities, conn.capabilities)) {
                return conn;
            }
        }
        return null;
    }

    private void persistWorker(WorkerConnection conn) {
        store.upsertWorker(new WorkerRecord(
                conn.workerId,
                conn.host,
                conn.capabilities,
                conn.state,
                conn.activePartitionId,
                conn.lastHeartbeatAt,
                conn.connectedAt
        ));
    }

    private SessionRuntime getOrRehydrateSession(String sessionId) {
        SessionRuntime existing = sessions.get(sessionId);
        if (existing != null) {
            return existing;
        }
        Optional<BuildSessionRecord> stored = store.findSession(sessionId);
        if (stored.isEmpty()) {
            return null;
        }
        BuildSessionRecord record = stored.get();
        SessionRuntime rehydrated = new SessionRuntime(
                record,
                config.defaultMaxPartitionSize(),
                config.defaultMaxRetries());
        rehydrated.cachedBlobs = Math.max(0, record.cachedBlobs());
        rehydrated.uploadedBlobs = Math.max(0, record.totalBlobs() - record.cachedBlobs());
        for (PartitionRecord part : store.listPartitions(sessionId)) {
            rehydrated.partitionIds.add(part.partitionId());
        }
        sessions.put(sessionId, rehydrated);
        return rehydrated;
    }

    private static String stringField(Map<String, Object> map, String key) {
        if (map == null) {
            return null;
        }
        Object v = map.get(key);
        return v != null ? v.toString() : null;
    }

    private static final class SessionRuntime {
        private BuildSessionRecord record;
        private final int maxPartitionSize;
        private final int maxRetries;
        private final List<String> partitionIds = new ArrayList<>();
        private int cachedBlobs;
        private int uploadedBlobs;

        private SessionRuntime(BuildSessionRecord record, int maxPartitionSize, int maxRetries) {
            this.record = record;
            this.maxPartitionSize = maxPartitionSize;
            this.maxRetries = maxRetries;
        }
    }

    private static final class PartitionRuntime {
        private PartitionRecord record;
        private final String moduleName;
        private final List<ArtifactRef> classpathArtifacts;
        private final Map<String, String> systemProperties;
        private final int maxRetries;
        private int retriesUsed;
        private long lastProgressAt = System.currentTimeMillis();

        private PartitionRuntime(PartitionRecord record,
                                 String moduleName,
                                 List<ArtifactRef> classpathArtifacts,
                                 Map<String, String> systemProperties,
                                 int maxRetries) {
            this.record = record;
            this.moduleName = moduleName;
            this.classpathArtifacts = classpathArtifacts;
            this.systemProperties = systemProperties;
            this.maxRetries = maxRetries;
        }
    }

    private static final class WorkerConnection {
        private final String connectionId;
        private final String workerId;
        private final String host;
        private final Map<String, String> capabilities;
        private final Consumer<WorkerWsMessage> sender;
        private final long connectedAt = System.currentTimeMillis();
        private long lastHeartbeatAt = System.currentTimeMillis();
        private WorkerState state = WorkerState.IDLE;
        private String activePartitionId;

        private WorkerConnection(String connectionId,
                                 String workerId,
                                 String host,
                                 Map<String, String> capabilities,
                                 Consumer<WorkerWsMessage> sender) {
            this.connectionId = connectionId;
            this.workerId = workerId;
            this.host = host;
            this.capabilities = capabilities;
            this.sender = sender;
        }
    }
}
