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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.citrusframework.distribution.TestExecutionIdentity;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionCompletedPayload;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.ReportLocalTestsRequest;
import org.citrusframework.distribution.model.SessionDetailResponse;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SessionSummary;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.TestExecutionRecord;
import org.citrusframework.distribution.model.WorkspaceChange;
import org.citrusframework.distribution.model.TestHistoryRun;
import org.citrusframework.distribution.model.TestHistorySummary;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerHeartbeatPayload;
import org.citrusframework.distribution.model.WorkerCacheStats;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerState;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.citrusframework.distribution.scheduler.CapabilityMatcher;
import org.citrusframework.distribution.scheduler.LptPartitionScheduler;
import org.citrusframework.distribution.store.DistributionStore;
import org.citrusframework.distribution.store.DistributionStore.RecoverableWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe coordinator for build sessions, LPT partition scheduling, worker matching, and fault recovery.
 */
public final class SessionCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(SessionCoordinator.class);

    private final DistributionCoordinatorConfig config;
    private final DistributionStore store;
    private final CoordinatorEventStreamHub eventHub;
    private final LptPartitionScheduler scheduler = new LptPartitionScheduler();

    private final Map<String, SessionRuntime> sessions = new ConcurrentHashMap<>();
    private final Map<String, PartitionRuntime> partitions = new ConcurrentHashMap<>();
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private long assignmentSequence;

    public SessionCoordinator(DistributionCoordinatorConfig config, DistributionStore store, CoordinatorEventStreamHub eventHub) {
        this.config = config;
        this.store = store;
        this.eventHub = eventHub;
    }

    synchronized void restorePendingWork() {
        for (WorkerRecord worker : store.listWorkers()) {
            if (worker.state() != WorkerState.OFFLINE) {
                store.upsertWorker(new WorkerRecord(worker.workerId(), worker.host(), worker.capabilities(), WorkerState.OFFLINE,
                        null, worker.lastHeartbeatAt(), worker.registeredAt(), worker.cacheStats()));
            }
        }
        Map<String, Map<String, PartitionRecord>> latest = new HashMap<>();
        for (RecoverableWork work : store.listRecoverableWork()) {
            Map<String, PartitionRecord> records = latest.computeIfAbsent(work.sessionId(), sessionId -> {
                Map<String, PartitionRecord> byId = new HashMap<>();
                for (PartitionRecord record : store.listPartitions(sessionId)) {
                    byId.merge(record.partitionId(), record, (a, b) -> a.attempt() > b.attempt() ? a : b);
                }
                return byId;
            });
            for (String partitionId : work.response().partitionIds()) {
                PartitionRecord record = records.get(partitionId);
                if (record == null || (record.state() != PartitionState.PENDING
                        && record.state() != PartitionState.ASSIGNED && record.state() != PartitionState.RUNNING)) {
                    continue;
                }
                PartitionRuntime runtime = new PartitionRuntime(record, work.request(), work.maxRetries());
                runtime.retriesUsed = Math.max(0, record.attempt() - 1);
                runtime.lastAssignedWorkerId = record.workerId();
                partitions.put(partitionId, runtime);
                if (record.state() != PartitionState.PENDING) {
                    restoreInFlightTests(runtime);
                    requeueOrLosePartition(runtime, record.workerId(), "Coordinator restarted during execution");
                }
            }
        }
    }

    private void restoreInFlightTests(PartitionRuntime runtime) {
        PartitionRecord partition = runtime.record;
        SessionRuntime session = getOrRehydrateSession(partition.sessionId());
        store.replayEvents(partition.sessionId(), 0, 0, event -> {
            if ("test.event".equals(event.type()) && partition.partitionId().equals(event.partitionId())
                    && intField(event.data(), "attempt") == partition.attempt()) {
                WorkerTestEventPayload test = DistributionJson.convert(event.data(), WorkerTestEventPayload.class);
                String key = session.testKey(test.workUnitKey(), test.className(), test.testName(), test.uniqueId());
                if (test.status() == TestOutcomeStatus.STARTED) {
                    runtime.inFlightTestKeys.add(key);
                } else {
                    runtime.inFlightTestKeys.remove(key);
                }
            }
        });
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
        String historyScope = DistributionSettings.historyScope(request.projectKey(), record.projectName());
        store.saveSession(record, request.maxPartitionSize(), historyScope, maxRetries);
        store.saveChangedFiles(sessionId, request.changedFiles(), request.workspaceChanges());

        sessions.put(sessionId, new SessionRuntime(record, maxPartSize, maxRetries, request.maxPartitionSize(), historyScope));
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
        Optional<SubmitWorkResponse> persisted = store.findWorkSubmission(sessionId, request.submissionId());
        if (persisted.isPresent()) {
            return persisted.get();
        }

        // Pre-workspace workers cannot materialize inputs, return outputs or expand the workspace token that hosts
        // put into forwarded system properties.
        boolean needsWorkspace = request.forkWorkspace()
                || !request.inputArtifacts().isEmpty()
                || !request.outputPaths().isEmpty()
                || (request.systemProperties() != null && request.systemProperties().values().stream()
                        .anyMatch(value -> value != null && value.contains(DistributionSettings.WORKSPACE_TOKEN)));
        List<WorkUnitSpec> units = request.workUnits() != null ? request.workUnits() : List.of();
        if (needsWorkspace) {
            units = units.stream().map(SessionCoordinator::requireWorkspaceCapability).toList();
        }
        List<String> keys = units.stream().map(WorkUnitSpec::key).toList();
        Map<String, Long> historicalDurations = store.findHistoricalDurationsMs(keys, 10, session.historyScope);

        boolean adaptive = session.requestedMaxPartitionSize <= 0 && request.outputPaths().isEmpty();

        // Schedule before touching any state: an invalid SINGLE submission must leave no trace.
        List<PartitionRecord> scheduled = scheduler.schedule(
                sessionId,
                units,
                historicalDurations,
                config.defaultDurationMs(),
                session.maxPartitionSize,
                request.partitionPolicy(),
                adaptive ? config.minPartitionDurationMs() : 0L,
                adaptive ? config.maxWavesPerWorker() : 0,
                this::countEligibleOnlineWorkers);

        session.cachedBlobs += Math.max(0, request.cachedBlobs());
        session.uploadedBlobs += Math.max(0, request.uploadedBlobs());
        session.submittedUnitCount += units.size();
        List<String> inputPaths = request.inputArtifacts().stream()
                .map(WorkspaceFileSet::relativePath)
                .toList();

        List<String> partitionIds = new ArrayList<>(scheduled.size());
        List<PartitionRuntime> runtimes = new ArrayList<>();
        for (PartitionRecord part : scheduled) {
            PartitionRecord enriched = new PartitionRecord(
                    part.partitionId(),
                    part.sessionId(),
                    part.workerId(),
                    part.state(),
                    part.attempt(),
                    part.estimatedDurationMs(),
                    part.startedAt(),
                    part.finishedAt(),
                    part.requiredCapabilities(),
                    part.workUnits(),
                    request.moduleName(),
                    request.forkWorkspace(),
                    request.workingDirectory(),
                    inputPaths,
                    request.outputPaths(),
                    List.of(),
                    null
            );
            PartitionRuntime runtime = new PartitionRuntime(
                    enriched, request, session.maxRetries);
            runtimes.add(runtime);
            partitionIds.add(enriched.partitionId());
        }
        SubmitWorkResponse response = new SubmitWorkResponse(
                request.submissionId(),
                scheduled.size(),
                partitionIds,
                request.cachedBlobs(),
                request.uploadedBlobs()
        );
        eventHub.publishSubmission(new RecoverableWork(sessionId, request, response, session.maxRetries),
                runtimes.stream().map(runtime -> runtime.record).toList(), Map.of(
                        "moduleName", request.moduleName() != null ? request.moduleName() : "",
                        "partitionsCreated", scheduled.size(),
                        "workUnits", units.size(),
                        "cachedBlobs", request.cachedBlobs(),
                        "uploadedBlobs", request.uploadedBlobs()));
        for (PartitionRuntime runtime : runtimes) {
            partitions.put(runtime.record.partitionId(), runtime);
            session.partitionIds.add(runtime.record.partitionId());
        }
        refreshRunningSessionSummary(session);

        dispatchPendingPartitions();
        return response;
    }

    public synchronized int reportLocalTests(String sessionId, ReportLocalTestsRequest request) {
        SessionRuntime session = getOrRehydrateSession(sessionId);
        if (session == null) {
            throw new IllegalArgumentException("Unknown sessionId: " + sessionId);
        }
        if (request == null || request.executions().isEmpty()) {
            return 0;
        }
        String workerId = (request.workerId() != null && !request.workerId().isBlank())
                ? request.workerId()
                : "local-host";
        String deliveryId = request.deliveryId();
        if (deliveryId != null) {
            if (!validDeliveryId(deliveryId) || request.executions().size() != 1) {
                throw new IllegalArgumentException("A local result delivery must contain one test and a valid delivery ID");
            }
            if (store.hasDelivery(deliveryId)) {
                return 0;
            }
        }
        int recorded = 0;
        for (WorkerTestEventPayload evt : request.executions()) {
            if (evt == null || evt.status() == null || evt.status() == TestOutcomeStatus.STARTED) {
                continue;
            }
            String testKey = session.testKey(evt.workUnitKey(), evt.className(), evt.testName(), evt.uniqueId());
            int localAttempt = session.nextAttemptFor(testKey);
            boolean flaky = localAttempt > 1
                    && evt.status() == TestOutcomeStatus.PASSED
                    && session.failedOrInterruptedTests.contains(testKey);
            TestExecutionRecord execRecord = new TestExecutionRecord(
                    deliveryId == null ? UUID.randomUUID().toString() : deliveryId,
                    sessionId,
                    "local",
                    workerId,
                    evt.workUnitKey(),
                    evt.className(),
                    evt.testName(),
                    evt.status(),
                    evt.durationMs(),
                    localAttempt,
                    flaky,
                    evt.errorMessage(),
                    evt.stackTrace(),
                    evt.endpoints() != null ? evt.endpoints() : List.of(),
                    System.currentTimeMillis(),
                    evt.uniqueId()
            );

            session.recordExecution(execRecord);
            if (evt.workUnitKey() != null && !evt.workUnitKey().isBlank()) {
                session.locallyExecutedWorkUnitKeys.add(evt.workUnitKey());
            }
            session.reportedLocalTestCount++;
            recorded++;

            @SuppressWarnings("unchecked")
            Map<String, Object> evtData = DistributionJson.convert(evt, Map.class);
            evtData.put("sessionId", sessionId);
            evtData.put("partitionId", "local");
            eventHub.publish("test.event", sessionId, "local", workerId, evtData, deliveryId, execRecord, List.of());
        }
        if (recorded > 0) {
            refreshRunningSessionSummary(session);
        }
        return recorded;
    }

    public synchronized BuildSessionRecord abortSession(String sessionId, String reason) {
        SessionRuntime session = getOrRehydrateSession(sessionId);
        if (session == null) {
            throw new IllegalArgumentException("Unknown sessionId: " + sessionId);
        }
        if (session.record.status() != SessionStatus.RUNNING) {
            return session.record;
        }
        String abortReason = (reason != null && !reason.isBlank()) ? reason : "Session aborted";
        long now = System.currentTimeMillis();

        Set<String> abortedPartitionIds = new HashSet<>();
        for (PartitionRuntime pr : partitions.values()) {
            if (!sessionId.equals(pr.record.sessionId())) {
                continue;
            }
            if (pr.record.state() == PartitionState.ASSIGNED || pr.record.state() == PartitionState.RUNNING) {
                String activeWorkerId = pr.record.workerId();
                abortedPartitionIds.add(pr.record.partitionId());
                releaseStalledWorker(pr);
                pr.activeConnectionId = null;
                pr.inFlightTestKeys.clear();
                pr.record = pr.buildRecord(
                        activeWorkerId,
                        PartitionState.LOST,
                        pr.record.attempt(),
                        pr.record.startedAt() > 0L ? pr.record.startedAt() : now,
                        now,
                        pr.record.producedOutputs(),
                        abortReason
                );
                store.savePartition(pr.record);
            } else if (pr.record.state() == PartitionState.PENDING) {
                abortedPartitionIds.add(pr.record.partitionId());
                pr.record = pr.buildRecord(
                        null,
                        PartitionState.LOST,
                        pr.record.attempt(),
                        0L,
                        now,
                        List.of(),
                        abortReason
                );
                store.savePartition(pr.record);
            }
        }

        for (PartitionRecord persisted : store.listPartitions(sessionId)) {
            if (persisted.state() == PartitionState.ASSIGNED
                    || persisted.state() == PartitionState.RUNNING
                    || persisted.state() == PartitionState.PENDING) {
                abortedPartitionIds.add(persisted.partitionId());
                store.savePartition(new PartitionRecord(
                        persisted.partitionId(),
                        persisted.sessionId(),
                        persisted.workerId(),
                        PartitionState.LOST,
                        persisted.attempt(),
                        persisted.estimatedDurationMs(),
                        persisted.state() == PartitionState.PENDING ? 0L : (persisted.startedAt() > 0L ? persisted.startedAt() : now),
                        now,
                        persisted.requiredCapabilities(),
                        persisted.workUnits(),
                        persisted.moduleName(),
                        persisted.forkWorkspace(),
                        persisted.workingDirectory(),
                        persisted.inputPaths(),
                        persisted.outputPaths(),
                        persisted.producedOutputs(),
                        abortReason
                ));
            }
        }

        if (!abortedPartitionIds.isEmpty()) {
            Set<String> liveWorkerIds = new HashSet<>();
            for (WorkerConnection wc : workers.values()) {
                liveWorkerIds.add(wc.workerId);
            }
            for (WorkerRecord wr : store.listWorkers()) {
                if (wr.state() == WorkerState.BUSY
                        && wr.activePartitionId() != null
                        && abortedPartitionIds.contains(wr.activePartitionId())
                        && !liveWorkerIds.contains(wr.workerId())) {
                    store.upsertWorker(new WorkerRecord(
                            wr.workerId(),
                            wr.host(),
                            wr.capabilities(),
                            WorkerState.IDLE,
                            null,
                            wr.lastHeartbeatAt(),
                            wr.registeredAt(),
                            wr.cacheStats()
                    ));
                }
            }
        }

        SessionSummary summary = computeSessionSummary(session, null, now);
        eventHub.finishSession("session.aborted", sessionId, now, SessionStatus.ABORTED, summary, abortReason, Map.of(
                "status", SessionStatus.ABORTED.name(),
                "reason", abortReason,
                "totalTests", summary.totalTests(),
                "passedTests", summary.passedTests(),
                "failedTests", summary.failedTests(),
                "skippedTests", summary.skippedTests(),
                "wallDurationMs", summary.wallDurationMs()
        ));
        BuildSessionRecord updated = store.findSession(sessionId).orElse(session.record);
        discardSessionRuntime(sessionId);
        dispatchPendingPartitions();
        return updated;
    }

    public synchronized BuildSessionRecord completeSession(String sessionId, CompleteSessionRequest request) {
        SessionRuntime session = getOrRehydrateSession(sessionId);
        if (session == null) {
            throw new IllegalArgumentException("Unknown sessionId: " + sessionId);
        }
        if (session.record.status() != SessionStatus.RUNNING) {
            return session.record;
        }

        long finishedAt = System.currentTimeMillis();
        SessionSummary summary = computeSessionSummary(session, request, finishedAt);

        Map<String, PartitionRecord> latestPartitions = new LinkedHashMap<>();
        for (PartitionRecord part : store.listPartitions(sessionId)) {
            PartitionRecord prev = latestPartitions.get(part.partitionId());
            if (prev == null || part.attempt() >= prev.attempt()) {
                latestPartitions.put(part.partitionId(), part);
            }
        }

        boolean unrecoveredPartitionFailure = false;
        for (PartitionRecord latest : latestPartitions.values()) {
            if (latest.state() == PartitionState.FAILED) {
                unrecoveredPartitionFailure = true;
                break;
            }
            if (latest.state() != PartitionState.COMPLETED) {
                boolean recoveredLocally = !latest.workUnits().isEmpty()
                        && latest.workUnits().stream().allMatch(u -> session.locallyExecutedWorkUnitKeys.contains(u.key()));
                if (!recoveredLocally) {
                    unrecoveredPartitionFailure = true;
                    break;
                }
            }
        }

        boolean buildFailed = request != null && request.buildFailed();
        SessionStatus finalStatus = (summary.failedTests() > 0 || unrecoveredPartitionFailure || buildFailed)
                ? SessionStatus.FAILED
                : SessionStatus.PASSED;
        String failureReason = finalStatus == SessionStatus.FAILED
                ? describeFailure(request, summary, unrecoveredPartitionFailure)
                : null;
        eventHub.finishSession("session.completed", sessionId, finishedAt, finalStatus, summary, failureReason, Map.of(
                "status", finalStatus.name(),
                "totalTests", summary.totalTests(),
                "passedTests", summary.passedTests(),
                "failedTests", summary.failedTests(),
                "skippedTests", summary.skippedTests(),
                "serialDurationMs", summary.serialDurationMs(),
                "wallDurationMs", summary.wallDurationMs()
        ));
        BuildSessionRecord updated = store.findSession(sessionId).orElse(session.record);
        discardSessionRuntime(sessionId);
        return updated;
    }

    private void discardSessionRuntime(String sessionId) {
        sessions.remove(sessionId);
        partitions.values().removeIf(partition -> sessionId.equals(partition.record.sessionId()));
    }

    private static String describeFailure(CompleteSessionRequest request, SessionSummary summary,
                                          boolean unrecoveredPartitionFailure) {
        if (request != null && request.failureReason() != null && !request.failureReason().isBlank()) {
            return summary.failedTests() > 0
                    ? request.failureReason() + " (" + summary.failedTests() + " test(s) failed)"
                    : request.failureReason();
        }
        if (unrecoveredPartitionFailure) {
            return "A partition did not complete and was not recovered locally";
        }
        if (summary.failedTests() > 0) {
            return summary.failedTests() + " test(s) failed";
        }
        return "Build failed outside distributed test execution (no reason reported by the launcher)";
    }

    public synchronized Optional<SessionDetailResponse> getSessionDetail(String sessionId) {
        return store.findSession(sessionId).map(session -> {
            List<WorkspaceChange> changes = store.findWorkspaceChanges(sessionId);
            return new SessionDetailResponse(session, changes.stream().map(WorkspaceChange::path).toList(),
                    store.listPartitions(sessionId), store.listTestExecutions(sessionId), changes);
        });
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

    public List<TestHistoryRun> listTestHistoryRuns(String scope, String workUnitKey, String className,
                                                   String testName, int limit) {
        return store.listTestHistoryRuns(scope, workUnitKey, className, testName, limit);
    }

    public Map<String, Long> findHistoricalDurations(List<String> testKeys, int limit) {
        return findHistoricalDurations(testKeys, limit, null);
    }

    public Map<String, Long> findHistoricalDurations(List<String> testKeys, int limit, String historyScope) {
        if (testKeys == null || testKeys.isEmpty()) {
            return Map.of();
        }
        List<String> keys = testKeys.stream()
                .filter(Objects::nonNull)
                .filter(key -> !key.isBlank())
                .distinct()
                .toList();
        int recentLimit = limit <= 0 ? 10 : Math.min(limit, 100);
        return store.findHistoricalDurationsMs(keys, recentLimit, historyScope);
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
        conn.cacheStats = payload.cacheStats();
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
        long receivedAt = System.currentTimeMillis();
        boolean wasSilent = conn.lastHeartbeatAt < receivedAt - config.leaseTimeoutMs();
        boolean wasCancelling = conn.cancelledPartitionId != null;
        conn.lastHeartbeatAt = receivedAt;

        processWorkerMessage(conn, message);

        // Dispatch only after the message is fully handled, so a message that makes the worker eligible again (back
        // within the liveness window, or confirming a cancellation) is never itself taken as part of new work.
        if (wasSilent || (wasCancelling && conn.cancelledPartitionId == null)) {
            dispatchPendingPartitions();
        }
    }

    private void processWorkerMessage(WorkerConnection conn, WorkerWsMessage message) {
        switch (message.type()) {
            case WorkerWsMessage.WORKER_HEARTBEAT -> {
                WorkerHeartbeatPayload hb = DistributionJson.convert(message.payload(), WorkerHeartbeatPayload.class);
                if (hb != null && hb.cacheStats() != null) {
                    conn.cacheStats = hb.cacheStats();
                }
                if (hb != null && conn.cancelledPartitionId != null
                        && !conn.cancelledPartitionId.equals(hb.activePartitionId())) {
                    confirmCancellation(conn);
                }
                if (hb != null && hb.activePartitionId() != null) {
                    PartitionRuntime pr = partitions.get(hb.activePartitionId());
                    if (isActiveOn(pr, conn)) {
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
                int startedAttempt = intField(message.payload(), "attempt");
                if (!isActiveOn(pr, conn) || pr.record.state() != PartitionState.ASSIGNED
                        || (startedAttempt > 0 && startedAttempt != pr.record.attempt())) {
                    LOG.warn("Discarding {} for partition {} from worker {} (connection {}): not the active assignment",
                            message.type(), partitionId, conn.workerId, conn.connectionId);
                } else {
                    long now = System.currentTimeMillis();
                    pr.lastProgressAt = now;
                    pr.record = pr.buildRecord(
                            conn.workerId,
                            PartitionState.RUNNING,
                            pr.record.attempt(),
                            pr.record.startedAt() > 0L ? pr.record.startedAt() : now,
                            0L,
                            pr.record.producedOutputs(),
                            null
                    );
                    eventHub.publish("partition.started", pr.record.sessionId(), partitionId, conn.workerId, Map.of(
                            "attempt", pr.record.attempt()), null, null, List.of(pr.record));
                }
            }
            case WorkerWsMessage.TEST_EVENT -> handleTestEvent(conn, message.payload());
            case WorkerWsMessage.PARTITION_COMPLETED -> handleCompletion(conn, message.payload());
            default -> LOG.debug("Ignoring unknown worker message type {}", message.type());
        }
    }

    private void handleTestEvent(WorkerConnection conn, Map<String, Object> payload) {
        WorkerTestEventPayload evt = DistributionJson.convert(payload, WorkerTestEventPayload.class);
        if (evt == null || evt.status() == null) {
            return;
        }
        String deliveryId = stringField(payload, WorkerWsMessage.DELIVERY_ID);
        if (deliveryId != null) {
            if (!validDeliveryId(deliveryId)) {
                return;
            }
            if (store.hasDelivery(deliveryId)) {
                acknowledgeResult(conn, deliveryId);
                return;
            }
        }
        PartitionRuntime runtime = partitions.get(evt.partitionId());
        boolean live = acceptsAttempt(runtime, conn, evt.attempt());
        PartitionRecord record = live ? runtime.record : null;
        if (record == null && deliveryId != null && evt.sessionId() != null) {
            // A durable worker result can arrive after the coordinator requeued that attempt.
            record = store.listPartitions(evt.sessionId()).stream()
                    .filter(p -> p.partitionId().equals(evt.partitionId()) && p.attempt() == evt.attempt()
                            && conn.workerId.equals(p.workerId()))
                    .findFirst().orElse(null);
        }
        if (record == null) {
            return;
        }
        if (live) {
            runtime.lastProgressAt = System.currentTimeMillis();
        }
        boolean running = live || store.findSession(record.sessionId()).map(s -> s.status() == SessionStatus.RUNNING).orElse(false);
        SessionRuntime session = running ? getOrRehydrateSession(record.sessionId()) : null;
        String testKey = session != null ? session.testKey(evt.workUnitKey(), evt.className(), evt.testName(), evt.uniqueId())
                : TestExecutionIdentity.key(evt.workUnitKey(), evt.className(), evt.testName(), evt.uniqueId());
        TestExecutionRecord result = null;
        if (evt.status() == TestOutcomeStatus.STARTED) {
            if (live) {
                runtime.inFlightTestKeys.add(testKey);
            }
        } else {
            if (live) {
                runtime.inFlightTestKeys.remove(testKey);
            }
            boolean flaky = record.attempt() > 1 && evt.status() == TestOutcomeStatus.PASSED
                    && session != null && session.failedOrInterruptedTests.contains(testKey);
            result = new TestExecutionRecord(deliveryId == null ? UUID.randomUUID().toString() : deliveryId,
                    record.sessionId(), record.partitionId(), conn.workerId, evt.workUnitKey(), evt.className(),
                    evt.testName(), evt.status(), evt.durationMs(), record.attempt(), flaky, evt.errorMessage(),
                    evt.stackTrace(), evt.endpoints() == null ? List.of() : evt.endpoints(), System.currentTimeMillis(), evt.uniqueId());
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> data = DistributionJson.convert(evt, Map.class);
        data.put("sessionId", record.sessionId());
        data.put("partitionId", record.partitionId());
        eventHub.publish("test.event", record.sessionId(), record.partitionId(), conn.workerId, data, deliveryId, result, List.of());
        if (result != null && session != null) {
            session.recordExecution(result);
            refreshRunningSessionSummary(session);
        }
        if (deliveryId != null) {
            acknowledgeResult(conn, deliveryId);
        }
    }

    private void handleCompletion(WorkerConnection conn, Map<String, Object> payload) {
        PartitionCompletedPayload done = DistributionJson.convert(payload, PartitionCompletedPayload.class);
        if (done == null) {
            return;
        }
        String deliveryId = stringField(payload, WorkerWsMessage.DELIVERY_ID);
        if (deliveryId != null) {
            if (!validDeliveryId(deliveryId)) {
                return;
            }
            if (store.hasDelivery(deliveryId)) {
                acknowledgeResult(conn, deliveryId);
                return;
            }
        }
        boolean accepted = acceptsAttempt(partitions.get(done.partitionId()), conn, done.attempt());
        if (done.partitionId() != null && done.partitionId().equals(conn.cancelledPartitionId)) {
            confirmCancellation(conn);
        }
        if (accepted) {
            completePartition(conn, done, deliveryId);
        } else if (deliveryId != null && done.sessionId() != null && store.listPartitions(done.sessionId()).stream()
                .anyMatch(p -> p.partitionId().equals(done.partitionId()) && p.attempt() == done.attempt()
                        && conn.workerId.equals(p.workerId()))) {
            // Retain the completion for diagnostics without replacing a newer attempt.
            eventHub.publish("partition.stale-completion", done.sessionId(), done.partitionId(), conn.workerId,
                    payload, deliveryId, null, List.of());
        } else {
            return;
        }
        if (deliveryId != null) {
            acknowledgeResult(conn, deliveryId);
        }
    }

    private static boolean validDeliveryId(String id) {
        return id.matches("[0-9]{19}-[a-f0-9-]{36}");
    }

    private void acknowledgeResult(WorkerConnection conn, String deliveryId) {
        conn.sender.accept(new WorkerWsMessage(WorkerWsMessage.RESULT_ACK, Map.of(WorkerWsMessage.DELIVERY_ID, deliveryId)));
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
                releaseStalledWorker(pr);
                requeueOrLosePartition(pr, stalledWorkerId, "Worker heartbeat lease expired");
            }
        }
        dispatchPendingPartitions();
    }

    private void completePartition(WorkerConnection conn, PartitionCompletedPayload done, String deliveryId) {
        PartitionRuntime pr = partitions.get(done.partitionId());
        if (pr != null) {
            pr.activeConnectionId = null;
            pr.inFlightTestKeys.clear();
            PartitionState endState = done.success() ? PartitionState.COMPLETED : PartitionState.FAILED;
            long now = System.currentTimeMillis();
            long started = pr.record.startedAt() > 0L ? pr.record.startedAt() : now;
            List<String> producedOutputs = done.outputArtifacts().stream()
                    .map(WorkspaceFileSet::relativePath)
                    .toList();
            pr.record = pr.buildRecord(
                    conn.workerId,
                    endState,
                    pr.record.attempt(),
                    started,
                    now,
                    producedOutputs,
                    done.errorMessage()
            );
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("state", endState.name());
            data.put("attempt", pr.record.attempt());
            data.put("workUnits", pr.record.workUnits().stream().map(WorkUnitSpec::key).toList());
            if (done.errorMessage() != null) {
                data.put("errorMessage", done.errorMessage());
            }
            if (!done.outputArtifacts().isEmpty()) {
                data.put("outputArtifacts", done.outputArtifacts());
            }
            eventHub.publish("partition.completed", pr.record.sessionId(), pr.record.partitionId(), conn.workerId, data,
                    deliveryId, null, List.of(pr.record));
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
        pr.activeConnectionId = null;
        SessionRuntime session = getOrRehydrateSession(pr.record.sessionId());
        List<String> interrupted = List.copyOf(pr.inFlightTestKeys);
        if (session != null) {
            session.failedOrInterruptedTests.addAll(interrupted);
        }
        pr.inFlightTestKeys.clear();
        long now = System.currentTimeMillis();
        long startedAt = pr.record.startedAt() > 0L ? pr.record.startedAt() : now;
        if (pr.retriesUsed < pr.maxRetries) {
            PartitionRecord evictedAttempt = pr.buildRecord(
                    previousWorkerId,
                    PartitionState.LOST,
                    pr.record.attempt(),
                    startedAt,
                    now,
                    pr.record.producedOutputs(),
                    reason
            );
            pr.retriesUsed++;
            int nextAttempt = pr.retriesUsed + 1;
            pr.record = pr.buildRecord(
                    null,
                    PartitionState.PENDING,
                    nextAttempt,
                    0L,
                    0L,
                    List.of(),
                    null
            );
            eventHub.publish("partition.requeued", pr.record.sessionId(), pr.record.partitionId(), previousWorkerId, Map.of(
                    "reason", reason,
                    "retriesUsed", pr.retriesUsed,
                    "maxRetries", pr.maxRetries,
                    "nextAttempt", nextAttempt,
                    "interruptedTestKeys", interrupted
            ), null, null, List.of(evictedAttempt, pr.record));
        } else {
            pr.record = pr.buildRecord(
                    previousWorkerId,
                    PartitionState.LOST,
                    pr.record.attempt(),
                    startedAt,
                    now,
                    pr.record.producedOutputs(),
                    reason
            );
            eventHub.publish("partition.lost", pr.record.sessionId(), pr.record.partitionId(), previousWorkerId, Map.of(
                    "reason", reason,
                    "workUnits", pr.record.workUnits().stream().map(WorkUnitSpec::key).toList(),
                    "interruptedTestKeys", interrupted
            ), null, null, List.of(pr.record));
        }
    }

    private void dispatchPendingPartitions() {
        List<PartitionRuntime> pending = partitions.values().stream()
                .filter(p -> p.record.state() == PartitionState.PENDING)
                .sorted((a, b) -> Long.compare(b.record.estimatedDurationMs(), a.record.estimatedDurationMs()))
                .toList();

        for (PartitionRuntime pr : pending) {
            if (pr.lastAssignedWorkerId != null) {
                pr.earlierAttemptWorkerIds.add(pr.lastAssignedWorkerId);
            }
            WorkerConnection matchingWorker = findIdleMatchingWorker(pr.record.requiredCapabilities(),
                    pr.earlierAttemptWorkerIds);
            if (matchingWorker == null) {
                continue;
            }
            pr.lastAssignedWorkerId = matchingWorker.workerId;
            int nextAttempt = pr.retriesUsed + 1;
            long now = System.currentTimeMillis();
            pr.lastProgressAt = now;
            pr.record = pr.buildRecord(
                    matchingWorker.workerId,
                    PartitionState.ASSIGNED,
                    nextAttempt,
                    now,
                    0L,
                    List.of(),
                    null
            );
            eventHub.publish("partition.assigned", pr.record.sessionId(), pr.record.partitionId(), matchingWorker.workerId, Map.of(
                    "attempt", nextAttempt,
                    "workUnits", pr.record.workUnits().size()
            ), null, null, List.of(pr.record));

            matchingWorker.state = WorkerState.BUSY;
            matchingWorker.lastAssignedAt = ++assignmentSequence;
            matchingWorker.activePartitionId = pr.record.partitionId();
            pr.activeConnectionId = matchingWorker.connectionId;
            persistWorker(matchingWorker);

            PartitionAssignPayload assignPayload = new PartitionAssignPayload(
                    pr.record.partitionId(),
                    pr.record.sessionId(),
                    nextAttempt,
                    pr.classpathArtifacts,
                    pr.systemProperties,
                    pr.record.workUnits(),
                    pr.inputArtifacts,
                    pr.outputPaths,
                    pr.forkWorkspace,
                    pr.workingDirectory
            );
            @SuppressWarnings("unchecked")
            Map<String, Object> payloadMap = DistributionJson.convert(assignPayload, Map.class);
            matchingWorker.sender.accept(new WorkerWsMessage(WorkerWsMessage.PARTITION_ASSIGN, payloadMap));

        }
    }

    /**
     * True when {@code pr} is currently assigned to (or running on) the worker behind {@code conn}.
     */
    private static boolean isActiveOn(PartitionRuntime pr, WorkerConnection conn) {
        return pr != null
                && (pr.record.state() == PartitionState.ASSIGNED || pr.record.state() == PartitionState.RUNNING)
                && conn.connectionId.equals(pr.activeConnectionId);
    }

    /**
     * Fence for test events and completions: the partition must be active on this exact connection and the payload
     * must report the current attempt. Attempt {@code 0} (a sender that does not report attempts) is only accepted
     * from a worker that never held an earlier attempt of this partition: from any such worker it can only mean the
     * current attempt, whereas a worker that held an earlier attempt — even after reconnecting on a new connection —
     * may still be reporting that earlier run.
     */
    private static boolean acceptsAttempt(PartitionRuntime pr, WorkerConnection conn, int payloadAttempt) {
        if (!isActiveOn(pr, conn)) {
            return false;
        }
        return payloadAttempt == pr.record.attempt()
                || (payloadAttempt == 0 && !pr.earlierAttemptWorkerIds.contains(conn.workerId));
    }

    /**
     * Tells the worker holding an expired lease to stop (best effort) and detaches it from the partition. The worker
     * stays out of dispatch until it confirms the cancelled attempt has ended — by reporting a completion for that
     * partition (discarded, since the attempt is stale) or a heartbeat no longer naming it — because a worker whose
     * lease expired is usually frozen or still running the attempt.
     */
    private void releaseStalledWorker(PartitionRuntime pr) {
        WorkerConnection stalled = pr.activeConnectionId != null ? workers.get(pr.activeConnectionId) : null;
        if (stalled == null) {
            return;
        }
        try {
            stalled.sender.accept(new WorkerWsMessage(WorkerWsMessage.PARTITION_CANCEL, Map.of(
                    "partitionId", pr.record.partitionId(),
                    "sessionId", pr.record.sessionId(),
                    "attempt", pr.record.attempt())));
        } catch (RuntimeException e) {
            LOG.debug("Failed to send {} to worker {}", WorkerWsMessage.PARTITION_CANCEL, stalled.workerId, e);
        }
        if (pr.record.partitionId().equals(stalled.activePartitionId)) {
            stalled.activePartitionId = null;
        }
        stalled.cancelledPartitionId = pr.record.partitionId();
        persistWorker(stalled);
    }

    /**
     * The worker reported that its cancelled attempt has ended: make it eligible for new work again. Dispatch happens
     * once the confirming message has been handled (see {@link #handleWorkerMessage(String, WorkerWsMessage)}).
     */
    private void confirmCancellation(WorkerConnection conn) {
        conn.cancelledPartitionId = null;
        if (conn.state == WorkerState.BUSY && conn.activePartitionId == null) {
            conn.state = WorkerState.IDLE;
        }
        persistWorker(conn);
    }

    private static WorkUnitSpec requireWorkspaceCapability(WorkUnitSpec unit) {
        Map<String, String> capabilities = new LinkedHashMap<>();
        if (unit.requiredCapabilities() != null) {
            capabilities.putAll(unit.requiredCapabilities());
        }
        capabilities.put(DistributionSettings.WORKSPACE_CAPABILITY_KEY, "true");
        return new WorkUnitSpec(unit.key(), unit.className(), unit.methodName(), unit.dynamicSourceName(),
                capabilities, unit.estimatedDurationMs(), unit.enclosingClassNames());
    }

    private int countEligibleOnlineWorkers(Map<String, String> requiredCapabilities) {
        long aliveSince = System.currentTimeMillis() - config.leaseTimeoutMs();
        return (int) workers.values().stream()
                .filter(conn -> conn.state == WorkerState.IDLE || conn.state == WorkerState.BUSY)
                .filter(conn -> conn.cancelledPartitionId == null && conn.lastHeartbeatAt >= aliveSince)
                .filter(conn -> CapabilityMatcher.matches(requiredCapabilities, conn.capabilities))
                .map(conn -> conn.workerId)
                .distinct()
                .count();
    }

    /**
     * An idle worker that is not finishing a cancelled attempt, has been heard from within the lease timeout, and
     * satisfies {@code requiredCapabilities}. Workers outside {@code preferAvoiding} (those that ran earlier attempts
     * of the partition) are chosen first.
     */
    private WorkerConnection findIdleMatchingWorker(Map<String, String> requiredCapabilities, Set<String> preferAvoiding) {
        long aliveSince = System.currentTimeMillis() - config.leaseTimeoutMs();
        // Least recently assigned first, so partitions spread over the pool instead of always hitting the worker that
        // happens to come first; a worker an earlier attempt ran on is only a fallback.
        WorkerConnection best = null;
        WorkerConnection fallback = null;
        for (WorkerConnection conn : workers.values()) {
            if (conn.state == WorkerState.IDLE && conn.activePartitionId == null && conn.cancelledPartitionId == null
                    && conn.lastHeartbeatAt >= aliveSince
                    && CapabilityMatcher.matches(requiredCapabilities, conn.capabilities)) {
                if (!preferAvoiding.contains(conn.workerId)) {
                    if (best == null || assignedEarlier(conn, best)) {
                        best = conn;
                    }
                } else if (fallback == null || assignedEarlier(conn, fallback)) {
                    fallback = conn;
                }
            }
        }
        return best != null ? best : fallback;
    }

    private static boolean assignedEarlier(WorkerConnection a, WorkerConnection b) {
        if (a.lastAssignedAt != b.lastAssignedAt) {
            return a.lastAssignedAt < b.lastAssignedAt;
        }
        return a.workerId.compareTo(b.workerId) < 0;
    }

    private void persistWorker(WorkerConnection conn) {
        store.upsertWorker(new WorkerRecord(
                conn.workerId,
                conn.host,
                conn.capabilities,
                conn.state,
                conn.activePartitionId,
                conn.lastHeartbeatAt,
                conn.connectedAt,
                conn.cacheStats
        ));
    }

    private void refreshRunningSessionSummary(SessionRuntime session) {
        if (session == null || session.record.status() != SessionStatus.RUNNING) {
            return;
        }
        int passed = 0;
        int failed = 0;
        int skipped = 0;
        long serialMs = 0L;
        for (TestExecutionRecord exec : session.latestExecutionsByTest.values()) {
            serialMs += Math.max(0L, exec.durationMs());
            if (exec.status() == TestOutcomeStatus.PASSED) {
                passed++;
            } else if (exec.status() == TestOutcomeStatus.FAILED) {
                failed++;
            } else if (exec.status() == TestOutcomeStatus.SKIPPED) {
                skipped++;
            }
        }
        int executedTests = passed + failed + skipped;
        int totalTests = Math.max(session.submittedUnitCount, executedTests);
        int totalBlobs = session.cachedBlobs + session.uploadedBlobs;
        SessionSummary summary = new SessionSummary(
                totalTests,
                passed,
                failed,
                skipped,
                totalBlobs,
                session.cachedBlobs,
                serialMs,
                0L
        );
        store.updateSessionStatus(session.record.sessionId(), SessionStatus.RUNNING, 0L, summary);
        session.record = store.findSession(session.record.sessionId()).orElse(session.record);
    }

    private static SessionSummary computeSessionSummary(SessionRuntime session, CompleteSessionRequest request, long finishedAt) {
        int passed = 0;
        int failed = 0;
        int skipped = 0;
        long serialMs = 0L;

        int recordedLocalPassed = 0;
        int recordedLocalFailed = 0;
        int recordedLocalSkipped = 0;
        long recordedLocalSerialMs = 0L;

        for (TestExecutionRecord exec : session.latestExecutionsByTest.values()) {
            long dur = Math.max(0L, exec.durationMs());
            serialMs += dur;
            boolean isLocal = "local".equals(exec.partitionId());
            if (isLocal) {
                recordedLocalSerialMs += dur;
            }
            if (exec.status() == TestOutcomeStatus.PASSED) {
                passed++;
                if (isLocal) {
                    recordedLocalPassed++;
                }
            } else if (exec.status() == TestOutcomeStatus.FAILED) {
                failed++;
                if (isLocal) {
                    recordedLocalFailed++;
                }
            } else if (exec.status() == TestOutcomeStatus.SKIPPED) {
                skipped++;
                if (isLocal) {
                    recordedLocalSkipped++;
                }
            }
        }

        if (request != null) {
            passed += Math.max(0, request.localPassedTests() - recordedLocalPassed);
            failed += Math.max(0, request.localFailedTests() - recordedLocalFailed);
            skipped += Math.max(0, request.localSkippedTests() - recordedLocalSkipped);
            serialMs += Math.max(0L, request.localSerialDurationMs() - recordedLocalSerialMs);
        }

        int totalTests = passed + failed + skipped;
        int totalBlobs = session.cachedBlobs + session.uploadedBlobs;
        long wallMs = Math.max(0L, finishedAt - session.record.startedAt());
        return new SessionSummary(
                totalTests,
                passed,
                failed,
                skipped,
                totalBlobs,
                session.cachedBlobs,
                serialMs,
                wallMs
        );
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
        int defaultPartitionSize = config.defaultMaxPartitionSize();
        // Sessions created before the requested size was stored keep the fixed coordinator default.
        int requestedMaxPartitionSize = store.findRequestedMaxPartitionSize(sessionId)
                .orElse(Math.max(1, defaultPartitionSize));
        int maxPartitionSize = requestedMaxPartitionSize > 0 ? requestedMaxPartitionSize : defaultPartitionSize;
        SessionRuntime rehydrated = new SessionRuntime(
                record, maxPartitionSize, store.findSessionMaxRetries(sessionId).orElse(config.defaultMaxRetries()), requestedMaxPartitionSize,
                store.findSessionHistoryScope(sessionId)
                        .orElseGet(() -> DistributionSettings.historyScope(null, record.projectName())));
        rehydrated.cachedBlobs = Math.max(0, record.cachedBlobs());
        rehydrated.uploadedBlobs = Math.max(0, record.totalBlobs() - record.cachedBlobs());
        Set<String> seenPartitionIds = new HashSet<>();
        for (PartitionRecord part : store.listPartitions(sessionId)) {
            if (seenPartitionIds.add(part.partitionId())) {
                rehydrated.partitionIds.add(part.partitionId());
                rehydrated.submittedUnitCount += part.workUnits().size();
            }
        }
        for (TestExecutionRecord exec : store.listTestExecutions(sessionId)) {
            rehydrated.recordExecution(exec);
            if ("local".equals(exec.partitionId())) {
                rehydrated.reportedLocalTestCount++;
                if (exec.workUnitKey() != null && !exec.workUnitKey().isBlank()) {
                    rehydrated.locallyExecutedWorkUnitKeys.add(exec.workUnitKey());
                }
            }
        }
        store.replayEvents(sessionId, 0, 0, event -> {
            Object interrupted = event.data().get("interruptedTestKeys");
            if (interrupted instanceof List<?> keys) {
                keys.stream().filter(String.class::isInstance).map(String.class::cast)
                        .forEach(rehydrated.failedOrInterruptedTests::add);
            }
        });
        sessions.put(sessionId, rehydrated);
        return rehydrated;
    }

    private static int intField(Map<String, Object> map, String key) {
        Object v = map != null ? map.get(key) : null;
        if (v instanceof Number number) {
            return number.intValue();
        }
        try {
            return v != null ? Integer.parseInt(v.toString()) : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
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
        private final int requestedMaxPartitionSize;
        private final String historyScope;
        private final List<String> partitionIds = new ArrayList<>();
        private final Set<String> failedOrInterruptedTests = new HashSet<>();
        private final Map<String, TestExecutionRecord> latestExecutionsByTest = new LinkedHashMap<>();
        private final TestExecutionIdentity testIdentities = new TestExecutionIdentity();
        private final Set<String> locallyExecutedWorkUnitKeys = new HashSet<>();
        private int cachedBlobs;
        private int uploadedBlobs;
        private int submittedUnitCount;
        private int reportedLocalTestCount;

        private SessionRuntime(BuildSessionRecord record, int maxPartitionSize, int maxRetries,
                               int requestedMaxPartitionSize, String historyScope) {
            this.record = record;
            this.maxPartitionSize = maxPartitionSize;
            this.maxRetries = maxRetries;
            this.requestedMaxPartitionSize = requestedMaxPartitionSize;
            this.historyScope = historyScope;
        }

        private String testKey(String workUnitKey, String className, String testName, String uniqueId) {
            return testIdentities.resolve(workUnitKey, className, testName, uniqueId, latestExecutionsByTest::containsKey);
        }

        private int nextAttemptFor(String key) {
            TestExecutionRecord previous = latestExecutionsByTest.get(key);
            return previous == null ? 1 : previous.attempt() + 1;
        }

        private void recordExecution(TestExecutionRecord exec) {
            String key = testKey(exec.workUnitKey(), exec.className(), exec.testName(), exec.uniqueId());
            if ("|#".equals(key)) {
                key = exec.id();
            }
            if (exec.status() == TestOutcomeStatus.FAILED) {
                failedOrInterruptedTests.add(key);
            }
            TestExecutionRecord previous = latestExecutionsByTest.get(key);
            if (previous == null
                    || exec.attempt() > previous.attempt()
                    || (exec.attempt() == previous.attempt() && exec.recordedAt() >= previous.recordedAt())) {
                latestExecutionsByTest.put(key, exec);
            }
        }
    }

    private static final class PartitionRuntime {
        private PartitionRecord record;
        private final String moduleName;
        private final List<ArtifactRef> classpathArtifacts;
        private final Map<String, String> systemProperties;
        private final List<WorkspaceFileSet> inputArtifacts;
        private final List<String> inputPaths;
        private final List<String> outputPaths;
        private final boolean forkWorkspace;
        private final String workingDirectory;
        private final int maxRetries;
        private int retriesUsed;
        private long lastProgressAt = System.currentTimeMillis();
        /** Connection of the worker currently holding this partition; {@code null} while not assigned. */
        private String activeConnectionId;
        /** Tests currently started on this attempt, reconstructed from events on restart. */
        private final Set<String> inFlightTestKeys = new HashSet<>();
        /** Worker that received the most recent attempt. */
        private String lastAssignedWorkerId;
        /** Workers that received an attempt before the current one. */
        private final Set<String> earlierAttemptWorkerIds = new HashSet<>();

        private PartitionRuntime(PartitionRecord record, SubmitWorkRequest request, int maxRetries) {
            this.record = record;
            this.moduleName = record.moduleName();
            this.classpathArtifacts = request.classpathArtifacts() == null ? List.of() : List.copyOf(request.classpathArtifacts());
            this.systemProperties = request.systemProperties() == null ? Map.of() : Map.copyOf(request.systemProperties());
            this.inputArtifacts = request.inputArtifacts();
            this.inputPaths = record.inputPaths();
            this.outputPaths = request.outputPaths();
            this.forkWorkspace = request.forkWorkspace();
            this.workingDirectory = request.workingDirectory();
            this.maxRetries = maxRetries;
        }

        private PartitionRecord buildRecord(String workerId,
                                            PartitionState state,
                                            int attempt,
                                            long startedAt,
                                            long finishedAt,
                                            List<String> producedOutputs,
                                            String errorMessage) {
            return new PartitionRecord(
                    record.partitionId(),
                    record.sessionId(),
                    workerId,
                    state,
                    attempt,
                    record.estimatedDurationMs(),
                    startedAt,
                    finishedAt,
                    record.requiredCapabilities(),
                    record.workUnits(),
                    moduleName,
                    forkWorkspace,
                    workingDirectory,
                    inputPaths,
                    outputPaths,
                    producedOutputs,
                    errorMessage
            );
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
        /** Sequence number of this worker's latest assignment (0 = never), for least-recently-used dispatch. */
        private long lastAssignedAt;
        private String activePartitionId;
        /** Latest local file cache telemetry reported by the worker; null for workers that report none. */
        private volatile WorkerCacheStats cacheStats;
        /** Partition whose expired attempt this worker was told to cancel and has not yet confirmed ending. */
        private String cancelledPartitionId;

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
