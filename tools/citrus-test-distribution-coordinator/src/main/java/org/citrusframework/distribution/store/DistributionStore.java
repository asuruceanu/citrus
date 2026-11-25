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

package org.citrusframework.distribution.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;

import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.CoordinatorEvent;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SessionSummary;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.TestExecutionRecord;
import org.citrusframework.distribution.model.WorkspaceChange;
import org.citrusframework.distribution.model.TestHistoryRun;
import org.citrusframework.distribution.model.TestHistorySummary;
import org.citrusframework.distribution.model.WorkerRecord;

/**
 * Persistence contract for distributed build sessions, worker registrations, partitions,
 * and per-test execution telemetry.
 */
public interface DistributionStore extends AutoCloseable {

    void initialize();

    void saveSession(BuildSessionRecord session);

    /** Saves the session and its requested partition size. A non-positive size requests coordinator-managed sizing. */
    default void saveSession(BuildSessionRecord session, int requestedMaxPartitionSize) {
        saveSession(session);
    }

    default void saveSession(BuildSessionRecord session, int requestedMaxPartitionSize, String historyScope) {
        saveSession(session, requestedMaxPartitionSize);
    }

    void saveSession(BuildSessionRecord session, int requestedMaxPartitionSize, String historyScope, int maxRetries);

    OptionalInt findSessionMaxRetries(String sessionId);

    default Optional<String> findSessionHistoryScope(String sessionId) {
        return Optional.empty();
    }

    /** Returns the original requested size when available, including zero for coordinator-managed sizing. */
    default OptionalInt findRequestedMaxPartitionSize(String sessionId) {
        return OptionalInt.empty();
    }

    /** Updates status and totals and clears any failure reason. */
    default void updateSessionStatus(String sessionId, SessionStatus status, long finishedAt, SessionSummary summary) {
        updateSessionStatus(sessionId, status, finishedAt, summary, null);
    }

    /** Updates status and totals; {@code failureReason} explains a failed or aborted session ({@code null} for none). */
    void updateSessionStatus(String sessionId, SessionStatus status, long finishedAt, SessionSummary summary,
                             String failureReason);

    long finishSession(CoordinatorEvent event, SessionStatus status, SessionSummary summary, String failureReason);

    Optional<BuildSessionRecord> findSession(String sessionId);

    List<BuildSessionRecord> listSessions(int limit);

    default void saveChangedFiles(String sessionId, List<String> changedFiles) {
        saveChangedFiles(sessionId, changedFiles, List.of());
    }

    void saveChangedFiles(String sessionId, List<String> changedFiles, List<WorkspaceChange> workspaceChanges);

    List<WorkspaceChange> findWorkspaceChanges(String sessionId);

    List<String> findChangedFiles(String sessionId);

    void upsertWorker(WorkerRecord worker);

    List<WorkerRecord> listWorkers();

    void savePartition(PartitionRecord partition);

    List<PartitionRecord> listPartitions(String sessionId);

    record RecoverableWork(String sessionId, SubmitWorkRequest request, SubmitWorkResponse response, int maxRetries) {
    }

    void saveWorkSubmission(String sessionId, SubmitWorkResponse response);

    long saveWorkSubmission(RecoverableWork work, List<PartitionRecord> partitions, CoordinatorEvent event);

    List<RecoverableWork> listRecoverableWork();

    Optional<SubmitWorkResponse> findWorkSubmission(String sessionId, String submissionId);

    default long appendEvent(CoordinatorEvent event) {
        return appendEvent(event, null, null, List.of());
    }

    long appendEvent(CoordinatorEvent event, String deliveryId, TestExecutionRecord result, List<PartitionRecord> partitions);

    boolean hasDelivery(String deliveryId);

    void replayEvents(String sessionId, long afterSequence, int globalLimit, Consumer<CoordinatorEvent> consumer);

    void saveTestExecution(TestExecutionRecord execution);

    List<TestExecutionRecord> listTestExecutions(String sessionId);

    default Map<String, Long> findHistoricalDurationsMs(Collection<String> workUnitKeys, int recentLimit) {
        return findHistoricalDurationsMs(workUnitKeys, recentLimit, null);
    }

    Map<String, Long> findHistoricalDurationsMs(Collection<String> workUnitKeys, int recentLimit, String historyScope);

    List<TestHistorySummary> listTestHistory(int limit);

    List<TestHistoryRun> listTestHistoryRuns(String projectScope, String workUnitKey, String className,
                                            String testName, int limit);

    @Override
    void close();
}
