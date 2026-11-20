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

import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SessionSummary;
import org.citrusframework.distribution.model.TestExecutionRecord;
import org.citrusframework.distribution.model.TestHistorySummary;
import org.citrusframework.distribution.model.WorkerRecord;

/**
 * Persistence contract for distributed build sessions, worker registrations, partitions,
 * and per-test execution telemetry.
 */
public interface DistributionStore extends AutoCloseable {

    void initialize();

    void saveSession(BuildSessionRecord session);

    void updateSessionStatus(String sessionId, SessionStatus status, long finishedAt, SessionSummary summary);

    Optional<BuildSessionRecord> findSession(String sessionId);

    List<BuildSessionRecord> listSessions(int limit);

    void saveChangedFiles(String sessionId, List<String> changedFiles);

    List<String> findChangedFiles(String sessionId);

    void upsertWorker(WorkerRecord worker);

    List<WorkerRecord> listWorkers();

    void savePartition(PartitionRecord partition);

    List<PartitionRecord> listPartitions(String sessionId);

    void saveTestExecution(TestExecutionRecord execution);

    List<TestExecutionRecord> listTestExecutions(String sessionId);

    Map<String, Long> findHistoricalDurationsMs(Collection<String> workUnitKeys, int recentLimit);

    List<TestHistorySummary> listTestHistory(int limit);

    @Override
    void close();
}
