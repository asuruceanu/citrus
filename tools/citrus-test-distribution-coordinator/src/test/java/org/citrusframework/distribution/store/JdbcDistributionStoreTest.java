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

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SessionSummary;
import org.citrusframework.distribution.model.TestExecutionRecord;
import org.citrusframework.distribution.model.TestHistorySummary;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcDistributionStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldPersistSessionsWorkersPartitionsAndTestDurationsAcrossRestart() {
        String jdbcUrl = "jdbc:hsqldb:file:" + tempDir.resolve("db/telemetry").toAbsolutePath() + ";shutdown=true";

        try (JdbcDistributionStore store = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            store.initialize();

            store.saveSession(new BuildSessionRecord(
                    "sess-100",
                    "citrus-demo",
                    "main",
                    "abc1234",
                    SessionStatus.RUNNING,
                    1_000L,
                    0L,
                    2, 0, 0, 0,
                    5, 4,
                    0L, 0L
            ));
            store.saveChangedFiles("sess-100", List.of("src/main/java/App.java", "pom.xml"));

            store.upsertWorker(new WorkerRecord(
                    "worker-1",
                    "node-a",
                    Map.of("docker", "true", "os", "linux"),
                    WorkerState.IDLE,
                    null,
                    1_500L,
                    1_000L
            ));

            store.savePartition(new PartitionRecord(
                    "part-1",
                    "sess-100",
                    "worker-1",
                    PartitionState.COMPLETED,
                    1,
                    1_200L,
                    1_100L,
                    2_300L,
                    Map.of("docker", "true"),
                    List.of(new WorkUnitSpec("OrderIT#createOrder", "com.acme.OrderIT", "createOrder", null, Map.of("docker", "true"), 1_200L))
            ));

            store.saveTestExecution(new TestExecutionRecord(
                    "exec-1",
                    "sess-100",
                    "part-1",
                    "worker-1",
                    "OrderIT#createOrder",
                    "com.acme.OrderIT",
                    "createOrder",
                    TestOutcomeStatus.PASSED,
                    1_450L,
                    1,
                    false,
                    null,
                    null,
                    List.of("ordersHttpClient", "ordersKafkaEndpoint"),
                    2_300L
            ));

            store.updateSessionStatus("sess-100", SessionStatus.PASSED, 2_500L,
                    new SessionSummary(1, 1, 0, 0, 5, 4, 1_450L, 1_500L));
        }

        // Reopen store from disk to verify persistence across broker restart
        try (JdbcDistributionStore reopened = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            reopened.initialize();

            BuildSessionRecord session = reopened.findSession("sess-100").orElseThrow();
            assertThat(session.status()).isEqualTo(SessionStatus.PASSED);
            assertThat(session.cachedBlobs()).isEqualTo(4);
            assertThat(reopened.findChangedFiles("sess-100"))
                    .containsExactlyInAnyOrder("src/main/java/App.java", "pom.xml");

            assertThat(reopened.listWorkers()).hasSize(1);
            assertThat(reopened.listWorkers().get(0).capabilities()).containsEntry("docker", "true");

            assertThat(reopened.listPartitions("sess-100")).hasSize(1);
            List<TestExecutionRecord> executions = reopened.listTestExecutions("sess-100");
            assertThat(executions).hasSize(1);
            assertThat(executions.get(0).endpoints()).containsExactly("ordersHttpClient", "ordersKafkaEndpoint");

            Map<String, Long> durations = reopened.findHistoricalDurationsMs(List.of("OrderIT#createOrder"), 5);
            assertThat(durations).containsEntry("OrderIT#createOrder", 1_450L);
        }
    }

    @Test
    void shouldResolveDialectLongTextTypeForPostgresAndHsqldb() {
        assertThat(JdbcDistributionStore.resolveLongTextType("jdbc:postgresql://localhost:5432/citrus"))
                .isEqualTo("TEXT");
        assertThat(JdbcDistributionStore.resolveLongTextType("jdbc:hsqldb:mem:test"))
                .isEqualTo("CLOB");
    }

    @Test
    void shouldPersistMultiplePartitionAttemptsWithExternalResourceMetadataAndComputeRichTestHistory() {
        String jdbcUrl = "jdbc:hsqldb:file:" + tempDir.resolve("db/history-and-attempts").toAbsolutePath() + ";shutdown=true";

        try (JdbcDistributionStore store = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            store.initialize();

            WorkUnitSpec unit = new WorkUnitSpec(
                    "OrderIT#createOrder", "com.acme.OrderIT", "createOrder", null, Map.of("docker", "true"), 500L);

            store.savePartition(new PartitionRecord(
                    "part-retry",
                    "sess-200",
                    "worker-1",
                    PartitionState.LOST,
                    1,
                    500L,
                    1_000L,
                    1_400L,
                    Map.of("docker", "true"),
                    List.of(unit),
                    "citrus-http",
                    true,
                    "sub-module",
                    List.of("src/test/resources/payload.json"),
                    List.of("target/generated-reports"),
                    List.of(),
                    "Worker heartbeat lease expired"
            ));

            store.savePartition(new PartitionRecord(
                    "part-retry",
                    "sess-200",
                    "worker-2",
                    PartitionState.COMPLETED,
                    2,
                    500L,
                    1_450L,
                    1_950L,
                    Map.of("docker", "true"),
                    List.of(unit),
                    "citrus-http",
                    true,
                    "sub-module",
                    List.of("src/test/resources/payload.json"),
                    List.of("target/generated-reports"),
                    List.of("target/generated-reports/summary.xml"),
                    null
            ));

            // Attempt 1 failed, Attempt 2 passed (flaky), and another test skipped
            store.saveTestExecution(new TestExecutionRecord(
                    "z-exec-1", "sess-200", "part-retry", "worker-1",
                    "OrderIT#createOrder", "com.acme.OrderIT", "createOrder",
                    TestOutcomeStatus.FAILED, 300L, 1, false,
                    "Connection reset", "trace", List.of("httpOrderClient"), 1_350L
            ));
            store.saveTestExecution(new TestExecutionRecord(
                    "a-exec-2", "sess-200", "part-retry", "worker-2",
                    "OrderIT#createOrder", "com.acme.OrderIT", "createOrder",
                    TestOutcomeStatus.PASSED, 900L, 2, true,
                    null, null, List.of("httpOrderClient"), 1_900L
            ));
            store.saveTestExecution(new TestExecutionRecord(
                    "exec-skip", "sess-200", "part-retry", "worker-2",
                    "OrderIT#skipOrder", "com.acme.OrderIT", "skipOrder",
                    TestOutcomeStatus.SKIPPED, 5L, 1, false,
                    null, null, List.of(), 1_910L
            ));

            List<PartitionRecord> partitions = store.listPartitions("sess-200");
            assertThat(partitions).hasSize(2);
            assertThat(partitions.get(0).attempt()).isEqualTo(1);
            assertThat(partitions.get(0).state()).isEqualTo(PartitionState.LOST);
            assertThat(partitions.get(0).workerId()).isEqualTo("worker-1");
            assertThat(partitions.get(0).errorMessage()).isEqualTo("Worker heartbeat lease expired");
            assertThat(partitions.get(0).moduleName()).isEqualTo("citrus-http");
            assertThat(partitions.get(0).forkWorkspace()).isTrue();
            assertThat(partitions.get(0).workingDirectory()).isEqualTo("sub-module");
            assertThat(partitions.get(0).inputPaths()).containsExactly("src/test/resources/payload.json");
            assertThat(partitions.get(0).outputPaths()).containsExactly("target/generated-reports");

            assertThat(partitions.get(1).attempt()).isEqualTo(2);
            assertThat(partitions.get(1).state()).isEqualTo(PartitionState.COMPLETED);
            assertThat(partitions.get(1).workerId()).isEqualTo("worker-2");
            assertThat(partitions.get(1).producedOutputs()).containsExactly("target/generated-reports/summary.xml");
            assertThat(partitions.get(1).errorMessage()).isNull();

            List<TestHistorySummary> history = store.listTestHistory(10);
            assertThat(history).hasSize(2);

            TestHistorySummary createHistory = history.stream()
                    .filter(h -> h.workUnitKey().equals("OrderIT#createOrder"))
                    .findFirst()
                    .orElseThrow();
            assertThat(createHistory.totalRuns()).isEqualTo(2);
            assertThat(createHistory.passedRuns()).isEqualTo(1);
            assertThat(createHistory.failedRuns()).isEqualTo(1);
            assertThat(createHistory.flakyRuns()).isEqualTo(1);
            assertThat(createHistory.avgDurationMs()).isEqualTo(600L);
            assertThat(createHistory.maxDurationMs()).isEqualTo(900L);
            // Note: latest recorded_at is 1_900L (id "a-exec-2", alphabetically before "z-exec-1"), status must be PASSED
            assertThat(createHistory.lastStatus()).isEqualTo(TestOutcomeStatus.PASSED);

            TestHistorySummary skipHistory = history.stream()
                    .filter(h -> h.workUnitKey().equals("OrderIT#skipOrder"))
                    .findFirst()
                    .orElseThrow();
            assertThat(skipHistory.totalRuns()).isEqualTo(1);
            assertThat(skipHistory.passedRuns()).isEqualTo(0);
            assertThat(skipHistory.failedRuns()).isEqualTo(0);
            assertThat(skipHistory.lastStatus()).isEqualTo(TestOutcomeStatus.SKIPPED);
        }
    }

    @Test
    void shouldMigrateLegacySingleColumnPartitionPrimaryKeyAndSeparateClassLevelWorkUnitHistory() throws Exception {
        String jdbcUrl = "jdbc:hsqldb:file:" + tempDir.resolve("db/legacy-upgrade").toAbsolutePath() + ";shutdown=true";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "SA", "");
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dist_partitions (" +
                    "partition_id VARCHAR(64) PRIMARY KEY, " +
                    "session_id VARCHAR(64), " +
                    "worker_id VARCHAR(128), " +
                    "state VARCHAR(32), " +
                    "attempt INT, " +
                    "estimated_duration_ms BIGINT, " +
                    "started_at BIGINT, " +
                    "finished_at BIGINT, " +
                    "required_caps_json VARCHAR(4096), " +
                    "work_units_json CLOB)");
            stmt.execute("SHUTDOWN");
        }

        try (JdbcDistributionStore store = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            store.initialize();

            WorkUnitSpec suiteUnit = new WorkUnitSpec("com.acme.SuiteIT", "com.acme.SuiteIT", null, null, Map.of(), 400L);
            store.savePartition(new PartitionRecord(
                    "part-legacy", "sess-legacy", "w1", PartitionState.LOST, 1,
                    400L, 1_000L, 1_200L, Map.of(), List.of(suiteUnit)));
            store.savePartition(new PartitionRecord(
                    "part-legacy", "sess-legacy", "w2", PartitionState.COMPLETED, 2,
                    400L, 1_250L, 1_650L, Map.of(), List.of(suiteUnit)));

            assertThat(store.listPartitions("sess-legacy")).hasSize(2);

            store.saveTestExecution(new TestExecutionRecord(
                    "suite-1", "sess-legacy", "part-legacy", "w2",
                    "com.acme.SuiteIT", "com.acme.SuiteIT", "first()",
                    TestOutcomeStatus.PASSED, 150L, 2, false,
                    null, null, List.of(), 1_500L));
            store.saveTestExecution(new TestExecutionRecord(
                    "suite-2", "sess-legacy", "part-legacy", "w2",
                    "com.acme.SuiteIT", "com.acme.SuiteIT", "second()",
                    TestOutcomeStatus.FAILED, 250L, 2, false,
                    "boom", "trace", List.of(), 1_600L));

            List<TestHistorySummary> history = store.listTestHistory(10);
            assertThat(history).hasSize(2);
            assertThat(history)
                    .filteredOn(h -> "first()".equals(h.testName()))
                    .singleElement()
                    .satisfies(h -> assertThat(h.lastStatus()).isEqualTo(TestOutcomeStatus.PASSED));
            assertThat(history)
                    .filteredOn(h -> "second()".equals(h.testName()))
                    .singleElement()
                    .satisfies(h -> assertThat(h.lastStatus()).isEqualTo(TestOutcomeStatus.FAILED));
        }
    }
}
