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
    void shouldPersistSessionFailureReasonAcrossRestartAndClearItWhenUnset() {
        String jdbcUrl = "jdbc:hsqldb:file:" + tempDir.resolve("db/reason").toAbsolutePath() + ";shutdown=true";
        SessionSummary summary = new SessionSummary(0, 0, 0, 0, 0, 0, 0L, 926L);

        try (JdbcDistributionStore store = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            store.initialize();
            store.saveSession(new BuildSessionRecord("sess-fail", "citrus-api", "main", "abc", SessionStatus.RUNNING,
                    1_000L, 0L, 0, 0, 0, 0, 0, 0, 0L, 0L));
            store.saveSession(new BuildSessionRecord("sess-ok", "citrus-api", "main", "abc", SessionStatus.RUNNING,
                    2_000L, 0L, 0, 0, 0, 0, 0, 0, 0L, 0L));

            store.updateSessionStatus("sess-fail", SessionStatus.FAILED, 1_926L, summary,
                    "citrus-api: NoClassDefFoundError: org/citrusframework/junit/jupiter/CitrusTestFactory");
            store.updateSessionStatus("sess-ok", SessionStatus.PASSED, 2_926L, summary);
        }

        try (JdbcDistributionStore reopened = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            reopened.initialize();
            assertThat(reopened.findSession("sess-fail").orElseThrow().failureReason())
                    .isEqualTo("citrus-api: NoClassDefFoundError: org/citrusframework/junit/jupiter/CitrusTestFactory");
            assertThat(reopened.findSession("sess-ok").orElseThrow().failureReason()).isNull();
            assertThat(reopened.listSessions(10))
                    .extracting(BuildSessionRecord::sessionId, BuildSessionRecord::failureReason)
                    .contains(org.assertj.core.groups.Tuple.tuple("sess-fail",
                            "citrus-api: NoClassDefFoundError: org/citrusframework/junit/jupiter/CitrusTestFactory"));
        }
    }

    @Test
    void shouldAddFailureReasonColumnToLegacySessionTable() throws Exception {
        String jdbcUrl = "jdbc:hsqldb:file:" + tempDir.resolve("db/legacy-sessions").toAbsolutePath() + ";shutdown=true";
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(jdbcUrl, "SA", "");
             java.sql.Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dist_sessions (session_id VARCHAR(64) PRIMARY KEY, project_name VARCHAR(255), "
                    + "git_branch VARCHAR(255), git_commit VARCHAR(64), status VARCHAR(32), started_at BIGINT, "
                    + "finished_at BIGINT, total_tests INT, passed_tests INT, failed_tests INT, skipped_tests INT, "
                    + "total_blobs INT, cached_blobs INT, serial_duration_ms BIGINT, wall_duration_ms BIGINT)");
            stmt.execute("INSERT INTO dist_sessions VALUES ('sess-old', 'p', 'b', 'c', 'PASSED', 1, 2, 0, 0, 0, 0, 0, 0, 0, 1)");
        }

        try (JdbcDistributionStore store = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            store.initialize();
            assertThat(store.findSession("sess-old").orElseThrow().failureReason()).isNull();
        }
    }

    @Test
    void shouldTolerateAColumnAnotherCoordinatorAddedAfterTheSchemaScan() throws Exception {
        String jdbcUrl = "jdbc:hsqldb:file:" + tempDir.resolve("db/race").toAbsolutePath() + ";shutdown=true";
        try (JdbcDistributionStore store = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            store.initialize();
        }
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(jdbcUrl, "SA", "");
             java.sql.Statement stmt = conn.createStatement()) {
            // The column exists already, as if a concurrently starting coordinator won the race.
            JdbcDistributionStore.addColumn(conn, stmt, "dist_sessions", "failure_reason", "VARCHAR(4096)");
        }
    }

    @Test
    void shouldStoreTheJUnitUniqueIdOfEachExecutionAndMigrateLegacyTables() throws Exception {
        String jdbcUrl = "jdbc:hsqldb:file:" + tempDir.resolve("db/legacy-tests").toAbsolutePath() + ";shutdown=true";
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(jdbcUrl, "SA", "");
             java.sql.Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dist_test_executions (id VARCHAR(64) PRIMARY KEY, session_id VARCHAR(64), "
                    + "partition_id VARCHAR(64), worker_id VARCHAR(128), work_unit_key VARCHAR(512), "
                    + "class_name VARCHAR(512), test_name VARCHAR(512), status VARCHAR(32), duration_ms BIGINT, "
                    + "attempt INT, flaky BOOLEAN, error_message VARCHAR(4096), stack_trace LONGVARCHAR, "
                    + "endpoints_json VARCHAR(4096), recorded_at BIGINT)");
        }
        String uid = "[engine:junit-jupiter]/[class:a.A]/[test-template:p(int)]/[test-template-invocation:#2]";
        try (JdbcDistributionStore store = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            store.initialize();
            store.saveTestExecution(new org.citrusframework.distribution.model.TestExecutionRecord("e1", "s1", "p1",
                    "worker-1", "a.A", "a.A", "[2] 2", org.citrusframework.distribution.model.TestOutcomeStatus.PASSED,
                    5L, 1, false, null, null, List.of(), 10L, uid));
            assertThat(store.listTestExecutions("s1")).singleElement()
                    .satisfies(e -> assertThat(e.uniqueId()).isEqualTo(uid));
        }
    }

    @Test
    void shouldListTheModulesOfEachSessionInTheOrderTheyStarted() {
        String jdbcUrl = "jdbc:hsqldb:mem:modules_" + System.nanoTime();
        try (JdbcDistributionStore store = new JdbcDistributionStore(jdbcUrl, "SA", "")) {
            store.initialize();
            store.saveSession(new BuildSessionRecord("sess-m", "citrus", "main", "abc", SessionStatus.RUNNING,
                    1_000L, 0L, 0, 0, 0, 0, 0, 0, 0L, 0L));
            store.savePartition(partition("p-base-1", "citrus-base", 3_000L));
            store.savePartition(partition("p-api", "citrus-api", 1_000L));
            store.savePartition(partition("p-base-2", "citrus-base", 3_500L));
            store.savePartition(partition("p-spring", "citrus-spring", 9_000L));

            assertThat(store.findSession("sess-m").orElseThrow().modules())
                    .containsExactly("citrus-api", "citrus-base", "citrus-spring");
            assertThat(store.listSessions(5).getFirst().modules())
                    .containsExactly("citrus-api", "citrus-base", "citrus-spring");
        }
    }

    private static PartitionRecord partition(String id, String module, long startedAt) {
        return new PartitionRecord(id, "sess-m", "worker-1", PartitionState.COMPLETED, 1, 100L, startedAt,
                startedAt + 100L, Map.of(), List.of(new WorkUnitSpec(id, "a.A", null, null, Map.of(), 100L)), module,
                true, "target", List.of(), List.of(), List.of(), null);
    }

    @Test
    void shouldSummarizeLargeTestHistoriesQuicklyWithTheLatestOutcomePerTest() {
        try (JdbcDistributionStore store = new JdbcDistributionStore("jdbc:hsqldb:mem:history_" + System.nanoTime(), "SA", "")) {
            store.initialize();
            int tests = 800;
            int runs = 50;
            for (int run = 0; run < runs; run++) {
                for (int t = 0; t < tests; t++) {
                    // The last run of every 10th test failed; all other runs passed.
                    boolean failed = run == runs - 1 && t % 10 == 0;
                    store.saveTestExecution(new org.citrusframework.distribution.model.TestExecutionRecord(
                            "e-" + run + "-" + t, "s-" + run, "p", "worker-1", "u" + t, "a.T" + t, "t()",
                            failed ? TestOutcomeStatus.FAILED : TestOutcomeStatus.PASSED,
                            t, 1, false, null, null, List.of(), 1_000L * run + t, null));
                }
            }

            List<org.citrusframework.distribution.model.TestHistorySummary> history =
                    org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(2),
                            () -> store.listTestHistory(tests));

            assertThat(history).hasSize(tests);
            assertThat(history).filteredOn(h -> h.workUnitKey().equals("u10")).singleElement()
                    .satisfies(h -> assertThat(h.lastStatus()).isEqualTo(TestOutcomeStatus.FAILED));
            assertThat(history).filteredOn(h -> h.workUnitKey().equals("u11")).singleElement()
                    .satisfies(h -> assertThat(h.lastStatus()).isEqualTo(TestOutcomeStatus.PASSED));
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
