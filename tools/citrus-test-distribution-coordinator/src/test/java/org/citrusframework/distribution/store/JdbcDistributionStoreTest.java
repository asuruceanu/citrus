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
import java.util.List;
import java.util.Map;

import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SessionSummary;
import org.citrusframework.distribution.model.TestExecutionRecord;
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
}
