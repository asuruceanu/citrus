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

package org.citrusframework.distribution.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.citrusframework.distribution.config.DistributionJson;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtocolModelSerializationTest {

    @Test
    void shouldRoundTripAgentPoolsAndRegistrationKeys() {
        Map<String, String> capabilities = Map.of("docker", "true");
        var pool = new AgentPoolRecord("p1", "Linux workers", capabilities, 0, 400L, 500L);
        assertRoundTrip(pool, AgentPoolRecord.class);
        assertRoundTrip(new AgentPoolSummary("p1", "Linux workers", capabilities, 0, 4, 1, 3, 2, 400L, 500L),
                AgentPoolSummary.class);

        var key = new RegistrationKeyRecord("k1", "p1", "ctrk_12345678", "CI worker", 500L, 700L, "w1", null);
        assertRoundTrip(key, RegistrationKeyRecord.class);
        assertRoundTrip(new GeneratedRegistrationKey(key, "example-secret"), GeneratedRegistrationKey.class);
    }

    @Test
    void shouldRoundTripWorkerRegistrationAndStatusMessages() {
        Map<String, String> capabilities = Map.of("docker", "true");
        var stats = new WorkerCacheStats(true, 1_000L, 2, 1_200L, 5_000L, 3L, false);
        var warning = new WorkerWarning(WorkerWarningCode.POOL_CAPABILITY_MISMATCH, "Missing docker", 70L, 100L);
        assertRoundTrip(warning, WorkerWarning.class);
        assertRoundTrip(new WorkerRejectedPayload("INVALID_REGISTRATION_KEY", "Invalid key"),
                WorkerRejectedPayload.class);
        assertRoundTrip(new WorkerRenewPayload("w1", "p1", "example-secret"), WorkerRenewPayload.class);
        assertRoundTrip(new WorkerRegisterPayload("w1", "host", "1.0", "p1", "example-secret", capabilities, stats),
                WorkerRegisterPayload.class);
        assertRoundTrip(new WorkerRecord("w1", "host", "1.0", "p1", "Linux workers", capabilities,
                WorkerState.BUSY, "part-1", "OrderIT > shouldCreate", 55L, 60L, 50L, stats, List.of(warning)),
                WorkerRecord.class);
        assertThat(WorkerWsMessage.WORKER_REJECTED).isEqualTo("worker.rejected");
        assertThat(WorkerWsMessage.WORKER_RENEW).isEqualTo("worker.renew");
        assertThat(WorkerWsMessage.WORKER_RENEWED).isEqualTo("worker.renewed");
    }

    @Test
    void shouldReadLegacyWorkerJsonAndPreserveLegacyConstructors() {
        Map<String, String> capabilities = Map.of("docker", "true");
        var stats = new WorkerCacheStats(true, 1_000L, 2, 1_200L, 5_000L, 3L, false);
        var legacyRegistration = new WorkerRegisterPayload("w1", "host", capabilities);
        assertThat(DistributionJson.mapper().readValue(
                """
                {"workerId":"w1","host":"host","capabilities":{"docker":"true"}}
                """, WorkerRegisterPayload.class)).isEqualTo(legacyRegistration);
        assertThat(new WorkerRegisterPayload("w1", "host", capabilities, stats).cacheStats()).isEqualTo(stats);

        var legacyWorker = new WorkerRecord("w1", "host", capabilities, WorkerState.IDLE, null, 60L, 50L);
        assertThat(DistributionJson.mapper().readValue(
                """
                {"workerId":"w1","host":"host","capabilities":{"docker":"true"},"state":"IDLE",
                 "lastHeartbeatAt":60,"registeredAt":50}
                """, WorkerRecord.class)).isEqualTo(legacyWorker);
        var workerWithCache = new WorkerRecord("w1", "host", capabilities, WorkerState.IDLE, null, 60L, 50L, stats);
        assertThat(workerWithCache.warnings()).isEmpty();
        assertThat(workerWithCache.cacheStats()).isEqualTo(stats);
    }

    @Test
    void shouldReadLegacyRegistrationWithNullCapabilityValues() {
        var registration = DistributionJson.mapper().readValue(
                """
                {"workerId":"w1","host":"host","capabilities":{"docker":null,"os":"linux"}}
                """, WorkerRegisterPayload.class);

        assertThat(registration.capabilities()).containsEntry("docker", null).containsEntry("os", "linux");
        assertRoundTrip(registration, WorkerRegisterPayload.class);
        assertThatThrownBy(() -> registration.capabilities().remove("docker"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldReadLegacyWorkerWithNullCapabilityValues() {
        var worker = DistributionJson.mapper().readValue(
                """
                {"workerId":"w1","host":"host","capabilities":{"docker":null,"os":"linux"},"state":"IDLE",
                 "lastHeartbeatAt":60,"registeredAt":50}
                """, WorkerRecord.class);

        assertThat(worker.capabilities()).containsEntry("docker", null).containsEntry("os", "linux");
        assertRoundTrip(worker, WorkerRecord.class);
        assertThatThrownBy(() -> worker.capabilities().remove("docker"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldRoundTripHistoricalUsageModels() {
        Map<String, String> capabilities = Map.of("docker", "true");
        var sample = new UsageSampleRecord(100L,
                List.of(new UsageSampleRecord.JobDemandBucket(capabilities, 1, 3, 2)),
                List.of(new UsageSampleRecord.PoolSampleBucket("p1", "Linux workers", 0, capabilities, 4, 1)),
                List.of(new UsageSampleRecord.UnpooledWorkerBucket(Map.of("os", "macos"), 2, 1)));
        assertRoundTrip(sample, UsageSampleRecord.class);

        var point = new UsagePoint(100L, 1, 3, 1, 3, 4);
        var pool = new PoolUsageSeries("p1", "Linux workers", capabilities, List.of(point));
        assertRoundTrip(new HistoricalUsageResponse("1h", 100L, 200L, 60L, capabilities, List.of(point), List.of(pool)),
                HistoricalUsageResponse.class);
    }

    @Test
    void shouldDefaultMissingCollectionsAndSnapshotMutableInputs() {
        var response = DistributionJson.mapper().readValue(
                """
                {"filter":null,"pools":[{"capabilities":null,"points":null}]}
                """, HistoricalUsageResponse.class);
        assertThat(response.filter()).isEmpty();
        assertThat(response.series()).isEmpty();
        assertThat(response.pools()).containsExactly(new PoolUsageSeries(null, null, Map.of(), List.of()));
        assertThat(DistributionJson.mapper().readValue("{}", UsageSampleRecord.class))
                .isEqualTo(new UsageSampleRecord(0L, List.of(), List.of(), List.of()));
        assertThat(DistributionJson.mapper().readValue("{\"warnings\":null}", WorkerRecord.class).warnings())
                .isEmpty();

        Map<String, String> capabilities = new HashMap<>(Map.of("docker", "true"));
        var activeJobs = new ArrayList<>(List.of(new UsageSampleRecord.JobDemandBucket(capabilities, 1, 1, 0)));
        var sample = new UsageSampleRecord(100L, activeJobs, null, null);
        var registration = new WorkerRegisterPayload("w1", "host", capabilities);
        var worker = new WorkerRecord("w1", "host", capabilities, WorkerState.IDLE, null, 60L, 50L);
        capabilities.clear();
        activeJobs.clear();
        assertThat(sample.activeJobs()).singleElement().satisfies(job ->
                assertThat(job.requiredCapabilities()).containsExactlyEntriesOf(Map.of("docker", "true")));
        assertThat(registration.capabilities()).containsExactlyEntriesOf(Map.of("docker", "true"));
        assertThat(worker.capabilities()).containsExactlyEntriesOf(Map.of("docker", "true"));
    }

    private static <T> void assertRoundTrip(T value, Class<T> type) {
        assertThat(DistributionJson.mapper().readValue(DistributionJson.mapper().writeValueAsString(value), type))
                .isEqualTo(value);
    }

    @Test
    void shouldRoundTripHistoricalDurationsDtos() {
        HistoricalDurationsRequest request = new HistoricalDurationsRequest(List.of("pkg.A"), 10);
        HistoricalDurationsResponse response = new HistoricalDurationsResponse(Map.of("pkg.A", 450L));

        assertThat(DistributionJson.fromJson(DistributionJson.toJson(request), HistoricalDurationsRequest.class))
                .isEqualTo(request);
        assertThat(DistributionJson.fromJson(DistributionJson.toJson(response), HistoricalDurationsResponse.class))
                .isEqualTo(response);
        assertThat(DistributionJson.fromJson("{\"testKeys\":null}", HistoricalDurationsRequest.class).testKeys())
                .isEmpty();
        assertThat(DistributionJson.fromJson("{\"durationsMs\":null}", HistoricalDurationsResponse.class).durationsMs())
                .isEmpty();
    }

    @Test
    void shouldRoundTripExtendedTestHistorySummaryAndSupportLegacyConstructor() {
        TestHistorySummary full = new TestHistorySummary(
                "OrderIT#createOrder",
                "com.acme.OrderIT",
                "createOrder",
                420L,
                850L,
                10,
                9,
                1,
                2,
                TestOutcomeStatus.PASSED
        );

        String json = DistributionJson.toJson(full);
        TestHistorySummary decoded = DistributionJson.fromJson(json, TestHistorySummary.class);
        assertThat(decoded).isEqualTo(full);
        assertThat(decoded.passedRuns()).isEqualTo(9);
        assertThat(decoded.maxDurationMs()).isEqualTo(850L);
        assertThat(decoded.lastStatus()).isEqualTo(TestOutcomeStatus.PASSED);

        TestHistorySummary legacy = new TestHistorySummary(
                "OrderIT#cancelOrder",
                "com.acme.OrderIT",
                "cancelOrder",
                300L,
                5,
                1,
                0
        );
        assertThat(legacy.passedRuns()).isEqualTo(4);
        assertThat(legacy.maxDurationMs()).isEqualTo(300L);
        assertThat(legacy.lastStatus()).isEqualTo(TestOutcomeStatus.FAILED);
    }

    @Test
    void shouldRoundTripExtendedPartitionRecordAndSupportLegacyConstructor() {
        WorkUnitSpec unit = new WorkUnitSpec(
                "OrderIT#createOrder", "com.acme.OrderIT", "createOrder", null, Map.of("docker", "true"), 500L);
        PartitionRecord full = new PartitionRecord(
                "part-1",
                "sess-1",
                "worker-1",
                PartitionState.FAILED,
                2,
                500L,
                1_000L,
                1_600L,
                Map.of("docker", "true"),
                List.of(unit),
                "citrus-http",
                true,
                "sub-dir",
                List.of("src/test/resources/data.json"),
                List.of("target/reports"),
                List.of("target/reports/out.xml"),
                "Undeclared CWD write detected: rogue.tmp"
        );

        String json = DistributionJson.toJson(full);
        PartitionRecord decoded = DistributionJson.fromJson(json, PartitionRecord.class);
        assertThat(decoded).isEqualTo(full);
        assertThat(decoded.moduleName()).isEqualTo("citrus-http");
        assertThat(decoded.forkWorkspace()).isTrue();
        assertThat(decoded.workingDirectory()).isEqualTo("sub-dir");
        assertThat(decoded.inputPaths()).containsExactly("src/test/resources/data.json");
        assertThat(decoded.outputPaths()).containsExactly("target/reports");
        assertThat(decoded.producedOutputs()).containsExactly("target/reports/out.xml");
        assertThat(decoded.errorMessage()).isEqualTo("Undeclared CWD write detected: rogue.tmp");

        PartitionRecord legacy = new PartitionRecord(
                "part-legacy",
                "sess-1",
                "worker-1",
                PartitionState.COMPLETED,
                1,
                500L,
                1_000L,
                1_500L,
                Map.of(),
                List.of(unit)
        );
        assertThat(legacy.moduleName()).isNull();
        assertThat(legacy.forkWorkspace()).isFalse();
        assertThat(legacy.workingDirectory()).isEmpty();
        assertThat(legacy.inputPaths()).isEmpty();
        assertThat(legacy.outputPaths()).isEmpty();
        assertThat(legacy.producedOutputs()).isEmpty();
        assertThat(legacy.errorMessage()).isNull();
    }

    @Test
    void shouldRoundTripReportLocalTestsRequest() {
        WorkerTestEventPayload localEvt = new WorkerTestEventPayload(
                "local-only",
                "sess-1",
                1,
                "LocalIT#runLocal",
                "com.acme.LocalIT",
                "runLocal",
                TestOutcomeStatus.PASSED,
                120L,
                null,
                null,
                List.of("localHttpServer")
        );
        ReportLocalTestsRequest req = new ReportLocalTestsRequest("local-maven", List.of(localEvt));

        String json = DistributionJson.toJson(req);
        ReportLocalTestsRequest decoded = DistributionJson.fromJson(json, ReportLocalTestsRequest.class);
        assertThat(decoded).isEqualTo(req);
        assertThat(decoded.workerId()).isEqualTo("local-maven");
        assertThat(decoded.executions()).hasSize(1);
    }

    @Test
    void shouldRoundTripCompleteSessionRequestWithBuildFailedFlagAndSupportLegacyConstructor() {
        CompleteSessionRequest full = new CompleteSessionRequest(2, 1, 0, 150L, true);
        String json = DistributionJson.toJson(full);
        CompleteSessionRequest decoded = DistributionJson.fromJson(json, CompleteSessionRequest.class);
        assertThat(decoded).isEqualTo(full);
        assertThat(decoded.buildFailed()).isTrue();

        CompleteSessionRequest legacy = new CompleteSessionRequest(2, 0, 1, 80L);
        assertThat(legacy.buildFailed()).isFalse();

        CompleteSessionRequest omittedJson = DistributionJson.fromJson(
                "{\"localPassedTests\":1,\"localFailedTests\":0,\"localSkippedTests\":0,\"localSerialDurationMs\":50}",
                CompleteSessionRequest.class);
        assertThat(omittedJson.buildFailed()).isFalse();
    }
}
