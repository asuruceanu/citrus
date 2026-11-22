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

import java.util.List;
import java.util.Map;

import org.citrusframework.distribution.config.DistributionJson;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolModelSerializationTest {

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
