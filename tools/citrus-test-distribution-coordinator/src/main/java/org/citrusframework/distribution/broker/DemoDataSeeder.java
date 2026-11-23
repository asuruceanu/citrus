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
import org.citrusframework.distribution.model.WorkerCacheStats;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerState;
import org.citrusframework.distribution.store.DistributionStore;

/**
 * Populates realistic Citrus multi-module sessions, worker pool topologies, partition timelines,
 * spot-worker failover retries, and failure traces for interactive UI inspection and demos.
 */
public final class DemoDataSeeder {

    private DemoDataSeeder() {
    }

    public static synchronized int seed(DistributionStore store) {
        long now = System.currentTimeMillis();

        // 1. Seed realistic heterogeneous worker pool (6 workers across IDLE, BUSY, DRAINING, OFFLINE)
        store.upsertWorker(new WorkerRecord(
                "k8s-worker-eu-01",
                "citrus-runner-7d9f8b-x2k4p.eu-central",
                Map.of("os", "linux", "arch", "amd64", "jdk", "21", "docker", "true", "k8s", "true", "cpuCores", "8", "memoryMb", "16384"),
                WorkerState.BUSY,
                "part-944-http",
                now - 1_800L,
                now - 3_600_000L,
                new WorkerCacheStats(true, 2_147_483_648L, 142, 1_932_735_283L, 48_318_382_080L, 37L, false)));

        store.upsertWorker(new WorkerRecord(
                "k8s-worker-eu-02",
                "citrus-runner-7d9f8b-m9q1z.eu-central",
                Map.of("os", "linux", "arch", "amd64", "jdk", "21", "docker", "true", "k8s", "true", "cpuCores", "8", "memoryMb", "16384"),
                WorkerState.IDLE,
                null,
                now - 2_400L,
                now - 3_550_000L,
                new WorkerCacheStats(true, 2_147_483_648L, 118, 1_610_612_736L, 52_613_349_376L, 12L, false)));

        store.upsertWorker(new WorkerRecord(
                "k8s-worker-gpu-01",
                "citrus-ai-runner-64b2c-v8n3w.eu-central",
                Map.of("os", "linux", "arch", "amd64", "jdk", "21", "docker", "true", "gpu", "true", "cpuCores", "16", "memoryMb", "32768"),
                WorkerState.IDLE,
                null,
                now - 3_100L,
                now - 3_400_000L,
                new WorkerCacheStats(false, 0L, 203, 3_221_225_472L, 96_636_764_160L, 0L, false)));

        store.upsertWorker(new WorkerRecord(
                "mac-studio-ci-01",
                "mac-studio-m3-ultra.citrus.local",
                Map.of("os", "macos", "arch", "aarch64", "jdk", "21", "docker", "true", "playwright", "true", "cpuCores", "24", "memoryMb", "65536"),
                WorkerState.BUSY,
                "part-944-camel",
                now - 1_200L,
                now - 7_200_000L,
                new WorkerCacheStats(true, 4_294_967_296L, 311, 6_442_450_944L, 3_221_225_472L, 468L, true)));

        store.upsertWorker(new WorkerRecord(
                "k8s-worker-drain-01",
                "citrus-runner-old-58c1a-p4t7r.eu-central",
                Map.of("os", "linux", "arch", "amd64", "jdk", "21", "docker", "true", "cpuCores", "4", "memoryMb", "8192"),
                WorkerState.DRAINING,
                null,
                now - 9_500L,
                now - 14_400_000L,
                new WorkerCacheStats(true, 1_073_741_824L, 64, 805_306_368L, 15_032_385_536L, 9L, false)));

        store.upsertWorker(new WorkerRecord(
                "k8s-worker-spot-lost",
                "citrus-spot-evicted-91a4f.eu-central",
                Map.of("os", "linux", "arch", "amd64", "jdk", "21", "docker", "true", "spot", "true", "cpuCores", "8", "memoryMb", "16384"),
                WorkerState.OFFLINE,
                null,
                now - 420_000L,
                now - 10_800_000L,
                new WorkerCacheStats(false, 0L, 22, 268_435_456L, -1L, 0L, false)));

        if (store.findSession("sess-citrus-pr-2108").isPresent()) {
            return 0;
        }

        // 2. Historical baseline sessions for rich Test History & Flakiness metrics
        seedHistoricalBaseline(store, "sess-hist-938", now - 7_200_000L, true);
        seedHistoricalBaseline(store, "sess-hist-939", now - 5_400_000L, false);

        // 3. Session 1: Clean PASSED main branch build (3.4x speedup, 93% blob cache hit)
        long s1Start = now - 1_800_000L;
        long s1End = s1Start + 18_400L;
        saveCompletedSession(
                store,
                new BuildSessionRecord(
                        "sess-citrus-main-942", "citrus", "main", "84cf5f8cdf",
                        SessionStatus.PASSED, s1Start, s1End,
                        16, 15, 0, 1, 44, 41, 62_800L, 18_400L),
                List.of(
                        "endpoints/citrus-http/src/main/java/org/citrusframework/http/client/HttpClient.java",
                        "endpoints/citrus-kafka/src/main/java/org/citrusframework/kafka/endpoint/KafkaEndpoint.java",
                        "runtime/citrus-junit-jupiter/src/main/java/org/citrusframework/junit/jupiter/CitrusExtension.java"));

        saveCompletedPartition(store, "part-942-01", "sess-citrus-main-942", "k8s-worker-eu-01",
                PartitionState.COMPLETED, 1, 16_000L, s1Start + 400L, s1Start + 16_900L,
                Map.of("docker", "true"),
                List.of(
                        unit("org.citrusframework.http.HttpServerIT", 6_200L, Map.of("docker", "true")),
                        unit("org.citrusframework.http.HttpClientTlsIT", 5_400L, Map.of("docker", "true")),
                        unit("org.citrusframework.http.HttpCompressionIT", 4_800L, Map.of("docker", "true"))));
        saveTest(store, "t-942-1", "sess-citrus-main-942", "part-942-01", "k8s-worker-eu-01",
                "org.citrusframework.http.HttpServerIT", "shouldHandleConcurrentPostRequests",
                TestOutcomeStatus.PASSED, 6_120L, 1, false, null, null, List.of("httpOrderServer", "httpOrderClient"), s1Start + 6_500L);
        saveTest(store, "t-942-2", "sess-citrus-main-942", "part-942-01", "k8s-worker-eu-01",
                "org.citrusframework.http.HttpClientTlsIT", "shouldValidateMutualTlsHandshake",
                TestOutcomeStatus.PASSED, 5_380L, 1, false, null, null, List.of("httpsMutualTlsClient"), s1Start + 11_900L);
        saveTest(store, "t-942-3", "sess-citrus-main-942", "part-942-01", "k8s-worker-eu-01",
                "org.citrusframework.http.HttpCompressionIT", "shouldDecompressGzipJsonPayload",
                TestOutcomeStatus.PASSED, 4_910L, 1, false, null, null, List.of("httpGzipClient"), s1Start + 16_850L);

        saveCompletedPartition(store, "part-942-02", "sess-citrus-main-942", "k8s-worker-eu-02",
                PartitionState.COMPLETED, 1, 15_500L, s1Start + 550L, s1Start + 17_800L,
                Map.of("docker", "true"),
                List.of(
                        unit("org.citrusframework.kafka.KafkaConsumerGroupIT", 9_100L, Map.of("docker", "true")),
                        unit("org.citrusframework.kafka.KafkaHeadersPropagationIT", 7_800L, Map.of("docker", "true"))));
        saveTest(store, "t-942-4", "sess-citrus-main-942", "part-942-02", "k8s-worker-eu-02",
                "org.citrusframework.kafka.KafkaConsumerGroupIT", "shouldRebalancePartitionsCleanly",
                TestOutcomeStatus.PASSED, 9_240L, 1, false, null, null, List.of("kafkaOrderEventsEndpoint"), s1Start + 9_800L);
        saveTest(store, "t-942-5", "sess-citrus-main-942", "part-942-02", "k8s-worker-eu-02",
                "org.citrusframework.kafka.KafkaHeadersPropagationIT", "shouldPreserveCorrelationHeaders",
                TestOutcomeStatus.PASSED, 7_680L, 1, false, null, null, List.of("kafkaHeadersEndpoint"), s1Start + 17_600L);

        saveCompletedPartition(store, "part-942-03", "sess-citrus-main-942", "mac-studio-ci-01",
                PartitionState.COMPLETED, 1, 14_000L, s1Start + 700L, s1Start + 14_900L,
                Map.of("playwright", "true"),
                List.of(unit("org.citrusframework.selenium.SimulatorUiSmokeIT", 14_000L, Map.of("playwright", "true"))));
        saveTest(store, "t-942-6", "sess-citrus-main-942", "part-942-03", "mac-studio-ci-01",
                "org.citrusframework.selenium.SimulatorUiSmokeIT", "shouldRenderScenarioExecutions",
                TestOutcomeStatus.PASSED, 13_950L, 1, false, null, null, List.of("seleniumBrowser", "simulatorRestEndpoint"), s1Start + 14_800L);

        // 4. Session 2: Rich FAILED PR build with spot-worker LOST -> Requeued #2, Flaky Recovery, and 2 Citrus Failures
        long s2Start = now - 620_000L;
        long s2End = s2Start + 26_500L;
        saveCompletedSession(
                store,
                new BuildSessionRecord(
                        "sess-citrus-pr-2108", "citrus", "feature/kafka-headers-dlq", "e91b42a71c",
                        SessionStatus.FAILED, s2Start, s2End,
                        14, 11, 2, 1, 46, 42, 56_400L, 26_500L),
                List.of(
                        "endpoints/citrus-kafka/src/main/java/org/citrusframework/kafka/message/KafkaMessageHeaderMapper.java",
                        "endpoints/citrus-kafka/src/test/java/org/citrusframework/kafka/KafkaDeadLetterTopicIT.java",
                        "endpoints/citrus-ws/src/main/java/org/citrusframework/ws/addressing/WsAddressingHeaders.java",
                        "endpoints/citrus-camel/src/test/java/org/citrusframework/camel/CamelJmsTransactionIT.java",
                        "tools/test-distribution/citrus-test-distribution-broker/pom.xml"));

        // Partition A: Spot worker evicted mid-run (LOST #1) -> Requeued and completed on k8s-worker-eu-02 (#2)
        WorkUnitSpec camelJmsUnit = unit("org.citrusframework.camel.CamelJmsTransactionIT", 8_400L, Map.of("docker", "true"));
        WorkUnitSpec jmsReplyUnit = unit("org.citrusframework.jms.JmsRequestReplyCorrelationIT", 5_100L, Map.of("docker", "true"));

        store.savePartition(new PartitionRecord(
                "part-902", "sess-citrus-pr-2108", "k8s-worker-spot-lost",
                PartitionState.LOST, 1, 13_500L, s2Start + 500L, s2Start + 9_200L,
                Map.of("docker", "true"), List.of(camelJmsUnit, jmsReplyUnit),
                "citrus-camel", false, ".", List.of(), List.of(), List.of(),
                "Worker disconnected during execution"));

        store.savePartition(new PartitionRecord(
                "part-902", "sess-citrus-pr-2108", "k8s-worker-eu-02",
                PartitionState.COMPLETED, 2, 13_500L, s2Start + 9_600L, s2Start + 23_800L,
                Map.of("docker", "true"), List.of(camelJmsUnit, jmsReplyUnit),
                "citrus-camel", false, ".", List.of(), List.of(), List.of(), null));

        saveTest(store, "t-2108-1", "sess-citrus-pr-2108", "part-902", "k8s-worker-eu-02",
                "org.citrusframework.camel.CamelJmsTransactionIT", "shouldRollbackJmsOnCamelException",
                TestOutcomeStatus.PASSED, 8_640L, 2, true, null, null,
                List.of("jmsOrderInboundQueue", "camelTxRouteEndpoint"), s2Start + 18_200L);
        saveTest(store, "t-2108-2", "sess-citrus-pr-2108", "part-902", "k8s-worker-eu-02",
                "org.citrusframework.jms.JmsRequestReplyCorrelationIT", "shouldCorrelateReplyByMessageId",
                TestOutcomeStatus.PASSED, 5_120L, 2, false, null, null,
                List.of("jmsReplyEndpoint"), s2Start + 23_700L);

        // Partition B: Kafka DLQ failure on k8s-worker-eu-01
        saveCompletedPartition(store, "part-901-kafka", "sess-citrus-pr-2108", "k8s-worker-eu-01",
                PartitionState.FAILED, 1, 18_200L, s2Start + 400L, s2Start + 19_400L,
                Map.of("docker", "true", "k8s", "true"),
                List.of(
                        unit("org.citrusframework.kafka.KafkaDeadLetterTopicIT", 9_800L, Map.of("docker", "true")),
                        unit("org.citrusframework.kafka.KafkaAvroSchemaRegistryIT", 8_400L, Map.of("docker", "true"))));

        saveTest(store, "t-2108-3", "sess-citrus-pr-2108", "part-901-kafka", "k8s-worker-eu-01",
                "org.citrusframework.kafka.KafkaDeadLetterTopicIT", "shouldRoutePoisonPayloadToDeadLetterTopicWithHeader",
                TestOutcomeStatus.FAILED, 9_850L, 1, false,
                "ValidationException: Header value mismatch for 'x-citrus-dlq-reason': expected 'SCHEMA_REJECTED' but was 'DESERIALIZATION_ERROR'",
                """
                org.citrusframework.exceptions.ValidationException: Header value mismatch for 'x-citrus-dlq-reason': expected 'SCHEMA_REJECTED' but was 'DESERIALIZATION_ERROR'
                \tat org.citrusframework.validation.DefaultMessageHeaderValidator.validateHeaders(DefaultMessageHeaderValidator.java:94)
                \tat org.citrusframework.actions.ReceiveMessageAction.validateMessage(ReceiveMessageAction.java:258)
                \tat org.citrusframework.actions.ReceiveMessageAction.doExecute(ReceiveMessageAction.java:154)
                \tat org.citrusframework.kafka.KafkaDeadLetterTopicIT.shouldRoutePoisonPayloadToDeadLetterTopicWithHeader(KafkaDeadLetterTopicIT.java:87)
                \tat java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
                """,
                List.of("kafkaOrderEventsEndpoint", "kafkaDlqEndpoint"), s2Start + 10_300L);

        saveTest(store, "t-2108-4", "sess-citrus-pr-2108", "part-901-kafka", "k8s-worker-eu-01",
                "org.citrusframework.kafka.KafkaAvroSchemaRegistryIT", "shouldValidateConfluentWireFormat",
                TestOutcomeStatus.PASSED, 8_720L, 1, false, null, null,
                List.of("kafkaAvroEndpoint", "schemaRegistryHttpServer"), s2Start + 19_200L);

        // Partition C: SOAP WS-Addressing failure on k8s-worker-gpu-01
        saveCompletedPartition(store, "part-903-soap", "sess-citrus-pr-2108", "k8s-worker-gpu-01",
                PartitionState.FAILED, 1, 15_000L, s2Start + 650L, s2Start + 16_800L,
                Map.of("gpu", "true", "docker", "true"),
                List.of(
                        unit("org.citrusframework.ws.SoapFaultWsAddressingIT", 8_200L, Map.of("gpu", "true")),
                        unit("org.citrusframework.ws.SoapAttachmentMtomIT", 6_800L, Map.of("gpu", "true"))));

        saveTest(store, "t-2108-5", "sess-citrus-pr-2108", "part-903-soap", "k8s-worker-gpu-01",
                "org.citrusframework.ws.SoapFaultWsAddressingIT", "shouldValidateMustUnderstandSoapFaultDetail",
                TestOutcomeStatus.FAILED, 8_190L, 1, false,
                "ActionTimeoutException: Action timeout after 5000 ms while receiving message on endpoint 'soapOrderEndpoint'",
                """
                org.citrusframework.exceptions.ActionTimeoutException: Action timeout after 5000 ms while receiving message on endpoint 'soapOrderEndpoint'
                \tat org.citrusframework.ws.client.WebServiceClient.receive(WebServiceClient.java:211)
                \tat org.citrusframework.actions.ReceiveMessageAction.doExecute(ReceiveMessageAction.java:132)
                \tat org.citrusframework.ws.SoapFaultWsAddressingIT.shouldValidateMustUnderstandSoapFaultDetail(SoapFaultWsAddressingIT.java:64)
                """,
                List.of("soapOrderEndpoint", "wsFaultCallbackServer"), s2Start + 8_900L);

        saveTest(store, "t-2108-6", "sess-citrus-pr-2108", "part-903-soap", "k8s-worker-gpu-01",
                "org.citrusframework.ws.SoapAttachmentMtomIT", "shouldStreamBinaryMtomAttachment",
                TestOutcomeStatus.PASSED, 7_450L, 1, false, null, null,
                List.of("soapMtomClient"), s2Start + 16_700L);

        // Partition D: Declarative YAML & XML dynamic tests on mac-studio-ci-01
        store.savePartition(new PartitionRecord(
                "part-904-yaml", "sess-citrus-pr-2108", "mac-studio-ci-01",
                PartitionState.COMPLETED, 1, 14_200L, s2Start + 700L, s2Start + 25_900L,
                Map.of("os", "macos", "workspace", "true"),
                List.of(
                        new WorkUnitSpec("org.citrusframework.yaml.YamlDeclarativeFactoryIT::order-fulfillment-flow.yaml",
                                "org.citrusframework.yaml.YamlDeclarativeFactoryIT", "yamlTests", "order-fulfillment-flow.yaml", Map.of("os", "macos"), 6_500L),
                        new WorkUnitSpec("org.citrusframework.yaml.YamlDeclarativeFactoryIT::payment-webhook-retry.yaml",
                                "org.citrusframework.yaml.YamlDeclarativeFactoryIT", "yamlTests", "payment-webhook-retry.yaml", Map.of("os", "macos"), 7_700L)),
                "citrus-yaml", true, ".",
                List.of("src/test/resources/scenarios"),
                List.of("target/citrus-reports"),
                List.of("target/citrus-reports"),
                null));

        for (int i = 0; i < 8; i++) {
            TestOutcomeStatus st = (i == 7) ? TestOutcomeStatus.SKIPPED : TestOutcomeStatus.PASSED;
            saveTest(store, "t-2108-yaml-" + i, "sess-citrus-pr-2108", "part-904-yaml", "mac-studio-ci-01",
                    "org.citrusframework.yaml.YamlDeclarativeFactoryIT",
                    "declarativeScenario_" + (i + 1) + ".yaml",
                    st, st == TestOutcomeStatus.SKIPPED ? 0L : 2_850L + (i * 190L), 1, false,
                    st == TestOutcomeStatus.SKIPPED ? "Disabled on non-K8s namespace" : null,
                    null, List.of("httpPaymentGateway", "sqlAuditDataSource"), s2Start + 4_000L + (i * 2_600L));
        }

        // 5. Session 3: Active RUNNING live build session
        long s3Start = now - 14_500L;
        store.saveSession(new BuildSessionRecord(
                "sess-citrus-live-944", "citrus", "fix/http-chunked-timeout", "4f77d190ab",
                SessionStatus.RUNNING, s3Start, 0L,
                8, 4, 0, 0, 44, 43, 19_200L, 0L));
        store.saveChangedFiles("sess-citrus-live-944", List.of(
                "endpoints/citrus-http/src/main/java/org/citrusframework/http/server/HttpServerAdapter.java",
                "endpoints/citrus-http/src/test/java/org/citrusframework/http/HttpChunkedTransferIT.java"));

        saveCompletedPartition(store, "part-944-done", "sess-citrus-live-944", "k8s-worker-eu-02",
                PartitionState.COMPLETED, 1, 9_200L, s3Start + 350L, s3Start + 9_800L,
                Map.of("docker", "true"),
                List.of(unit("org.citrusframework.http.HttpHeaderFilterIT", 9_200L, Map.of("docker", "true"))));
        saveTest(store, "t-944-1", "sess-citrus-live-944", "part-944-done", "k8s-worker-eu-02",
                "org.citrusframework.http.HttpHeaderFilterIT", "shouldStripHopByHopHeaders",
                TestOutcomeStatus.PASSED, 9_120L, 1, false, null, null, List.of("httpProxyServer"), s3Start + 9_750L);

        saveCompletedPartition(store, "part-944-http", "sess-citrus-live-944", "k8s-worker-eu-01",
                PartitionState.RUNNING, 1, 14_000L, s3Start + 500L, 0L,
                Map.of("docker", "true"),
                List.of(unit("org.citrusframework.http.HttpChunkedTransferIT", 14_000L, Map.of("docker", "true"))));

        saveCompletedPartition(store, "part-944-camel", "sess-citrus-live-944", "mac-studio-ci-01",
                PartitionState.RUNNING, 1, 12_500L, s3Start + 650L, 0L,
                Map.of("os", "macos"),
                List.of(unit("org.citrusframework.camel.CamelHttpBridgeIT", 12_500L, Map.of("os", "macos"))));

        // 6. Session 4: ABORTED release dry-run session
        long s4Start = now - 3_100_000L;
        saveCompletedSession(
                store,
                new BuildSessionRecord(
                        "sess-citrus-release-530", "citrus", "release/5.3.0", "19a0c33e8f",
                        SessionStatus.ABORTED, s4Start, s4Start + 7_400L,
                        4, 2, 0, 2, 38, 30, 8_100L, 7_400L),
                List.of("pom.xml"));

        return 6;
    }

    private static void seedHistoricalBaseline(DistributionStore store, String sessionId, long startedAt, boolean flakyFailedFirst) {
        saveCompletedSession(
                store,
                new BuildSessionRecord(
                        sessionId, "citrus", "main", "71bc09a11e",
                        SessionStatus.PASSED, startedAt, startedAt + 21_000L,
                        3, 3, 0, 0, 40, 38, 22_000L, 21_000L),
                List.of("pom.xml"));

        saveTest(store, sessionId + "-t1", sessionId, sessionId + "-p1", "k8s-worker-eu-01",
                "org.citrusframework.camel.CamelJmsTransactionIT", "shouldRollbackJmsOnCamelException",
                TestOutcomeStatus.PASSED, 8_310L, flakyFailedFirst ? 2 : 1, flakyFailedFirst,
                null, null, List.of("jmsOrderInboundQueue"), startedAt + 9_000L);

        saveTest(store, sessionId + "-t2", sessionId, sessionId + "-p1", "k8s-worker-eu-01",
                "org.citrusframework.kafka.KafkaDeadLetterTopicIT", "shouldRoutePoisonPayloadToDeadLetterTopicWithHeader",
                TestOutcomeStatus.PASSED, 9_410L, 1, false,
                null, null, List.of("kafkaOrderEventsEndpoint"), startedAt + 18_500L);
    }

    private static void saveCompletedSession(DistributionStore store, BuildSessionRecord record, List<String> changedFiles) {
        store.saveSession(record);
        store.updateSessionStatus(
                record.sessionId(),
                record.status(),
                record.finishedAt(),
                new SessionSummary(
                        record.totalTests(),
                        record.passedTests(),
                        record.failedTests(),
                        record.skippedTests(),
                        record.totalBlobs(),
                        record.cachedBlobs(),
                        record.serialDurationMs(),
                        record.wallDurationMs()));
        store.saveChangedFiles(record.sessionId(), changedFiles);
    }

    private static void saveCompletedPartition(DistributionStore store,
                                               String partitionId,
                                               String sessionId,
                                               String workerId,
                                               PartitionState state,
                                               int attempt,
                                               long estMs,
                                               long startedAt,
                                               long finishedAt,
                                               Map<String, String> caps,
                                               List<WorkUnitSpec> units) {
        store.savePartition(new PartitionRecord(
                partitionId, sessionId, workerId, state, attempt, estMs, startedAt, finishedAt, caps, units));
    }

    private static void saveTest(DistributionStore store,
                                 String id,
                                 String sessionId,
                                 String partitionId,
                                 String workerId,
                                 String className,
                                 String testName,
                                 TestOutcomeStatus status,
                                 long durationMs,
                                 int attempt,
                                 boolean flaky,
                                 String errorMessage,
                                 String stackTrace,
                                 List<String> endpoints,
                                 long recordedAt) {
        store.saveTestExecution(new TestExecutionRecord(
                id,
                sessionId,
                partitionId,
                workerId,
                className + "#" + testName,
                className,
                testName,
                status,
                durationMs,
                attempt,
                flaky,
                errorMessage,
                stackTrace,
                endpoints,
                recordedAt));
    }

    private static WorkUnitSpec unit(String className, long estMs, Map<String, String> caps) {
        return new WorkUnitSpec(className, className, null, null, caps, estMs);
    }
}
