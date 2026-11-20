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

package org.citrusframework.distribution.junit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.citrusframework.distribution.annotations.AgentRequirement;
import org.citrusframework.distribution.annotations.LocalOnly;
import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.worker.DistributionWorkerClient;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import static org.assertj.core.api.Assertions.assertThat;

class TestDistributionEndToEndIT {

    @TempDir
    Path tempDir;

    @AfterEach
    void clearProperties() {
        System.clearProperty("junit.platform.launcher.interceptors.enabled");
        System.clearProperty(DistributionSettings.ENABLED_PROPERTY);
        System.clearProperty(DistributionSettings.ROLE_PROPERTY);
        System.clearProperty(DistributionSettings.BROKER_URL_PROPERTY);
        System.clearProperty(DistributionSettings.REPORT_DIR_PROPERTY);
        System.clearProperty(DistributionSettings.MODULE_NAME_PROPERTY);
        System.clearProperty(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY);
        System.clearProperty(DistributionSettings.MAX_RETRIES_PROPERTY);
    }

    @Test
    void shouldDistributeAcrossMultipleWorkersReuseBlobCacheHonorCapabilitiesAndWriteSurefireXml() throws Exception {
        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:e2e_dist_" + System.nanoTime())
                .defaultMaxPartitionSize(1)
                .defaultMaxRetries(2)
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();

            DistributionWorkerClient workerAlpha = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                    .workerId("worker-alpha")
                    .brokerUrl(broker.baseUrl())
                    .cacheDir(tempDir.resolve("worker-alpha-cache"))
                    .capabilities(Map.of("gpu", "false", "citrus-worker", "true"))
                    .build());
            DistributionWorkerClient workerBetaGpu = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                    .workerId("worker-beta-gpu")
                    .brokerUrl(broker.baseUrl())
                    .cacheDir(tempDir.resolve("worker-beta-cache"))
                    .capabilities(Map.of("gpu", "true", "citrus-worker", "true"))
                    .build());

            workerAlpha.start();
            workerBetaGpu.start();
            try {
                Path reportDirRun1 = tempDir.resolve("reports-run1");
                configureCoordinatorJvm(broker.baseUrl(), reportDirRun1);

                SummaryGeneratingListener summary1 = executeDistributedSuite(
                        SampleStandardSuite.class, SampleGpuSuite.class, SampleLocalSuite.class, SampleDynamicSuite.class);
                assertThat(summary1.getSummary().getTestsFailedCount()).isZero();
                assertThat(summary1.getSummary().getTestsSucceededCount()).isGreaterThanOrEqualTo(4);
                assertThat(SampleLocalSuite.RAN_LOCALLY).isTrue();
                assertThat(Files.exists(reportDirRun1.resolve("TEST-" + SampleStandardSuite.class.getName() + ".xml"))).isTrue();
                assertThat(Files.exists(reportDirRun1.resolve("TEST-" + SampleGpuSuite.class.getName() + ".xml"))).isTrue();

                Path reportDirRun2 = tempDir.resolve("reports-run2");
                System.setProperty(DistributionSettings.REPORT_DIR_PROPERTY, reportDirRun2.toString());
                SummaryGeneratingListener summary2 = executeDistributedSuite(SampleStandardSuite.class, SampleGpuSuite.class);
                assertThat(summary2.getSummary().getTestsFailedCount()).isZero();

                BuildSessionRecord secondSession = broker.store().listSessions(5).get(0);
                assertThat(secondSession.status()).isEqualTo(SessionStatus.PASSED);
                assertThat(secondSession.cachedBlobs()).isGreaterThan(0);

                PartitionRecord gpuPartition = broker.store().listPartitions(secondSession.sessionId()).stream()
                        .filter(p -> "true".equals(p.requiredCapabilities().get("gpu")))
                        .findFirst()
                        .orElseThrow();
                assertThat(gpuPartition.workerId()).isEqualTo("worker-beta-gpu");
            } finally {
                workerAlpha.close();
                workerBetaGpu.close();
            }
        }
    }

    @Test
    void shouldRequeueInFlightPartitionWhenWorkerDropsMidExecution() throws Exception {
        SlowSampleSuite.ENTERED_FIRST_ATTEMPT = new CountDownLatch(1);
        SlowSampleSuite.ALLOW_FINISH_FIRST_ATTEMPT = new CountDownLatch(1);
        SlowSampleSuite.FIRST_CALL = new AtomicBoolean(true);

        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("failover-broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:e2e_failover_" + System.nanoTime())
                .defaultMaxPartitionSize(1)
                .defaultMaxRetries(2)
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();

            DistributionWorkerClient doomedWorker = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                    .workerId("worker-doomed")
                    .brokerUrl(broker.baseUrl())
                    .cacheDir(tempDir.resolve("worker-doomed-cache"))
                    .capabilities(Map.of("citrus-worker", "true"))
                    .build());
            doomedWorker.start();

            Thread killerAndBackupStarter = new Thread(() -> {
                try {
                    if (SlowSampleSuite.ENTERED_FIRST_ATTEMPT.await(5, TimeUnit.SECONDS)) {
                        doomedWorker.close();
                        SlowSampleSuite.ALLOW_FINISH_FIRST_ATTEMPT.countDown();
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }, "doomed-worker-killer");
            killerAndBackupStarter.start();

            DistributionWorkerClient survivorWorker = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                    .workerId("worker-survivor")
                    .brokerUrl(broker.baseUrl())
                    .cacheDir(tempDir.resolve("worker-survivor-cache"))
                    .capabilities(Map.of("citrus-worker", "true"))
                    .build());

            Thread delayedSurvivor = new Thread(() -> {
                try {
                    SlowSampleSuite.ENTERED_FIRST_ATTEMPT.await(5, TimeUnit.SECONDS);
                    Thread.sleep(150);
                    survivorWorker.start();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }, "survivor-starter");
            delayedSurvivor.start();

            try {
                configureCoordinatorJvm(broker.baseUrl(), tempDir.resolve("failover-reports"));
                SummaryGeneratingListener summary = executeDistributedSuite(SlowSampleSuite.class);
                assertThat(summary.getSummary().getTestsFailedCount()).isZero();
                assertThat(summary.getSummary().getTestsSucceededCount()).isEqualTo(1);

                BuildSessionRecord session = broker.store().listSessions(5).get(0);
                PartitionRecord part = broker.store().listPartitions(session.sessionId()).get(0);
                assertThat(part.state()).isEqualTo(PartitionState.COMPLETED);
                assertThat(part.attempt()).isEqualTo(2);
                assertThat(part.workerId()).isEqualTo("worker-survivor");
            } finally {
                survivorWorker.close();
            }
        }
    }

    @Test
    void shouldNeverWrapLauncherWhenRunningInsideWorkerRole() {
        System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
        System.setProperty(DistributionSettings.ROLE_PROPERTY, "worker");

        assertThat(DistributionLauncherInterceptor.isActiveForCurrentProcess()).isFalse();
    }

    @Test
    void shouldExecuteParameterizedMethodLevelAgentRequirementOnMatchingWorker() throws Exception {
        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("param-broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:e2e_param_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();
            DistributionWorkerClient gpuWorker = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                    .workerId("worker-gpu-param")
                    .brokerUrl(broker.baseUrl())
                    .cacheDir(tempDir.resolve("worker-gpu-param"))
                    .capabilities(Map.of("gpu", "true", "citrus-worker", "true"))
                    .build());
            gpuWorker.start();
            Awaitility.await().atMost(Duration.ofSeconds(5))
                    .until(() -> broker.store().listWorkers().size() == 1);

            try {
                configureCoordinatorJvm(broker.baseUrl(), tempDir.resolve("param-reports"));
                SummaryGeneratingListener summary = executeDistributedSuite(SampleMixedParameterizedGpuSuite.class);
                assertThat(summary.getSummary().getTestsFailedCount()).isZero();
                assertThat(summary.getSummary().getTestsSucceededCount()).isEqualTo(2);
            } finally {
                gpuWorker.close();
            }
        }
    }

    private void configureCoordinatorJvm(String brokerUrl, Path reportDir) {
        SampleLocalSuite.RAN_LOCALLY = false;
        System.setProperty("junit.platform.launcher.interceptors.enabled", "true");
        System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
        System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, brokerUrl);
        System.setProperty(DistributionSettings.REPORT_DIR_PROPERTY, reportDir.toString());
        System.setProperty(DistributionSettings.MODULE_NAME_PROPERTY, "citrus-test-distribution");
        System.setProperty(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY, "1");
    }

    private SummaryGeneratingListener executeDistributedSuite(Class<?>... classes) {
        LauncherDiscoveryRequestBuilder builder = LauncherDiscoveryRequestBuilder.request();
        for (Class<?> cls : classes) {
            builder.selectors(DiscoverySelectors.selectClass(cls));
        }
        LauncherDiscoveryRequest request = builder.build();
        SummaryGeneratingListener summary = new SummaryGeneratingListener();
        try (LauncherSession session = LauncherFactory.openSession()) {
            Launcher launcher = session.getLauncher();
            launcher.registerTestExecutionListeners(summary);
            launcher.execute(request);
        }
        return summary;
    }

    public static class SampleStandardSuite {
        @Test
        void fastTestOne() {
            assertThat(1 + 1).isEqualTo(2);
        }
    }

    @AgentRequirement(key = "gpu", value = "true")
    public static class SampleGpuSuite {
        @Test
        void requiresGpuWorker() {
            assertThat("gpu").isNotBlank();
        }
    }

    @LocalOnly
    public static class SampleLocalSuite {
        static volatile boolean RAN_LOCALLY;

        @Test
        void pinnedToBuildHost() {
            RAN_LOCALLY = true;
        }
    }

    public static class SampleDynamicSuite {
        @TestFactory
        Stream<DynamicTest> dynamicScenarios() {
            return Stream.of(DynamicTest.dynamicTest("xmlScenarioAlpha", () -> assertThat(true).isTrue()));
        }
    }

    public static class SlowSampleSuite {
        static volatile CountDownLatch ENTERED_FIRST_ATTEMPT = new CountDownLatch(1);
        static volatile CountDownLatch ALLOW_FINISH_FIRST_ATTEMPT = new CountDownLatch(1);
        static volatile AtomicBoolean FIRST_CALL = new AtomicBoolean(true);

        @Test
        void resilientDistributedTest() throws InterruptedException {
            if (FIRST_CALL.compareAndSet(true, false)) {
                ENTERED_FIRST_ATTEMPT.countDown();
                ALLOW_FINISH_FIRST_ATTEMPT.await(5, TimeUnit.SECONDS);
                Thread.sleep(300);
            }
        }
    }

    public static class SampleMixedParameterizedGpuSuite {
        @Test
        void plainMethod() {
            assertThat(true).isTrue();
        }

        @Test
        @AgentRequirement(key = "gpu", value = "true")
        void gpuMethodWithInjectedParameter(org.junit.jupiter.api.TestInfo testInfo) {
            assertThat(testInfo.getDisplayName()).isNotBlank();
        }
    }
}
