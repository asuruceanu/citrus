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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.citrusframework.distribution.annotations.LocalOnly;
import org.citrusframework.distribution.annotations.RemoteOnly;
import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.worker.DistributionWorkerClient;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DistributedLauncherTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void resetProps() {
        System.clearProperty(DistributionSettings.ENABLED_PROPERTY);
        System.clearProperty(DistributionSettings.BROKER_URL_PROPERTY);
        System.clearProperty(DistributionSettings.FALLBACK_LOCAL_PROPERTY);
        System.clearProperty(DistributionSettings.REPORT_DIR_PROPERTY);
        System.clearProperty(DistributionSettings.CONNECT_TIMEOUT_MS_PROPERTY);
    }

    @Test
    void shouldDistributeRemoteTestsPinLocalOnlyAndWriteSurefireXml() throws Exception {
        LocalPinnedFixture.EXECUTED_IN_COORDINATOR.set(false);
        RemoteSampleFixture.EXECUTED_IN_COORDINATOR.set(false);

        DistributionBrokerConfig brokerCfg = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:junit_dist_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerCfg)) {
            broker.start();

            DistributionWorkerConfig workerCfg = DistributionWorkerConfig.builder()
                    .workerId("worker-1")
                    .brokerUrl(broker.baseUrl())
                    .cacheDir(tempDir.resolve("worker-cache"))
                    .capabilities(Map.of("os", "linux"))
                    .build();

            try (DistributionWorkerClient worker = new DistributionWorkerClient(workerCfg)) {
                worker.start();

                Path reportDir = tempDir.resolve("surefire-reports");
                System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
                System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, broker.baseUrl());
                System.setProperty(DistributionSettings.REPORT_DIR_PROPERTY, reportDir.toString());

                Launcher rawLauncher = LauncherFactory.create();
                DistributedLauncher distributed = new DistributedLauncher(rawLauncher);

                AtomicInteger passedCount = new AtomicInteger();
                LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                        .selectors(
                                DiscoverySelectors.selectClass(RemoteSampleFixture.class),
                                DiscoverySelectors.selectClass(LocalPinnedFixture.class))
                        .build();

                distributed.execute(request, new TestExecutionListener() {
                    @Override
                    public void executionFinished(TestIdentifier id, TestExecutionResult result) {
                        if (id.isTest() && result.getStatus() == TestExecutionResult.Status.SUCCESSFUL) {
                            passedCount.incrementAndGet();
                        }
                    }
                });

                assertThat(passedCount.get()).isEqualTo(2);
                assertThat(LocalPinnedFixture.EXECUTED_IN_COORDINATOR.get()).isTrue();
                // RemoteSampleFixture ran inside IsolatedPartitionRunner's child ClassLoader, NOT coordinator static!
                assertThat(RemoteSampleFixture.EXECUTED_IN_COORDINATOR.get()).isFalse();

                Path xmlReport = reportDir.resolve("TEST-" + RemoteSampleFixture.class.getName() + ".xml");
                assertThat(Files.exists(xmlReport)).isTrue();
                assertThat(Files.readString(xmlReport)).contains("shouldRunOnRemoteWorker");
            }
        }
    }

    @Test
    void shouldFallbackLocallyWhenBrokerUnreachableAndFailFastOnRemoteOnly() {
        RemoteSampleFixture.EXECUTED_IN_COORDINATOR.set(false);
        System.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
        System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, "http://127.0.0.1:1");
        System.setProperty(DistributionSettings.CONNECT_TIMEOUT_MS_PROPERTY, "500");
        System.setProperty(DistributionSettings.FALLBACK_LOCAL_PROPERTY, "true");

        DistributedLauncher distributed = new DistributedLauncher(LauncherFactory.create());
        LauncherDiscoveryRequest fallbackReq = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(RemoteSampleFixture.class))
                .build();
        distributed.execute(fallbackReq);
        assertThat(RemoteSampleFixture.EXECUTED_IN_COORDINATOR.get()).isTrue();

        LauncherDiscoveryRequest remoteOnlyReq = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(StrictRemoteOnlyFixture.class))
                .build();
        assertThatThrownBy(() -> distributed.execute(remoteOnlyReq))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("broker unreachable");
    }

    @Test
    void shouldForwardCustomSurefireSystemPropertiesWhileExcludingHostAndDistributionInternals() {
        java.util.Properties source = new java.util.Properties();
        source.setProperty("citrus.default.timeout", "15000");
        source.setProperty("spring.profiles.active", "integration");
        source.setProperty("custom.service.port", "19090");
        source.setProperty("citrus.test.distribution.broker.url", "http://127.0.0.1:18080");
        source.setProperty("junit.platform.launcher.interceptors.enabled", "true");
        source.setProperty("java.home", "/host/jdk-21");
        source.setProperty("user.dir", "/host/workspace");
        source.setProperty("os.name", "Mac OS X");
        source.setProperty("surefire.real.class.path", "/host/target/surefirebooter.jar");
        source.setProperty("maven.multiModuleProjectDirectory", "/host/workspace");
        source.setProperty("basedir", "/host/workspace/module");
        source.setProperty("localRepository", "/host/.m2/repository");

        java.util.Map<String, String> forwarded = DistributedLauncher.collectPropagatedProperties(source);

        assertThat(forwarded)
                .containsEntry("citrus.default.timeout", "15000")
                .containsEntry("spring.profiles.active", "integration")
                .containsEntry("custom.service.port", "19090")
                .doesNotContainKeys(
                        "citrus.test.distribution.broker.url",
                        "junit.platform.launcher.interceptors.enabled",
                        "java.home",
                        "user.dir",
                        "os.name",
                        "surefire.real.class.path",
                        "maven.multiModuleProjectDirectory",
                        "basedir",
                        "localRepository");
    }

    public static class RemoteSampleFixture {
        static final AtomicBoolean EXECUTED_IN_COORDINATOR = new AtomicBoolean(false);

        @Test
        void shouldRunOnRemoteWorker() {
            if (!DistributionSettings.isWorkerRole()) {
                EXECUTED_IN_COORDINATOR.set(true);
            }
        }
    }

    @LocalOnly
    public static class LocalPinnedFixture {
        static final AtomicBoolean EXECUTED_IN_COORDINATOR = new AtomicBoolean(false);

        @Test
        void shouldStayOnCoordinator() {
            if (!DistributionSettings.isWorkerRole()) {
                EXECUTED_IN_COORDINATOR.set(true);
            }
        }
    }

    @RemoteOnly
    public static class StrictRemoteOnlyFixture {
        @Test
        void mustRunRemotely() {
        }
    }
}
