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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.config.DistributionSettings.WorkerForkMode;
import org.citrusframework.distribution.coordinator.DistributionCoordinatorConfig;
import org.citrusframework.distribution.coordinator.DistributionCoordinatorServer;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.worker.DistributionWorkerClient;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.launcher.TagFilter;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.support.testng.engine.TestNGTestEngine;
import org.testng.annotations.DataProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

class TestNgDistributionTest {

    private static final String EXPECTED_SHARED_JVM = "testng.distribution.expectedSharedJvm";
    private static final String LAUNCHER_PID = "testng.distribution.launcherPid";

    @TempDir
    Path tempDir;

    @Test
    void executesDataProviderAndPrunedGroupsInWorkerJvm() throws Exception {
        executeOnWorker(WorkerForkMode.AUTO);
    }

    @Test
    void executesDataProviderAndPrunedGroupsInForkedJvm() throws Exception {
        executeOnWorker(WorkerForkMode.ALWAYS);
    }

    private void executeOnWorker(WorkerForkMode mode) throws Exception {
        try (var coordinator = new DistributionCoordinatorServer(DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("coordinator"))
                .jdbcUrl("jdbc:hsqldb:mem:testng_" + System.nanoTime())
                .build())) {
            coordinator.start();
            try (var worker = new DistributionWorkerClient(DistributionWorkerConfig.builder()
                    .workerId("testng-worker")
                    .coordinatorUrl(coordinator.baseUrl())
                    .cacheDir(tempDir.resolve("worker"))
                    .forkMode(mode)
                    .build())) {
                worker.start();
                assertThat(worker.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();

                Map<String, String> previous = new HashMap<>();
                setProperty(previous, DistributionSettings.ROLE_PROPERTY, null);
                setProperty(previous, DistributionSettings.ENABLED_PROPERTY, "true");
                setProperty(previous, DistributionSettings.COORDINATOR_URL_PROPERTY, coordinator.baseUrl());
                setProperty(previous, DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY, "0");
                setProperty(previous, DistributionSettings.MIN_REMOTE_COLD_UNITS_PROPERTY, "1");
                setProperty(previous, DistributionSettings.FALLBACK_LOCAL_PROPERTY, "false");
                setProperty(previous, DistributionSettings.REPORT_DIR_PROPERTY, tempDir.resolve("reports").toString());
                setProperty(previous, EXPECTED_SHARED_JVM, Boolean.toString(mode == WorkerForkMode.AUTO));
                setProperty(previous, LAUNCHER_PID, Long.toString(ProcessHandle.current().pid()));

                var summaryListener = new SummaryGeneratingListener();
                try {
                    var request = LauncherDiscoveryRequestBuilder.request()
                            .selectors(selectClass(DataProviderNgFixture.class), selectClass(MixedGroupNgFixture.class))
                            .filters(TagFilter.excludeTags("should-fail"))
                            .build();
                    new DistributedLauncher(LauncherFactory.create()).execute(request, summaryListener);
                } finally {
                    previous.forEach(TestNgDistributionTest::setProperty);
                }

                var summary = summaryListener.getSummary();
                assertThat(summary.getFailures()).isEmpty();
                assertThat(summary.getTestsSucceededCount()).isEqualTo(4);
                assertThat(summary.getTestsSkippedCount()).isZero();

                String session = coordinator.store().listSessions(1).getFirst().sessionId();
                assertThat(coordinator.store().listPartitions(session)).isNotEmpty().allSatisfy(partition -> {
                    assertThat(partition.workerId()).isEqualTo("testng-worker");
                    assertThat(partition.state()).isEqualTo(PartitionState.COMPLETED);
                });
            }
        }
    }

    private static void setProperty(Map<String, String> previous, String key, String value) {
        previous.put(key, System.getProperty(key));
        setProperty(key, value);
    }

    private static void setProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    static void assertOnRequestedWorker() {
        assertThat(System.getProperty(DistributionSettings.ROLE_PROPERTY)).isEqualTo(DistributionSettings.ROLE_WORKER);
        boolean sharedJvm = ProcessHandle.current().pid() == Long.parseLong(System.getProperty(LAUNCHER_PID));
        assertThat(sharedJvm).isEqualTo(Boolean.parseBoolean(System.getProperty(EXPECTED_SHARED_JVM)));
        assertThat(TestNGTestEngine.class.getClassLoader()).isSameAs(TestNgDistributionTest.class.getClassLoader());
    }
}

class DataProviderNgFixture {
    @DataProvider
    public Object[][] rows() {
        return new Object[][] {{1}, {2}, {3}};
    }

    @org.testng.annotations.Test(dataProvider = "rows")
    public void usesDataProvider(int value) {
        assertThat(value).isBetween(1, 3);
        TestNgDistributionTest.assertOnRequestedWorker();
    }
}

class MixedGroupNgFixture {
    @org.testng.annotations.Test
    public void included() {
        TestNgDistributionTest.assertOnRequestedWorker();
    }

    @org.testng.annotations.Test(groups = "should-fail", dataProvider = "rows",
            dataProviderClass = DataProviderNgFixture.class)
    public void excluded(int value) {
        throw new AssertionError("The excluded group ran on the worker with value " + value);
    }
}
