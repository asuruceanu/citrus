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

package org.citrusframework.agent;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import org.citrusframework.api.agent.CitrusAgentConfiguration;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.parallel.ResourceLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ResourceLock("citrus-agent-worker-cache-settings")
class CitrusAgentWorkerOptionsTest {

    private static CitrusAgentConfiguration parse(String... args) {
        return new CitrusAgentOptions().apply(args);
    }

    @TestFactory
    Stream<DynamicTest> shouldParseCacheDir() {
        return Stream.of(
                new String[] {"--cache-dir", "/tmp/c1"},
                new String[] {"--cache-dir=/tmp/c1"})
                .map(args -> DynamicTest.dynamicTest(String.join(" ", args), () -> {
                    CitrusAgentConfiguration configuration = parse(args);
                    assertThat(configuration.getCacheDir()).isEqualTo("/tmp/c1");
                    assertThat(DistributionWorkerBridge.buildWorkerConfig(configuration).cacheDir())
                            .isEqualTo(Paths.get("/tmp/c1"));
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldParseCacheCleanup() {
        return Stream.of(
                cleanupCase(true, "--cache-cleanup"),
                cleanupCase(true, "--cache-cleanup=true"),
                cleanupCase(true, "--cache-cleanup", "TRUE"),
                cleanupCase(false, "--no-cache-cleanup"),
                cleanupCase(false, "--no-cache-cleanup=true"),
                cleanupCase(false, "--no-cache-cleanup", "true"),
                cleanupCase(false, "--cache-cleanup=false"),
                cleanupCase(false, "--cache-cleanup", "false"),
                cleanupCase(true, "--no-cache-cleanup=false"),
                cleanupCase(true, "--no-cache-cleanup", "false"),
                cleanupCase(true, "--no-cache-cleanup", "--cache-cleanup"),
                cleanupCase(false, "--cache-cleanup", "--no-cache-cleanup"));
    }

    private static DynamicTest cleanupCase(boolean expected, String... args) {
        return DynamicTest.dynamicTest(String.join(" ", args) + " -> " + expected, () -> {
            CitrusAgentConfiguration configuration = parse(args);
            assertThat(configuration.getCacheCleanup()).isEqualTo(expected);
            assertThat(DistributionWorkerBridge.buildWorkerConfig(configuration).cacheCleanup()).isEqualTo(expected);
        });
    }

    @Test
    void shouldNotConsumeFollowingFlagAfterBareCacheCleanup() {
        CitrusAgentConfiguration configuration = parse("--no-cache-cleanup", "--broker-url", "http://broker:9000");
        assertThat(configuration.getCacheCleanup()).isFalse();
        assertThat(configuration.getBrokerUrl()).isEqualTo("http://broker:9000");
    }

    @TestFactory
    Stream<DynamicTest> shouldParseCacheFreeSpaceBufferSize() {
        return Stream.of(
                new String[] {"--cache-free-space-buffer-size", "512MiB"},
                new String[] {"--cache-free-space-buffer-size=512MiB"})
                .map(args -> DynamicTest.dynamicTest(String.join(" ", args), () -> {
                    CitrusAgentConfiguration configuration = parse(args);
                    assertThat(configuration.getCacheFreeSpaceBufferSize()).isEqualTo("512MiB");
                    assertThat(DistributionWorkerBridge.buildWorkerConfig(configuration).cacheFreeSpaceBufferBytes())
                            .isEqualTo(536_870_912L);
                }));
    }

    @Test
    void shouldInheritSystemPropertiesWhenCacheOptionsUnset() {
        System.setProperty(DistributionSettings.WORKER_CACHE_DIR_PROPERTY, "/tmp/sysprop-cache");
        System.setProperty(DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY, "false");
        System.setProperty(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY, "1MiB");
        try {
            CitrusAgentConfiguration configuration = parse("--mode", "worker", "--broker-url", "http://broker:9000");
            assertThat(configuration.getCacheCleanup()).isNull();
            assertThat(configuration.getCacheFreeSpaceBufferSize()).isNull();

            DistributionWorkerConfig config = DistributionWorkerBridge.buildWorkerConfig(configuration);
            assertThat(config.cacheDir()).isEqualTo(Paths.get("/tmp/sysprop-cache"));
            assertThat(config.cacheCleanup()).isFalse();
            assertThat(config.cacheFreeSpaceBufferBytes()).isEqualTo(1_048_576L);
        } finally {
            System.clearProperty(DistributionSettings.WORKER_CACHE_DIR_PROPERTY);
            System.clearProperty(DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY);
            System.clearProperty(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY);
        }
    }

    @TestFactory
    Stream<DynamicTest> shouldRejectInvalidCacheOptions() {
        return Stream.of(
                List.of("--cache"),
                List.of("--cache-foo"),
                List.of("--no-cache"),
                List.of("--cache-cleanup", "flase"),
                List.of("--cache-cleanup=flase"),
                List.of("--cache-cleanup="),
                List.of("--no-cache-cleanup=maybe"),
                List.of("--no-cache-cleanup="),
                List.of("--cache-dir"),
                List.of("--cache-dir="),
                List.of("--cache-dir=  "),
                List.of("--cache-dir", " "),
                List.of("--cache-dir", "--broker-url", "http://x"),
                List.of("--cache-free-space-buffer-size"),
                List.of("--cache-free-space-buffer-size="),
                List.of("--cache-free-space-buffer-size", "--broker-url", "http://x"),
                List.of("--cache-free-space-buffer-size=2GB"))
                .map(args -> DynamicTest.dynamicTest(String.join(" ", args), () ->
                        assertThatThrownBy(() -> parse(args.toArray(String[]::new)))
                                .isInstanceOf(IllegalArgumentException.class)));
    }
}
