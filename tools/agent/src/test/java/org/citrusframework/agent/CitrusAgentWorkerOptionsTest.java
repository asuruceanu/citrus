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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
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

    @Test
    void shouldResolveLastCacheCleanupFlagAndKeepOtherOptions() {
        CitrusAgentConfiguration configuration = parse("--mode", "worker", "--no-cache-cleanup",
                "--broker-url", "http://broker:9000", "--cache-cleanup=true");
        assertThat(configuration.getCacheCleanup()).isTrue();
        assertThat(configuration.getMode()).isEqualTo("worker");
        assertThat(configuration.getBrokerUrl()).isEqualTo("http://broker:9000");
    }

    @Test
    void shouldNotTreatValuesOfOtherOptionsAsCacheOptions() {
        CitrusAgentConfiguration configuration = parse("--auth-token", "--cache-x", "--broker-url", "--no-cache-cleanup");
        assertThat(configuration.getAuthToken()).isEqualTo("--cache-x");
        assertThat(configuration.getBrokerUrl()).isEqualTo("--no-cache-cleanup");
        assertThat(configuration.getCacheCleanup()).isNull();
    }

    @Test
    void shouldValidateCacheOptionsEvenWhenHelpIsRequested() {
        assertThatThrownBy(() -> parse("--help", "--cache"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown cache option: --cache");
        assertThatThrownBy(() -> parse("-h", "--cache-dir"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Missing parameter value for --cache-dir option");
    }

    @TestFactory
    Stream<DynamicTest> shouldRejectInvalidCacheOptions() {
        return Stream.of(
                invalidCase("Unknown cache option: --cache", "--cache"),
                invalidCase("Unknown cache option: --cache-foo", "--cache-foo"),
                invalidCase("Unknown cache option: --no-cache", "--no-cache"),
                invalidCase("cache-cleanup", "--cache-cleanup", "flase"),
                invalidCase("cache-cleanup", "--cache-cleanup=flase"),
                invalidCase("cache-cleanup", "--cache-cleanup="),
                invalidCase("cache-cleanup", "--no-cache-cleanup=maybe"),
                invalidCase("cache-cleanup", "--no-cache-cleanup="),
                invalidCase("--cache-dir", "--cache-dir"),
                invalidCase("--cache-dir", "--cache-dir="),
                invalidCase("--cache-dir", "--cache-dir=  "),
                invalidCase("--cache-dir", "--cache-dir", " "),
                invalidCase("--cache-dir", "--cache-dir", "--broker-url", "http://x"),
                invalidCase("--cache-free-space-buffer-size", "--cache-free-space-buffer-size"),
                invalidCase("--cache-free-space-buffer-size", "--cache-free-space-buffer-size="),
                invalidCase("--cache-free-space-buffer-size", "--cache-free-space-buffer-size", "--broker-url", "http://x"),
                invalidCase("--cache-free-space-buffer-size", "--cache-free-space-buffer-size=2GB"));
    }

    private static DynamicTest invalidCase(String expectedMessagePart, String... args) {
        return DynamicTest.dynamicTest(String.join(" ", args) + " -> " + expectedMessagePart, () ->
                assertThatThrownBy(() -> parse(args))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining(expectedMessagePart));
    }

    @Test
    void shouldReportInvalidCacheOptionAsOneLineOperatorError() {
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8);

        assertThat(CitrusAgentServer.parseOptions(new String[] {"--mode", "worker", "--cache-dir"}, err)).isEmpty();

        assertThat(errors.toString(StandardCharsets.UTF_8))
                .isEqualTo("Invalid Citrus agent options: Missing parameter value for --cache-dir option"
                        + System.lineSeparator());
    }

    @Test
    void shouldReportOtherInvalidOptionsTheSameWay() {
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8);

        assertThat(CitrusAgentServer.parseOptions(new String[] {"--mode"}, err)).isEmpty();

        assertThat(errors.toString(StandardCharsets.UTF_8))
                .isEqualTo("Invalid Citrus agent options: Missing parameter value for -m/--mode option"
                        + System.lineSeparator());
    }

    @Test
    void shouldReturnParsedOptionsWithoutOperatorError() {
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8);

        assertThat(CitrusAgentServer.parseOptions(new String[] {"--mode", "worker", "--no-cache-cleanup"}, err))
                .hasValueSatisfying(configuration -> assertThat(configuration.getCacheCleanup()).isFalse());
        assertThat(errors.toString(StandardCharsets.UTF_8)).isEmpty();
    }

    @Test
    void shouldExitNonZeroWithoutStartingServerOnInvalidOptions() {
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8);
        List<Integer> exitCodes = new ArrayList<>();

        CitrusAgentServer.entrypoint(new String[] {"--cache-cleanup", "flase"}, List.of(), err, exitCodes::add);

        assertThat(exitCodes).containsExactly(CitrusAgentServer.INVALID_OPTIONS_EXIT_CODE);
        assertThat(CitrusAgentServer.INVALID_OPTIONS_EXIT_CODE).isNotZero();
        assertThat(errors.toString(StandardCharsets.UTF_8))
                .startsWith("Invalid Citrus agent options: Invalid value 'flase' for --[no-]cache-cleanup")
                .doesNotContain("\tat ")
                .hasLineCount(1);
    }
}
