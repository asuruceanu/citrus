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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.citrusframework.api.agent.CitrusAgentConfiguration;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.worker.DistributionWorkerClient;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.citrusframework.exceptions.CitrusRuntimeException;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ResourceLock("citrus-agent-worker-cache-settings")
class CitrusAgentWorkerOptionsTest {

    @TempDir
    Path tempDir;

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

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldPreserveWhitespaceInCacheDirectory() {
        Path cacheDir = tempDir.resolve(" worker cache ");
        assertThat(DistributionWorkerBridge.buildWorkerConfig(parse("--cache-dir", cacheDir.toString())).cacheDir())
                .isEqualTo(cacheDir);
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
                .hasValueSatisfying(options -> assertThat(options.configuration().getCacheCleanup()).isFalse());
        assertThat(errors.toString(StandardCharsets.UTF_8)).isEmpty();
    }

    @Test
    void shouldExitNonZeroWithoutStartingServerOnInvalidOptions() {
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8);
        List<Integer> exitCodes = new ArrayList<>();

        CitrusAgentServer.entrypoint(new String[] {"--cache-cleanup", "flase"}, List.of(), System.out, err, exitCodes::add);

        assertThat(exitCodes).containsExactly(CitrusAgentServer.STARTUP_FAILURE_EXIT_CODE);
        assertThat(CitrusAgentServer.STARTUP_FAILURE_EXIT_CODE).isNotZero();
        assertThat(errors.toString(StandardCharsets.UTF_8))
                .startsWith("Invalid Citrus agent options: Invalid value 'flase' for --[no-]cache-cleanup")
                .doesNotContain("\tat ")
                .hasLineCount(1);
    }

    @Test
    void shouldParseHelpWithoutConsumingTheNextOption() {
        CitrusAgentOptions options = new CitrusAgentOptions();
        assertThat(options.parse(new CitrusAgentConfiguration(), new String[] {"-h", "--mode", "worker"})
                .configuration().getMode()).isEqualTo("worker");
        assertThat(options.parse(new CitrusAgentConfiguration(), new String[] {"--auth-token", "--help"}))
                .satisfies(parsed -> {
                    assertThat(parsed.configuration().getAuthToken()).isEqualTo("--help");
                    assertThat(parsed.usage()).isNull();
                });
    }

    @Test
    void shouldKeepHelpConfigurationBehaviorForDirectOptionsCallers() {
        CitrusAgentConfiguration configuration = parse("-h", "--mode", "worker");
        assertThat(configuration.getMode()).isEqualTo("worker");
        assertThat(configuration.isSkipTests()).isTrue();
        assertThat(configuration.getTimeToLive()).isEqualTo(1000L);
    }

    @Test
    void shouldPrintWorkerHelpWithoutStartingServer() {
        Path cacheDir = tempDir.resolve("help-cache");
        List<String[]> cases = List.of(
                new String[] {"--help", "--mode", "worker", "--port", "0", "--cache-dir", cacheDir.toString()},
                new String[] {"--mode", "worker", "--port", "0", "--cache-dir", cacheDir.toString(), "-h"});

        for (String[] args : cases) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ByteArrayOutputStream errors = new ByteArrayOutputStream();
            List<Integer> exitCodes = new ArrayList<>();
            CitrusAgentServer.entrypoint(args, List.of(router -> {
                throw new AssertionError("Help should not start the HTTP application");
            }), new PrintStream(output, true, StandardCharsets.UTF_8),
                    new PrintStream(errors, true, StandardCharsets.UTF_8), exitCodes::add);

            assertThat(output.toString(StandardCharsets.UTF_8)).contains("Citrus application option usage:",
                    "--cache-dir", "--cache-cleanup", "--no-cache-cleanup", "--cache-free-space-buffer-size");
            assertThat(errors.toString(StandardCharsets.UTF_8)).isEmpty();
            assertThat(exitCodes).isEmpty();
            assertThat(cacheDir).doesNotExist();
        }
    }

    @Test
    void shouldExitWithoutDeployingHttpWhenWorkerCacheSettingIsInvalid() throws InterruptedException {
        String key = DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY;
        String previous = System.getProperty(key);
        System.setProperty(key, "flase");
        try {
            assertWorkerStartupFails(tempDir.resolve("invalid-settings-cache"), key);
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    @Test
    void shouldExitWithoutDeployingHttpWhenWorkerCacheIsLocked() throws InterruptedException {
        Path cacheDir = tempDir.resolve("shared-cache");
        DistributionWorkerConfig config = DistributionWorkerConfig.builder()
                .brokerUrl("http://127.0.0.1:1")
                .cacheDir(cacheDir)
                .cacheCleanup(false)
                .cacheFreeSpaceBufferBytes(0)
                .build();
        try (DistributionWorkerClient owner = new DistributionWorkerClient(config)) {
            owner.start();
            assertWorkerStartupFails(cacheDir, "--cache-dir");
        }
    }

    @Test
    @ResourceLock(Resources.SYSTEM_OUT)
    void shouldReleaseWorkerCacheWhenHttpListenerCannotBind() throws Exception {
        Path cacheDir = tempDir.resolve("failed-http-cache");
        String brokerUrl = "http://127.0.0.1:1";
        PrintStream originalOutput = System.out;

        try (ServerSocket occupiedPort = new ServerSocket(0, 1, InetAddress.getByName("0.0.0.0"))) {
            CitrusAgentConfiguration configuration = parse("--mode", "worker", "--broker-url", brokerUrl,
                    "--port", Integer.toString(occupiedPort.getLocalPort()), "--cache-dir", cacheDir.toString());
            CitrusAgentServer server = new CitrusAgentServer(configuration);
            try {
                assertThatThrownBy(server::start).isInstanceOf(CitrusRuntimeException.class)
                        .hasMessageContaining("Could not start HTTP listener on port " + occupiedPort.getLocalPort());
                assertThat(server.completed.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(server.isRunning()).isFalse();

                try (DistributionWorkerClient worker = new DistributionWorkerClient(
                        DistributionWorkerBridge.buildWorkerConfig(configuration))) {
                    worker.start();
                }
            } finally {
                server.stop();
            }
        } finally {
            System.setOut(originalOutput);
        }
    }

    @Test
    @ResourceLock(Resources.SYSTEM_OUT)
    void shouldKeepOneWorkerAndListenerAcrossConcurrentStarts() throws Exception {
        PrintStream originalOutput = System.out;
        CitrusAgentConfiguration configuration = parse("--mode", "worker", "--port", "0", "--duration", "-1",
                "--broker-url", "http://127.0.0.1:1", "--cache-dir", tempDir.resolve("repeat-start-cache").toString());
        AtomicInteger routesCreated = new AtomicInteger();
        CountDownLatch buildingRoutes = new CountDownLatch(1);
        CompletableFuture<Void> allowListen = new CompletableFuture<>();
        CitrusAgentServer server = new CitrusAgentServer(configuration, List.of(router -> {
            routesCreated.incrementAndGet();
            buildingRoutes.countDown();
            allowListen.orTimeout(5, TimeUnit.SECONDS).join();
        }));
        try {
            CompletableFuture<Void> firstStart = CompletableFuture.runAsync(server::start);
            assertThat(buildingRoutes.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture.runAsync(server::start).get(5, TimeUnit.SECONDS);
            allowListen.complete(null);
            firstStart.get(5, TimeUnit.SECONDS);
            server.start();

            assertThat(routesCreated.get()).isEqualTo(1);
            try (DistributionWorkerClient secondWorker = new DistributionWorkerClient(
                    DistributionWorkerBridge.buildWorkerConfig(configuration))) {
                assertThatThrownBy(secondWorker::start).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("--cache-dir");
            }

            server.stop();
            assertThat(server.completed.get(5, TimeUnit.SECONDS)).isTrue();
            try (DistributionWorkerClient replacement = new DistributionWorkerClient(
                    DistributionWorkerBridge.buildWorkerConfig(configuration))) {
                replacement.start();
            }
        } finally {
            allowListen.complete(null);
            server.stop();
            System.setOut(originalOutput);
        }
    }

    @Test
    @ResourceLock(Resources.SYSTEM_OUT)
    void shouldCompleteOnlyAfterShutdownOnTheHttpEventLoop() throws Exception {
        PrintStream originalOutput = System.out;
        CitrusAgentConfiguration configuration = parse("--port", "0", "--duration", "-1", "--skipTests", "true");
        CompletableFuture<Context> httpContext = new CompletableFuture<>();
        CompletableFuture<Void> allowEventLoopToExit = new CompletableFuture<>();
        CountDownLatch stopReturned = new CountDownLatch(1);
        CitrusAgentServer server = new CitrusAgentServer(configuration,
                List.of(router -> httpContext.complete(Vertx.currentContext())));

        try {
            server.start();
            httpContext.get(5, TimeUnit.SECONDS).runOnContext(ignored -> {
                server.stop();
                stopReturned.countDown();
                allowEventLoopToExit.orTimeout(5, TimeUnit.SECONDS).join();
            });
            assertThat(stopReturned.await(5, TimeUnit.SECONDS)).isTrue();
            server.stop();
            assertThat(server.completed).isNotDone();

            allowEventLoopToExit.complete(null);
            assertThat(server.completed.get(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            allowEventLoopToExit.complete(null);
            server.stop();
            System.setOut(originalOutput);
        }
    }

    private static void assertWorkerStartupFails(Path cacheDir, String expectedMessage) throws InterruptedException {
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        List<Integer> exitCodes = new ArrayList<>();
        CountDownLatch deployed = new CountDownLatch(1);

        CitrusAgentServer.entrypoint(new String[] {"--mode", "worker", "--port", "0", "--duration", "1000",
                        "--broker-url", "http://127.0.0.1:1", "--cache-dir", cacheDir.toString()},
                List.of(router -> deployed.countDown()), System.out,
                new PrintStream(errors, true, StandardCharsets.UTF_8), exitCodes::add);

        assertThat(exitCodes).containsExactly(CitrusAgentServer.STARTUP_FAILURE_EXIT_CODE);
        assertThat(errors.toString(StandardCharsets.UTF_8)).startsWith("Failed to start Citrus agent:")
                .contains(expectedMessage).hasLineCount(1);
        assertThat(deployed.await(200, TimeUnit.MILLISECONDS)).isFalse();
    }
}
