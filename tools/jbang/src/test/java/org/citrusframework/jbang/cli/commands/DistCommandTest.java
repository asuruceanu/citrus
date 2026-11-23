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

package org.citrusframework.jbang.cli.commands;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.aesh.AeshRuntimeRunner;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.jbang.cli.CitrusJBangMain;
import org.citrusframework.jbang.cli.StringPrinter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ResourceLock("citrus-jbang-run")
class DistCommandTest extends CommandTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldPrintDistHelpTree() {
        CitrusJBangMain main = createCitrusJBangMain();
        assertThat(main.execute("dist")).isZero();
        assertThat(printer.getOutput()).contains("broker").contains("worker").contains("sessions").contains("status");

        printer = new StringPrinter();
        main = createCitrusJBangMain();
        assertThat(main.execute("dist", "broker")).isZero();
        assertThat(printer.getOutput()).contains("start");

        printer = new StringPrinter();
        main = createCitrusJBangMain();
        assertThat(main.execute("dist", "worker")).isZero();
        assertThat(printer.getOutput()).contains("start");

        assertThat(main.execute("dist", "--help")).isZero();
        assertThat(main.execute("dist", "broker", "start", "--help")).isZero();
        assertThat(main.execute("dist", "worker", "start", "--help")).isZero();
        assertThat(main.execute("dist", "sessions", "--help")).isZero();
        assertThat(main.execute("dist", "status", "--help")).isZero();
    }

    @Test
    void shouldQueryLiveBrokerSessionsAndStatus() throws Exception {
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("cache"))
                .jdbcUrl("jdbc:hsqldb:mem:jbang_dist_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(config)) {
            broker.start();

            HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(broker.baseUrl() + "/api/v1/sessions"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(DistributionJson.toJson(
                                    new CreateSessionRequest("citrus-demo", "main", "deadbeef", List.of(), 5, 1))))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());

            CitrusJBangMain main = createCitrusJBangMain();
            main.execute("dist", "sessions", "--broker-url", broker.baseUrl());
            assertThat(printer.getOutput()).contains("citrus-demo").contains("RUNNING");

            printer = new StringPrinter();
            main = createCitrusJBangMain();
            main.execute("dist", "status", "--broker-url", broker.baseUrl(), "--json");
            assertThat(printer.getOutput()).contains("\"status\"").contains("UP");
        }
    }

    @Test
    void shouldStartBrokerAndWorkerWithBoundedDuration() throws Exception {
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("worker-broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:jbang_worker_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(config)) {
            broker.start();

            CitrusJBangMain main = createCitrusJBangMain();
            main.execute("dist", "worker", "start",
                    "--broker-url", broker.baseUrl(),
                    "--worker-id", "jbang-worker-1",
                    "--cache-dir", tempDir.resolve("wcache").toString(),
                    "--duration-seconds", "1");
            assertThat(printer.getOutput()).contains("jbang-worker-1");
        }

        printer = new StringPrinter();
        CitrusJBangMain main = createCitrusJBangMain();
        main.execute("dist", "broker", "start",
                "--port", "0",
                "--jdbc-url", "jdbc:hsqldb:mem:jbang_broker_cli_" + System.nanoTime(),
                "--cache-dir", tempDir.resolve("bcache").toString(),
                "--duration-seconds", "1");
        assertThat(printer.getOutput()).contains("Citrus Test Distribution Broker listening on");
    }

    @Test
    void shouldListWorkerCacheOptionsInHelp() {
        CitrusJBangMain main = createCitrusJBangMain();
        assertThat(main.execute("dist", "worker", "start", "--help")).isZero();
        assertThat(printer.getOutput())
                .contains("--cache-dir")
                .contains("--cache-cleanup")
                .contains("--no-cache-cleanup")
                .contains("--cache-free-space-buffer-size");
    }

    static Stream<Arguments> validWorkerStartArgs() {
        return Stream.of(
                Arguments.of(List.of("--cache-dir", "/tmp/c1"), List.of("--cache-dir=/tmp/c1")),
                Arguments.of(List.of("--cache-dir=/tmp/c1"), List.of("--cache-dir=/tmp/c1")),
                Arguments.of(List.of("--cache-cleanup"), List.of("--cache-cleanup=true")),
                Arguments.of(List.of("--cache-cleanup", "TRUE"), List.of("--cache-cleanup=true")),
                Arguments.of(List.of("--no-cache-cleanup"), List.of("--cache-cleanup=false")),
                Arguments.of(List.of("--no-cache-cleanup=true"), List.of("--cache-cleanup=false")),
                Arguments.of(List.of("--cache-cleanup=false"), List.of("--cache-cleanup=false")),
                Arguments.of(List.of("--cache-cleanup", "false"), List.of("--cache-cleanup=false")),
                Arguments.of(List.of("--no-cache-cleanup=false"), List.of("--cache-cleanup=true")),
                Arguments.of(List.of("--no-cache-cleanup", "false"), List.of("--cache-cleanup=true")),
                Arguments.of(List.of("--no-cache-cleanup", "--cache-cleanup"), List.of("--cache-cleanup=true")),
                Arguments.of(List.of("--cache-cleanup", "--no-cache-cleanup"), List.of("--cache-cleanup=false")),
                Arguments.of(List.of("--no-cache-cleanup", "--broker-url", "http://broker:9000"),
                        List.of("--broker-url", "http://broker:9000", "--cache-cleanup=false")),
                Arguments.of(List.of("--cache-free-space-buffer-size", "512MiB"),
                        List.of("--cache-free-space-buffer-size=512MiB")),
                Arguments.of(List.of("--no-cache-cleanup", "--cache-cleanup=true"), List.of("--cache-cleanup=true")),
                Arguments.of(List.of("--auth-token", "--cache-x", "--worker-id", "--no-cache-cleanup"),
                        List.of("--auth-token", "--cache-x", "--worker-id", "--no-cache-cleanup")),
                Arguments.of(List.of("--cache-dir", "/tmp/c1", "--worker-id", "w1", "--cache-cleanup"),
                        List.of("--worker-id", "w1", "--cache-dir=/tmp/c1", "--cache-cleanup=true")),
                Arguments.of(List.of("--cache-free-space-buffer-size=512MiB"),
                        List.of("--cache-free-space-buffer-size=512MiB")));
    }

    @ParameterizedTest
    @MethodSource("validWorkerStartArgs")
    void shouldNormalizeValidWorkerCacheArgs(List<String> input, List<String> expected) {
        String[] normalized = DistWorkerStart.normalizeWorkerStartArgs(
                Stream.concat(Stream.of("dist", "worker", "start"), input.stream()).toArray(String[]::new));
        assertThat(normalized).containsExactlyElementsOf(
                Stream.concat(Stream.of("dist", "worker", "start"), expected.stream()).toList());
    }

    static Stream<Arguments> invalidWorkerStartArgs() {
        return Stream.of(
                Arguments.of(List.of("--cache"), "Unknown cache option: --cache"),
                Arguments.of(List.of("--cache-foo"), "Unknown cache option: --cache-foo"),
                Arguments.of(List.of("--no-cache"), "Unknown cache option: --no-cache"),
                Arguments.of(List.of("--cache-cleanup", "flase"), "cache-cleanup"),
                Arguments.of(List.of("--cache-cleanup=flase"), "cache-cleanup"),
                Arguments.of(List.of("--cache-cleanup="), "cache-cleanup"),
                Arguments.of(List.of("--no-cache-cleanup=maybe"), "cache-cleanup"),
                Arguments.of(List.of("--no-cache-cleanup="), "cache-cleanup"),
                Arguments.of(List.of("--cache-dir"), "--cache-dir"),
                Arguments.of(List.of("--cache-dir="), "--cache-dir"),
                Arguments.of(List.of("--cache-dir=  "), "--cache-dir"),
                Arguments.of(List.of("--cache-dir", " "), "--cache-dir"),
                Arguments.of(List.of("--cache-dir", "--broker-url", "http://x"), "--cache-dir"),
                Arguments.of(List.of("--cache-free-space-buffer-size"), "--cache-free-space-buffer-size"),
                Arguments.of(List.of("--cache-free-space-buffer-size="), "--cache-free-space-buffer-size"),
                Arguments.of(List.of("--cache-free-space-buffer-size", "--broker-url", "http://x"),
                        "--cache-free-space-buffer-size"),
                Arguments.of(List.of("--cache-free-space-buffer-size=2GB"), "--cache-free-space-buffer-size"),
                Arguments.of(List.of("--cache", "--help"), "Unknown cache option: --cache"));
    }

    @ParameterizedTest
    @MethodSource("invalidWorkerStartArgs")
    void shouldRejectInvalidWorkerCacheArgs(List<String> input, String expectedMessagePart) {
        String[] args = Stream.concat(Stream.of("dist", "worker", "start"), input.stream()).toArray(String[]::new);
        assertThatThrownBy(() -> DistWorkerStart.normalizeWorkerStartArgs(args))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedMessagePart);
    }

    static Stream<Arguments> invalidWorkerStartCommandLines() {
        return Stream.of(
                Arguments.of(List.of("--cache"), "--cache"),
                Arguments.of(List.of("--cache-cleanup", "flase"), "flase"),
                Arguments.of(List.of("--cache-dir", "--broker-url", "http://x"), "--cache-dir"),
                Arguments.of(List.of("--cache-free-space-buffer-size="), "--cache-free-space-buffer-size"),
                Arguments.of(List.of("--cache-dir", " "), "Missing parameter value for --cache-dir option"),
                Arguments.of(List.of("--cache-dir="), "Missing parameter value for --cache-dir option"));
    }

    @ParameterizedTest
    @MethodSource("invalidWorkerStartCommandLines")
    void shouldFailWorkerStartOnInvalidCacheArgs(List<String> input, String expectedMessagePart) {
        CitrusJBangMain main = createCitrusJBangMain(1);
        String[] args = Stream.concat(Stream.of("dist", "worker", "start"), input.stream()).toArray(String[]::new);
        assertThat(main.execute(args)).isEqualTo(1);
        assertThat(printer.getOutput()).contains(expectedMessagePart);
    }

    @Test
    void shouldLeaveOtherCommandsUntouched() {
        String[] args = {"run", "--cache-foo"};
        assertThat(DistWorkerStart.normalizeWorkerStartArgs(args)).containsExactly("run", "--cache-foo");
        String[] broker = {"dist", "broker", "start", "--cache-dir", "/tmp/b"};
        assertThat(DistWorkerStart.normalizeWorkerStartArgs(broker)).containsExactly(broker);
        String[] help = {"dist", "worker", "start", "--help"};
        assertThat(DistWorkerStart.normalizeWorkerStartArgs(help)).containsExactly(help);
        String[] positional = {"other", "dist", "worker", "start", "--cache-foo"};
        assertThat(DistWorkerStart.normalizeWorkerStartArgs(positional)).containsExactly(positional);
    }

    @Test
    void shouldNormalizeWorkerStartArgsAfterLeadingOptions() {
        assertThat(DistWorkerStart.normalizeWorkerStartArgs(
                new String[] {"-V", "dist", "worker", "start", "--cache-cleanup", "--worker-id", "w1"}))
                .containsExactly("-V", "dist", "worker", "start", "--worker-id", "w1", "--cache-cleanup=true");
        assertThat(DistWorkerStart.normalizeWorkerStartArgs(
                new String[] {"--verbose", "dist", "worker", "start", "--no-cache-cleanup", "--cache-cleanup=true"}))
                .containsExactly("--verbose", "dist", "worker", "start", "--cache-cleanup=true");
        String[] invalid = {"-V", "dist", "worker", "start", "--cache-foo"};
        assertThatThrownBy(() -> DistWorkerStart.normalizeWorkerStartArgs(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown cache option: --cache-foo");
    }

    @Test
    void shouldBuildWorkerConfigFromCacheOptions() {
        DistWorkerStart command = new DistWorkerStart();
        command.brokerUrl = "http://localhost:8765";
        command.cacheDir = tempDir.resolve("cfg-cache").toString();
        command.cacheCleanup = "false";
        command.cacheFreeSpaceBufferSize = "64MiB";

        DistributionWorkerConfig config = command.buildWorkerConfig();
        assertThat(config.cacheDir()).isEqualTo(tempDir.resolve("cfg-cache"));
        assertThat(config.cacheCleanup()).isFalse();
        assertThat(config.cacheFreeSpaceBufferBytes()).isEqualTo(64L * 1024 * 1024);

        DistWorkerStart noCleanup = new DistWorkerStart();
        noCleanup.brokerUrl = "http://localhost:8765";
        noCleanup.noCacheCleanup = true;
        assertThat(noCleanup.buildWorkerConfig().cacheCleanup()).isFalse();
    }

    static Stream<Arguments> boundCacheCommandLines() {
        return Stream.of(
                Arguments.of(List.of(), List.of("--no-cache-cleanup", "--cache-free-space-buffer-size=64MiB"), false),
                Arguments.of(List.of(), List.of("--cache-cleanup=false", "--cache-free-space-buffer-size", "64MiB"), false),
                Arguments.of(List.of(), List.of("--cache-cleanup", "--no-cache-cleanup",
                        "--cache-free-space-buffer-size=64MiB"), false),
                Arguments.of(List.of("-V"), List.of("--cache-cleanup", "--cache-free-space-buffer-size=64MiB"), true),
                Arguments.of(List.of("-V"), List.of("--no-cache-cleanup", "--cache-cleanup=true",
                        "--cache-free-space-buffer-size", "64MiB"), true));
    }

    @ParameterizedTest
    @MethodSource("boundCacheCommandLines")
    void shouldBindNormalizedCacheOptionsThroughTheCommandParser(List<String> leading, List<String> input,
                                                                  boolean expectedCleanup) {
        DistributionWorkerConfig config = bindWorkerStart(leading, Stream.concat(Stream.of(
                "--cache-dir", tempDir.resolve("bound-cache").toString()), input.stream()).toList());

        assertThat(config.cacheDir()).isEqualTo(tempDir.resolve("bound-cache"));
        assertThat(config.cacheCleanup()).isEqualTo(expectedCleanup);
        assertThat(config.cacheFreeSpaceBufferBytes()).isEqualTo(67_108_864L);
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldPreserveWhitespaceInBoundCacheDirectory() {
        Path cacheDir = tempDir.resolve(" worker cache ");
        DistributionWorkerConfig config = bindWorkerStart(List.of(), List.of("--cache-dir", cacheDir.toString()));

        assertThat(config.cacheDir()).isEqualTo(cacheDir);
    }

    @Test
    void shouldInheritSystemPropertyForUnsetCacheOptions() {
        String property = DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY;
        String previous = System.getProperty(property);
        System.setProperty(property, "1MiB");
        try {
            DistributionWorkerConfig config = bindWorkerStart(List.of(), List.of());
            assertThat(config.cacheFreeSpaceBufferBytes()).isEqualTo(1_048_576L);
        } finally {
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    /** Normalizes a worker start command line and lets the command parser bind it to the real worker options. */
    private static DistributionWorkerConfig bindWorkerStart(List<String> leading, List<String> options) {
        List<String> command = Stream.of(leading.stream(), Stream.of("dist", "worker", "start", "--broker-url",
                "http://localhost:1"), options.stream()).flatMap(s -> s).toList();
        String[] normalized = DistWorkerStart.normalizeWorkerStartArgs(command.toArray(String[]::new));
        int optionsStart = leading.size() + 3;
        assertThat(Arrays.asList(normalized).subList(0, optionsStart)).containsExactlyElementsOf(
                command.subList(0, optionsStart));
        CapturingWorkerStart.captured = null;

        CommandResult result = AeshRuntimeRunner.builder()
                .command(CapturingWorkerStart.class)
                .args(Arrays.copyOfRange(normalized, optionsStart, normalized.length))
                .execute();

        assertThat(result).isEqualTo(CommandResult.SUCCESS);
        assertThat(CapturingWorkerStart.captured).isNotNull();
        return CapturingWorkerStart.captured;
    }

    /** Lets the command parser populate the real worker start options, then captures the built configuration. */
    @CommandDefinition(name = "start", description = "Captures the worker configuration")
    public static class CapturingWorkerStart extends DistWorkerStart {
        static volatile DistributionWorkerConfig captured;

        @Override
        public CommandResult execute(CommandInvocation invocation) {
            captured = buildWorkerConfig();
            return CommandResult.SUCCESS;
        }
    }

    @Test
    void shouldStartWorkerWithCacheOptionsAndBoundedDuration() throws Exception {
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("cache-opts-broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:jbang_worker_cache_opts_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(config)) {
            broker.start();

            CitrusJBangMain main = createCitrusJBangMain();
            assertThat(main.execute("dist", "worker", "start",
                    "--broker-url", broker.baseUrl(),
                    "--worker-id", "jbang-cache-worker",
                    "--cache-dir", tempDir.resolve("cache-opts-wcache").toString(),
                    "--no-cache-cleanup",
                    "--cache-free-space-buffer-size=64MiB",
                    "--duration-seconds", "1")).isZero();
            assertThat(printer.getOutput()).contains("jbang-cache-worker");
        }
    }

    @Test
    void shouldHoldShutdownHookUntilWorkerClientIsClosed() throws Exception {
        CountDownLatch stop = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        Thread hook = DistWorkerStart.shutdownHook(stop, closed, 10_000L);

        hook.start();
        assertThat(stop.await(5, TimeUnit.SECONDS)).isTrue();
        hook.join(200);
        assertThat(hook.isAlive()).as("hook must wait for the client to be closed").isTrue();

        closed.countDown();
        hook.join(5_000);
        assertThat(hook.isAlive()).isFalse();
    }

    @Test
    void shouldReleaseWorkerCacheLockBeforeWorkerStartReturns() throws Exception {
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("lock-broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:jbang_worker_lock_" + System.nanoTime())
                .build();
        Path workerCache = tempDir.resolve("lock-wcache");

        try (DistributionBrokerServer broker = new DistributionBrokerServer(config)) {
            broker.start();
            for (String workerId : new String[] {"jbang-lock-worker-1", "jbang-lock-worker-2"}) {
                printer = new StringPrinter();
                CitrusJBangMain main = createCitrusJBangMain();
                assertThat(main.execute("dist", "worker", "start",
                        "--broker-url", broker.baseUrl(),
                        "--worker-id", workerId,
                        "--cache-dir", workerCache.toString(),
                        "--duration-seconds", "1")).isZero();
                assertThat(printer.getOutput()).contains(workerId);
            }
        }
    }
}
