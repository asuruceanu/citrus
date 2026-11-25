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
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import io.vertx.core.Vertx;
import org.aesh.AeshRuntimeRunner;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.citrusframework.distribution.coordinator.DistributionCoordinatorConfig;
import org.citrusframework.distribution.coordinator.DistributionCoordinatorServer;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.jbang.cli.CitrusJBangMain;
import org.citrusframework.jbang.cli.StringPrinter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@ResourceLock("citrus-jbang-run")
class DistCommandTest extends CommandTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldPrintDistHelpTree() {
        CitrusJBangMain main = createCitrusJBangMain();
        assertThat(main.execute("dist")).isZero();
        assertThat(printer.getOutput()).contains("coordinator").contains("worker").contains("sessions").contains("status");

        printer = new StringPrinter();
        main = createCitrusJBangMain();
        assertThat(main.execute("dist", "coordinator")).isZero();
        assertThat(printer.getOutput()).contains("start");

        printer = new StringPrinter();
        main = createCitrusJBangMain();
        assertThat(main.execute("dist", "worker")).isZero();
        assertThat(printer.getOutput()).contains("start");

        assertThat(main.execute("dist", "--help")).isZero();
        assertThat(main.execute("dist", "coordinator", "start", "--help")).isZero();
        assertThat(main.execute("dist", "worker", "start", "--help")).isZero();
        assertThat(main.execute("dist", "sessions", "--help")).isZero();
        assertThat(main.execute("dist", "status", "--help")).isZero();
    }

    @Test
    void shouldQueryLiveCoordinatorSessionsAndStatus() throws Exception {
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("jbang_dist_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer coordinator = new DistributionCoordinatorServer(config)) {
            coordinator.start();

            HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(coordinator.baseUrl() + "/api/v1/sessions"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(DistributionJson.toJson(
                                    new CreateSessionRequest("citrus-demo", "main", "deadbeef", List.of(), 5, 1))))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());

            CitrusJBangMain main = createCitrusJBangMain();
            assertThat(main.execute("dist", "sessions", "-u", coordinator.baseUrl())).isZero();
            assertThat(printer.getOutput()).contains("citrus-demo").contains("RUNNING");

            printer = new StringPrinter();
            main = createCitrusJBangMain();
            assertThat(main.execute("dist", "status", "--coordinator-url", coordinator.baseUrl(), "--json")).isZero();
            assertThat(printer.getOutput()).contains("\"status\"").contains("UP")
                    .contains("\"coordinatorUrl\":\"" + coordinator.baseUrl() + "\"");

            printer = new StringPrinter();
            main = createCitrusJBangMain();
            assertThat(main.execute("dist", "status", "-u", coordinator.baseUrl())).isZero();
            assertThat(printer.getOutput()).contains("Coordinator: " + coordinator.baseUrl() + " [UP]");
        }
    }

    @Test
    void shouldStartCoordinatorAndWorkerWithBoundedDuration() throws Exception {
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("worker-coordinator-cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("jbang_worker_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer coordinator = new DistributionCoordinatorServer(config)) {
            coordinator.start();

            CitrusJBangMain main = createCitrusJBangMain();
            assertThat(main.execute("dist", "worker", "start",
                    "--no-cache-cleanup", "-u", coordinator.baseUrl(),
                    "--worker-id", "jbang-worker-1",
                    "--cache-dir", tempDir.resolve("wcache").toString(),
                    "--duration-seconds", "1")).isZero();
            assertThat(printer.getOutput()).contains("jbang-worker-1 connected to " + coordinator.baseUrl());
        }

        printer = new StringPrinter();
        CitrusJBangMain main = createCitrusJBangMain();
        assertThat(main.execute("dist", "coordinator", "start",
                "--port", "0",
                "--jdbc-url", "jdbc:hsqldb:file:" + tempDir.resolve("jbang_coordinator_cli_" + System.nanoTime()) + ";shutdown=true",
                "--cache-dir", tempDir.resolve("coordinator-cache").toString(),
                "--duration-seconds", "1")).isZero();
        assertThat(printer.getOutput()).contains("Citrus Test Distribution Coordinator listening on");
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
                Arguments.of(List.of("--no-cache-cleanup", "--coordinator-url", "http://coordinator:9000"),
                        List.of("--coordinator-url", "http://coordinator:9000", "--cache-cleanup=false")),
                Arguments.of(List.of("--no-cache-cleanup", "-u", "http://coordinator:9000"),
                        List.of("-u", "http://coordinator:9000", "--cache-cleanup=false")),
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
                Arguments.of(List.of("--cache-dir", "--coordinator-url", "http://x"), "--cache-dir"),
                Arguments.of(List.of("--cache-free-space-buffer-size"), "--cache-free-space-buffer-size"),
                Arguments.of(List.of("--cache-free-space-buffer-size="), "--cache-free-space-buffer-size"),
                Arguments.of(List.of("--cache-free-space-buffer-size", "--coordinator-url", "http://x"),
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
                Arguments.of(List.of("--cache-dir", "--coordinator-url", "http://x"), "--cache-dir"),
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
        String[] coordinator = {"dist", "coordinator", "start", "--cache-dir", "/tmp/b"};
        assertThat(DistWorkerStart.normalizeWorkerStartArgs(coordinator)).containsExactly(coordinator);
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
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void shouldInheritWorkerSettingsWhenPoolOptionsAreAbsent() {
        var settings = Map.of(DistributionSettings.WORKER_POOL_PROPERTY, "inherited",
                DistributionSettings.WORKER_REGISTRATION_KEY_PROPERTY, "inherited-key",
                DistributionSettings.WORKER_CAPABILITIES_PROPERTY, "specialized=true");
        var saved = new HashMap<String, String>();
        settings.forEach((key, value) -> saved.put(key, System.setProperty(key, value)));
        try {
            var config = bindWorkerStart(List.of(), List.of());
            var expected = DistributionWorkerConfig.fromSettings();
            assertThat(config.poolId()).isEqualTo(expected.poolId());
            assertThat(expected.registrationKey().equals(config.registrationKey())).as("inherited registration key").isTrue();
            assertThat(config.capabilities()).containsAllEntriesOf(expected.capabilities());
        } finally {
            saved.forEach((key, value) -> {
                if (value == null) {
                    System.clearProperty(key);
                } else {
                    System.setProperty(key, value);
                }
            });
        }
    }

    @Test
    void shouldBindPoolCredentialsAndPreserveCacheFlags() {
        String[] args = {"dist", "worker", "start", "-p", "pool1", "-k", "ctrk_1_2", "--no-cache-cleanup"};
        assertThat(DistWorkerStart.normalizeWorkerStartArgs(args)).containsExactly(
                "dist", "worker", "start", "-p", "pool1", "-k", "ctrk_1_2", "--cache-cleanup=false");
        assertThat(DistWorkerStart.normalizeWorkerStartArgs(new String[] {"dist", "worker", "start",
                "--pool", "--cache-dir", "--registration-key", "--cache-x"})).containsExactly(
                "dist", "worker", "start", "--pool", "--cache-dir", "--registration-key", "--cache-x");
        var config = bindWorkerStart(List.of(), Arrays.asList(args).subList(3, args.length));
        assertThat(config.poolId()).isEqualTo("pool1");
        assertThat(config.registrationKey()).isEqualTo("ctrk_1_2");
        assertThat(config.cacheCleanup()).isFalse();
    }

    @Test
    void shouldExitIndefiniteWorkerCommandOnRegistrationRejection() throws Exception {
        var config = DistributionCoordinatorConfig.builder().port(0).requireAgentPool(true)
                .cacheDir(tempDir.resolve("coordinator"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("jbang-reject") + ";shutdown=true").build();
        try (var coordinator = new DistributionCoordinatorServer(config)) {
            coordinator.start();
            var main = createCitrusJBangMain(1);
            assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
                    assertThat(main.execute("dist", "worker", "start", "-u", coordinator.baseUrl(),
                            "-p", "missing-pool", "-k", "test-key",
                            "--cache-dir", tempDir.resolve("worker").toString())).isEqualTo(1));
            assertThat(printer.getOutput()).contains("POOL_NOT_FOUND").doesNotContain("test-key");
        }
    }

    @Test
    void shouldExitIndefiniteWorkerCommandOnRenewalRejection() throws Exception {
        var vertx = Vertx.vertx();
        try {
            var server = vertx.createHttpServer().webSocketHandler(socket -> socket.textMessageHandler(json -> {
                String type = DistributionJson.fromJson(json, WorkerWsMessage.class).type();
                if (WorkerWsMessage.WORKER_REGISTER.equals(type)) {
                    socket.writeTextMessage(DistributionJson.toJson(new WorkerWsMessage(WorkerWsMessage.WORKER_REGISTERED, Map.of())));
                } else if (WorkerWsMessage.WORKER_RENEW.equals(type)) {
                    socket.writeTextMessage(DistributionJson.toJson(new WorkerWsMessage(WorkerWsMessage.WORKER_REJECTED,
                            Map.of("code", "INVALID_REGISTRATION_KEY", "message", "Key revoked: test-secret"))));
                }
            })).listen(0, "127.0.0.1").toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            var command = new DistWorkerStart(createCitrusJBangMain()) {
                @Override
                DistributionWorkerConfig buildWorkerConfig() {
                    return DistributionWorkerConfig.builder().coordinatorUrl("http://127.0.0.1:" + server.actualPort())
                            .poolId("pool1").registrationKey("test-secret").registrationRenewalIntervalMs(100)
                            .cacheDir(tempDir.resolve("worker")).build();
                }
            };
            assertTimeoutPreemptively(Duration.ofSeconds(5),
                    () -> assertThat(command.execute(null)).isEqualTo(CommandResult.FAILURE));
            assertThat(printer.getOutput()).contains("INVALID_REGISTRATION_KEY", "Key revoked").doesNotContain("test-secret");
        } finally {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldBuildWorkerConfigFromCacheOptions() {
        DistWorkerStart command = new DistWorkerStart();
        command.coordinatorUrl = "http://localhost:8765";
        command.cacheDir = tempDir.resolve("cfg-cache").toString();
        command.cacheCleanup = "false";
        command.cacheFreeSpaceBufferSize = "64MiB";

        DistributionWorkerConfig config = command.buildWorkerConfig();
        assertThat(config.cacheDir()).isEqualTo(tempDir.resolve("cfg-cache"));
        assertThat(config.cacheCleanup()).isFalse();
        assertThat(config.cacheFreeSpaceBufferBytes()).isEqualTo(64L * 1024 * 1024);

        DistWorkerStart noCleanup = new DistWorkerStart();
        noCleanup.coordinatorUrl = "http://localhost:8765";
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
        List<String> command = Stream.of(leading.stream(), Stream.of("dist", "worker", "start", "--coordinator-url",
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
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("cache-opts-coordinator-cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("jbang_worker_cache_opts_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer coordinator = new DistributionCoordinatorServer(config)) {
            coordinator.start();

            CitrusJBangMain main = createCitrusJBangMain();
            assertThat(main.execute("dist", "worker", "start",
                    "--coordinator-url", coordinator.baseUrl(),
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
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("lock-coordinator-cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("jbang_worker_lock_" + System.nanoTime()) + ";shutdown=true")
                .build();
        Path workerCache = tempDir.resolve("lock-wcache");

        try (DistributionCoordinatorServer coordinator = new DistributionCoordinatorServer(config)) {
            coordinator.start();
            for (String workerId : new String[] {"jbang-lock-worker-1", "jbang-lock-worker-2"}) {
                printer = new StringPrinter();
                CitrusJBangMain main = createCitrusJBangMain();
                assertThat(main.execute("dist", "worker", "start",
                        "--coordinator-url", coordinator.baseUrl(),
                        "--worker-id", workerId,
                        "--cache-dir", workerCache.toString(),
                        "--duration-seconds", "1")).isZero();
                assertThat(printer.getOutput()).contains(workerId);
            }
        }
    }
}
