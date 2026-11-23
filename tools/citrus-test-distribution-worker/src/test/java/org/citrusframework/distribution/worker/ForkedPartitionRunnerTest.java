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

package org.citrusframework.distribution.worker;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.config.DistributionSettings.WorkerForkMode;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.engine.CancellationToken;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class ForkedPartitionRunnerTest {

    @TempDir
    Path tempDir;

    /** Runs in the child JVM; plain JUnit so the child needs no assertion library. */
    public static class WorkspaceFixture {
        @Test
        void readInputsAndWriteOutputs() throws Exception {
            String greeting = Files.readString(new File("src/test/resources/greeting.txt").toPath());
            check("hello".equals(greeting.trim()), "relative input readable from CWD, got " + greeting);
            Path basedir = Path.of(System.getProperty("basedir"));
            check(Path.of("").toRealPath().equals(basedir.toRealPath()), "CWD is the workspace");
            check(System.getProperty("custom.dir").equals(basedir + File.separator + "conf"), "token expanded: "
                    + System.getProperty("custom.dir"));
            String tmp = System.getProperty("java.io.tmpdir");
            check(File.separatorChar == '\\' || tmp.length() <= 40, "short tmpdir: " + tmp);
            check(System.getProperty("citrus.report.directory") == null,
                    "forked JVMs keep Citrus' default report directory (relative to the working directory, as locally)");
            check(DistributionSettings.ROLE_WORKER.equals(System.getProperty(DistributionSettings.ROLE_PROPERTY)), "role");
            Files.createDirectories(Path.of("target/docs"));
            Files.writeString(Path.of("target/docs/report.html"), "<html>report</html>");
        }

        @Test
        void failingAssertion() {
            throw new AssertionError("expected failure");
        }
    }

    /** Expects the module's Surefire working directory (target/) inside the workspace. */
    public static class WorkingDirectoryFixture {
        @Test
        void runInTargetDirectory() throws Exception {
            Path basedir = Path.of(System.getProperty("basedir")).toRealPath();
            check(Path.of("").toRealPath().equals(basedir.resolve("target")), "CWD is <workspace>/target: " + Path.of("").toRealPath());
            Files.writeString(Path.of("relative-output.txt"), "written relative to the working directory");
        }
    }

    public static class PlainFixture {
        @Test
        void passes() {
        }
    }

    public static class BlockingFixture {
        @Test
        void spawnGrandchildAndBlock() throws Exception {
            Process grandchild = new ProcessBuilder("sleep", "300").start();
            Files.createDirectories(Path.of("target/docs"));
            Files.writeString(Path.of("target/docs/out.txt"), "partial");
            Path info = Path.of(System.getProperty("fixture.info"));
            Files.writeString(info.resolveSibling(info.getFileName() + ".tmp"), ProcessHandle.current().pid() + "\n"
                    + grandchild.pid() + "\n" + System.getProperty("basedir") + "\n" + System.getProperty("java.io.tmpdir"));
            Files.move(info.resolveSibling(info.getFileName() + ".tmp"), info);
            Thread.sleep(300_000);
        }
    }

    /** Leaves a background process behind and passes. */
    public static class LeakingFixture {
        @Test
        void leaveProcessBehind() throws Exception {
            Process leaked = new ProcessBuilder("sleep", "300").start();
            Files.writeString(Path.of(System.getProperty("fixture.info")), String.valueOf(leaked.pid()));
        }
    }

    /** Writes a declared output from a JVM shutdown hook, the way coverage agents and log flushers do. */
    public static class ShutdownHookFixture {
        @Test
        void registerHook() {
            Path out = Path.of("target/docs/hook.txt").toAbsolutePath();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Thread.sleep(200);
                    Files.createDirectories(out.getParent());
                    Files.writeString(out, "written on exit");
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }));
        }
    }

    /** Invokes the worker's real callback in a separate JVM because it permanently fences fork launches. */
    public static class WorkerShutdownProbe {
        public static void main(String[] args) throws Exception {
            Path root = Path.of(args[0]);
            Path info = root.resolve("shutdown-info.txt");
            DistributionWorkerConfig activeConfig = workerConfig(root.resolve("active-cache"), "http://127.0.0.1:9");
            PartitionAssignPayload blocking = assignment(new BlobCacheDirectory(activeConfig.cacheDir()),
                    Map.of("fixture.info", info.toString()), List.of(), List.of(),
                    unit(BlockingFixture.class, "spawnGrandchildAndBlock"));
            ForkedPartitionRunner activeRunner = new ForkedPartitionRunner(activeConfig);
            CompletableFuture<IsolatedPartitionRunner.PartitionExecutionResult> running =
                    CompletableFuture.supplyAsync(() -> activeRunner.execute(blocking, CancellationToken.create(), e -> { }));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!Files.exists(info) && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertThat(info).as("forked JVM started").exists();
            List<String> lines = Files.readAllLines(info);
            ProcessHandle child = ProcessHandle.of(Long.parseLong(lines.get(0))).orElseThrow();
            ProcessHandle grandchild = ProcessHandle.of(Long.parseLong(lines.get(1))).orElseThrow();

            DistributionWorkerConfig lateConfig = workerConfig(root.resolve("late-cache"), "http://127.0.0.1:9");
            PartitionAssignPayload late = assignment(new BlobCacheDirectory(lateConfig.cacheDir()),
                    Map.of(), List.of(), List.of(), unit(PlainFixture.class, "passes"));
            CountDownLatch preparingClasspath = new CountDownLatch(1);
            CountDownLatch resumePreparation = new CountDownLatch(1);
            List<ArtifactRef> delayedClasspath = new AbstractList<>() {
                @Override
                public ArtifactRef get(int index) {
                    preparingClasspath.countDown();
                    try {
                        assertThat(resumePreparation.await(20, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    return late.classpathArtifacts().get(index);
                }

                @Override
                public int size() {
                    return late.classpathArtifacts().size();
                }
            };
            PartitionAssignPayload pending = new PartitionAssignPayload(late.partitionId(), late.sessionId(),
                    late.attempt(), delayedClasspath, late.systemProperties(), late.workUnits());
            ForkedPartitionRunner lateRunner = new ForkedPartitionRunner(lateConfig);
            CompletableFuture<IsolatedPartitionRunner.PartitionExecutionResult> preparing =
                    CompletableFuture.supplyAsync(() -> lateRunner.execute(pending, CancellationToken.create(), e -> { }));
            try {
                assertThat(preparingClasspath.await(10, TimeUnit.SECONDS)).isTrue();
                Field field = ForkedPartitionRunner.class.getDeclaredField("SHUTDOWN_HOOK");
                field.setAccessible(true);
                ((Thread) field.get(null)).run();
            } finally {
                resumePreparation.countDown();
            }

            assertThat(running.get(10, TimeUnit.SECONDS).success()).isFalse();
            assertThat(child.isAlive()).as("forked JVM terminated").isFalse();
            assertThat(grandchild.isAlive()).as("grandchild process terminated").isFalse();
            assertThat(Path.of(lines.get(3))).as("active fork temp directory").doesNotExist();

            IsolatedPartitionRunner.PartitionExecutionResult pendingResult = preparing.get(20, TimeUnit.SECONDS);
            assertThat(pendingResult.success()).as("fork prepared before shutdown").isFalse();
            assertThat(pendingResult.errorMessage()).containsIgnoringCase("cancelled");
            IsolatedPartitionRunner.PartitionExecutionResult lateResult =
                    lateRunner.execute(late, CancellationToken.create(), e -> { });
            assertThat(lateResult.success()).as("fork requested after shutdown").isFalse();
            assertThat(lateResult.errorMessage()).containsIgnoringCase("cancelled");
            assertThat(root.toFile().list((directory, name) -> name.startsWith("cdist-")))
                    .as("active and delayed fork workspaces").isEmpty();
        }
    }

    public static class CrashingFixture {
        @Test
        void exitAbruptly() {
            System.err.println("FATAL BOOM");
            System.exit(42);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    @Test
    void shouldMaterializeRelativeCwdInputsRewriteBasedirAndExportDifferentialOutputs() throws Exception {
        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:forked_" + System.nanoTime())
                .build();
        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();
            DistributionWorkerConfig config = workerConfig(tempDir.resolve("worker-cache"), broker.baseUrl());
            BlobCacheDirectory cache = new BlobCacheDirectory(config.cacheDir());

            Path module = Files.createDirectories(tempDir.resolve("module"));
            Files.createDirectories(module.resolve("src/test/resources"));
            Files.writeString(module.resolve("src/test/resources/greeting.txt"), "hello");
            Files.createDirectories(module.resolve("target/docs"));
            Files.writeString(module.resolve("target/docs/preexisting.txt"), "kept");
            List<WorkspaceFileSet> inputs = List.of(
                    WorkspacePackager.packageInputToCache(cache, module, "src/test/resources", false).orElseThrow(),
                    WorkspacePackager.packageInputToCache(cache, module, "target/docs", false).orElseThrow());

            List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
            IsolatedPartitionRunner.PartitionExecutionResult result = new ForkedPartitionRunner(config).execute(
                    assignment(cache, Map.of("custom.dir", DistributionSettings.WORKSPACE_TOKEN + "/conf"), inputs,
                            List.of("target/docs"), unit(WorkspaceFixture.class, "readInputsAndWriteOutputs"),
                            unit(WorkspaceFixture.class, "failingAssertion")),
                    CancellationToken.create(), events::add);

            assertThat(result.success()).as(result.errorMessage()).isTrue();
            assertThat(finished(events)).extracting(WorkerTestEventPayload::testName, WorkerTestEventPayload::status)
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple("readInputsAndWriteOutputs()", TestOutcomeStatus.PASSED),
                            org.assertj.core.groups.Tuple.tuple("failingAssertion()", TestOutcomeStatus.FAILED));
            assertThat(finished(events).getFirst().errorMessage()).isNull();

            assertThat(result.outputArtifacts()).singleElement().satisfies(out -> assertThat(out.relativePath()).isEqualTo("target/docs"));
            Path materialized = Files.createDirectories(tempDir.resolve("host"));
            List<WorkspacePackager.MaterializedEntry> files = WorkspacePackager.materializeIntoDirectory(
                    cache, result.outputArtifacts().getFirst(), materialized);
            assertThat(files).extracting(WorkspacePackager.MaterializedEntry::workspaceRelativePath)
                    .as("only the new file, not the unchanged input")
                    .containsExactly("target/docs/report.html");

            HttpResponse<Void> uploaded = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(
                    broker.baseUrl() + "/api/v1/cache/blobs/" + result.outputArtifacts().getFirst().artifact().sha256())).GET().build(),
                    HttpResponse.BodyHandlers.discarding());
            assertThat(uploaded.statusCode()).as("output blob uploaded to the coordinator").isEqualTo(200);
        }
    }

    @Test
    void shouldRunTestsInTheRequestedWorkingDirectoryInsideTheWorkspace() throws Exception {
        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:forked_wd_" + System.nanoTime())
                .build();
        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();
            DistributionWorkerConfig config = workerConfig(tempDir.resolve("worker-cache"), broker.baseUrl());
            BlobCacheDirectory cache = new BlobCacheDirectory(config.cacheDir());
            PartitionAssignPayload base = assignment(cache, Map.of(), List.of(), List.of("target"),
                    unit(WorkingDirectoryFixture.class, "runInTargetDirectory"));
            PartitionAssignPayload inTarget = new PartitionAssignPayload(base.partitionId(), base.sessionId(), base.attempt(),
                    base.classpathArtifacts(), base.systemProperties(), base.workUnits(), base.inputArtifacts(),
                    base.outputPaths(), true, "target");

            List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
            IsolatedPartitionRunner.PartitionExecutionResult result = new ForkedPartitionRunner(config).execute(
                    inTarget, CancellationToken.create(), events::add);

            assertThat(result.success()).as(result.errorMessage()).isTrue();
            assertThat(finished(events)).singleElement().satisfies(e -> assertThat(e.status()).as(e.errorMessage())
                    .isEqualTo(TestOutcomeStatus.PASSED));
            Path host = Files.createDirectories(tempDir.resolve("host-wd"));
            assertThat(WorkspacePackager.materializeIntoDirectory(cache, result.outputArtifacts().getFirst(), host))
                    .extracting(WorkspacePackager.MaterializedEntry::workspaceRelativePath)
                    .containsExactly("target/relative-output.txt");
        }
    }

    @Test
    void shouldRejectWorkingDirectoriesOutsideTheWorkspace() throws Exception {
        DistributionWorkerConfig config = workerConfig(tempDir.resolve("worker-cache"), "http://127.0.0.1:9");
        BlobCacheDirectory cache = new BlobCacheDirectory(config.cacheDir());
        PartitionAssignPayload base = assignment(cache, Map.of(), List.of(), List.of(), unit(PlainFixture.class, "passes"));
        PartitionAssignPayload escaping = new PartitionAssignPayload(base.partitionId(), base.sessionId(), base.attempt(),
                base.classpathArtifacts(), base.systemProperties(), base.workUnits(), List.of(), List.of(), true, "../outside");

        IsolatedPartitionRunner.PartitionExecutionResult result = new ForkedPartitionRunner(config).execute(
                escaping, CancellationToken.create(), e -> { });
        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).contains("../outside");
    }

    @Test
    void shouldLaunchChildJvmWhenWorkspaceAndClasspathContainSpacesAndHash() throws Exception {
        DistributionWorkerConfig config = workerConfig(tempDir.resolve("dir #1 with spaces/cache \"q\""), "http://127.0.0.1:9");
        BlobCacheDirectory cache = new BlobCacheDirectory(config.cacheDir());
        List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();

        IsolatedPartitionRunner.PartitionExecutionResult result = new ForkedPartitionRunner(config).execute(
                assignment(cache, Map.of(), List.of(), List.of(), unit(PlainFixture.class, "passes")),
                CancellationToken.create(), events::add);

        assertThat(result.success()).as(result.errorMessage()).isTrue();
        assertThat(finished(events)).singleElement().satisfies(e -> assertThat(e.status()).isEqualTo(TestOutcomeStatus.PASSED));
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldKillChildAndDescendantProcessesOnCancellationAndSkipOutputUpload() throws Exception {
        DistributionWorkerConfig config = workerConfig(tempDir.resolve("worker-cache"), "http://127.0.0.1:9");
        BlobCacheDirectory cache = new BlobCacheDirectory(config.cacheDir());
        Path info = tempDir.resolve("fixture-info.txt");
        CancellationToken token = CancellationToken.create();

        PartitionAssignPayload blocking = assignment(cache, Map.of("fixture.info", info.toString()), List.of(),
                List.of("target/docs"), unit(BlockingFixture.class, "spawnGrandchildAndBlock"));
        CompletableFuture<IsolatedPartitionRunner.PartitionExecutionResult> running = CompletableFuture.supplyAsync(() ->
                new ForkedPartitionRunner(config).execute(blocking, token, e -> { }));

        long deadline = System.currentTimeMillis() + 30_000;
        while (!Files.exists(info) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertThat(info).as("fixture started").exists();
        List<String> lines = Files.readAllLines(info);
        ProcessHandle child = ProcessHandle.of(Long.parseLong(lines.get(0))).orElseThrow();
        ProcessHandle grandchild = ProcessHandle.of(Long.parseLong(lines.get(1))).orElseThrow();
        Path partitionRoot = Path.of(lines.get(2)).getParent();
        Path partitionTmp = Path.of(lines.get(3));

        token.cancel();
        IsolatedPartitionRunner.PartitionExecutionResult result = running.get(20, TimeUnit.SECONDS);

        assertThat(result.success()).isFalse();
        assertThat(result.outputArtifacts()).isEmpty();
        assertThat(child.isAlive()).isFalse();
        long until = System.currentTimeMillis() + 5_000;
        while (grandchild.isAlive() && System.currentTimeMillis() < until) {
            Thread.sleep(50);
        }
        assertThat(grandchild.isAlive()).as("grandchild process terminated").isFalse();
        assertThat(partitionRoot).doesNotExist();
        assertThat(partitionTmp).doesNotExist();
    }

    @Test
    void shouldRunAWarmForkedPartitionWithoutRehashingClasspathOrInputBlobs() throws Exception {
        DistributionWorkerConfig config = workerConfig(tempDir.resolve("worker-cache"), "http://127.0.0.1:9");
        BlobCacheDirectory seeding = new BlobCacheDirectory(config.cacheDir());
        Path module = Files.createDirectories(tempDir.resolve("module/src/test/resources")).getParent().getParent().getParent();
        Files.writeString(module.resolve("src/test/resources/greeting.txt"), "hello");
        List<WorkspaceFileSet> inputs = List.of(
                WorkspacePackager.packageInputToCache(seeding, module, "src/test/resources", false).orElseThrow());
        PartitionAssignPayload cold = assignment(seeding, Map.of(), inputs, List.of(), unit(PlainFixture.class, "passes"));
        PartitionAssignPayload warm = assignment(seeding, Map.of(), inputs, List.of(), unit(PlainFixture.class, "passes"));

        AtomicInteger hashes = new AtomicInteger();
        try (WorkerFileCache cache = WorkerFileCache.open(config.cacheDir(), false, 0L, null, Clock.systemUTC(),
                FileChannel::tryLock, root -> new BlobCacheDirectory(root) {
                    @Override
                    public boolean isIntact(String sha256) throws IOException {
                        hashes.incrementAndGet();
                        return super.isIntact(sha256);
                    }

                    @Override
                    public Path getVerifiedPath(String sha256) {
                        throw new AssertionError("blobs are verified through WorkerFileCache only");
                    }
                });
             BrokerBlobClient blobs = new BrokerBlobClient(config, cache)) {
            ForkedPartitionRunner runner = new ForkedPartitionRunner(config, cache, blobs);
            IsolatedPartitionRunner.PartitionExecutionResult first = runner.execute(cold, CancellationToken.create(), e -> { });
            assertThat(first.success()).as(first.errorMessage()).isTrue();
            assertThat(hashes).as("each blob found on disk is verified once")
                    .hasValue(cold.classpathArtifacts().size() + inputs.size());

            IsolatedPartitionRunner.PartitionExecutionResult second = runner.execute(warm, CancellationToken.create(), e -> { });
            assertThat(second.success()).as(second.errorMessage()).isTrue();
            assertThat(hashes).as("a warm partition re-hashes nothing")
                    .hasValue(cold.classpathArtifacts().size() + inputs.size());
        }
    }

    @Test
    void shouldPreventAForkStillPreparingItsClasspathFromLaunchingAfterShutdown() throws Exception {
        DistributionWorkerConfig config = workerConfig(tempDir.resolve("worker-cache"), "http://127.0.0.1:9");
        BlobCacheDirectory seeding = new BlobCacheDirectory(config.cacheDir());
        PartitionAssignPayload pending = assignment(seeding, Map.of(), List.of(), List.of(),
                unit(PlainFixture.class, "passes"));
        CountDownLatch verifying = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (WorkerFileCache cache = WorkerFileCache.open(config.cacheDir(), false, 0L, null, Clock.systemUTC(),
                FileChannel::tryLock, root -> new BlobCacheDirectory(root) {
                    @Override
                    public boolean isIntact(String sha256) throws IOException {
                        verifying.countDown();
                        try {
                            if (!release.await(10, TimeUnit.SECONDS)) {
                                throw new IOException("Timed out waiting to continue classpath verification");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                        return super.isIntact(sha256);
                    }
                });
             BrokerBlobClient blobs = new BrokerBlobClient(config, cache)) {
            ForkedPartitionRunner runner = new ForkedPartitionRunner(config, cache, blobs);
            List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
            CompletableFuture<IsolatedPartitionRunner.PartitionExecutionResult> execution =
                    CompletableFuture.supplyAsync(() -> runner.execute(pending, CancellationToken.create(), events::add));
            try {
                assertThat(verifying.await(5, TimeUnit.SECONDS)).isTrue();
                runner.killActiveForks();
            } finally {
                release.countDown();
            }
            IsolatedPartitionRunner.PartitionExecutionResult result = execution.get(20, TimeUnit.SECONDS);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).containsIgnoringCase("cancelled");
            assertThat(events).isEmpty();
        }
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldKillTheRunningForkWhenAskedToKillActiveForks() throws Exception {
        DistributionWorkerConfig config = workerConfig(tempDir.resolve("worker-cache"), "http://127.0.0.1:9");
        BlobCacheDirectory cache = new BlobCacheDirectory(config.cacheDir());
        Path info = tempDir.resolve("kill-info.txt");
        PartitionAssignPayload blocking = assignment(cache, Map.of("fixture.info", info.toString()), List.of(),
                List.of(), unit(BlockingFixture.class, "spawnGrandchildAndBlock"));
        ForkedPartitionRunner runner = new ForkedPartitionRunner(config);
        CompletableFuture<IsolatedPartitionRunner.PartitionExecutionResult> running = CompletableFuture.supplyAsync(() ->
                runner.execute(blocking, CancellationToken.create(), e -> { }));

        long deadline = System.currentTimeMillis() + 30_000;
        while (!Files.exists(info) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertThat(info).as("fixture started").exists();
        List<String> lines = Files.readAllLines(info);
        ProcessHandle child = ProcessHandle.of(Long.parseLong(lines.get(0))).orElseThrow();
        ProcessHandle grandchild = ProcessHandle.of(Long.parseLong(lines.get(1))).orElseThrow();

        runner.killActiveForks();
        IsolatedPartitionRunner.PartitionExecutionResult result = running.get(20, TimeUnit.SECONDS);

        assertThat(result.success()).isFalse();
        assertThat(child.isAlive()).isFalse();
        long until = System.currentTimeMillis() + 5_000;
        while (grandchild.isAlive() && System.currentTimeMillis() < until) {
            Thread.sleep(50);
        }
        assertThat(grandchild.isAlive()).as("grandchild process terminated").isFalse();
        assertThatNoException().as("nothing left to kill").isThrownBy(runner::killActiveForks);
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldReapActiveForksAndPreventLaterLaunchesOnJvmShutdown() throws Exception {
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        Path log = tempDir.resolve("shutdown-probe.log");
        Process probe = new ProcessBuilder(java.toString(), "-Djava.io.tmpdir=" + tempDir,
                "-cp", System.getProperty("java.class.path"),
                WorkerShutdownProbe.class.getName(), tempDir.toString())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        try {
            assertThat(probe.waitFor(60, TimeUnit.SECONDS)).as("worker shutdown probe finished").isTrue();
            assertThat(probe.exitValue()).as(Files.readString(log)).isZero();
        } finally {
            if (probe.isAlive()) {
                List<ProcessHandle> handles = new ArrayList<>(probe.descendants().toList());
                handles.add(probe.toHandle());
                ForkedPartitionRunner.destroyAll(handles);
            }
        }
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldKillProcessesLeftBehindByPassingTests() throws Exception {
        DistributionWorkerConfig config = workerConfig(tempDir.resolve("worker-cache"), "http://127.0.0.1:9");
        BlobCacheDirectory cache = new BlobCacheDirectory(config.cacheDir());
        Path info = tempDir.resolve("leaked.pid");

        IsolatedPartitionRunner.PartitionExecutionResult result = new ForkedPartitionRunner(config).execute(
                assignment(cache, Map.of("fixture.info", info.toString()), List.of(), List.of(),
                        unit(LeakingFixture.class, "leaveProcessBehind")),
                CancellationToken.create(), e -> { });

        assertThat(result.success()).as(result.errorMessage()).isTrue();
        Optional<ProcessHandle> leaked = ProcessHandle.of(Long.parseLong(Files.readString(info).trim()));
        long until = System.currentTimeMillis() + 5_000;
        while (leaked.map(ProcessHandle::isAlive).orElse(false) && System.currentTimeMillis() < until) {
            Thread.sleep(50);
        }
        assertThat(leaked.map(ProcessHandle::isAlive).orElse(false)).as("process left behind by the test is killed").isFalse();
    }

    @Test
    void shouldRunChildShutdownHooksBeforePackagingOutputs() throws Exception {
        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:forked_hook_" + System.nanoTime())
                .build();
        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();
            DistributionWorkerConfig config = workerConfig(tempDir.resolve("worker-cache"), broker.baseUrl());
            BlobCacheDirectory cache = new BlobCacheDirectory(config.cacheDir());

            IsolatedPartitionRunner.PartitionExecutionResult result = new ForkedPartitionRunner(config).execute(
                    assignment(cache, Map.of(), List.of(), List.of("target/docs"), unit(ShutdownHookFixture.class, "registerHook")),
                    CancellationToken.create(), e -> { });

            assertThat(result.success()).as(result.errorMessage()).isTrue();
            assertThat(result.outputArtifacts()).as("file written by a shutdown hook is packaged").singleElement();
            Path host = Files.createDirectories(tempDir.resolve("host"));
            assertThat(WorkspacePackager.materializeIntoDirectory(cache, result.outputArtifacts().getFirst(), host))
                    .extracting(WorkspacePackager.MaterializedEntry::workspaceRelativePath)
                    .containsExactly("target/docs/hook.txt");
        }
    }

    @Test
    void shouldRejectPartitionClasspathWithOldJUnitPlatform() throws Exception {
        Path oldPlatform = Files.createDirectories(tempDir.resolve("old/org/junit/platform/engine"));
        Files.writeString(oldPlatform.resolve("TestEngine.class"), "x");
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        ForkedPartitionRunner.verifyJUnitPlatform(List.of(tempDir.resolve("old"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("JUnit Platform");
        Files.writeString(oldPlatform.resolve("CancellationToken.class"), "x");
        ForkedPartitionRunner.verifyJUnitPlatform(List.of(tempDir.resolve("old")));
        ForkedPartitionRunner.verifyJUnitPlatform(List.of());
    }

    @Test
    void shouldPutWorkerRuntimeBeforeThePartitionClasspath() throws Exception {
        Path partitionEntry = Files.createDirectories(tempDir.resolve("module-classes"));
        List<Path> classpath = ForkedPartitionRunner.childClasspath(List.of(partitionEntry));
        Path workerClasses = Path.of(ForkedPartitionMain.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Path protocolClasses = Path.of(DistributionSettings.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        assertThat(classpath.indexOf(workerClasses)).isLessThan(classpath.indexOf(partitionEntry));
        assertThat(classpath.indexOf(protocolClasses)).isLessThan(classpath.indexOf(partitionEntry));
        assertThat(classpath).doesNotHaveDuplicates();
    }

    @Test
    void shouldReportCrashDiagnosticsWhenChildCallsSystemExit() throws Exception {
        DistributionWorkerConfig config = workerConfig(tempDir.resolve("worker-cache"), "http://127.0.0.1:9");
        BlobCacheDirectory cache = new BlobCacheDirectory(config.cacheDir());

        IsolatedPartitionRunner.PartitionExecutionResult result = new ForkedPartitionRunner(config).execute(
                assignment(cache, Map.of(), List.of(), List.of("target/docs"), unit(CrashingFixture.class, "exitAbruptly")),
                CancellationToken.create(), e -> { });

        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).contains("42").contains("FATAL BOOM");
        assertThat(result.outputArtifacts()).isEmpty();
    }

    @Test
    void shouldQuoteArgfileClasspathAndRejectLineBreaks() {
        assertThat(ForkedPartitionRunner.argfileClasspathLine(List.of(Path.of("/a b/#1"), Path.of("/c\\d/\"e\""))))
                .isEqualTo("-cp \"/a b/#1" + File.pathSeparator + "/c\\\\d/\\\"e\\\"\"");
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        ForkedPartitionRunner.argfileClasspathLine(List.of(Path.of("/a\nb"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRouteWorkspaceAndForcedPartitionsToTheForkedRunner() {
        DistributionWorkerConfig auto = DistributionWorkerConfig.builder().cacheDir(tempDir).forkMode(WorkerForkMode.AUTO).build();
        DistributionWorkerConfig always = DistributionWorkerConfig.builder().cacheDir(tempDir).forkMode(WorkerForkMode.ALWAYS).build();
        PartitionAssignPayload plain = new PartitionAssignPayload("p", "s", 1, List.of(), Map.of(), List.of());
        PartitionAssignPayload forked = new PartitionAssignPayload("p", "s", 1, List.of(), Map.of(), List.of(), List.of(), List.of(), true);
        PartitionAssignPayload outputs = new PartitionAssignPayload("p", "s", 1, List.of(), Map.of(), List.of(), List.of(),
                List.of("target/docs"), false);

        assertThat(DistributionWorkerClient.requiresFork(auto, plain)).isFalse();
        assertThat(DistributionWorkerClient.requiresFork(always, plain)).isTrue();
        assertThat(DistributionWorkerClient.requiresFork(auto, forked)).isTrue();
        assertThat(DistributionWorkerClient.requiresFork(auto, outputs)).isTrue();
    }

    private static DistributionWorkerConfig workerConfig(Path cacheDir, String brokerUrl) {
        return DistributionWorkerConfig.builder()
                .workerId("forked-" + UUID.randomUUID().toString().substring(0, 6))
                .cacheDir(cacheDir)
                .brokerUrl(brokerUrl)
                .forkJvmArgs(List.of("-Xmx256m"))
                .build();
    }

    /**
     * Builds an assignment whose classpath (test classes plus the JUnit Jupiter jars) is already in the worker cache.
     */
    private static PartitionAssignPayload assignment(BlobCacheDirectory cache, Map<String, String> props,
                                                     List<WorkspaceFileSet> inputs, List<String> outputs,
                                                     WorkUnitSpec... units) throws Exception {
        List<ArtifactRef> classpath = new ArrayList<>();
        for (Class<?> anchor : List.of(ForkedPartitionRunnerTest.class, Test.class,
                Class.forName("org.junit.jupiter.engine.JupiterTestEngine"))) {
            Path location = Path.of(anchor.getProtectionDomain().getCodeSource().getLocation().toURI());
            ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(location);
            if (packaged.sourceFile() != null) {
                cache.putFromFile(packaged.ref().sha256(), packaged.sourceFile());
            } else {
                cache.put(packaged.ref().sha256(), packaged.bytes());
            }
            classpath.add(packaged.ref());
        }
        return new PartitionAssignPayload("p-" + UUID.randomUUID(), "s1", 1, classpath, props, List.of(units),
                inputs, outputs, !inputs.isEmpty() || !outputs.isEmpty());
    }

    private static WorkUnitSpec unit(Class<?> fixture, String method) {
        return new WorkUnitSpec(fixture.getName() + "#" + method, fixture.getName(), method, null, Map.of(), 0L);
    }

    private static List<WorkerTestEventPayload> finished(List<WorkerTestEventPayload> events) {
        return events.stream().filter(e -> e.status() != TestOutcomeStatus.STARTED).toList();
    }
}
