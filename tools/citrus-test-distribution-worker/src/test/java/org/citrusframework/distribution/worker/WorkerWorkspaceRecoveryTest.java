/*
 * Copyright the original author or authors.
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

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class WorkerWorkspaceRecoveryTest {

    private static final int HALTED = 42;
    private static final Duration START_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration HELPER_LIFETIME = Duration.ofMinutes(2);
    private static final String SENTINEL = "workspace-data";

    @TempDir
    Path tempDir;

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldReclaimOnlyWorkspacesRegisteredToTheReopenedCacheAfterHalt() throws Exception {
        Path cacheDir = tempDir.resolve("worker");
        Path otherCacheDir = tempDir.resolve("other-worker");
        try (WorkspacePaths abandoned = haltWorker(cacheDir, "halt");
             WorkspacePaths other = haltWorker(otherCacheDir, "halt")) {
            String forkedName = abandoned.forkedRoot().getFileName().toString();
            String replacementDigit = forkedName.endsWith("a") ? "b" : "a";
            String unregisteredName = forkedName.substring(0, forkedName.length() - 1) + replacementDigit;
            Path unregistered = Files.createDirectory(abandoned.forkedRoot().resolveSibling(unregisteredName));
            try {
                writeSentinel(unregistered);
                assertPresent(abandoned.inProcessRoot(), abandoned.forkedRoot(), abandoned.childTmpDir());
                assertPresent(other.inProcessRoot(), other.forkedRoot(), other.childTmpDir());

                openCache(cacheDir);

                assertThat(abandoned.inProcessRoot()).doesNotExist();
                assertThat(abandoned.forkedRoot()).doesNotExist();
                assertThat(abandoned.childTmpDir()).doesNotExist();
                assertPresent(unregistered, other.inProcessRoot(), other.forkedRoot(), other.childTmpDir());

                openCache(otherCacheDir);

                assertThat(other.inProcessRoot()).doesNotExist();
                assertThat(other.forkedRoot()).doesNotExist();
                assertThat(other.childTmpDir()).doesNotExist();
                assertPresent(unregistered);
            } finally {
                deleteTestTree(unregistered);
            }
        }
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldPreserveASymlinkThatReplacedARecordedWorkspaceAndItsTarget() throws Exception {
        Path cacheDir = tempDir.resolve("worker");
        Path target = Files.createDirectory(tempDir.resolve("unrelated"));
        writeSentinel(target);
        try (WorkspacePaths paths = haltWorker(cacheDir, "halt")) {
            Path replaced = paths.inProcessRoot();
            deleteTestTree(replaced);
            try {
                Files.createSymbolicLink(replaced, target);
            } catch (IOException | UnsupportedOperationException e) {
                assumeTrue(false, "file system supports symbolic links");
            }

            openCache(cacheDir);

            assertThat(Files.isSymbolicLink(replaced)).isTrue();
            assertThat(Files.readSymbolicLink(replaced)).isEqualTo(target);
            assertPresent(target);
            assertThat(paths.forkedRoot()).doesNotExist();
            assertThat(paths.childTmpDir()).doesNotExist();
        }
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldPreserveLiveWorkspacesAndRecoverOnlyTheOrphanAfterForcibleExit() throws Exception {
        Path cacheDir = tempDir.resolve("worker");
        Path ready = tempDir.resolve("ready");
        // The test owns the survivor directly so it can terminate and reap it even on hosts with no orphan reaper.
        Process child = javaProcess(ForkedChildMain.class).start();
        try {
            try (WorkerProcess worker = startWorker("hold", cacheDir, ready.toString(), Long.toString(child.pid()))) {
                worker.awaitReady(ready);
                try (WorkspacePaths paths = WorkspacePaths.read(ready)) {
                    assertThat(child.isAlive()).isTrue();
                    try (WorkerProcess contender = startWorker("open", cacheDir)) {
                        contender.awaitExit();
                        assertThat(contender.process().exitValue()).isNotZero();
                        assertThat(Files.readString(contender.log()))
                                .contains(IllegalStateException.class.getName(), "--cache-dir");
                    }
                    assertPresent(paths.inProcessRoot(), paths.forkedRoot(), paths.childTmpDir());
                    stop(worker.process());

                    openCache(cacheDir);

                    assertThat(paths.inProcessRoot()).doesNotExist();
                    assertThat(child.isAlive()).isTrue();
                    assertPresent(paths.forkedRoot(), paths.childTmpDir());
                    stop(child);

                    openCache(cacheDir);

                    assertThat(paths.forkedRoot()).doesNotExist();
                    assertThat(paths.childTmpDir()).doesNotExist();
                }
            }
        } finally {
            stop(child);
        }
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldReclaimWorkspacesOnlyAfterUnrecordedProcessesReleaseThem() throws Exception {
        Path cacheDir = tempDir.resolve("worker");
        Path ready = tempDir.resolve("worker.ready");
        try (WorkerProcess worker = startWorker("waitForKill", cacheDir, ready.toString())) {
            worker.awaitReady(ready);
            try (WorkspacePaths paths = WorkspacePaths.read(ready)) {
                Path processDirectory = Files.createDirectory(paths.inProcessRoot().resolve("work"));
                try (WorkerProcess directoryUser = startBackgroundUser("cwd", processDirectory, null);
                     WorkerProcess fileUser = startBackgroundUser("fd", tempDir, paths.childTmpDir().resolve(SENTINEL))) {
                    stop(worker.process());
                    openCache(cacheDir);

                    assertThat(processDirectory).isDirectory();
                    assertPresent(paths.inProcessRoot(), paths.forkedRoot(), paths.childTmpDir());
                    directoryUser.assertCanReadWorkspaceFile();
                    fileUser.assertCanReadWorkspaceFile();

                    stop(directoryUser.process());
                    openCache(cacheDir);

                    assertThat(paths.inProcessRoot()).doesNotExist();
                    assertPresent(paths.forkedRoot(), paths.childTmpDir());

                    stop(fileUser.process());
                    openCache(cacheDir);

                    assertThat(paths.forkedRoot()).doesNotExist();
                    assertThat(paths.childTmpDir()).doesNotExist();
                }
            }
        }
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldPreserveBothForkedDirectoriesWhenABackgroundProcessStillUsesOne() throws Exception {
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir.resolve("worker"), false, 0L);
             WorkerWorkspaceRegistry.Workspace workspace = cache.createWorkspace(true)) {
            Path childTmpDir = workspace.childTmpDir();
            try {
                writeSentinel(workspace.root());
                writeSentinel(childTmpDir);
                try (WorkerProcess fileUser = startBackgroundUser("fd", tempDir, childTmpDir.resolve(SENTINEL))) {
                    workspace.close();

                    assertPresent(workspace.root(), childTmpDir);
                    fileUser.assertCanReadWorkspaceFile();
                }
            } finally {
                deleteTestTree(childTmpDir);
                deleteTestTree(workspace.root());
            }
        }
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldPreserveForkedWorkspaceWhenWorkerHaltedWithAnUnresolvedLaunch() throws Exception {
        Path cacheDir = tempDir.resolve("worker");
        try (WorkspacePaths paths = haltWorker(cacheDir, "launching")) {
            openCache(cacheDir);

            assertThat(paths.inProcessRoot()).doesNotExist();
            assertPresent(paths.forkedRoot(), paths.childTmpDir());
        }
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldPreserveWorkspaceWithAnUnattributedUnixSocketPath() throws Exception {
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir.resolve("worker"), false, 0L);
             WorkerWorkspaceRegistry.Workspace workspace = cache.createWorkspace(true)) {
            Path socket = workspace.childTmpDir().resolve("socket");
            try {
                try (ServerSocketChannel server = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
                    server.bind(UnixDomainSocketAddress.of(socket));
                }
                // A pathname alone cannot establish whether a listener opened it under a different name.
                workspace.close();

                assertThat(socket).exists();
                assertThat(workspace.root()).exists();
            } finally {
                deleteTestTree(workspace.childTmpDir());
                deleteTestTree(workspace.root());
            }
        }
    }

    @Test
    @DisabledOnOs({OS.LINUX, OS.MAC})
    void shouldDeferStartupCleanupWhenProcessReferencesCannotBeChecked() throws Exception {
        Path cacheDir = tempDir.resolve("worker");
        try (WorkspacePaths paths = haltWorker(cacheDir, "halt")) {
            openCache(cacheDir);

            assertPresent(paths.inProcessRoot(), paths.forkedRoot(), paths.childTmpDir());
        }
    }

    @Test
    void shouldRemoveForkedChildTempDirectoryAfterItsContentsHaveBeenReplaced() throws Exception {
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir.resolve("worker"), false, 0L)) {
            WorkerWorkspaceRegistry.Workspace workspace = cache.createWorkspace(true);
            Path childTmpDir = workspace.childTmpDir();
            try {
                try (workspace) {
                    List<Path> contents;
                    try (Stream<Path> entries = Files.list(childTmpDir)) {
                        contents = entries.toList();
                    }
                    for (Path entry : contents) {
                        deleteTestTree(entry);
                    }
                    writeSentinel(childTmpDir);
                }

                assertThat(childTmpDir).doesNotExist();
                assertThat(workspace.root()).doesNotExist();
            } finally {
                deleteTestTree(childTmpDir);
                deleteTestTree(workspace.root());
            }
        }
    }

    private WorkspacePaths haltWorker(Path cacheDir, String mode) throws Exception {
        Path ready = tempDir.resolve(cacheDir.getFileName() + ".ready");
        try (WorkerProcess worker = startWorker(mode, cacheDir, ready.toString())) {
            worker.assertExitCode(HALTED);
            return WorkspacePaths.read(ready);
        }
    }

    private void openCache(Path cacheDir) throws Exception {
        try (WorkerProcess worker = startWorker("open", cacheDir)) {
            worker.assertExitCode(0);
        }
    }

    private WorkerProcess startWorker(String mode, Path cacheDir, String... args) throws IOException {
        Path log = Files.createTempFile(tempDir, "worker-", ".log");
        List<String> workerArgs = new ArrayList<>(List.of(mode, cacheDir.toString()));
        workerArgs.addAll(List.of(args));
        Process process = javaProcess(WorkerMain.class, workerArgs.toArray(String[]::new))
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        return new WorkerProcess(process, log);
    }

    private WorkerProcess startBackgroundUser(String id, Path workingDirectory, Path heldFile) throws Exception {
        Path ready = tempDir.resolve(id + ".ready");
        Path log = tempDir.resolve(id + ".log");
        String[] args = heldFile == null ? new String[] { ready.toString() }
                : new String[] { ready.toString(), heldFile.toString() };
        Process process = javaProcess(ForkedChildMain.class, args)
                .directory(workingDirectory.toFile())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile()).start();
        WorkerProcess background = new WorkerProcess(process, log);
        try {
            background.awaitReady(ready);
            return background;
        } catch (Exception | AssertionError e) {
            try {
                background.close();
            } catch (Exception cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    private ProcessBuilder javaProcess(Class<?> main, String... args) {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djava.io.tmpdir=" + tempDir,
                "-cp", System.getProperty("java.class.path"), main.getName()));
        command.addAll(List.of(args));
        return new ProcessBuilder(command);
    }

    private static void stop(Process process) throws InterruptedException {
        if (process.isAlive()) {
            assertThat(process.destroyForcibly().waitFor(START_TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                    .as("process %s exited", process.pid()).isTrue();
        }
    }

    private static void writeSentinel(Path directory) throws IOException {
        Files.writeString(directory.resolve(SENTINEL), SENTINEL);
    }

    private static void assertPresent(Path... directories) {
        for (Path directory : directories) {
            assertThat(directory.resolve(SENTINEL)).hasContent(SENTINEL);
        }
    }

    private static void deleteTestTree(Path root) throws IOException {
        if (Files.exists(root, NOFOLLOW_LINKS)) {
            List<Path> paths;
            try (Stream<Path> tree = Files.walk(root)) {
                paths = tree.sorted(Comparator.reverseOrder()).toList();
            }
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
        }
    }

    private record WorkspacePaths(Path inProcessRoot, Path forkedRoot, Path childTmpDir) implements AutoCloseable {

        static WorkspacePaths read(Path ready) throws IOException {
            List<String> manifest = Files.readAllLines(ready);
            return new WorkspacePaths(Path.of(manifest.get(0)), Path.of(manifest.get(1)), Path.of(manifest.get(2)));
        }

        @Override
        public void close() throws IOException {
            deleteTestTree(childTmpDir);
            deleteTestTree(forkedRoot);
            deleteTestTree(inProcessRoot);
        }
    }

    private record WorkerProcess(Process process, Path log) implements AutoCloseable {

        void awaitReady(Path ready) throws IOException, InterruptedException {
            long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
            while (!Files.exists(ready) && process.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(ready).as("worker output: %s", Files.readString(log)).exists();
        }

        void awaitExit() throws InterruptedException {
            assertThat(process.waitFor(START_TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                    .as("worker exited (log at %s)", log).isTrue();
        }

        void assertCanReadWorkspaceFile() throws IOException, InterruptedException {
            process.getOutputStream().write(1);
            process.getOutputStream().flush();
            long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
            while (!Files.readAllLines(log).contains(SENTINEL) && process.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(Files.readAllLines(log)).contains(SENTINEL);
            assertThat(process.isAlive()).isTrue();
        }

        void assertExitCode(int expected) throws IOException, InterruptedException {
            awaitExit();
            assertThat(process.exitValue()).as("worker output: %s", Files.readString(log)).isEqualTo(expected);
        }

        @Override
        public void close() throws InterruptedException {
            stop(process);
        }
    }

    public static final class WorkerMain {

        public static void main(String[] args) throws Exception {
            String mode = args[0];
            try (WorkerFileCache cache = WorkerFileCache.open(Path.of(args[1]), false, 0L)) {
                if ("open".equals(mode)) {
                    return;
                }
                try (WorkerWorkspaceRegistry.Workspace inProcess = cache.createWorkspace(false);
                     WorkerWorkspaceRegistry.Workspace forked = cache.createWorkspace(true)) {
                    writeSentinel(inProcess.root());
                    writeSentinel(forked.root());
                    writeSentinel(forked.childTmpDir());
                    if ("hold".equals(mode) || "launching".equals(mode)) {
                        forked.markLaunching();
                    }
                    if ("hold".equals(mode)) {
                        forked.markLaunched(ProcessHandle.of(Long.parseLong(args[3])).orElseThrow());
                    }
                    writeReady(Path.of(args[2]), List.of(inProcess.root().toString(), forked.root().toString(),
                            forked.childTmpDir().toString()));
                    if ("hold".equals(mode) || "waitForKill".equals(mode)) {
                        Thread.sleep(HELPER_LIFETIME.toMillis());
                    } else {
                        Runtime.getRuntime().halt(HALTED);
                    }
                }
            }
        }

        private static void writeReady(Path ready, List<String> paths) throws IOException {
            Path staging = Files.write(ready.resolveSibling(ready.getFileName() + ".tmp"), paths);
            Files.move(staging, ready, ATOMIC_MOVE);
        }
    }

    public static final class ForkedChildMain {

        public static void main(String[] args) throws IOException, InterruptedException {
            try (var heldFile = args.length == 2 ? Files.newInputStream(Path.of(args[1])) : null) {
                if (args.length > 0) {
                    Files.createFile(Path.of(args[0]));
                    if (System.in.read() == -1) {
                        return;
                    }
                    Path sentinel = args.length == 2 ? Path.of(args[1]) : Path.of("..", SENTINEL);
                    System.out.println(Files.readString(sentinel));
                }
                Thread.sleep(HELPER_LIFETIME.toMillis());
            }
        }
    }
}
