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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.citrusframework.distribution.workspace.WorkspaceLease;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerWorkspaceLeaseTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int WORKER_HALTED = 42;
    static final String LEASE_ACQUIRED = "lease-acquired";
    static final String SENTINEL = "workspace-data";

    @TempDir
    Path tempDir;

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldRecoverOnlyAfterTheLeaseHolderExitsFollowingAWorkerHalt() throws Exception {
        Path cache = tempDir.resolve("cache");
        Path ready = tempDir.resolve("workspace.ready");
        try (TestProcess worker = start(WorkerMain.class, cache.toString(), ready.toString())) {
            worker.awaitReady(ready);
            List<String> paths = Files.readAllLines(ready);
            Path ticket = Path.of(paths.get(0));
            Path workspace = Path.of(paths.get(1));
            Path childTemp = Path.of(paths.get(2));
            try (TestProcess helper = startLeaseHolder(tempDir, ticket)) {
                helper.awaitOutput(LEASE_ACQUIRED);
                worker.signal();
                worker.assertExit(WORKER_HALTED);

                restartWorker(cache);

                assertThat(workspace.resolve(SENTINEL)).hasContent(SENTINEL);
                assertThat(childTemp).isDirectory();
                assertRetired(ticket);
                helper.signal();
                helper.awaitOutput(SENTINEL);
                helper.close();

                restartWorker(cache);

                assertThat(workspace.getParent()).doesNotExist();
                assertThat(childTemp).doesNotExist();
                assertRetired(ticket);
            } finally {
                // The separate fork temp directory can be outside the JUnit temporary directory.
                deleteTestTree(childTemp);
            }
        }
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldKeepExistingSameJvmLeasesUsableAfterRetiringTheTicket() throws IOException {
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir.resolve("cache"), false, 0L);
             WorkerWorkspaceRegistry.Workspace workspace = cache.createWorkspace(false, true)) {
            Path expected = Files.createDirectories(workspace.root().resolve("workspace"));
            Files.writeString(expected.resolve(SENTINEL), SENTINEL);
            Path ticket = workspace.leaseTicket();
            try (WorkspaceLease first = WorkspaceLease.acquire(ticket);
                 WorkspaceLease second = WorkspaceLease.acquire(ticket)) {
                assertThat(first.workspace()).isEqualTo(expected);
                assertThat(second.workspace()).isEqualTo(expected);
                workspace.close();

                assertThat(first.workspace().resolve(SENTINEL)).hasContent(SENTINEL);
                assertRetired(ticket);
                first.close();
                workspace.close();

                assertThat(second.workspace().resolve(SENTINEL)).hasContent(SENTINEL);
            }

            workspace.close();

            assertThat(workspace.root()).doesNotExist();
            assertRetired(ticket);
        }
    }

    static void assertRetired(Path ticket) {
        assertThatThrownBy(() -> {
            try (WorkspaceLease lease = WorkspaceLease.acquire(ticket)) {
                lease.workspace();
            }
        }).isInstanceOf(IOException.class);
    }

    private void restartWorker(Path cache) throws Exception {
        try (TestProcess worker = start(WorkerMain.class, cache.toString())) {
            worker.assertExit(0);
        }
    }

    static TestProcess startLeaseHolder(Path tempDir, Path ticket) throws IOException {
        return start(tempDir, LeaseHolderMain.class, ticket.toString());
    }

    private TestProcess start(Class<?> main, String... args) throws IOException {
        return start(tempDir, main, args);
    }

    private static TestProcess start(Path tempDir, Class<?> main, String... args) throws IOException {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djava.io.tmpdir=" + tempDir,
                "-cp", System.getProperty("java.class.path"), main.getName()));
        command.addAll(List.of(args));
        Path log = Files.createTempFile(tempDir, "workspace-process-", ".log");
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        return new TestProcess(process, log);
    }

    static void deleteTestTree(Path root) throws IOException {
        if (Files.exists(root)) {
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    record TestProcess(Process process, Path log) implements AutoCloseable {

        void awaitReady(Path ready) throws IOException, InterruptedException {
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (!Files.exists(ready) && process.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(ready).as("worker output: %s", Files.readString(log)).exists();
        }

        void awaitOutput(String output) throws IOException, InterruptedException {
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (!Files.readAllLines(log).contains(output) && process.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(Files.readAllLines(log)).contains(output);
        }

        void signal() throws IOException {
            process.getOutputStream().write(1);
            process.getOutputStream().flush();
        }

        void assertExit(int status) throws IOException, InterruptedException {
            assertThat(process.waitFor(TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                    .as("process exited (log at %s)", log).isTrue();
            assertThat(process.exitValue()).as("process output: %s", Files.readString(log)).isEqualTo(status);
        }

        @Override
        public void close() throws InterruptedException {
            if (process.isAlive()) {
                assertThat(process.destroyForcibly().waitFor(TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                        .as("process %s exited", process.pid()).isTrue();
            }
        }
    }

    public static final class WorkerMain {

        public static void main(String[] args) throws IOException {
            try (WorkerFileCache cache = WorkerFileCache.open(Path.of(args[0]), false, 0L)) {
                if (args.length == 1) {
                    return;
                }
                try (WorkerWorkspaceRegistry.Workspace workspace = cache.createWorkspace(true, true)) {
                    Path path = Files.createDirectories(workspace.root().resolve("workspace"));
                    Files.writeString(path.resolve(SENTINEL), SENTINEL);
                    Path ready = Path.of(args[1]);
                    Path staging = Files.write(ready.resolveSibling("workspace.pending"),
                            List.of(workspace.leaseTicket().toString(), path.toString(), workspace.childTmpDir().toString()));
                    Files.move(staging, ready, ATOMIC_MOVE);
                    if (System.in.read() != -1) {
                        Runtime.getRuntime().halt(WORKER_HALTED);
                    }
                }
            }
        }
    }

    public static final class LeaseHolderMain {

        public static void main(String[] args) throws IOException {
            try (WorkspaceLease lease = WorkspaceLease.acquire(Path.of(args[0]))) {
                Path path = lease.workspace();
                System.out.println(LEASE_ACQUIRED);
                // Between these signals the helper keeps only the pathname and the lease outside the workspace.
                if (System.in.read() != -1) {
                    System.out.println(Files.readString(path.resolve(SENTINEL)));
                    System.in.read();
                }
            }
        }
    }
}
