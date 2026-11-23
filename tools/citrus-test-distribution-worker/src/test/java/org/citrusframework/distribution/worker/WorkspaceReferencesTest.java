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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkspaceReferencesTest {

    private static final List<Path> ROOTS = List.of(Path.of("/private/tmp/cdtmp-1"));

    @TempDir
    Path tempDir;

    @Test
    void shouldAvoidSiblingPrefixFalsePositiveWhenMatchingLsofPaths() {
        assertThat(WorkspaceReferences.matchesAnyRoot("n/private/tmp/cdtmp-10/open.txt", ROOTS)).isFalse();
        assertThat(WorkspaceReferences.matchesAnyRoot("n/private/tmp/cdtmp-10", ROOTS)).isFalse();
        assertThat(WorkspaceReferences.matchesAnyRoot("n/private/tmp/cdtmp-1", ROOTS)).isTrue();
        assertThat(WorkspaceReferences.matchesAnyRoot("n/private/tmp/cdtmp-1/sub/file.txt", ROOTS)).isTrue();
    }

    @Test
    void shouldTreatExitedTargetPidExitCodeOneWithEmptyOutputAsUnused() {
        assertThat(WorkspaceReferences.parseMacOsProcessLsofOutput("", 1, ROOTS)).contains(false);
    }

    @Test
    void shouldReturnEmptyForFallbackWhenLsofEmitsWarningOrUnexpectedExitCode() {
        assertThat(WorkspaceReferences.parseMacOsProcessLsofOutput("p123\nlsof: WARNING: incomplete scan", 1, ROOTS))
                .isEmpty();
        assertThat(WorkspaceReferences.parseMacOsProcessLsofOutput("", 2, ROOTS)).isEmpty();
    }

    @Test
    void shouldRecognizeAReferenceEvenWithDiagnosticsAndNonzeroExitCode() {
        String output = "p123\nfcwd\nn/private/tmp/cdtmp-1/sub\nlsof: WARNING: incomplete scan";
        assertThat(WorkspaceReferences.parseMacOsProcessLsofOutput(output, 2, ROOTS)).contains(true);
    }

    @Test
    @EnabledOnOs(OS.MAC)
    void shouldSkipFullLsofOnMacOsWhenNoPostLaunchProcessesSurvived() throws IOException {
        Map<Long, Instant> baseline = WorkspaceReferences.captureMacOsBaselineProcesses();
        List<Path> roots = List.of(Files.createDirectory(tempDir.resolve("workspace")),
                Files.createDirectory(tempDir.resolve("child-tmp")));
        List<List<String>> commands = new ArrayList<>();
        AtomicInteger snapshots = new AtomicInteger();

        boolean inUse = WorkspaceReferences.inUse(roots, false, null, baseline, () -> {
            snapshots.incrementAndGet();
            return Stream.of(ProcessHandle.current());
        }, command -> {
            commands.add(command);
            return new WorkspaceReferences.LsofResult("", 1);
        });

        assertThat(inUse).isFalse();
        assertThat(commands).isEmpty();
        assertThat(snapshots).hasValue(1);
    }

    @Test
    void shouldFallbackConservativelyWhenSurvivorBatchExceedsMaxPids() throws IOException {
        List<Long> survivors = LongStream.rangeClosed(1, 257).boxed().toList();
        List<List<String>> commands = new ArrayList<>();

        boolean inUse = WorkspaceReferences.macOsProcessReferences(ROOTS, survivors, command -> {
            commands.add(command);
            return new WorkspaceReferences.LsofResult("123\n", 1);
        });

        assertThat(inUse).isTrue();
        assertThat(commands).containsExactly(List.of("/usr/sbin/lsof", "-n", "-P", "-t", "+w", "-x", "f",
                "+D", ROOTS.getFirst().toString()));
    }

    @Test
    void shouldFallbackWhenTargetedScanFails() throws IOException {
        List<List<String>> commands = new ArrayList<>();

        boolean inUse = WorkspaceReferences.macOsProcessReferences(ROOTS, List.of(123L, 456L), command -> {
            commands.add(command);
            if (command.contains("-p")) {
                throw new IOException("targeted scan timed out");
            }
            return new WorkspaceReferences.LsofResult("", 1);
        });

        assertThat(inUse).isFalse();
        assertThat(commands).hasSize(2);
        assertThat(commands.getFirst()).containsExactly("/usr/sbin/lsof", "-n", "-P", "-Fn", "+w", "-p", "123,456");
        assertThat(commands.getLast()).contains("+D", ROOTS.getFirst().toString());
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldFallbackWhenATargetedMissMayHaveOpenedAnOutsideHardLink() throws IOException {
        Path root = Files.createDirectory(tempDir.resolve("workspace"));
        Path file = Files.createFile(root.resolve("file"));
        Path outside = Files.createLink(tempDir.resolve("outside"), file);
        List<List<String>> commands = new ArrayList<>();

        boolean inUse = WorkspaceReferences.macOsProcessReferences(List.of(root), List.of(123L), command -> {
            commands.add(command);
            return command.contains("-p") ? new WorkspaceReferences.LsofResult("p123\nn" + outside, 0)
                    : new WorkspaceReferences.LsofResult("123\n", 0);
        });

        assertThat(inUse).isTrue();
        assertThat(commands).hasSize(2);
        assertThat(commands.getFirst()).contains("-p", "123");
        assertThat(commands.getLast()).contains("+D", root.toString());
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldKeepTheTargetedMissForSinglyLinkedFiles() throws IOException {
        Path root = Files.createDirectory(tempDir.resolve("workspace"));
        Files.createFile(root.resolve("file"));
        List<List<String>> commands = new ArrayList<>();

        boolean inUse = WorkspaceReferences.macOsProcessReferences(List.of(root), List.of(123L), command -> {
            commands.add(command);
            return new WorkspaceReferences.LsofResult("", 1);
        });

        assertThat(inUse).isFalse();
        assertThat(commands).singleElement().satisfies(command -> assertThat(command).contains("-p", "123"));
    }

    @Test
    void shouldFallbackWhenTheFileLinkCountsCannotBeChecked() throws IOException {
        Path missingRoot = tempDir.resolve("missing");
        List<List<String>> commands = new ArrayList<>();

        boolean inUse = WorkspaceReferences.macOsProcessReferences(List.of(missingRoot), List.of(123L), command -> {
            commands.add(command);
            return new WorkspaceReferences.LsofResult("", 1);
        });

        assertThat(inUse).isFalse();
        assertThat(commands).hasSize(2);
        assertThat(commands.getLast()).contains("+D", missingRoot.toString());
    }

    @Test
    @EnabledOnOs(OS.MAC)
    void shouldDetectANativeProcessThatHoldsAnOutsideHardLink() throws Exception {
        Path root = Files.createDirectory(tempDir.resolve("workspace"));
        Path file = Files.createFile(root.resolve("file"));
        Path outside = Files.createLink(tempDir.resolve("outside"), file);
        Path ready = tempDir.resolve("ready");
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                WorkerWorkspaceRecoveryTest.ForkedChildMain.class.getName(), ready.toString(), outside.toString())
                .start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!Files.exists(ready) && process.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(ready).exists();
            assertThat(process.isAlive()).isTrue();

            assertThat(WorkspaceReferences.macOsProcessReferences(List.of(root), List.of(process.pid()),
                    WorkspaceReferences::runLsof)).isTrue();
        } finally {
            assertThat(process.destroyForcibly().waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @EnabledOnOs(OS.MAC)
    void shouldFallbackWhenAnotherSurvivorAppearsAfterTheTargetedScan() throws Exception {
        Process first = new ProcessBuilder("/bin/cat").start();
        try {
            Process later = new ProcessBuilder("/bin/cat").start();
            try {
                AtomicInteger snapshots = new AtomicInteger();
                List<List<String>> commands = new ArrayList<>();

                boolean inUse = WorkspaceReferences.inUse(List.of(tempDir), false, null, Map.of(),
                        () -> snapshots.getAndIncrement() == 0 ? Stream.of(first.toHandle()) : Stream.of(later.toHandle()),
                        command -> {
                            commands.add(command);
                            return command.contains("-p") ? new WorkspaceReferences.LsofResult("", 1)
                                    : new WorkspaceReferences.LsofResult(later.pid() + "\n", 0);
                        });

                assertThat(inUse).isTrue();
                assertThat(snapshots).hasValue(2);
                assertThat(commands).hasSize(2);
                assertThat(commands.getFirst()).contains("-p", Long.toString(first.pid()));
                assertThat(commands.getLast()).contains("+D", tempDir.toString());
            } finally {
                assertThat(later.destroyForcibly().waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        } finally {
            assertThat(first.destroyForcibly().waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @EnabledOnOs(OS.MAC)
    void shouldKeepTheTargetedMissForAReportedLiveSurvivor() throws Exception {
        verifyLiveSurvivorCoverage(true);
    }

    @Test
    @EnabledOnOs(OS.MAC)
    void shouldFallbackWhenTargetedOutputOmitsALiveSurvivor() throws Exception {
        verifyLiveSurvivorCoverage(false);
    }

    private void verifyLiveSurvivorCoverage(boolean appearedInOutput) throws Exception {
        Process process = new ProcessBuilder("/bin/cat").start();
        try {
            AtomicInteger snapshots = new AtomicInteger();
            List<List<String>> commands = new ArrayList<>();
            String targetedOutput = appearedInOutput ? "p" + process.pid() + "\n" : "";

            boolean inUse = WorkspaceReferences.inUse(List.of(tempDir), false, null, Map.of(), () -> {
                snapshots.incrementAndGet();
                return Stream.of(process.toHandle());
            }, command -> {
                commands.add(command);
                return command.contains("-p") ? new WorkspaceReferences.LsofResult(targetedOutput, 1)
                        : new WorkspaceReferences.LsofResult(process.pid() + "\n", 0);
            });

            assertThat(snapshots).hasValue(2);
            assertThat(inUse).isEqualTo(!appearedInOutput);
            assertThat(commands).hasSize(appearedInOutput ? 1 : 2);
        } finally {
            assertThat(process.destroyForcibly().waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void shouldDeferCleanupIfTargetedScanIsInterrupted() {
        List<List<String>> commands = new ArrayList<>();
        try {
            assertThatThrownBy(() -> WorkspaceReferences.macOsProcessReferences(ROOTS, List.of(123L), command -> {
                commands.add(command);
                Thread.currentThread().interrupt();
                throw new IOException("targeted scan interrupted");
            })).isInstanceOf(IOException.class).hasMessage("targeted scan interrupted");

            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(commands).hasSize(1);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldMatchAbsoluteAndRealRootPaths() throws IOException {
        Path realRoot = Files.createDirectory(tempDir.resolve("real")).toRealPath();
        Path alias = Files.createSymbolicLink(realRoot.resolveSibling("alias"), realRoot);
        List<List<String>> commands = new ArrayList<>();

        assertThat(WorkspaceReferences.macOsProcessReferences(List.of(alias), List.of(123L), command -> {
            commands.add(command);
            return new WorkspaceReferences.LsofResult("p123\nn" + realRoot.resolve("file"), 0);
        })).isTrue();
        assertThat(WorkspaceReferences.macOsProcessReferences(List.of(alias), List.of(123L), command -> {
            commands.add(command);
            return new WorkspaceReferences.LsofResult("p123\nn" + alias.resolve("file"), 0);
        })).isTrue();
        assertThat(commands).allSatisfy(command -> assertThat(command).contains("-p"));
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldUseFullScanForWorkspacePathsThatLsofCanEscape() throws IOException {
        Path root = tempDir.resolve("tab\tinside");
        List<List<String>> commands = new ArrayList<>();

        boolean inUse = WorkspaceReferences.macOsProcessReferences(List.of(root), List.of(123L), command -> {
            commands.add(command);
            return new WorkspaceReferences.LsofResult("123\n", 0);
        });

        assertThat(inUse).isTrue();
        assertThat(commands).singleElement().satisfies(command -> assertThat(command).contains("+D", root.toString()));
    }

    @Test
    @EnabledOnOs(OS.MAC)
    void shouldRunFullScanAndPreserveAFlagSetBeforeCleanup() throws IOException {
        List<List<String>> commands = new ArrayList<>();
        try {
            Thread.currentThread().interrupt();

            boolean inUse = WorkspaceReferences.inUse(List.of(tempDir), false, null, Map.of(), Stream::empty, command -> {
                assertThat(Thread.currentThread().isInterrupted()).isFalse();
                commands.add(command);
                return new WorkspaceReferences.LsofResult("", 1);
            });

            assertThat(inUse).isFalse();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(commands).singleElement().satisfies(command -> assertThat(command).contains("+D"));
        } finally {
            Thread.interrupted();
        }
    }
}
