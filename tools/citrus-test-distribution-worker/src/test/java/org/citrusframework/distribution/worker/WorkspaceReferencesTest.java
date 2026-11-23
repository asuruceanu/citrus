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

        boolean inUse = WorkspaceReferences.inUse(roots, false, null, baseline, () -> Stream.of(ProcessHandle.current()),
                command -> {
                    commands.add(command);
                    return new WorkspaceReferences.LsofResult("", 1);
                });

        assertThat(inUse).isFalse();
        assertThat(commands).isEmpty();
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
