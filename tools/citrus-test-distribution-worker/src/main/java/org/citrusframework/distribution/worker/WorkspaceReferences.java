/*
 * Copyright 2006-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.citrusframework.distribution.worker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Checks filesystem references that can outlive both JVMs and no longer appear in their process trees. */
final class WorkspaceReferences {

    private static final int UNIX_FILE_TYPE_MASK = 0170000;
    private static final int UNIX_SOCKET_TYPE = 0140000;
    private static final int MAX_TARGETED_PIDS = 256;
    private static final int MAX_LSOF_OUTPUT_BYTES = 8 * 1024 * 1024;
    private static final int LSOF_TIMEOUT_SECONDS = 5;

    private WorkspaceReferences() {
    }

    static LinuxWorkspaceReferences.StartTicks linuxWorkerStart() {
        return osName().contains("linux") ? LinuxWorkspaceReferences.currentStart() : null;
    }

    static Map<Long, Instant> captureMacOsBaselineProcesses() {
        if (!osName().startsWith("mac")) {
            return Map.of();
        }
        Map<Long, Instant> baseline = new HashMap<>();
        try (var processes = ProcessHandle.allProcesses()) {
            processes.filter(ProcessHandle::isAlive).forEach(process ->
                    process.info().startInstant().ifPresent(start -> baseline.put(process.pid(), start)));
        }
        return baseline;
    }

    static boolean inUse(List<Path> roots, boolean recovering, LinuxWorkspaceReferences.StartTicks linuxWorkerStart)
            throws IOException {
        return inUse(roots, recovering, linuxWorkerStart, null);
    }

    static boolean inUse(List<Path> roots, boolean recovering, LinuxWorkspaceReferences.StartTicks linuxWorkerStart,
                         Map<Long, Instant> macOsBaselineProcesses) throws IOException {
        return inUse(roots, recovering, linuxWorkerStart, macOsBaselineProcesses, ProcessHandle::allProcesses,
                WorkspaceReferences::runLsof);
    }

    static boolean inUse(List<Path> roots, boolean recovering, LinuxWorkspaceReferences.StartTicks linuxWorkerStart,
                         Map<Long, Instant> macOsBaselineProcesses, Supplier<Stream<ProcessHandle>> allProcesses,
                         CommandRunner commandRunner) throws IOException {
        String os = osName();
        if (os.contains("linux")) {
            return LinuxWorkspaceReferences.inUse(roots, linuxWorkerStart) || containsUnixSocket(roots);
        }
        if (os.startsWith("mac")) {
            if (recovering || macOsBaselineProcesses == null || Thread.currentThread().isInterrupted()) {
                return macOsReferences(roots, commandRunner) || containsUnixSocket(roots);
            }
            if (containsUnixSocket(roots)) {
                return true;
            }
            Optional<List<SurvivorProcess>> snapshot = postLaunchProcesses(macOsBaselineProcesses, allProcesses);
            if (snapshot.isEmpty()) {
                checkInterrupted();
                return macOsReferences(roots, commandRunner);
            }
            List<SurvivorProcess> survivors = snapshot.get();
            return macOsProcessReferences(roots, survivors.stream().map(SurvivorProcess::pid).toList(), commandRunner,
                    output -> postLaunchProcesses(macOsBaselineProcesses, allProcesses)
                            .map(rechecked -> hasUnverifiedSurvivors(survivors, rechecked, output)).orElse(true));
        }
        // An orphan on a platform without an inspection backend has no owner left to vouch for it.
        return recovering;
    }

    private static Optional<List<SurvivorProcess>> postLaunchProcesses(Map<Long, Instant> baseline,
                                                                       Supplier<Stream<ProcessHandle>> allProcesses) {
        try (var processes = allProcesses.get()) {
            ProcessHandle current = ProcessHandle.current();
            String currentUser = current.info().user().orElse(null);
            // A process that exits after enumeration may already have handed its references to a new child.
            return Optional.of(processes.filter(process -> process.pid() != current.pid())
                    .map(process -> {
                        ProcessHandle.Info info = process.info();
                        Instant startedAt = info.startInstant().orElse(null);
                        if ((currentUser == null || currentUser.equals(info.user().orElse(currentUser)))
                                && (startedAt == null || !Objects.equals(baseline.get(process.pid()), startedAt))) {
                            return new SurvivorProcess(process.pid(), startedAt);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .limit(MAX_TARGETED_PIDS + 1L)
                    .toList());
        } catch (RuntimeException e) {
            // A failed native inventory or metadata lookup cannot establish which processes survived.
            return Optional.empty();
        }
    }

    private static boolean hasUnverifiedSurvivors(List<SurvivorProcess> before, List<SurvivorProcess> after,
                                                  String output) {
        // A selected process can hand off its references to a new process while lsof runs. A still-live PID
        // missing from the output or without comparable start metadata also makes the targeted scan inconclusive.
        Set<SurvivorProcess> selected = Set.copyOf(before);
        Set<String> inspected = output.lines().filter(line -> line.startsWith("p")).collect(Collectors.toSet());
        return after.stream().anyMatch(process -> process.startedAt() == null || !selected.contains(process)
                || !inspected.contains("p" + process.pid()));
    }

    private static String osName() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    }

    private static boolean containsUnixSocket(List<Path> roots) throws IOException {
        // The OS can report a socket by the relative name it originally bound, even after its listener changes
        // directories. A socket node alone is insufficient to prove that no process is still serving it.
        for (Path root : roots) {
            try (var entries = Files.find(root, Integer.MAX_VALUE, (path, attributes) -> attributes.isOther())) {
                var specialFiles = entries.iterator();
                while (specialFiles.hasNext()) {
                    Path path = specialFiles.next();
                    int mode = (int) Files.getAttribute(path, "unix:mode", LinkOption.NOFOLLOW_LINKS);
                    if ((mode & UNIX_FILE_TYPE_MASK) == UNIX_SOCKET_TYPE) {
                        return true;
                    }
                }
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }
        return false;
    }

    private static boolean containsMultiplyLinkedRegularFile(List<Path> roots) throws IOException {
        for (Path root : roots) {
            try (var entries = Files.find(root.toRealPath(), Integer.MAX_VALUE,
                    (path, attributes) -> attributes.isRegularFile())) {
                var regularFiles = entries.iterator();
                while (regularFiles.hasNext()) {
                    checkInterrupted();
                    Path path = regularFiles.next();
                    int linkCount = (int) Files.getAttribute(path, "unix:nlink", LinkOption.NOFOLLOW_LINKS);
                    if (linkCount > 1) {
                        return true;
                    }
                }
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }
        return false;
    }

    static boolean macOsProcessReferences(List<Path> roots, List<Long> postLaunchPids, CommandRunner commandRunner)
            throws IOException {
        return macOsProcessReferences(roots, postLaunchPids, commandRunner, output -> false);
    }

    private static boolean macOsProcessReferences(List<Path> roots, List<Long> postLaunchPids,
                                                   CommandRunner commandRunner, Predicate<String> hasUnverifiedSurvivors)
            throws IOException {
        checkInterrupted();
        if (postLaunchPids.isEmpty()) {
            return false;
        }
        if (postLaunchPids.size() > MAX_TARGETED_PIDS) {
            return macOsReferences(roots, commandRunner);
        }
        List<Path> normalizedRoots = normalizedRoots(roots);
        // Depending on its locale, lsof can escape backslashes and non-ASCII or non-printable path characters.
        if (normalizedRoots.stream().anyMatch(WorkspaceReferences::mayBeEscaped)) {
            checkInterrupted();
            return macOsReferences(roots, commandRunner);
        }
        String pids = postLaunchPids.stream().map(String::valueOf).collect(Collectors.joining(","));
        List<String> command = List.of("/usr/sbin/lsof", "-n", "-P", "-Fn", "+w", "-p", pids);
        try {
            LsofResult result = commandRunner.run(command);
            Optional<Boolean> references =
                    parseMacOsProcessLsofOutput(result.output(), result.exitCode(), normalizedRoots);
            checkInterrupted();
            if (references.orElse(false)) {
                return true;
            }
            // A process can open a workspace file through an outside hard link. The full scan matches its inode.
            if (references.isPresent() && !containsMultiplyLinkedRegularFile(roots)
                    && !hasUnverifiedSurvivors.test(result.output())) {
                checkInterrupted();
                return false;
            }
        } catch (IOException | SecurityException | UnsupportedOperationException e) {
            if (Thread.currentThread().isInterrupted()) {
                throw e;
            }
            // A failed targeted inspection or verification still permits the full directory scan.
        }
        checkInterrupted();
        return macOsReferences(roots, commandRunner);
    }

    private static boolean mayBeEscaped(Path path) {
        return path.toString().replace(path.getFileSystem().getSeparator(), "/")
                .chars().anyMatch(ch -> ch < ' ' || ch > '~' || ch == '\\');
    }

    private static List<Path> normalizedRoots(List<Path> roots) {
        List<Path> normalizedRoots = new ArrayList<>();
        for (Path root : roots) {
            Path absolute = root.toAbsolutePath().normalize();
            normalizedRoots.add(absolute);
            try {
                Path real = root.toRealPath();
                if (!real.equals(absolute)) {
                    normalizedRoots.add(real);
                }
            } catch (IOException ignored) {
                // The absolute path remains useful when the directory cannot be resolved.
            }
        }
        return normalizedRoots;
    }

    static boolean matchesAnyRoot(String lsofNameField, List<Path> normalizedRoots) {
        if (!lsofNameField.startsWith("n") || lsofNameField.length() == 1) {
            return false;
        }
        try {
            Path name = Path.of(lsofNameField.substring(1));
            return normalizedRoots.stream().anyMatch(name::startsWith);
        } catch (InvalidPathException e) {
            // The name field can also contain values such as socket identifiers.
            return false;
        }
    }

    static Optional<Boolean> parseMacOsProcessLsofOutput(String stdout, int exitCode, List<Path> normalizedRoots) {
        boolean hasDiagnostics = false;
        var lines = stdout.lines().iterator();
        while (lines.hasNext()) {
            String line = lines.next();
            if (matchesAnyRoot(line, normalizedRoots)) {
                return Optional.of(true);
            }
            if (!line.isEmpty() && !line.startsWith("p") && !line.startsWith("f") && !line.startsWith("n")) {
                hasDiagnostics = true;
            }
        }
        return hasDiagnostics || (exitCode != 0 && exitCode != 1) ? Optional.empty() : Optional.of(false);
    }

    private static boolean macOsReferences(List<Path> roots, CommandRunner commandRunner) throws IOException {
        // Terse mode disables warnings by default. Re-enable them to detect partial directory scans.
        List<String> command = new ArrayList<>(List.of("/usr/sbin/lsof", "-n", "-P", "-t", "+w", "-x", "f"));
        for (Path root : roots) {
            command.add("+D");
            command.add(root.toString());
        }
        // Cancellation cleanup still needs the full scan when the caller already has its interrupt flag set.
        boolean wasInterrupted = Thread.interrupted();
        try {
            LsofResult result = commandRunner.run(command);
            // macOS lsof can return 1 with matching PIDs. Only 1 with no output confirms no matches.
            // Diagnostics also keep the workspace, since they may indicate a partial scan.
            return !result.output().isEmpty() || result.exitCode() != 1;
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static LsofResult runLsof(List<String> command) throws IOException {
        checkInterrupted();
        // Field output can exceed the pipe buffer while we wait for lsof to exit.
        Path output = Files.createTempFile("citrus-lsof-", ".log");
        Process inspection = null;
        try {
            inspection = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
            if (!inspection.waitFor(LSOF_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IOException("Timed out checking workspace process references");
            }
            checkInterrupted();
            long outputBytes = Files.size(output);
            if (command.contains("-t")) {
                // Terse mode needs only the presence of a PID or diagnostic.
                return new LsofResult(outputBytes == 0 ? "" : "lsof output", inspection.exitValue());
            }
            if (outputBytes > MAX_LSOF_OUTPUT_BYTES) {
                throw new IOException("Too much output while checking workspace process references");
            }
            return new LsofResult(Files.readString(output), inspection.exitValue());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted checking workspace process references", e);
        } finally {
            if (inspection != null && inspection.isAlive()) {
                inspection.destroyForcibly();
            }
            Files.deleteIfExists(output);
        }
    }

    private static void checkInterrupted() throws IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new IOException("Interrupted checking workspace process references");
        }
    }

    @FunctionalInterface
    interface CommandRunner {
        LsofResult run(List<String> command) throws IOException;
    }

    record LsofResult(String output, int exitCode) {
    }

    private record SurvivorProcess(long pid, Instant startedAt) {
    }
}
