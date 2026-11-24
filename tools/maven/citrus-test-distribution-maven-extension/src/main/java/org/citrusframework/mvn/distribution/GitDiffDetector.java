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

package org.citrusframework.mvn.distribution;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.citrusframework.distribution.model.WorkspaceChange;
import org.citrusframework.distribution.model.WorkspaceChange.Kind;

/**
 * Inspects workspace git metadata (branch, HEAD SHA, changed paths) for session recording.
 */
public final class GitDiffDetector {

    static final long DEFAULT_GIT_TIMEOUT_MS = 5_000L;
    private static final long PATCH_BUDGET_MS = 2_000L;
    private static final int MAX_GIT_OUTPUT_BYTES = 2 * 1024 * 1024;
    private static final int MAX_FILE_PATCH_BYTES = 8 * 1024;
    private static final int MAX_SESSION_PATCH_BYTES = 32 * 1024;

    private GitDiffDetector() {
    }

    public record GitWorkspaceInfo(String branch, String commit, List<WorkspaceChange> workspaceChanges) {
        public List<String> changedFiles() {
            return workspaceChanges.stream().map(WorkspaceChange::path).toList();
        }
    }

    private record GitChange(String path, Kind kind, String previousPath, boolean stagedOnly) {

        WorkspaceChange withPreview(Integer additions, Integer deletions, String patch, boolean truncated) {
            return new WorkspaceChange(path, kind, previousPath, additions, deletions, patch, truncated);
        }
    }

    private record CommandOutput(String text, int bytes, boolean truncated) {
        private static final CommandOutput EMPTY = new CommandOutput("", 0, false);
    }

    public static GitWorkspaceInfo inspect(Path baseDir) {
        if (baseDir == null) {
            return new GitWorkspaceInfo("unknown", "unknown", List.of());
        }
        String root = runGitSingleLine(baseDir, "rev-parse", "--show-toplevel");
        if (root.isBlank()) {
            return new GitWorkspaceInfo("unknown", "unknown", List.of());
        }
        Path repo = Path.of(root);
        String branch = runGitSingleLine(repo, "rev-parse", "--abbrev-ref", "HEAD");
        String commit = runGitSingleLine(repo, "rev-parse", "--short=12", "HEAD");
        Map<String, GitChange> changed = new LinkedHashMap<>();
        collectChanges(repo, changed, false);
        collectChanges(repo, changed, true);

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(PATCH_BUDGET_MS);
        int remainingBytes = MAX_SESSION_PATCH_BYTES;
        List<WorkspaceChange> changes = new ArrayList<>();
        for (GitChange change : changed.values()) {
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remainingMillis > 0 && remainingBytes > 0) {
                CommandOutput preview = filePreview(repo, change, remainingMillis,
                        Math.min(remainingBytes, MAX_FILE_PATCH_BYTES));
                remainingBytes -= preview.bytes();
                changes.add(parsePreview(change, preview));
            } else {
                changes.add(change.withPreview(null, null, null, false));
            }
        }
        return new GitWorkspaceInfo(
                branch.isBlank() ? "unknown" : branch,
                commit.isBlank() ? "unknown" : commit,
                List.copyOf(changes));
    }

    private static void collectChanges(Path dir, Map<String, GitChange> changes, boolean stagedOnly) {
        List<String> command = diffCommand(stagedOnly);
        command.addAll(List.of("--name-status", "-z", "HEAD"));
        CommandOutput result = runCommand(dir, DEFAULT_GIT_TIMEOUT_MS, MAX_GIT_OUTPUT_BYTES,
                command.toArray(String[]::new));
        String[] fields = result.text().split("\0", -1);
        for (int index = 0; index + 1 < fields.length; ) {
            String status = fields[index++];
            String path = fields[index++];
            if (status.isEmpty() || path.isEmpty()) {
                break;
            }
            Kind kind = switch (status.charAt(0)) {
                case 'A' -> Kind.ADDED;
                case 'M' -> Kind.MODIFIED;
                case 'D' -> Kind.DELETED;
                case 'R' -> Kind.RENAMED;
                case 'C' -> Kind.COPIED;
                case 'T' -> Kind.TYPE_CHANGED;
                case 'U' -> Kind.CONFLICTED;
                default -> Kind.UNKNOWN;
            };
            String previousPath = null;
            if (kind == Kind.RENAMED || kind == Kind.COPIED) {
                if (index >= fields.length || fields[index].isEmpty()) {
                    break;
                }
                previousPath = path;
                path = fields[index++];
            }
            changes.putIfAbsent(path, new GitChange(path, kind, previousPath, stagedOnly));
        }
    }

    private static List<String> diffCommand(boolean stagedOnly) {
        List<String> command = new ArrayList<>(List.of("git", "--literal-pathspecs", "diff",
                "--find-renames", "--no-color", "--no-ext-diff", "--no-textconv", "--submodule=short"));
        if (stagedOnly) {
            command.add("--cached");
        }
        return command;
    }

    private static CommandOutput filePreview(Path dir, GitChange change, long timeoutMs, int maxBytes) {
        List<String> command = diffCommand(change.stagedOnly());
        command.addAll(List.of("--numstat", "--patch", "-z", "HEAD", "--"));
        if (change.previousPath() != null) {
            command.add(change.previousPath());
        }
        command.add(change.path());
        return runCommand(dir, timeoutMs, maxBytes, command.toArray(String[]::new));
    }

    private static WorkspaceChange parsePreview(GitChange change, CommandOutput preview) {
        String text = preview.text();
        // Git puts the NUL-delimited numstat record ahead of the patch.
        int patchStart = text.indexOf("\0diff --git ");
        int statsEnd = text.indexOf('\0');
        String[] counts = statsEnd < 0 ? new String[0] : text.substring(0, statsEnd).split("\t", 3);
        Integer additions = counts.length >= 2 ? parseCount(counts[0]) : null;
        Integer deletions = counts.length >= 2 ? parseCount(counts[1]) : null;
        String patch = patchStart < 0 ? null : text.substring(patchStart + 1);
        return change.withPreview(additions, deletions, patch, preview.truncated());
    }

    private static Integer parseCount(String value) {
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            return null; // Git uses '-' for binary files.
        }
    }

    static String runGitSingleLine(Path dir, String... args) {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "git";
        System.arraycopy(args, 0, cmd, 1, args.length);
        List<String> lines = runBoundedCommand(dir, DEFAULT_GIT_TIMEOUT_MS, cmd);
        return lines.isEmpty() ? "" : lines.getFirst();
    }

    static List<String> runBoundedCommand(Path dir, long timeoutMs, String... command) {
        return runCommand(dir, timeoutMs, MAX_GIT_OUTPUT_BYTES, command).text().lines()
                .map(String::trim).filter(line -> !line.isEmpty()).toList();
    }

    private static CommandOutput runCommand(Path dir, long timeoutMs, int maxBytes, String... command) {
        if (dir == null || command == null || command.length == 0) {
            return CommandOutput.EMPTY;
        }
        Process proc = null;
        try {
            proc = new ProcessBuilder(command)
                    .directory(dir.toFile())
                    .redirectError(ProcessBuilder.Redirect.DISCARD)
                    .start();
            Process started = proc;
            FutureTask<byte[]> capture = new FutureTask<>(() -> {
                try (var input = started.getInputStream()) {
                    byte[] bytes = input.readNBytes(maxBytes + 1);
                    input.transferTo(OutputStream.nullOutputStream());
                    return bytes;
                }
            });
            Thread.startVirtualThread(capture);

            if (!proc.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
                destroyProcessTree(proc);
                return CommandOutput.EMPTY;
            }
            if (proc.exitValue() != 0) {
                return CommandOutput.EMPTY;
            }
            byte[] bytes = capture.get(Math.max(1L, timeoutMs), TimeUnit.MILLISECONDS);
            int length = Math.min(bytes.length, maxBytes);
            return new CommandOutput(new String(bytes, 0, length, StandardCharsets.UTF_8),
                    length, bytes.length > maxBytes);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (proc != null) {
                destroyProcessTree(proc);
            }
            return CommandOutput.EMPTY;
        } catch (Exception e) {
            if (proc != null && proc.isAlive()) {
                destroyProcessTree(proc);
            }
            return CommandOutput.EMPTY;
        }
    }

    private static void destroyProcessTree(Process proc) {
        try {
            proc.descendants().forEach(ProcessHandle::destroyForcibly);
        } catch (Exception ignored) {
            // best-effort descendant termination
        }
        proc.destroyForcibly();
        try {
            proc.getInputStream().close();
        } catch (Exception ignored) {
            // unblock drain reader if open
        }
    }
}
