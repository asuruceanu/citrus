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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Inspects workspace git metadata (branch, HEAD SHA, changed paths) for session recording.
 */
public final class GitDiffDetector {

    static final long DEFAULT_GIT_TIMEOUT_MS = 5_000L;

    private GitDiffDetector() {
    }

    public record GitWorkspaceInfo(String branch, String commit, List<String> changedFiles) {
    }

    public static GitWorkspaceInfo inspect(Path baseDir) {
        if (baseDir == null) {
            return new GitWorkspaceInfo("unknown", "unknown", List.of());
        }
        String branch = runGitSingleLine(baseDir, "rev-parse", "--abbrev-ref", "HEAD");
        String commit = runGitSingleLine(baseDir, "rev-parse", "--short=12", "HEAD");
        Set<String> changed = new LinkedHashSet<>();
        changed.addAll(runGitLines(baseDir, "diff", "--name-only", "HEAD"));
        changed.addAll(runGitLines(baseDir, "diff", "--name-only", "--cached"));
        return new GitWorkspaceInfo(
                branch.isBlank() ? "unknown" : branch,
                commit.isBlank() ? "unknown" : commit,
                List.copyOf(changed));
    }

    static String runGitSingleLine(Path dir, String... args) {
        List<String> lines = runGitLines(dir, args);
        return lines.isEmpty() ? "" : lines.get(0).trim();
    }

    private static List<String> runGitLines(Path dir, String... args) {
        if (dir == null || args == null || args.length == 0) {
            return List.of();
        }
        String[] cmd;
        if ("git".equals(args[0])) {
            cmd = args;
        } else {
            cmd = new String[args.length + 1];
            cmd[0] = "git";
            System.arraycopy(args, 0, cmd, 1, args.length);
        }
        return runBoundedCommand(dir, DEFAULT_GIT_TIMEOUT_MS, cmd);
    }

    static List<String> runBoundedCommand(Path dir, long timeoutMs, String... command) {
        if (dir == null || command == null || command.length == 0) {
            return List.of();
        }
        Process proc = null;
        try {
            proc = new ProcessBuilder(command)
                    .directory(dir.toFile())
                    .redirectErrorStream(true)
                    .start();
            Process started = proc;
            List<String> collected = new ArrayList<>();
            Thread drainThread = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(started.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String trimmed = line.trim();
                        if (!trimmed.isEmpty()) {
                            collected.add(trimmed);
                        }
                    }
                } catch (Exception ignored) {
                    // stream closed or process terminated
                }
            }, "citrus-git-stdout-drain");
            drainThread.setDaemon(true);
            drainThread.start();

            if (!proc.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
                destroyProcessTree(proc);
                return List.of();
            }
            if (proc.exitValue() != 0) {
                return List.of();
            }
            drainThread.join(1_000L);
            return List.copyOf(collected);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            if (proc != null) {
                destroyProcessTree(proc);
            }
            return List.of();
        } catch (Exception e) {
            if (proc != null && proc.isAlive()) {
                destroyProcessTree(proc);
            }
            return List.of();
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
