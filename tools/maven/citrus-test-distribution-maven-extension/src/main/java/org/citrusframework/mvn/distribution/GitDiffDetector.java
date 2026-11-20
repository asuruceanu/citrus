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

    private static String runGitSingleLine(Path dir, String... args) {
        List<String> lines = runGitLines(dir, args);
        return lines.isEmpty() ? "" : lines.get(0).trim();
    }

    private static List<String> runGitLines(Path dir, String... args) {
        List<String> cmd = new ArrayList<>(args.length + 1);
        cmd.add("git");
        cmd.addAll(List.of(args));
        try {
            Process proc = new ProcessBuilder(cmd)
                    .directory(dir.toFile())
                    .redirectErrorStream(true)
                    .start();
            List<String> lines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty()) {
                        lines.add(trimmed);
                    }
                }
            }
            if (!proc.waitFor(5, TimeUnit.SECONDS) || proc.exitValue() != 0) {
                return List.of();
            }
            return lines;
        } catch (Exception e) {
            return List.of();
        }
    }
}
