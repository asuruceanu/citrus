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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.citrusframework.distribution.model.WorkspaceChange;
import org.citrusframework.distribution.model.WorkspaceChange.Kind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class GitDiffDetectorTest {

    @TempDir
    Path repo;

    @Test
    void capturesPatchAndFileStatusRelativeToHead() throws Exception {
        git("init", "-q");
        git("config", "user.name", "Citrus Test");
        git("config", "user.email", "citrus@example.test");
        git("config", "commit.gpgsign", "false");
        Path module = Files.createDirectory(repo.resolve("module"));
        Files.writeString(module.resolve("edit.txt"), "before\n");
        Files.writeString(repo.resolve("old name.txt"), "same content\n");
        Files.writeString(repo.resolve("delete.txt"), "remove me\n");
        git("add", ".");
        git("commit", "-qm", "Baseline");
        Files.writeString(module.resolve("edit.txt"), "after\n");
        Files.delete(repo.resolve("delete.txt"));
        Files.writeString(repo.resolve("added.txt"), "new content\n");
        git("add", "added.txt");
        git("mv", "old name.txt", "new name.txt");

        List<WorkspaceChange> changes = GitDiffDetector.inspect(module).workspaceChanges();
        assertThat(changes).extracting(WorkspaceChange::kind)
                .containsExactlyInAnyOrder(Kind.ADDED, Kind.DELETED, Kind.MODIFIED, Kind.RENAMED);
        assertThat(changes).filteredOn(change -> change.kind() == Kind.MODIFIED).singleElement().satisfies(change -> {
            assertThat(change.path()).isEqualTo("module/edit.txt");
            assertThat(change.additions()).isEqualTo(1);
            assertThat(change.deletions()).isEqualTo(1);
            assertThat(change.patch()).contains("-before", "+after");
        });
        assertThat(changes).filteredOn(change -> change.kind() == Kind.RENAMED).singleElement().satisfies(change -> {
            assertThat(change.path()).isEqualTo("new name.txt");
            assertThat(change.previousPath()).isEqualTo("old name.txt");
            assertThat(change.patch()).contains("rename from old name.txt", "rename to new name.txt");
        });
    }

    private void git(String... arguments) throws Exception {
        var command = new java.util.ArrayList<>(List.of("git"));
        command.addAll(List.of(arguments));
        Process process = new ProcessBuilder(command).directory(repo.toFile()).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(process.waitFor()).as(output).isZero();
    }
}
