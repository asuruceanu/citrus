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

package org.citrusframework.distribution.junit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HostOutputCommitterTest {

    @TempDir
    Path tempDir;

    private Path host;
    private BlobCacheDirectory cache;

    @BeforeEach
    void setUp() throws IOException {
        host = Files.createDirectories(tempDir.resolve("module"));
        cache = new BlobCacheDirectory(tempDir.resolve("cache"));
    }

    @Test
    void shouldAllowIdenticalSharedOutputsAndRejectConflictingPartitionOutputsWithoutTouchingHost() throws Exception {
        write(host, "target/docs/existing.html", "old");
        HostOutputCommitter committer = committer();
        committer.recordPartitionOutputs("p1", outputs("p1", Map.of("target/docs/logo.png", "PNG", "target/docs/a.html", "A")), cache);
        committer.recordPartitionOutputs("p2", outputs("p2", Map.of("target/docs/logo.png", "PNG", "target/docs/a.html", "CONFLICT_B")), cache);

        assertThat(committer.conflictError()).hasValueSatisfying(error -> assertThat(error)
                .contains("target/docs/a.html").contains("p1").contains("p2").doesNotContain("logo.png"));
        assertThatThrownBy(committer::commitOrRollback).isInstanceOf(IllegalStateException.class);
        committer.discardStaging();

        assertThat(host.resolve("target/docs/existing.html")).hasContent("old");
        assertThat(host.resolve("target/docs/logo.png")).doesNotExist();
        assertThat(host.resolve("target/docs/a.html")).doesNotExist();
        assertNoTransactionLeft();
    }

    @Test
    void shouldCommitNewAndReplacedFilesOnceForIdenticalOutputs() throws Exception {
        write(host, "target/docs/index.html", "old index");
        HostOutputCommitter committer = committer();
        committer.recordPartitionOutputs("p1", outputs("p1", Map.of("target/docs/index.html", "new index", "target/docs/shared.css", "css")), cache);
        committer.recordPartitionOutputs("p2", outputs("p2", Map.of("target/docs/shared.css", "css", "target/docs/sub/page.html", "page")), cache);
        committer.recordPartitionOutputs("p3", List.of(), cache);

        assertThat(committer.conflictError()).isEmpty();
        committer.commitOrRollback();

        assertThat(host.resolve("target/docs/index.html")).hasContent("new index");
        assertThat(host.resolve("target/docs/shared.css")).hasContent("css");
        assertThat(host.resolve("target/docs/sub/page.html")).hasContent("page");
        assertNoTransactionLeft();
        try (Stream<Path> root = Files.list(host)) {
            assertThat(root.map(p -> p.getFileName().toString())).as("nothing written into the module root").containsExactly("target");
        }
    }

    @Test
    void shouldRestorePreExistingHostFilesWhenMidCommitWriteFails() throws Exception {
        write(host, "target/docs/first.html", "original");
        HostOutputCommitter committer = committer();
        committer.recordPartitionOutputs("p1", outputs("p1", Map.of(
                "target/docs/first.html", "updated",
                "target/docs/sub/second.html", "new",
                "target/docs/third/deep/file.txt", "deep")), cache);
        committer.failBeforeMove(dest -> dest.endsWith("third/deep/file.txt"));

        assertThatThrownBy(committer::commitOrRollback).isInstanceOf(IOException.class);

        assertThat(host.resolve("target/docs/first.html")).hasContent("original");
        assertThat(host.resolve("target/docs/sub")).as("created directory removed").doesNotExist();
        assertThat(host.resolve("target/docs/third")).doesNotExist();
        assertNoTransactionLeft();
    }

    @Test
    void shouldValidateEveryDestinationBeforeWritingAnything() throws Exception {
        write(host, "target/docs/first.html", "original");
        write(host, "target/docs/sub", "a file where a directory is needed");
        HostOutputCommitter committer = committer();
        committer.recordPartitionOutputs("p1", outputs("p1", Map.of(
                "target/docs/first.html", "updated",
                "target/docs/sub/second.html", "new")), cache);

        assertThatThrownBy(committer::commitOrRollback).isInstanceOf(IOException.class).hasMessageContaining("sub");
        assertThat(host.resolve("target/docs/first.html")).hasContent("original");
        assertThat(host.resolve("target/docs/sub")).hasContent("a file where a directory is needed");
        assertNoTransactionLeft();
    }

    @Test
    void shouldRefuseToWriteThroughSymlinksInTheModule() throws Exception {
        Path outside = Files.createDirectories(tempDir.resolve("outside"));
        Files.createDirectories(host.resolve("target"));
        Files.createSymbolicLink(host.resolve("target/docs"), outside);
        HostOutputCommitter committer = committer();
        committer.recordPartitionOutputs("p1", outputs("p1", Map.of("target/docs/index.html", "x")), cache);

        assertThatThrownBy(committer::commitOrRollback).isInstanceOf(IOException.class).hasMessageContaining("Symbolic link");
        try (Stream<Path> written = Files.list(outside)) {
            assertThat(written).isEmpty();
        }
        assertNoTransactionLeft();
    }

    @Test
    void shouldTreatOutputsDifferingOnlyInCaseAsConflict() throws Exception {
        write(host, "target/docs/a.html", "original");
        HostOutputCommitter committer = committer();
        committer.recordPartitionOutputs("p1", outputs("p1", Map.of("target/docs/A.html", "from-p1")), cache);
        committer.recordPartitionOutputs("p2", outputs("p2", Map.of("target/docs/a.html", "from-p2")), cache);

        assertThat(committer.conflictError()).hasValueSatisfying(error -> assertThat(error)
                .contains("target/docs/A.html").contains("target/docs/a.html"));
        assertThatThrownBy(committer::commitOrRollback).isInstanceOf(IllegalStateException.class);
        assertThat(Files.readString(host.resolve("target/docs/a.html"))).isEqualTo("original");
    }

    @Test
    void shouldRestoreReplacedFilesWhoseBackupNamesCollideOnCaseInsensitiveDisks() throws Exception {
        write(host, "target/docs/a.html", "original-a");
        write(host, "target/docs/sub/b.html", "original-b");
        HostOutputCommitter committer = committer();
        committer.recordPartitionOutputs("p1", outputs("p1", Map.of(
                "target/docs/a.html", "new-a",
                "target/docs/sub/b.html", "new-b",
                "target/docs/zz/fail.txt", "x")), cache);
        committer.failBeforeMove(dest -> dest.endsWith("zz/fail.txt"));

        assertThatThrownBy(committer::commitOrRollback).isInstanceOf(IOException.class)
                .satisfies(e -> assertThat(e.getSuppressed()).as("rollback succeeded").isEmpty());
        assertThat(host.resolve("target/docs/a.html")).hasContent("original-a");
        assertThat(host.resolve("target/docs/sub/b.html")).hasContent("original-b");
    }

    @Test
    void shouldRejectOutputsOutsideTheDeclaredPathsAndInternalDirectories() throws Exception {
        Path workspace = Files.createDirectories(tempDir.resolve("ws-rogue"));
        write(workspace, "pom.xml", "<project>evil</project>");
        WorkspaceFileSet rogue = WorkspacePackager.packageModifiedOutputToCache(cache, workspace, "pom.xml", Map.of()).orElseThrow();
        write(workspace, "target/.citrus-dist-outputs/commit.lock", "x");
        WorkspaceFileSet internal = WorkspacePackager.packageModifiedOutputToCache(
                cache, workspace, "target/.citrus-dist-outputs", Map.of()).orElseThrow();

        HostOutputCommitter committer = new HostOutputCommitter(host, List.of("target/docs", "target"));
        assertThatThrownBy(() -> committer.recordPartitionOutputs("p1", List.of(rogue), cache))
                .isInstanceOf(IOException.class).hasMessageContaining("pom.xml");
        assertThatThrownBy(() -> committer.recordPartitionOutputs("p1", List.of(internal), cache))
                .isInstanceOf(IOException.class).hasMessageContaining(".citrus-dist-outputs");

        Path upper = Files.createDirectories(tempDir.resolve("ws-upper"));
        write(upper, "target/.CITRUS-DIST-OUTPUTS/commit.lock", "x");
        WorkspaceFileSet internalUpper = WorkspacePackager.packageModifiedOutputToCache(
                cache, upper, "target/.CITRUS-DIST-OUTPUTS", Map.of()).orElseThrow();
        assertThatThrownBy(() -> committer.recordPartitionOutputs("p1", List.of(internalUpper), cache))
                .as("internal directories are protected whatever the letter case")
                .isInstanceOf(IOException.class).hasMessageContaining("internal directory");

        HostOutputCommitter undeclared = new HostOutputCommitter(host, List.of());
        assertThatThrownBy(() -> undeclared.recordPartitionOutputs("p1", outputs("p1", Map.of("target/docs/x.html", "x")), cache))
                .isInstanceOf(IOException.class).hasMessageContaining("target/docs");
        assertThat(host.resolve("pom.xml")).doesNotExist();
    }

    @Test
    void shouldKeepPermissionsOfReplacedFiles() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                java.nio.file.FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        Path script = write(host, "target/docs/run.sh", "old");
        Files.setPosixFilePermissions(script, java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-x---"));
        HostOutputCommitter committer = committer();
        committer.recordPartitionOutputs("p1", outputs("p1", Map.of("target/docs/run.sh", "new")), cache);
        committer.commitOrRollback();

        assertThat(script).hasContent("new");
        assertThat(java.nio.file.attribute.PosixFilePermissions.toString(Files.getPosixFilePermissions(script))).isEqualTo("rwxr-x---");
    }

    @Test
    void shouldCommitSingleRegularFileOutputs() throws Exception {
        Path workspace = Files.createDirectories(tempDir.resolve("ws-file"));
        write(workspace, "target/report.xml", "<report/>");
        WorkspaceFileSet single = WorkspacePackager.packageModifiedOutputToCache(cache, workspace, "target/report.xml", Map.of()).orElseThrow();

        HostOutputCommitter committer = committer();
        committer.recordPartitionOutputs("p1", List.of(single), cache);
        committer.commitOrRollback();

        assertThat(host.resolve("target/report.xml")).hasContent("<report/>");
    }

    private HostOutputCommitter committer() {
        return new HostOutputCommitter(host, List.of("target/docs", "target/report.xml"));
    }

    private List<WorkspaceFileSet> outputs(String partition, Map<String, String> files) throws IOException {
        Path workspace = Files.createDirectories(tempDir.resolve("ws-" + partition + "-" + System.nanoTime()));
        for (Map.Entry<String, String> file : files.entrySet()) {
            write(workspace, file.getKey(), file.getValue());
        }
        return WorkspacePackager.packageModifiedOutputToCache(cache, workspace, "target/docs", Map.of()).stream().toList();
    }

    private static Path write(Path root, String relative, String content) throws IOException {
        Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        return Files.writeString(file, content);
    }

    private void assertNoTransactionLeft() throws IOException {
        Path outputs = host.resolve("target/.citrus-dist-outputs");
        if (Files.isDirectory(outputs)) {
            try (Stream<Path> entries = Files.list(outputs)) {
                assertThat(entries.map(p -> p.getFileName().toString())).noneMatch(name -> name.startsWith("tx-"));
            }
        }
    }
}
