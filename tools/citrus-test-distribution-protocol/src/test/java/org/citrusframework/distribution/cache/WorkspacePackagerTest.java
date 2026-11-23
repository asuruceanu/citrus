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

package org.citrusframework.distribution.cache;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.citrusframework.distribution.cache.WorkspacePackager.MaterializedEntry;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkspacePackagerTest {

    @Test
    void shouldRejectTraversalAbsoluteWindowsAndSymlinkInputs(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path baseDir = Files.createDirectories(tempDir.resolve("module"));
        assertThatThrownBy(() -> WorkspacePackager.validateRelativePath("../secret.txt"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkspacePackager.validateRelativePath("/etc/passwd"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkspacePackager.validateRelativePath("C:\\temp\\a.txt"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkspacePackager.validateRelativePath("."))
                .isInstanceOf(IllegalArgumentException.class);

        Path outside = Files.writeString(tempDir.resolve("outside.txt"), "secret");
        Files.createSymbolicLink(baseDir.resolve("link.txt"), outside);
        assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, baseDir, "link.txt", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Symbolic link");
    }

    @Test
    void shouldRejectUnsafeRelativePaths() {
        List<String> unsafe = List.of("", " ", "\\\\server\\share\\a", "\\a", "a/../b", "a/./b", "a//b", "a/",
                "a\u0000b", "a\tb", "x:y", "file:/a", "CON", "con.txt", "src/NUL", "aux.tar.gz", "COM1", "lpt9.log",
                "..", "a/..");
        for (String raw : unsafe) {
            assertThatThrownBy(() -> WorkspacePackager.validateRelativePath(raw))
                    .as("'%s'", raw)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldNormalizeSafeRelativePaths() {
        assertThat(WorkspacePackager.validateRelativePath("src\\test\\resources")).isEqualTo("src/test/resources");
        assertThat(WorkspacePackager.validateRelativePath("console/com10/connection.xml"))
                .isEqualTo("console/com10/connection.xml");
        assertThat(WorkspacePackager.validateRelativePath("none")).isEqualTo("none");
        assertThatThrownBy(() -> WorkspacePackager.validateRelativePath(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFailOnMissingRequiredInputAndSkipMissingOptionalInput(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path baseDir = tempDir.resolve("module");
        assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, baseDir, "src/test/resources", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("src/test/resources");
        assertThat(WorkspacePackager.packageInputToCache(cache, baseDir, "src/test/resources", true)).isEmpty();
    }

    @Test
    void shouldRejectSymlinksInsideInputDirectoriesAndOnTheParentChain(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path baseDir = Files.createDirectories(tempDir.resolve("module"));
        Path outsideDir = Files.createDirectories(tempDir.resolve("outside"));
        Files.writeString(outsideDir.resolve("secret.txt"), "secret");

        Path resources = Files.createDirectories(baseDir.resolve("src/test/resources"));
        Files.writeString(resources.resolve("ok.txt"), "ok");
        Files.createSymbolicLink(resources.resolve("escape"), outsideDir);
        assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, baseDir, "src/test/resources", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Symbolic link");

        Files.createSymbolicLink(baseDir.resolve("linked"), outsideDir);
        assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, baseDir, "linked/secret.txt", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Symbolic link");
        assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, baseDir, "linked/missing.txt", true))
                .as("an optional path behind a symlink must still be rejected, not silently skipped")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Symbolic link");
        assertThat(tmpFiles(cache)).isEmpty();
    }

    @Test
    void shouldPackageDirectoryInputDeterministicallyAndRestoreEmptyDirectories(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path baseDir = Files.createDirectories(tempDir.resolve("module"));
        Path resources = Files.createDirectories(baseDir.resolve("src/test/resources/sub"));
        Files.createDirectories(baseDir.resolve("src/test/resources/empty"));
        Files.writeString(resources.resolve("a.xml"), "<a/>");
        Files.writeString(baseDir.resolve("src/test/resources/b.txt"), "b");

        WorkspaceFileSet first = WorkspacePackager.packageInputToCache(cache, baseDir, "src\\test\\resources", false).orElseThrow();
        WorkspaceFileSet second = WorkspacePackager.packageInputToCache(cache, baseDir, "src/test/resources", false).orElseThrow();
        assertThat(first.relativePath()).isEqualTo("src/test/resources");
        assertThat(first.artifact().kind()).isEqualTo(ArtifactKind.DIRECTORY_ZIP);
        assertThat(first.artifact().sha256()).isEqualTo(second.artifact().sha256());
        assertThat(first.artifact().sizeBytes()).isEqualTo(Files.size(cache.resolveBlobPath(first.artifact().sha256())));
        assertThat(tmpFiles(cache)).isEmpty();

        Path workspace = Files.createDirectories(tempDir.resolve("ws"));
        List<MaterializedEntry> entries = WorkspacePackager.materializeIntoDirectory(cache, first, workspace);
        assertThat(Files.readString(workspace.resolve("src/test/resources/sub/a.xml"))).isEqualTo("<a/>");
        assertThat(Files.readString(workspace.resolve("src/test/resources/b.txt"))).isEqualTo("b");
        assertThat(workspace.resolve("src/test/resources/empty")).isEmptyDirectory();
        assertThat(entries).extracting(MaterializedEntry::workspaceRelativePath)
                .containsExactly("src/test/resources/b.txt", "src/test/resources/sub/a.xml");
        assertThat(entries.getFirst().sha256()).isEqualTo(ClasspathPackager.sha256Hex("b".getBytes(StandardCharsets.UTF_8)));
        assertThat(entries.getFirst().extractedFile()).isEqualTo(workspace.resolve("src/test/resources/b.txt"));
    }

    @Test
    void shouldLeaveExcludedDirectoriesOutOfDirectoryInputs(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path baseDir = Files.createDirectories(tempDir.resolve("module"));
        Files.createDirectories(baseDir.resolve("target/generated"));
        Files.writeString(baseDir.resolve("target/generated/a.txt"), "a");
        Files.createDirectories(baseDir.resolve("target/.citrus-dist/cache/blobs"));
        Files.writeString(baseDir.resolve("target/.citrus-dist/cache/blobs/x"), "blob");

        WorkspaceFileSet target = WorkspacePackager.packageInputToCache(cache, baseDir, "target", false,
                List.of("target/.citrus-dist")).orElseThrow();
        Path workspace = Files.createDirectories(tempDir.resolve("ws"));
        assertThat(WorkspacePackager.materializeIntoDirectory(cache, target, workspace))
                .extracting(MaterializedEntry::workspaceRelativePath)
                .containsExactly("target/generated/a.txt");
        assertThat(workspace.resolve("target/.citrus-dist")).doesNotExist();
    }

    @Test
    void shouldPackageAndMaterializeSingleRegularFileInput(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path baseDir = Files.createDirectories(tempDir.resolve("module"));
        Files.writeString(baseDir.resolve("pom.xml"), "<project/>");

        WorkspaceFileSet pom = WorkspacePackager.packageInputToCache(cache, baseDir, "pom.xml", false).orElseThrow();
        assertThat(pom.relativePath()).isEqualTo("pom.xml");
        assertThat(pom.artifact().kind()).isEqualTo(ArtifactKind.REGULAR_FILE);
        assertThat(pom.artifact().sizeBytes()).isEqualTo(10L);
        assertThat(pom.artifact().sha256()).isEqualTo(ClasspathPackager.sha256Hex("<project/>".getBytes(StandardCharsets.UTF_8)));

        Path workspace = Files.createDirectories(tempDir.resolve("ws"));
        List<MaterializedEntry> entries = WorkspacePackager.materializeIntoDirectory(cache, pom, workspace);
        assertThat(Files.readString(workspace.resolve("pom.xml"))).isEqualTo("<project/>");
        assertThat(entries).containsExactly(new MaterializedEntry("pom.xml", pom.artifact().sha256(), workspace.resolve("pom.xml")));
    }

    @Test
    void shouldPackageOnlyNewOrModifiedOutputFilesAndIgnoreEmptyTargetDir(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path ws = Files.createDirectories(tempDir.resolve("ws"));
        Files.createDirectories(ws.resolve("target/docs"));
        Files.writeString(ws.resolve("target/docs/unchanged.txt"), "v1");
        Files.writeString(ws.resolve("target/docs/modified.txt"), "v1");

        Map<String, String> baseline = WorkspacePackager.snapshotBaselineHashes(ws, List.of("target/docs"));
        assertThat(WorkspacePackager.packageModifiedOutputToCache(cache, ws, "target/docs", baseline)).isEmpty();

        Files.writeString(ws.resolve("target/docs/modified.txt"), "v2");
        Files.writeString(ws.resolve("target/docs/created.txt"), "new");

        Optional<WorkspaceFileSet> out = WorkspacePackager.packageModifiedOutputToCache(cache, ws, "target/docs", baseline);
        assertThat(out).isPresent();

        Path dest = Files.createDirectories(tempDir.resolve("dest"));
        WorkspacePackager.materializeIntoDirectory(cache, out.orElseThrow(), dest);
        assertThat(dest.resolve("target/docs/unchanged.txt")).doesNotExist();
        assertThat(Files.readString(dest.resolve("target/docs/modified.txt"))).isEqualTo("v2");
        assertThat(Files.readString(dest.resolve("target/docs/created.txt"))).isEqualTo("new");
    }

    @Test
    void shouldHandleMissingEmptyAndSingleFileOutputs(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path ws = Files.createDirectories(tempDir.resolve("ws"));

        Map<String, String> baseline = WorkspacePackager.snapshotBaselineHashes(ws, List.of("target/docs", "target/report.xml"));
        assertThat(baseline).isEmpty();
        assertThat(WorkspacePackager.packageModifiedOutputToCache(cache, ws, "target/docs", baseline)).isEmpty();

        Files.createDirectories(ws.resolve("target/docs/nested"));
        assertThat(WorkspacePackager.packageModifiedOutputToCache(cache, ws, "target/docs", baseline))
                .as("an output directory with only empty sub-directories produces no artifact")
                .isEmpty();

        Files.writeString(ws.resolve("target/report.xml"), "<report/>");
        WorkspaceFileSet report = WorkspacePackager.packageModifiedOutputToCache(cache, ws, "target/report.xml", baseline).orElseThrow();
        assertThat(report.relativePath()).isEqualTo("target/report.xml");
        assertThat(report.artifact().kind()).isEqualTo(ArtifactKind.REGULAR_FILE);

        Map<String, String> after = WorkspacePackager.snapshotBaselineHashes(ws, List.of("target/report.xml"));
        assertThat(after).containsOnlyKeys("target/report.xml");
        assertThat(WorkspacePackager.packageModifiedOutputToCache(cache, ws, "target/report.xml", after)).isEmpty();
        assertThat(tmpFiles(cache)).isEmpty();
    }

    @Test
    void shouldRejectSymlinksInsideOutputDirectories(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path ws = Files.createDirectories(tempDir.resolve("ws"));
        Path docs = Files.createDirectories(ws.resolve("target/docs"));
        Files.writeString(docs.resolve("index.html"), "x");
        Files.createSymbolicLink(docs.resolve("passwd"), Files.writeString(tempDir.resolve("secret"), "s"));

        assertThatThrownBy(() -> WorkspacePackager.packageModifiedOutputToCache(cache, ws, "target/docs", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Symbolic link");
    }

    @Test
    void shouldNotWriteThroughSymlinksWhenMaterializing(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path baseDir = Files.createDirectories(tempDir.resolve("module"));
        Files.createDirectories(baseDir.resolve("target/docs"));
        Files.writeString(baseDir.resolve("target/docs/index.html"), "doc");
        WorkspaceFileSet docs = WorkspacePackager.packageInputToCache(cache, baseDir, "target/docs", false).orElseThrow();

        Path outside = Files.createDirectories(tempDir.resolve("outside"));
        Path workspace = Files.createDirectories(tempDir.resolve("ws"));
        Files.createSymbolicLink(workspace.resolve("target"), outside);

        assertThatThrownBy(() -> WorkspacePackager.materializeIntoDirectory(cache, docs, workspace))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Symbolic link");
        try (Stream<Path> written = Files.list(outside)) {
            assertThat(written).isEmpty();
        }
    }

    @Test
    void shouldFailMaterializingMissingOrCorruptedBlobsAndRejectJarKind(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path workspace = Files.createDirectories(tempDir.resolve("ws"));
        String sha = ClasspathPackager.sha256Hex("x".getBytes(StandardCharsets.UTF_8));

        WorkspaceFileSet missing = new WorkspaceFileSet("a.txt", new ArtifactRef("a.txt", sha, ArtifactKind.REGULAR_FILE, 1));
        assertThatThrownBy(() -> WorkspacePackager.materializeIntoDirectory(cache, missing, workspace))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(sha);

        Path blob = cache.put(sha, "x".getBytes(StandardCharsets.UTF_8));
        Files.writeString(blob, "y");
        assertThatThrownBy(() -> WorkspacePackager.materializeIntoDirectory(cache, missing, workspace))
                .isInstanceOf(IOException.class);
        assertThat(blob).doesNotExist();

        cache.put(sha, "x".getBytes(StandardCharsets.UTF_8));
        WorkspaceFileSet jar = new WorkspaceFileSet("lib/a.jar", new ArtifactRef("a.jar", sha, ArtifactKind.JAR, 1));
        assertThatThrownBy(() -> WorkspacePackager.materializeIntoDirectory(cache, jar, workspace))
                .isInstanceOf(IllegalArgumentException.class);

        WorkspaceFileSet traversal = new WorkspaceFileSet("../x", new ArtifactRef("x", sha, ArtifactKind.REGULAR_FILE, 1));
        assertThatThrownBy(() -> WorkspacePackager.materializeIntoDirectory(cache, traversal, workspace))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldEvictCorruptedCachedBlobAndEnforceZipEntryLimit(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(payload);
        Path blob = cache.put(sha, payload);
        assertThat(cache.getVerifiedPath(sha)).isEqualTo(blob);
        Files.writeString(blob, "tampered");
        assertThat(cache.getVerifiedPath(sha)).isNull();
        assertThat(blob).doesNotExist();
        assertThat(cache.getVerifiedPath(sha)).as("missing blob").isNull();

        Path zip = writeZip(tempDir.resolve("many.zip"), Map.of("a.txt", "a", "b.txt", "b", "c.txt", "c"));
        assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(zip, tempDir.resolve("x"), 2, 1_000, 1_000))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("entries");
    }

    @Test
    void shouldEnforceUncompressedSizeCapsWhileStreaming(@TempDir Path tempDir) throws Exception {
        String big = "0".repeat(10_000);
        Path zip = writeZip(tempDir.resolve("bomb.zip"), Map.of("a.txt", big, "b.txt", big));
        assertThat(Files.size(zip)).isLessThan(1_000L);

        assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(zip, tempDir.resolve("perEntry"), 10, 100_000, 5_000))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("a.txt");
        assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(zip, tempDir.resolve("total"), 10, 15_000, 10_000))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("total");

        List<BlobCacheDirectory.ExtractedFile> ok = BlobCacheDirectory.extractZipSafely(zip, tempDir.resolve("ok"), 10, 20_000, 10_000);
        assertThat(ok).extracting(BlobCacheDirectory.ExtractedFile::relativePath).containsExactly("a.txt", "b.txt");
    }

    @Test
    void shouldRejectZipSlipAndUnsafeEntryNames(@TempDir Path tempDir) throws Exception {
        for (String name : List.of("../evil.txt", "/abs.txt", "a/../../evil.txt", "C:/evil.txt", "dir/CON")) {
            Path zip = writeZip(tempDir.resolve("slip-" + Math.abs(name.hashCode()) + ".zip"), Map.of(name, "x"));
            Path target = tempDir.resolve("target-" + Math.abs(name.hashCode()));
            assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(zip, target))
                    .as(name)
                    .isInstanceOf(IOException.class);
        }
        assertThat(tempDir.resolve("evil.txt")).doesNotExist();
        assertThat(tempDir.getParent().resolve("evil.txt")).doesNotExist();
    }

    @Test
    void shouldRejectDirectoryEntriesCarryingPayload(@TempDir Path tempDir) throws Exception {
        Path zip = writeZip(tempDir.resolve("dirPayload.zip"), Map.of("d/", "0".repeat(10_000)));
        assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(zip, tempDir.resolve("x")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("d/");
    }

    @Test
    void shouldRejectDuplicateAndCaseCollidingEntries(@TempDir Path tempDir) throws Exception {
        Path duplicate = tempDir.resolve("dup.zip");
        try (OutputStream out = Files.newOutputStream(duplicate); ZipOutputStream zos = new ZipOutputStream(out)) {
            // ZipOutputStream refuses exact duplicates, so vary the name on the wire via a directory alias.
            zos.putNextEntry(new ZipEntry("a.txt"));
            zos.write("first".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("a.txt/"));
            zos.closeEntry();
        }
        assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(duplicate, tempDir.resolve("dup")))
                .isInstanceOf(IOException.class);

        Path caseCollision = writeZip(tempDir.resolve("case.zip"), Map.of("x.txt", "first", "X.txt", "second"));
        Path target = tempDir.resolve("case");
        boolean caseInsensitiveFs = Files.exists(Files.createDirectories(tempDir.resolve("probe")).resolve("..").resolve("PROBE"));
        if (caseInsensitiveFs) {
            assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(caseCollision, target))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("x.txt");
        } else {
            assertThat(BlobCacheDirectory.extractZipSafely(caseCollision, target)).hasSize(2);
        }
    }

    @Test
    void shouldRejectExactDuplicateEntryNames(@TempDir Path tempDir) throws Exception {
        Path zip = writeZip(tempDir.resolve("exact.zip"), Map.of("a.txt", "first", "b.txt", "second"));
        // ZipOutputStream refuses duplicate names, so rename "b.txt" to "a.txt" in the raw bytes
        // (local header and central directory both carry the name).
        byte[] bytes = Files.readAllBytes(zip);
        byte[] from = "b.txt".getBytes(StandardCharsets.UTF_8);
        byte[] to = "a.txt".getBytes(StandardCharsets.UTF_8);
        int replaced = 0;
        for (int i = 0; i + from.length <= bytes.length; i++) {
            if (Arrays.equals(bytes, i, i + from.length, from, 0, from.length)) {
                System.arraycopy(to, 0, bytes, i, to.length);
                replaced++;
            }
        }
        assertThat(replaced).isEqualTo(2);
        Files.write(zip, bytes);

        assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(zip, tempDir.resolve("x")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Duplicate");
        assertThat(Files.readString(tempDir.resolve("x/a.txt"))).isEqualTo("first");
    }

    @Test
    void shouldDropTmpSourceWhenBlobIsAlreadyStored(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        byte[] payload = "same".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(payload);
        Path stored = cache.put(sha, payload);

        Path tmp = Files.write(cache.newTempFile("ws-"), payload);
        assertThat(cache.putFromFile(sha, tmp)).isEqualTo(stored);
        assertThat(tmp).doesNotExist();
        assertThat(tmpFiles(cache)).isEmpty();
    }

    @Test
    void shouldRejectFileAndDirectoryNameConflicts(@TempDir Path tempDir) throws Exception {
        Path zip = writeZip(tempDir.resolve("conflict.zip"), Map.of("a", "file", "a/b.txt", "nested"));
        assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(zip, tempDir.resolve("x")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void shouldNotFollowSymlinkPlantedBelowExtractionDestination(@TempDir Path tempDir) throws Exception {
        Path outside = Files.createDirectories(tempDir.resolve("outside"));
        Path target = Files.createDirectories(tempDir.resolve("target"));
        Files.createSymbolicLink(target.resolve("sub"), outside);
        Path zip = writeZip(tempDir.resolve("pivot.zip"), Map.of("sub/evil.txt", "x"));

        assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(zip, target))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Symbolic link");
        assertThat(outside.resolve("evil.txt")).doesNotExist();
    }

    @Test
    void shouldReportMalformedArchivesAsIOException(@TempDir Path tempDir) throws Exception {
        Path badName = tempDir.resolve("badName.zip");
        try (OutputStream out = Files.newOutputStream(badName);
             ZipOutputStream zos = new ZipOutputStream(out, java.nio.charset.StandardCharsets.ISO_8859_1)) {
            zos.putNextEntry(new ZipEntry("caf\u00e9.txt"));
            zos.write('x');
            zos.closeEntry();
        }
        // The entry name bytes are Latin-1 and not valid UTF-8; JDK 21 throws IllegalArgumentException here.
        assertThatThrownBy(() -> BlobCacheDirectory.extractZipSafely(badName, tempDir.resolve("x")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void shouldRejectNamesAtPackagingTimeThatExtractionWouldRefuse(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path baseDir = Files.createDirectories(tempDir.resolve("module"));
        Path resources = Files.createDirectories(baseDir.resolve("res"));
        Files.writeString(resources.resolve("report-12:30.html"), "x");
        assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, baseDir, "res", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("report-12:30.html");

        Files.delete(resources.resolve("report-12:30.html"));
        Files.writeString(resources.resolve("aux.json"), "x");
        assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, baseDir, "res", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("aux.json");

        Files.delete(resources.resolve("aux.json"));
        Files.writeString(resources.resolve("a\\b.txt"), "x");
        assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, baseDir, "res", false))
                .as("a POSIX file name containing a backslash would be re-read as a separator")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a\\b.txt");
        assertThat(tmpFiles(cache)).isEmpty();
    }

    @Test
    void shouldSkipSpecialFilesWhenPackagingOutputsButRejectThemAsInputs(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path ws = Files.createDirectories(tempDir.resolve("ws"));
        Path docs = Files.createDirectories(ws.resolve("target/docs"));
        Files.writeString(docs.resolve("index.html"), "x");
        Process mkfifo = new ProcessBuilder("mkfifo", docs.resolve("pipe").toString()).start();
        org.junit.jupiter.api.Assumptions.assumeTrue(mkfifo.waitFor() == 0, "mkfifo not available");

        WorkspaceFileSet out = WorkspacePackager.packageModifiedOutputToCache(cache, ws, "target/docs", Map.of()).orElseThrow();
        Path dest = Files.createDirectories(tempDir.resolve("dest"));
        assertThat(WorkspacePackager.materializeIntoDirectory(cache, out, dest))
                .extracting(MaterializedEntry::workspaceRelativePath)
                .containsExactly("target/docs/index.html");

        assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, ws, "target/docs", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pipe");
    }

    @Test
    void shouldVerifyClasspathBlobsBeforeExtracting(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path classes = Files.createDirectories(tempDir.resolve("classes"));
        Files.writeString(classes.resolve("A.class"), "a");
        ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(classes);
        Path blob = cache.put(packaged.ref().sha256(), packaged.bytes());
        Files.writeString(blob, "garbage");

        assertThatThrownBy(() -> cache.materializeForClasspath(packaged.ref(), tempDir.resolve("ws")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(packaged.ref().sha256());
        assertThat(blob).as("corrupted blob is evicted").doesNotExist();
    }

    @Test
    void shouldEnforceMaxBlobBytesOnPutFromFile(@TempDir Path tempDir) throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path sparse = tempDir.resolve("huge.bin");
        try (java.io.RandomAccessFile file = new java.io.RandomAccessFile(sparse.toFile(), "rw")) {
            file.setLength(ClasspathPackager.MAX_BLOB_BYTES + 1);
        }
        assertThatThrownBy(() -> cache.putFromFile("0".repeat(64), sparse))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(ClasspathPackager.MAX_BLOB_BYTES));
    }

    @Test
    void shouldMaterializeFromAnAlreadyVerifiedBlobWithoutRehashingIt(@TempDir Path tempDir) throws Exception {
        HashCountingCache cache = new HashCountingCache(tempDir.resolve("cache"));
        Path baseDir = Files.createDirectories(tempDir.resolve("module/src/test/resources")).getParent().getParent().getParent();
        Files.writeString(baseDir.resolve("src/test/resources/greeting.txt"), "hello");
        WorkspaceFileSet input = WorkspacePackager.packageInputToCache(cache, baseDir, "src/test/resources", false).orElseThrow();
        Path inputBlob = cache.resolveBlobPath(input.artifact().sha256());
        Path classes = Files.createDirectories(tempDir.resolve("classes"));
        Files.writeString(classes.resolve("A.class"), "a");
        ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(classes);
        Path classesBlob = cache.put(packaged.ref().sha256(), packaged.bytes());
        cache.hashes.set(0);

        Path workspace = Files.createDirectories(tempDir.resolve("ws"));
        WorkspacePackager.materializeIntoDirectory(cache, input, inputBlob, workspace);
        Path extracted = cache.materializeForClasspath(packaged.ref(), classesBlob, tempDir.resolve("partition"));

        assertThat(workspace.resolve("src/test/resources/greeting.txt")).hasContent("hello");
        assertThat(extracted.resolve("A.class")).hasContent("a");
        assertThat(cache.hashes).as("a verified blob is not hashed again").hasValue(0);

        WorkspacePackager.materializeIntoDirectory(cache, input, Files.createDirectories(tempDir.resolve("ws-2")));
        cache.materializeForClasspath(packaged.ref(), tempDir.resolve("partition-2"));
        assertThat(cache.hashes).as("the overloads without a verified blob still verify").hasValue(2);

        assertThatThrownBy(() -> WorkspacePackager.materializeIntoDirectory(cache, input, classesBlob,
                Files.createDirectories(tempDir.resolve("ws-3"))))
                .as("the verified blob must be the cache's blob of the artifact")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.materializeForClasspath(packaged.ref(), inputBlob, tempDir.resolve("partition-3")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Counts full content verifications. */
    static final class HashCountingCache extends BlobCacheDirectory {
        final java.util.concurrent.atomic.AtomicInteger hashes = new java.util.concurrent.atomic.AtomicInteger();

        HashCountingCache(Path rootDir) {
            super(rootDir);
        }

        @Override
        public boolean isIntact(String sha256) throws IOException {
            hashes.incrementAndGet();
            return super.isIntact(sha256);
        }
    }

    private static Path writeZip(Path zip, Map<String, String> entries) throws IOException {
        try (OutputStream out = Files.newOutputStream(zip); ZipOutputStream zos = new ZipOutputStream(out)) {
            for (String name : entries.keySet().stream().sorted().toList()) {
                zos.putNextEntry(new ZipEntry(name));
                zos.write(entries.get(name).getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        return zip;
    }

    private static List<Path> tmpFiles(BlobCacheDirectory cache) throws IOException {
        Path tmp = cache.getRootDir().resolve("tmp");
        if (!Files.isDirectory(tmp)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(tmp)) {
            return files.toList();
        }
    }
}
