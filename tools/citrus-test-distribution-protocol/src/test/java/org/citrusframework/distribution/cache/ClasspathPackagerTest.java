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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClasspathPackagerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldProduceDeterministicSha256ForIdenticalDirectories() throws Exception {
        Path dirA = Files.createDirectories(tempDir.resolve("dirA/org/example"));
        Files.writeString(dirA.resolve("Sample.txt"), "hello-citrus", StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("dirA/root.properties"), "k=v", StandardCharsets.UTF_8);

        Path dirB = Files.createDirectories(tempDir.resolve("dirB/org/example"));
        Files.writeString(tempDir.resolve("dirB/root.properties"), "k=v", StandardCharsets.UTF_8);
        Files.writeString(dirB.resolve("Sample.txt"), "hello-citrus", StandardCharsets.UTF_8);

        ClasspathPackager.PackagedArtifact artifactA = ClasspathPackager.packagePath(tempDir.resolve("dirA"));
        ClasspathPackager.PackagedArtifact artifactB = ClasspathPackager.packagePath(tempDir.resolve("dirB"));

        assertThat(artifactA.ref().kind()).isEqualTo(ArtifactKind.DIRECTORY_ZIP);
        assertThat(artifactA.ref().sha256()).isEqualTo(artifactB.ref().sha256());
        assertThat(artifactA.bytes()).isEqualTo(artifactB.bytes());
    }

    @Test
    void shouldStoreVerifyAndMaterializeBlobsWhileRejectingInvalidDigests() throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));

        assertThatThrownBy(() -> cache.resolveBlobPath("../../etc/passwd"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.resolveBlobPath("not-a-sha256"))
                .isInstanceOf(IllegalArgumentException.class);

        Path sourceDir = Files.createDirectories(tempDir.resolve("classes/demo"));
        Files.writeString(sourceDir.resolve("test.yaml"), "name: demo-test", StandardCharsets.UTF_8);
        ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(tempDir.resolve("classes"));

        String wrongDigest = "0".repeat(64);
        assertThatThrownBy(() -> cache.put(wrongDigest, packaged.bytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256 mismatch");

        cache.put(packaged.ref().sha256(), packaged.bytes());
        assertThat(cache.contains(packaged.ref().sha256())).isTrue();

        Path workspaceEntry = cache.materializeForClasspath(packaged.ref(), tempDir.resolve("workspace"));
        assertThat(Files.isDirectory(workspaceEntry)).isTrue();
        assertThat(Files.readString(workspaceEntry.resolve("demo/test.yaml"), StandardCharsets.UTF_8))
                .isEqualTo("name: demo-test");
    }

    @Test
    void shouldStreamDeterministicZipToFileWithSortedDirectoryEntriesAndNoExtraFields() throws Exception {
        Path dirA = Files.createDirectories(tempDir.resolve("a/sub"));
        Files.createDirectories(tempDir.resolve("a/empty"));
        Files.writeString(dirA.resolve("z.txt"), "z", StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("a/b.txt"), "b", StandardCharsets.UTF_8);

        Path dirB = Files.createDirectories(tempDir.resolve("b"));
        Files.writeString(dirB.resolve("b.txt"), "b", StandardCharsets.UTF_8);
        Files.createDirectories(dirB.resolve("empty"));
        Files.writeString(Files.createDirectories(dirB.resolve("sub")).resolve("z.txt"), "z", StandardCharsets.UTF_8);

        Path zipA = tempDir.resolve("a.zip");
        Path zipB = tempDir.resolve("b.zip");
        String shaA = ClasspathPackager.createDeterministicZipToFile(tempDir.resolve("a"), zipA);
        String shaB = ClasspathPackager.createDeterministicZipToFile(dirB, zipB);

        assertThat(shaA).isEqualTo(shaB).isEqualTo(ClasspathPackager.sha256Hex(zipA));
        assertThat(Files.readAllBytes(zipA)).isEqualTo(Files.readAllBytes(zipB));

        List<String> names = new ArrayList<>();
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipA))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                names.add(entry.getName());
                assertThat(entry.getExtra()).as(entry.getName()).isNull();
                assertThat(entry.getComment()).isNull();
                assertThat(entry.getTimeLocal()).isEqualTo(LocalDateTime.of(1980, 2, 1, 0, 0));
            }
        }
        assertThat(names).containsExactly("b.txt", "empty/", "sub/", "sub/z.txt");
    }

    @Test
    void shouldRejectSymlinksAndOversizedArchivesWithoutLeavingPartialFiles() throws Exception {
        Path dir = Files.createDirectories(tempDir.resolve("withLink"));
        Files.writeString(dir.resolve("ok.txt"), "ok", StandardCharsets.UTF_8);
        Files.createSymbolicLink(dir.resolve("link.txt"), Files.writeString(tempDir.resolve("secret"), "s"));
        Path zip = tempDir.resolve("link.zip");
        assertThatThrownBy(() -> ClasspathPackager.createDeterministicZipToFile(dir, zip))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Symbolic link");
        assertThat(zip).doesNotExist();

        Path big = Files.createDirectories(tempDir.resolve("big"));
        byte[] random = new byte[64 * 1024];
        new Random(42).nextBytes(random);
        Files.write(big.resolve("random.bin"), random);
        Path bigZip = tempDir.resolve("big.zip");
        assertThatThrownBy(() -> ClasspathPackager.createDeterministicZipToFile(big, bigZip, 1_024L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1024");
        assertThat(bigZip).doesNotExist();

        Path file = Files.writeString(tempDir.resolve("plain.txt"), "x");
        assertThatThrownBy(() -> ClasspathPackager.createDeterministicZipToFile(file, tempDir.resolve("plain.zip")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a directory");
    }

    @Test
    void shouldStoreFromFileVerifyDigestAndRejectRegularFileOnClasspath() throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path source = Files.writeString(tempDir.resolve("source.txt"), "content", StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(source);

        assertThatThrownBy(() -> cache.putFromFile("0".repeat(64), source))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256 mismatch");
        Path stored = cache.putFromFile(sha, source);
        assertThat(stored).isEqualTo(cache.resolveBlobPath(sha));
        assertThat(Files.readString(stored)).isEqualTo("content");
        assertThat(source).as("caller keeps ownership of a source outside the cache").exists();
        assertThat(cache.putFromFile(sha, source)).isEqualTo(stored);
        try (var tmp = Files.list(cache.getRootDir().resolve("tmp"))) {
            assertThat(tmp).isEmpty();
        }

        ArtifactRef regular = new ArtifactRef("source.txt", sha, ArtifactKind.REGULAR_FILE, 7L);
        assertThatThrownBy(() -> cache.materializeForClasspath(regular, tempDir.resolve("workspace")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("REGULAR_FILE");
    }
}
