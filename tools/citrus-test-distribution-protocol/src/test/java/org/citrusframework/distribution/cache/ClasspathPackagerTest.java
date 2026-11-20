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

import org.citrusframework.distribution.model.ArtifactKind;
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
}
