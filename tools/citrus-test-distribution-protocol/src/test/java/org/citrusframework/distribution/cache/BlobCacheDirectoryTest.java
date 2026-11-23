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

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class BlobCacheDirectoryTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldPutJarsOnTheClasspathUnderTheirOriginalJarFileName() throws Exception {
        // Libraries look at classpath file names: Testcontainers' MountableFile only extracts a resource from an
        // archive when its URL contains ".jar!", and copies the whole archive otherwise.
        byte[] jar = jarWith("testcontainers/entrypoint-tc.sh", "#!/usr/bin/env bash\n");
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(jar));
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        cache.put(sha, jar);
        ArtifactRef ref = new ArtifactRef("testcontainers-redpanda-2.0.5.jar", sha, ArtifactKind.JAR, jar.length);

        Path onClasspath = cache.materializeForClasspath(ref, Files.createDirectories(tempDir.resolve("partition")));

        assertThat(onClasspath.getFileName().toString()).isEqualTo("testcontainers-redpanda-2.0.5.jar");
        assertThat(onClasspath).hasBinaryContent(jar);
        assertThat(cache.materializeForClasspath(ref, tempDir.resolve("other-partition")))
                .as("shared by every partition of the worker").isEqualTo(onClasspath);
    }

    @Test
    void shouldKeepAJarExtensionAndAPlainFileNameForAnyArtifactName() throws Exception {
        byte[] jar = jarWith("a.txt", "a");
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(jar));
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        cache.put(sha, jar);

        Path onClasspath = cache.materializeForClasspath(
                new ArtifactRef("../../evil name", sha, ArtifactKind.JAR, jar.length), tempDir);

        assertThat(onClasspath.getFileName().toString()).endsWith(".jar").doesNotContain("/").doesNotContain("..");
        assertThat(onClasspath.startsWith(cache.getRootDir())).isTrue();
    }

    private static byte[] jarWith(String entry, String content) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry(entry));
            zip.write(content.getBytes());
            zip.closeEntry();
        }
        return bytes.toByteArray();
    }
}
