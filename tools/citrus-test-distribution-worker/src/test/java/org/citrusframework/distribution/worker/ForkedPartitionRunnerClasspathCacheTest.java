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


package org.citrusframework.distribution.worker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.commons.JUnitException;
import org.junit.platform.engine.TestEngine;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ForkedPartitionRunnerClasspathCacheTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldOpenContentAddressedCachedJarOnlyOnceAcrossPartitionsEvenWhenMtimeChanges() throws Exception {
        Path digestDir = Files.createDirectories(tempDir.resolve("classpath")
                .resolve("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"));
        Path library = writeJar(digestDir.resolve("sample.jar"));
        Path junitEngine = Files.copy(locationOf(TestEngine.class), tempDir.resolve("junit-platform-engine.jar"));
        Path junitCommons = Files.copy(locationOf(JUnitException.class), tempDir.resolve("junit-platform-commons.jar"));
        List<Path> partitionClasspath = List.of(library, junitEngine, junitCommons);
        long before = ForkedPartitionRunner.jarOpenCountForTesting();

        ForkedPartitionRunner.verifyJUnitPlatform(partitionClasspath);
        List<Path> firstClasspath = ForkedPartitionRunner.childClasspath(partitionClasspath);
        long afterFirst = ForkedPartitionRunner.jarOpenCountForTesting();
        assertThat(afterFirst - before).isEqualTo(3);

        FileTime modified = Files.getLastModifiedTime(library);
        Files.setLastModifiedTime(library, FileTime.fromMillis(modified.toMillis() + 5_000L));
        assertThat(Files.getLastModifiedTime(library)).isNotEqualTo(modified);
        ForkedPartitionRunner.verifyJUnitPlatform(partitionClasspath);
        assertThat(ForkedPartitionRunner.childClasspath(partitionClasspath)).isEqualTo(firstClasspath);
        assertThat(ForkedPartitionRunner.jarOpenCountForTesting()).isEqualTo(afterFirst);
    }

    @Test
    void shouldInvalidateExternalJarCacheWhenExternalJarIsModifiedInPlace() throws Exception {
        Path library = writeJar(Files.createDirectories(tempDir.resolve("external")).resolve("lib.jar"),
                "tools/jackson/databind/ObjectMappes.class");
        Path workerJackson = locationOf(ObjectMapper.class);

        assertThat(ForkedPartitionRunner.childClasspath(List.of(library))).contains(workerJackson);
        long afterFirst = ForkedPartitionRunner.jarOpenCountForTesting();

        FileTime modified = Files.getLastModifiedTime(library);
        long size = Files.size(library);
        writeJar(library, "tools/jackson/databind/ObjectMapper.class");
        Files.setLastModifiedTime(library, FileTime.fromMillis(modified.toMillis() + 5_000L));
        assertThat(Files.size(library)).isEqualTo(size);
        assertThat(ForkedPartitionRunner.childClasspath(List.of(library))).contains(library).doesNotContain(workerJackson);
        assertThat(ForkedPartitionRunner.jarOpenCountForTesting()).isEqualTo(afterFirst + 1);
    }

    @Test
    void shouldNotCacheCorruptJarFailureAndRecoverWhenReplaced() throws IOException {
        Path broken = Files.writeString(tempDir.resolve("broken.jar"), "corrupt archive");
        Path junitEngine = tempDir.resolve("junit-platform-engine.jar");
        List<Path> partitionClasspath = List.of(broken, junitEngine);
        long before = ForkedPartitionRunner.jarOpenCountForTesting();

        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> ForkedPartitionRunner.verifyJUnitPlatform(partitionClasspath))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasMessageContaining("broken.jar");
        }
        assertThat(ForkedPartitionRunner.jarOpenCountForTesting()).isEqualTo(before + 2);

        writeJar(broken, "org/junit/platform/engine/TestEngine.class");
        writeJar(junitEngine, "org/junit/platform/engine/CancellationToken.class");
        ForkedPartitionRunner.verifyJUnitPlatform(partitionClasspath);
        assertThat(ForkedPartitionRunner.jarOpenCountForTesting()).isEqualTo(before + 4);
    }

    @Test
    void shouldRejectCorruptZipClasspathEntryAndRecoverWhenReplaced() throws IOException {
        Path library = writeJar(tempDir.resolve("library.zip"),
                "org/junit/platform/engine/TestEngine.class",
                "org/junit/platform/engine/CancellationToken.class");
        List<Path> partitionClasspath = List.of(library);
        ForkedPartitionRunner.verifyJUnitPlatform(partitionClasspath);

        Files.writeString(library, "corrupt archive");
        assertThatThrownBy(() -> ForkedPartitionRunner.verifyJUnitPlatform(partitionClasspath))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("library.zip");

        writeJar(library, "org/junit/platform/engine/TestEngine.class",
                "org/junit/platform/engine/CancellationToken.class", "resource.txt");
        ForkedPartitionRunner.verifyJUnitPlatform(partitionClasspath);
    }

    @Test
    void shouldBoundJarResourceCacheAtMaxEntries() throws IOException {
        Path first = writeJar(tempDir.resolve("first.jar"));
        Path second = writeJar(tempDir.resolve("second.jar"));
        ForkedPartitionRunner.verifyJUnitPlatform(List.of(first, second));
        for (int i = 0; i < 4_095; i++) {
            Path jar = writeJar(tempDir.resolve("lib-" + i + ".jar"));
            ForkedPartitionRunner.verifyJUnitPlatform(List.of(first, jar));
            assertThat(ForkedPartitionRunner.jarResourceCacheSizeForTesting()).isLessThanOrEqualTo(4_096);
        }

        long afterFill = ForkedPartitionRunner.jarOpenCountForTesting();
        ForkedPartitionRunner.verifyJUnitPlatform(List.of(first));
        assertThat(ForkedPartitionRunner.jarOpenCountForTesting()).isEqualTo(afterFill);
        ForkedPartitionRunner.verifyJUnitPlatform(List.of(second));
        assertThat(ForkedPartitionRunner.jarOpenCountForTesting()).isEqualTo(afterFill + 1);
        assertThat(ForkedPartitionRunner.jarResourceCacheSizeForTesting()).isEqualTo(4_096);
    }

    private static Path locationOf(Class<?> type) throws Exception {
        return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
    }

    private static Path writeJar(Path path, String... resources) throws IOException {
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(path))) {
            for (String resource : resources) {
                jar.putNextEntry(new JarEntry(resource));
                jar.closeEntry();
            }
        }
        return path;
    }
}
