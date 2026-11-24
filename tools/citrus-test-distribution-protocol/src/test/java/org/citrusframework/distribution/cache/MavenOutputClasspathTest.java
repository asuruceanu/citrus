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

import java.nio.file.Files;
import java.nio.file.Path;

import org.citrusframework.distribution.model.ArtifactRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class MavenOutputClasspathTest {

    @TempDir
    Path tempDir;

    @Test
    void preservesReactorModuleNameWhenMaterializingClasspath() throws Exception {
        Path output = Files.createDirectories(tempDir.resolve("citrus-groovy/target/classes"));
        Files.writeString(output.resolve("loader"), "groovy");

        var artifact = ClasspathPackager.packagePath(output);
        assertThat(artifact.ref().name()).isEqualTo("citrus-groovy-classes.zip");

        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        cache.put(artifact.ref().sha256(), artifact.bytes());
        Path workspace = tempDir.resolve("workspace");
        Path entry = cache.materializeForClasspath(artifact.ref(), workspace);

        assertThat(entry.getFileName().toString()).endsWith("-citrus-groovy-classes");
        assertThat(entry.resolve("loader")).hasContent("groovy");

        var unexpectedName = new ArtifactRef("../citrus-groovy-classes.zip", artifact.ref().sha256(),
                artifact.ref().kind(), artifact.ref().sizeBytes());
        assertThat(cache.materializeForClasspath(unexpectedName, workspace))
                .isEqualTo(workspace.resolve("extracted").resolve(artifact.ref().sha256()));
    }
}
