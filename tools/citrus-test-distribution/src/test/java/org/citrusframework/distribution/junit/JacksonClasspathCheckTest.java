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

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class JacksonClasspathCheckTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldReportEveryJacksonLibraryFoundInMoreThanOneLocation() throws Exception {
        Path moduleCopy = jar("module-jackson", "tools/jackson/databind/ObjectMapper.class");
        Path injectedCopy = jar("injected-jackson", "tools/jackson/databind/ObjectMapper.class");
        Path annotations = jar("annotations", "com/fasterxml/jackson/annotation/JsonProperty.class");

        try (URLClassLoader loader = new URLClassLoader(new URL[] {
                moduleCopy.toUri().toURL(), injectedCopy.toUri().toURL(), annotations.toUri().toURL()}, null)) {
            assertThat(JacksonClasspathCheck.conflicts(loader))
                    .singleElement()
                    .satisfies(conflict -> assertThat(conflict)
                            .contains("jackson-databind")
                            .contains("module-jackson")
                            .contains("injected-jackson"));
        }
    }

    @Test
    void shouldReportNothingForASingleCopy() throws Exception {
        try (URLClassLoader loader = new URLClassLoader(new URL[] {
                jar("only", "tools/jackson/databind/ObjectMapper.class").toUri().toURL()}, null)) {
            assertThat(JacksonClasspathCheck.conflicts(loader)).isEmpty();
        }
    }

    private Path jar(String name, String resource) throws Exception {
        Path dir = tempDir.resolve(name);
        Path file = dir.resolve(resource);
        Files.createDirectories(file.getParent());
        Files.write(file, new byte[] {1});
        return dir;
    }
}
