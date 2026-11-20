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

package org.citrusframework.distribution.ui;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DistributionUiResourcesTest {

    @Test
    void shouldPackageDistUiStaticAssetsWithFixedTemplateEvaluator() throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        try (InputStream index = cl.getResourceAsStream("META-INF/resources/dist-ui/index.html");
             InputStream styles = cl.getResourceAsStream("META-INF/resources/dist-ui/styles.css");
             InputStream app = cl.getResourceAsStream("META-INF/resources/dist-ui/app.js");
             InputStream htm = cl.getResourceAsStream("META-INF/resources/dist-ui/vendor/htm-mini.js")) {

            assertThat(index).isNotNull();
            assertThat(styles).isNotNull();
            assertThat(app).isNotNull();
            assertThat(htm).isNotNull();

            String stylesText = new String(styles.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(stylesText).contains("--citrus-orange: #f5921d");

            String htmText = new String(htm.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(htmText)
                    .contains("export function html")
                    .contains("fields[built[i++] - 1]");
        }
    }
}
