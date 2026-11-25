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

package org.citrusframework.api.agent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Isolated;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.properties.SystemProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Isolated("Replaces the JVM-wide environment variables and system properties")
@ExtendWith(SystemStubsExtension.class)
class CitrusAgentSettingsTest {

    private static final String MODE_PROPERTY = "citrus.agent.mode";
    private static final String MODE_ENV = "CITRUS_AGENT_MODE";

    @SystemStub
    private SystemProperties properties;

    @SystemStub
    private EnvironmentVariables environment;

    @BeforeEach
    void clearMode() {
        properties.remove(MODE_PROPERTY);
        environment.remove(MODE_ENV);
    }

    @Test
    void shouldParseModeCaseInsensitivelyAndTrim() {
        assertThat(CitrusAgentSettings.parseMode(" WORKER ", "test")).isEqualTo("worker");
        assertThat(CitrusAgentSettings.parseMode("Standalone", "test")).isEqualTo("standalone");
    }

    @Test
    void shouldFallBackToStandaloneForBlankMode() {
        assertThat(CitrusAgentSettings.parseMode(null, "test")).isEqualTo("standalone");
        assertThat(CitrusAgentSettings.getMode()).isEqualTo("standalone");
        environment.set(MODE_ENV, "   ");
        assertThat(CitrusAgentSettings.getMode()).isEqualTo("standalone");
    }

    @Test
    void shouldRejectInvalidMode() {
        assertThatThrownBy(() -> CitrusAgentSettings.parseMode("bogus", "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bogus")
                .hasMessageContaining("test");
    }

    @Test
    void shouldPopulateModeFromEnvironmentInAgentConfiguration() {
        environment.set(MODE_ENV, " WORKER ");
        assertThat(CitrusAgentConfiguration.fromEnvVars(source -> null).getMode()).isEqualTo("worker");
    }

    @Test
    void shouldPreferSystemPropertyOverEnvironment() {
        environment.set(MODE_ENV, "bogus");
        properties.set(MODE_PROPERTY, " woRKer ");
        assertThat(CitrusAgentSettings.getMode()).isEqualTo("worker");
    }

    @Test
    void shouldIdentifySourceOfInvalidMode() {
        environment.set(MODE_ENV, "bad-env");
        assertThatThrownBy(CitrusAgentSettings::getMode)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bad-env")
                .hasMessageContaining(MODE_ENV)
                .hasMessageNotContaining(MODE_PROPERTY);

        properties.set(MODE_PROPERTY, "bad-property");
        assertThatThrownBy(CitrusAgentSettings::getMode)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bad-property")
                .hasMessageContaining(MODE_PROPERTY)
                .hasMessageNotContaining(MODE_ENV);
    }
}
