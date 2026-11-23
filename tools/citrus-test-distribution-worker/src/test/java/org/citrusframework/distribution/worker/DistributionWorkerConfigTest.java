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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.citrusframework.distribution.config.DistributionSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DistributionWorkerConfigTest {

    private static final String[] CACHE_PROPERTIES = {
            DistributionSettings.WORKER_CACHE_DIR_PROPERTY,
            DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY,
            DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY
    };

    private final Map<String, String> savedProperties = new HashMap<>();

    @BeforeEach
    void saveProperties() {
        for (String key : CACHE_PROPERTIES) {
            savedProperties.put(key, System.getProperty(key));
        }
    }

    @AfterEach
    void restoreProperties() {
        savedProperties.forEach((key, value) -> {
            if (value == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
        });
    }

    @Test
    void shouldLetExplicitCacheCleanupOverrideAnInvalidSystemProperty() {
        System.setProperty(DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY, "yes");

        DistributionWorkerConfig config = DistributionWorkerConfig.builder().cacheCleanup(true).build();

        assertThat(config.cacheCleanup()).isTrue();
    }

    @Test
    void shouldLetExplicitFreeSpaceBufferOverrideAnInvalidSystemProperty() {
        System.setProperty(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY, "2GB");

        DistributionWorkerConfig config = DistributionWorkerConfig.builder()
                .cacheFreeSpaceBufferBytes(67_108_864L).build();

        assertThat(config.cacheFreeSpaceBufferBytes()).isEqualTo(67_108_864L);
    }

    @Test
    void shouldRejectAnInvalidSystemPropertyThatIsNotOverridden() {
        System.setProperty(DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY, "yes");
        DistributionWorkerConfig.Builder cleanupBuilder = DistributionWorkerConfig.builder();
        assertThatThrownBy(cleanupBuilder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY)
                .hasMessageContaining("yes");

        System.clearProperty(DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY);
        System.setProperty(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY, "2GB");
        DistributionWorkerConfig.Builder bufferBuilder = DistributionWorkerConfig.builder().cacheCleanup(false);
        assertThatThrownBy(bufferBuilder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY);
    }

    @Test
    void shouldResolveUnsetCacheSettingsFromSystemPropertiesAtBuildTime() {
        DistributionWorkerConfig.Builder builder = DistributionWorkerConfig.builder();
        System.setProperty(DistributionSettings.WORKER_CACHE_DIR_PROPERTY, "/tmp/late-cache");
        System.setProperty(DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY, "false");
        System.setProperty(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY, "1KiB");

        DistributionWorkerConfig config = builder.build();

        assertThat(config.cacheDir()).isEqualTo(Path.of("/tmp/late-cache"));
        assertThat(config.cacheCleanup()).isFalse();
        assertThat(config.cacheFreeSpaceBufferBytes()).isEqualTo(1024L);
    }

    @Test
    void shouldPreferExplicitCacheDirAndRejectNull() {
        System.setProperty(DistributionSettings.WORKER_CACHE_DIR_PROPERTY, "/tmp/from-property");

        assertThat(DistributionWorkerConfig.builder().cacheDir(Path.of("/tmp/explicit")).build().cacheDir())
                .isEqualTo(Path.of("/tmp/explicit"));
        DistributionWorkerConfig.Builder builder = DistributionWorkerConfig.builder();
        assertThatThrownBy(() -> builder.cacheDir(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("cacheDir");
    }

    @Test
    void shouldParseExplicitFreeSpaceBufferSizeExpression() {
        System.setProperty(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY, "2GB");

        assertThat(DistributionWorkerConfig.builder().cacheFreeSpaceBufferSize("64MiB").build()
                .cacheFreeSpaceBufferBytes()).isEqualTo(67_108_864L);
        assertThat(DistributionWorkerConfig.builder().cacheFreeSpaceBufferSize(" 1 KiB ").build()
                .cacheFreeSpaceBufferBytes()).isEqualTo(1024L);
        assertThat(DistributionWorkerConfig.builder().cacheFreeSpaceBufferSize("0").build()
                .cacheFreeSpaceBufferBytes()).isZero();
    }

    @Test
    void shouldRejectMissingOrInvalidFreeSpaceBufferSizeExpression() {
        DistributionWorkerConfig.Builder builder = DistributionWorkerConfig.builder();
        for (String invalid : new String[] {null, "", "  ", "2GB", "-1", "1.5GiB"}) {
            assertThatThrownBy(() -> builder.cacheFreeSpaceBufferSize(invalid))
                    .as("cacheFreeSpaceBufferSize(%s)", invalid)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cacheFreeSpaceBufferSize");
        }
    }
}
