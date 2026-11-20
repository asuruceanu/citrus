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

package org.citrusframework.junit.jupiter;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CitrusDynamicSourceFilterTest {

    @AfterEach
    void tearDown() {
        CitrusDynamicSourceFilter.clear();
        System.clearProperty(CitrusDynamicSourceFilter.DYNAMIC_SOURCES_PROPERTY);
    }

    @Test
    void shouldReturnAllSourcesWhenUnfiltered() {
        CitrusTestFactorySupport factory = CitrusTestFactorySupport.factory("fake");

        List<String> names = factory.dynamicTests("org.citrusframework.junit.jupiter.source", "alpha", "beta", "gamma")
                .map(DynamicTest::getDisplayName)
                .toList();

        assertThat(names).containsExactly("alpha", "beta", "gamma");
    }

    @Test
    void shouldFilterDynamicTestsByThreadLocalAllowedSources() {
        CitrusDynamicSourceFilter.setAllowedSources(Set.of("beta"));
        CitrusTestFactorySupport factory = CitrusTestFactorySupport.factory("fake");

        List<String> names = factory.dynamicTests("org.citrusframework.junit.jupiter.source", "alpha", "beta", "gamma")
                .map(DynamicTest::getDisplayName)
                .toList();

        assertThat(names).containsExactly("beta");
    }

    @Test
    void shouldFilterPackageScanBySystemPropertyFallback() {
        System.setProperty(CitrusDynamicSourceFilter.DYNAMIC_SOURCES_PROPERTY, "EchoActionIT, CreateVariablesIT");
        CitrusTestFactorySupport factory = CitrusTestFactorySupport.springXml();

        List<String> names = factory.packageScan("org.citrusframework.junit.jupiter.integration.actions")
                .map(DynamicTest::getDisplayName)
                .toList();

        assertThat(names).containsExactlyInAnyOrder("EchoActionIT", "CreateVariablesIT");
    }

    @Test
    void shouldProbePackageScanSourcesWithoutCreatingDynamicTestsOrRequiringLoader() {
        CitrusDynamicSourceFilter.startProbe();
        // Use a non-existent loader type to prove probe mode does not instantiate TestLoader
        CitrusTestFactorySupport factory = CitrusTestFactorySupport.xml();

        List<DynamicTest> emitted = factory.packageScan("org.citrusframework.junit.jupiter.integration.actions")
                .toList();
        List<String> probed = CitrusDynamicSourceFilter.finishProbe();

        assertThat(emitted).isEmpty();
        assertThat(probed).containsExactlyInAnyOrder("CreateVariablesIT", "EchoActionIT", "FailActionIT");
    }
}
