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

package org.citrusframework.distribution.scheduler;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.citrusframework.distribution.annotations.AgentRequirement;
import org.citrusframework.distribution.annotations.LocalOnly;
import org.citrusframework.distribution.annotations.RemoteOnly;
import org.citrusframework.distribution.config.DistributionSettings;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CapabilityMatcherTest {

    @LocalOnly(reason = "Uses host loopback hardware")
    static class LocalOnlySampleTest {
        void testCase() {
        }
    }

    @RemoteOnly(reason = "Requires cluster")
    @AgentRequirement(key = "os", value = "linux")
    @AgentRequirement(key = "memoryMb", value = "4096")
    static class RemoteCapabilitySampleTest {
        @AgentRequirement(key = "docker")
        @AgentRequirement(key = "memoryMb", value = ">=8192")
        void heavyDockerTest() {
        }

        void normalRemoteTest() {
        }
    }

    @Test
    void shouldDefaultBareAgentRequirementsAndCapabilitiesToTrue() throws Exception {
        Method method = RemoteCapabilitySampleTest.class.getDeclaredMethod("heavyDockerTest");
        assertThat(method.getAnnotationsByType(AgentRequirement.class))
                .anyMatch(requirement -> requirement.key().equals("docker") && requirement.value().equals("true"));
        Map<String, String> worker = DistributionSettings.parseStrictCapabilitiesCsv("docker,postgres=18");
        assertThat(worker).containsExactlyInAnyOrderEntriesOf(Map.of("docker", "true", "postgres", "18"));
        assertThat(CapabilityMatcher.matches(Map.of("docker", "true"), worker)).isTrue();
    }

    @Test
    void shouldRejectInvalidStrictCapabilityEntries() {
        for (String csv : List.of("a=b=c", "bad space=1", "=val", "key=", "docker,,postgres", "docker,")) {
            assertThatThrownBy(() -> DistributionSettings.parseStrictCapabilitiesCsv(csv))
                    .as("strict capability entry in %s", csv)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(DistributionSettings.parseStrictCapabilitiesCsv("label_1.2-x=value-2.0"))
                .containsExactlyEntriesOf(Map.of("label_1.2-x", "value-2.0"));
        assertThat(DistributionSettings.parseStrictCapabilitiesCsv(null)).isEmpty();
        assertThat(DistributionSettings.parseStrictCapabilitiesCsv(" ")).isEmpty();
        assertThat(DistributionSettings.parseCapabilitiesCsv("bad space=1,a=b=c,key="))
                .containsExactlyInAnyOrderEntriesOf(Map.of("bad space", "1", "a", "b=c", "key", ""));
    }

    @Test
    void shouldValidateCapabilityMapsWithoutChangingMatcherSemantics() {
        CapabilityMatcher.validateCapabilities(Map.of("capability_1.2-x", "value-2.0"));
        assertThatThrownBy(() -> CapabilityMatcher.validateCapabilities(Map.of("bad key", "value")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CapabilityMatcher.validateCapabilities(Map.of("key", "bad=value")))
                .isInstanceOf(IllegalArgumentException.class);
        Map<String, String> nullValue = new HashMap<>();
        nullValue.put("key", null);
        assertThatThrownBy(() -> CapabilityMatcher.validateCapabilities(nullValue))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(CapabilityMatcher.matches(Map.of("memoryMb", ">=8192"), Map.of("memoryMb", "16384"))).isTrue();
    }

    @Test
    void shouldDetectLocalAndRemotePlacementAnnotations() throws Exception {
        Method localMethod = LocalOnlySampleTest.class.getDeclaredMethod("testCase");
        assertThat(CapabilityMatcher.isLocalOnly(LocalOnlySampleTest.class, localMethod)).isTrue();
        assertThat(CapabilityMatcher.isRemoteOnly(LocalOnlySampleTest.class, localMethod)).isFalse();

        Method remoteMethod = RemoteCapabilitySampleTest.class.getDeclaredMethod("normalRemoteTest");
        assertThat(CapabilityMatcher.isLocalOnly(RemoteCapabilitySampleTest.class, remoteMethod)).isFalse();
        assertThat(CapabilityMatcher.isRemoteOnly(RemoteCapabilitySampleTest.class, remoteMethod)).isTrue();
    }

    @Test
    void shouldMergeClassAndMethodAgentRequirements() throws Exception {
        Method heavyMethod = RemoteCapabilitySampleTest.class.getDeclaredMethod("heavyDockerTest");
        Map<String, String> requirements = CapabilityMatcher.extractRequiredCapabilities(
                RemoteCapabilitySampleTest.class, heavyMethod);

        assertThat(requirements)
                .containsEntry("os", "linux")
                .containsEntry("docker", "true")
                .containsEntry("memoryMb", ">=8192");
    }

    @Test
    void shouldMatchExactAndNumericThresholdCapabilities() {
        Map<String, String> required = Map.of(
                "os", "linux",
                "docker", "true",
                "memoryMb", "4096",
                "cpuCores", ">=4"
        );

        Map<String, String> smallWorker = Map.of(
                "os", "linux",
                "docker", "true",
                "memoryMb", "2048",
                "cpuCores", "4"
        );
        Map<String, String> noDockerWorker = Map.of(
                "os", "linux",
                "docker", "false",
                "memoryMb", "8192",
                "cpuCores", "8"
        );
        Map<String, String> matchingWorker = Map.of(
                "os", "linux",
                "docker", "true",
                "memoryMb", "8192",
                "cpuCores", "8"
        );

        assertThat(CapabilityMatcher.matches(required, smallWorker)).isFalse();
        assertThat(CapabilityMatcher.matches(required, noDockerWorker)).isFalse();
        assertThat(CapabilityMatcher.matches(required, matchingWorker)).isTrue();
    }
}
