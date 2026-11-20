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
import java.util.Map;

import org.citrusframework.distribution.annotations.AgentRequirement;
import org.citrusframework.distribution.annotations.LocalOnly;
import org.citrusframework.distribution.annotations.RemoteOnly;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
        @AgentRequirement(key = "docker", value = "true")
        @AgentRequirement(key = "memoryMb", value = ">=8192")
        void heavyDockerTest() {
        }

        void normalRemoteTest() {
        }
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
