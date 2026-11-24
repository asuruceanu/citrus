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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.citrusframework.api.main.TestRunConfiguration;
import org.citrusframework.distribution.annotations.RemoteOnly;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.junit.jupiter.JUnitJupiterEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndependentJupiterEnginePlacementTest {

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void independentCitrusEngineStillRequiresAWorkerDuringAnotherLocalRun() throws Exception {
        Map<String, String> systemProperties = Map.of(
                "junit.platform.launcher.interceptors.enabled", "true",
                DistributionSettings.ENABLED_PROPERTY, "true",
                DistributionSettings.BROKER_URL_PROPERTY, "http://127.0.0.1:0",
                DistributionSettings.FALLBACK_LOCAL_PROPERTY, "false");
        var previous = new java.util.HashMap<String, String>();
        systemProperties.forEach((key, value) -> previous.put(key, System.setProperty(key, value)));

        var config = new TestRunConfiguration();
        config.setPackages(List.of(getClass().getPackageName()));
        config.setIncludes(new String[] { "^.*RemoteOnlyEngineFixture$" });
        config.setVerbose(false);
        config.setReset(false);

        try (var executor = Executors.newSingleThreadExecutor()) {
            executor.submit(() -> { }).get(5, TimeUnit.SECONDS);

            DistributionLauncherInterceptor.runLocally(() ->
                    assertThatThrownBy(() -> executor.submit(() -> new JUnitJupiterEngine(config).run())
                            .get(5, TimeUnit.SECONDS))
                            .isInstanceOf(ExecutionException.class)
                            .hasCauseInstanceOf(IllegalStateException.class)
                            .hasStackTraceContaining("and remote execution is required"));
        } finally {
            previous.forEach((key, value) -> {
                if (value == null) {
                    System.clearProperty(key);
                } else {
                    System.setProperty(key, value);
                }
            });
        }
    }
}

@RemoteOnly
class RemoteOnlyEngineFixture {

    @Test
    void workerOnly() {
        throw new AssertionError("Remote-only fixture ran on the host");
    }
}
