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

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.citrusframework.distribution.annotations.LocalOnly;
import org.citrusframework.distribution.annotations.RemoteOnly;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

import static org.assertj.core.api.Assertions.assertThat;

class DistributionPlacementRegressionTest {

    @Test
    void localExecutionAlsoCoversAnExistingExecutorThread() throws Exception {
        try (var executor = Executors.newSingleThreadExecutor()) {
            assertThat(executor.submit(DistributionLauncherInterceptor::isExecutingLocally).get()).isFalse();

            DistributionLauncherInterceptor.runLocally(() -> {
                try {
                    assertThat(executor.submit(DistributionLauncherInterceptor::isExecutingLocally).get()).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                } catch (ExecutionException e) {
                    throw new AssertionError(e);
                }
            });

            assertThat(executor.submit(DistributionLauncherInterceptor::isExecutingLocally).get()).isFalse();
        }
    }

    @Test
    void overridingAMethodPinRestoresTheClassRemoteOnlyRequirement() {
        var plan = LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(RemoteSuite.class))
                .build());
        String selector = RemoteSuite.class.getName() + "#hostException";

        var defaultPlacement = DiscoveredWorkExtractor.extract(plan, 1_000);
        assertThat(defaultPlacement.localUnits()).hasSize(1);
        assertThat(defaultPlacement.remoteUnits()).isEmpty();

        var override = DiscoveredWorkExtractor.extract(plan, 1_000, Set.of(), Set.of(), Set.of(selector));
        assertThat(override.localUnits()).isEmpty();
        assertThat(override.remoteUnits()).hasSize(1);
        assertThat(override.hasRemoteOnlyUnits()).isTrue();
    }

    @RemoteOnly
    static class RemoteSuite {

        @Test
        @LocalOnly
        void hostException() {
        }
    }
}
