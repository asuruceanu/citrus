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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.citrusframework.distribution.annotations.LocalOnly;
import org.citrusframework.distribution.annotations.RemoteOnly;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.junit.jupiter.JUnitJupiterEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DistributionPlacementFollowupTest {

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void independentAndEmbeddedRequestsKeepTheirPlacementOnAnExistingExecutor() throws Exception {
        String oldBroker = System.getProperty(DistributionSettings.BROKER_URL_PROPERTY);
        String oldFallback = System.getProperty(DistributionSettings.FALLBACK_LOCAL_PROPERTY);
        System.setProperty(DistributionSettings.BROKER_URL_PROPERTY, "http://127.0.0.1:0");
        System.setProperty(DistributionSettings.FALLBACK_LOCAL_PROPERTY, "false");

        var launcher = new DistributedLauncher(LauncherFactory.create());
        var independent = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectMethod(RemoteSuite.class, "remoteOnly"))
                .build();
        var embedded = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectMethod(RemoteSuite.class, "remoteOnly"))
                .configurationParameter(JUnitJupiterEngine.EMBEDDED_LAUNCHER_PARAMETER, "true")
                .build();
        try (var executor = Executors.newSingleThreadExecutor()) {
            executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            RemoteSuite.runs.set(0);

            DistributionLauncherInterceptor.runLocally(() -> {
                assertThatThrownBy(() -> executor.submit(() -> launcher.execute(independent)).get(10, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(IllegalStateException.class);
                assertThat(RemoteSuite.runs.get()).isZero();

                try {
                    executor.submit(() -> launcher.execute(embedded)).get(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });

            assertThat(RemoteSuite.runs.get()).isOne();
        } finally {
            restore(DistributionSettings.BROKER_URL_PROPERTY, oldBroker);
            restore(DistributionSettings.FALLBACK_LOCAL_PROPERTY, oldFallback);
        }
    }

    @Test
    void localExceptionDoesNotDisableFallbackForAnOrdinarySuite() {
        var plan = LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectMethod(RemoteSuite.class, "localException"),
                           DiscoverySelectors.selectClass(SignatureSuite.class))
                .build());
        var placement = DiscoveredWorkExtractor.extract(plan, 1_000);

        assertThat(placement.localUnits()).hasSize(2);
        assertThat(placement.remoteUnits()).hasSize(1);
        assertThat(placement.hasRemoteOnlyUnits()).isFalse();
    }

    @Test
    void signatureWithoutWhitespaceMatchesJUnitSpelling() {
        var plan = LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(SignatureSuite.class))
                .build());
        String selector = SignatureSuite.class.getName() + "#pinned(org.junit.jupiter.api.TestInfo,org.junit.jupiter.api.TestReporter)";
        var placement = DiscoveredWorkExtractor.extract(plan, 1_000, Set.of(), Set.of(), Set.of(selector));

        assertThat(placement.localUnits()).isEmpty();
        assertThat(placement.remoteUnits()).hasSize(2);
    }

    private static void restore(String property, String value) {
        if (value == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, value);
        }
    }

    @RemoteOnly
    static class RemoteSuite {
        static final AtomicInteger runs = new AtomicInteger();

        @Test
        void remoteOnly() {
            runs.incrementAndGet();
        }

        @Test
        @LocalOnly
        void localException() {
        }
    }

    static class SignatureSuite {
        @Test
        void ordinary() {
        }

        @Test
        @LocalOnly
        void pinned(TestInfo info, TestReporter reporter) {
        }
    }
}
