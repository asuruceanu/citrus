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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;

/**
 * Executes {@code @LocalOnly}, fallback, or {@code LOST} work units locally using the underlying raw JUnit {@link Launcher}.
 */
public final class LocalFallbackExecutor {

    private LocalFallbackExecutor() {
    }

    public record LocalExecutionStats(int passed, int failed, int skipped, long serialDurationMs) {
    }

    public static LocalExecutionStats executeLocally(Launcher delegate,
                                                     List<WorkUnitSpec> units,
                                                     TestExecutionListener[] additionalListeners) {
        if (units.isEmpty()) {
            return new LocalExecutionStats(0, 0, 0, 0L);
        }

        AtomicInteger passed = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger skipped = new AtomicInteger();
        long startNs = System.nanoTime();

        TestExecutionListener countingListener = new TestExecutionListener() {
            @Override
            public void executionSkipped(TestIdentifier testIdentifier, String reason) {
                if (testIdentifier.isTest()) {
                    skipped.incrementAndGet();
                }
            }

            @Override
            public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult result) {
                if (testIdentifier.isTest()) {
                    switch (result.getStatus()) {
                        case SUCCESSFUL -> passed.incrementAndGet();
                        case FAILED -> failed.incrementAndGet();
                        case ABORTED -> skipped.incrementAndGet();
                    }
                }
            }
        };

        TestExecutionListener[] merged = mergeListeners(additionalListeners, countingListener);

        List<DiscoverySelector> regularSelectors = new ArrayList<>();
        Set<String> allowedDynamicSources = new LinkedHashSet<>();
        List<DiscoverySelector> dynamicSelectors = new ArrayList<>();

        for (WorkUnitSpec unit : units) {
            DiscoverySelector selector = toSelector(unit);
            if (unit.dynamicSourceName() != null && !unit.dynamicSourceName().isBlank()) {
                allowedDynamicSources.add(unit.dynamicSourceName());
                if (!dynamicSelectors.contains(selector)) {
                    dynamicSelectors.add(selector);
                }
            } else {
                regularSelectors.add(selector);
            }
        }

        if (!regularSelectors.isEmpty()) {
            LauncherDiscoveryRequest req = LauncherDiscoveryRequestBuilder.request()
                    .selectors(regularSelectors)
                    .build();
            delegate.execute(req, merged);
        }

        if (!dynamicSelectors.isEmpty()) {
            CitrusDynamicSourceFilter.setAllowedSources(allowedDynamicSources);
            try {
                LauncherDiscoveryRequest req = LauncherDiscoveryRequestBuilder.request()
                        .selectors(dynamicSelectors)
                        .build();
                delegate.execute(req, merged);
            } finally {
                CitrusDynamicSourceFilter.clear();
            }
        }

        AtomicLong durationMs = new AtomicLong(Math.max(1L, (System.nanoTime() - startNs) / 1_000_000L));
        return new LocalExecutionStats(passed.get(), failed.get(), skipped.get(), durationMs.get());
    }

    private static DiscoverySelector toSelector(WorkUnitSpec unit) {
        if (unit.methodName() != null && !unit.methodName().isBlank()) {
            return DiscoverySelectors.selectMethod(unit.className() + "#" + unit.methodName());
        }
        return DiscoverySelectors.selectClass(unit.className());
    }

    private static TestExecutionListener[] mergeListeners(TestExecutionListener[] existing,
                                                          TestExecutionListener extra) {
        if (existing == null || existing.length == 0) {
            return new TestExecutionListener[]{extra};
        }
        TestExecutionListener[] out = new TestExecutionListener[existing.length + 1];
        System.arraycopy(existing, 0, out, 0, existing.length);
        out[existing.length] = extra;
        return out;
    }
}
