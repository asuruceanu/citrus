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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.citrusframework.distribution.config.CitrusEndpointCollector;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
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

    public record LocalExecutionStats(
            int passed,
            int failed,
            int skipped,
            long serialDurationMs,
            List<WorkerTestEventPayload> executions) {

        public LocalExecutionStats {
            executions = executions != null ? List.copyOf(executions) : List.of();
        }

        public LocalExecutionStats(int passed, int failed, int skipped, long serialDurationMs) {
            this(passed, failed, skipped, serialDurationMs, List.of());
        }
    }

    public static LocalExecutionStats executeLocally(Launcher delegate,
                                                     List<WorkUnitSpec> units,
                                                     TestExecutionListener[] additionalListeners) {
        if (units.isEmpty()) {
            return new LocalExecutionStats(0, 0, 0, 0L, List.of());
        }

        Map<String, String> unitKeysByMethod = new LinkedHashMap<>();
        Map<String, String> unitKeysByClass = new LinkedHashMap<>();
        for (WorkUnitSpec unit : units) {
            unitKeysByClass.putIfAbsent(unit.className(), unit.key());
            if (unit.methodName() != null && !unit.methodName().isBlank()
                    && (unit.dynamicSourceName() == null || unit.dynamicSourceName().isBlank())) {
                unitKeysByMethod.putIfAbsent(unit.className() + "#" + unit.methodName(), unit.key());
                if (unit.methodName().endsWith("()")) {
                    String bare = unit.methodName().substring(0, unit.methodName().length() - 2);
                    unitKeysByMethod.putIfAbsent(unit.className() + "#" + bare, unit.key());
                }
            }
        }

        AtomicInteger passed = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger skipped = new AtomicInteger();
        List<WorkerTestEventPayload> executions = Collections.synchronizedList(new ArrayList<>());
        Map<String, Long> testStartTimes = new ConcurrentHashMap<>();
        AtomicReference<String> activeDynamicUnitKey = new AtomicReference<>();
        long startNs = System.nanoTime();

        try (CitrusEndpointCollector endpointCollector =
                     CitrusEndpointCollector.install(Thread.currentThread().getContextClassLoader())) {
            TestExecutionListener countingListener = new TestExecutionListener() {
                @Override
                public void executionStarted(TestIdentifier testIdentifier) {
                    if (testIdentifier.isTest()) {
                        endpointCollector.startTest();
                        testStartTimes.put(testIdentifier.getUniqueId(), System.currentTimeMillis());
                    }
                }

                @Override
                public void executionSkipped(TestIdentifier testIdentifier, String reason) {
                    if (testIdentifier.isTest() || testIdentifier.getSource().isPresent()) {
                        skipped.incrementAndGet();
                        String className = resolveClassName(testIdentifier, unitKeysByClass);
                        String workUnitKey = resolveWorkUnitKey(
                                testIdentifier, className, activeDynamicUnitKey.get(), unitKeysByMethod, unitKeysByClass);
                        executions.add(new WorkerTestEventPayload(
                                "local",
                                "",
                                1,
                                workUnitKey,
                                className,
                                testIdentifier.getDisplayName(),
                                TestOutcomeStatus.SKIPPED,
                                0L,
                                reason,
                                null,
                                List.of()));
                    }
                }

                @Override
                public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult result) {
                    if (!testIdentifier.isTest()) {
                        if (result.getStatus() == TestExecutionResult.Status.FAILED) {
                            failed.incrementAndGet();
                            String className = resolveClassName(testIdentifier, unitKeysByClass);
                            String workUnitKey = resolveWorkUnitKey(
                                    testIdentifier, className, activeDynamicUnitKey.get(), unitKeysByMethod, unitKeysByClass);
                            executions.add(new WorkerTestEventPayload(
                                    "local",
                                    "",
                                    1,
                                    workUnitKey,
                                    className,
                                    testIdentifier.getDisplayName(),
                                    TestOutcomeStatus.FAILED,
                                    0L,
                                    result.getThrowable().map(Throwable::getMessage).orElse("Container failed"),
                                    result.getThrowable().map(LocalFallbackExecutor::formatStackTrace).orElse(null),
                                    endpointCollector.drainEndpoints()));
                        }
                        return;
                    }
                    long startedAt = testStartTimes.getOrDefault(testIdentifier.getUniqueId(), System.currentTimeMillis());
                    long elapsed = Math.max(0L, System.currentTimeMillis() - startedAt);
                    TestOutcomeStatus outcomeStatus = switch (result.getStatus()) {
                        case SUCCESSFUL -> {
                            passed.incrementAndGet();
                            yield TestOutcomeStatus.PASSED;
                        }
                        case FAILED -> {
                            failed.incrementAndGet();
                            yield TestOutcomeStatus.FAILED;
                        }
                        case ABORTED -> {
                            skipped.incrementAndGet();
                            yield TestOutcomeStatus.SKIPPED;
                        }
                    };
                    String className = resolveClassName(testIdentifier, unitKeysByClass);
                    String workUnitKey = resolveWorkUnitKey(
                            testIdentifier, className, activeDynamicUnitKey.get(), unitKeysByMethod, unitKeysByClass);
                    executions.add(new WorkerTestEventPayload(
                            "local",
                            "",
                            1,
                            workUnitKey,
                            className,
                            testIdentifier.getDisplayName(),
                            outcomeStatus,
                            elapsed,
                            result.getThrowable().map(Throwable::getMessage).orElse(null),
                            result.getThrowable().map(LocalFallbackExecutor::formatStackTrace).orElse(null),
                            endpointCollector.drainEndpoints()));
                }
            };

            TestExecutionListener[] merged = mergeListeners(additionalListeners, countingListener);

            List<DiscoverySelector> regularSelectors = new ArrayList<>();
            List<WorkUnitSpec> dynamicUnits = new ArrayList<>();

            for (WorkUnitSpec unit : units) {
                if (unit.dynamicSourceName() != null && !unit.dynamicSourceName().isBlank()) {
                    dynamicUnits.add(unit);
                } else {
                    regularSelectors.add(toSelector(unit));
                }
            }

            if (!regularSelectors.isEmpty()) {
                LauncherDiscoveryRequest req = LauncherDiscoveryRequestBuilder.request()
                        .selectors(regularSelectors)
                        .build();
                delegate.execute(req, merged);
            }

            for (WorkUnitSpec dynamicUnit : dynamicUnits) {
                activeDynamicUnitKey.set(dynamicUnit.key());
                CitrusDynamicSourceFilter.setAllowedSources(new LinkedHashSet<>(Set.of(dynamicUnit.dynamicSourceName())));
                try {
                    LauncherDiscoveryRequest req = LauncherDiscoveryRequestBuilder.request()
                            .selectors(DiscoverySelectors.selectClass(dynamicUnit.className()))
                            .build();
                    delegate.execute(req, merged);
                } finally {
                    CitrusDynamicSourceFilter.clear();
                    activeDynamicUnitKey.set(null);
                }
            }
        }

        AtomicLong durationMs = new AtomicLong(Math.max(1L, (System.nanoTime() - startNs) / 1_000_000L));
        return new LocalExecutionStats(passed.get(), failed.get(), skipped.get(), durationMs.get(), executions);
    }

    private static String resolveClassName(TestIdentifier identifier, Map<String, String> unitKeysByClass) {
        TestSource source = identifier.getSource().orElse(null);
        if (source instanceof MethodSource ms) {
            return ms.getClassName();
        }
        if (source instanceof ClassSource cs) {
            return cs.getClassName();
        }
        return unitKeysByClass.keySet().stream().findFirst().orElse("local");
    }

    private static String resolveWorkUnitKey(
            TestIdentifier identifier,
            String className,
            String activeDynamicKey,
            Map<String, String> unitKeysByMethod,
            Map<String, String> unitKeysByClass) {
        if (activeDynamicKey != null && !activeDynamicKey.isBlank()) {
            return activeDynamicKey;
        }
        TestSource source = identifier.getSource().orElse(null);
        if (source instanceof MethodSource ms) {
            if (!ms.getMethodParameterTypes().isBlank()) {
                String overloaded = unitKeysByMethod.get(
                        className + "#" + ms.getMethodName() + "(" + ms.getMethodParameterTypes() + ")");
                if (overloaded != null) {
                    return overloaded;
                }
            } else {
                String withParens = unitKeysByMethod.get(className + "#" + ms.getMethodName() + "()");
                if (withParens != null) {
                    return withParens;
                }
                String exact = unitKeysByMethod.get(className + "#" + ms.getMethodName());
                if (exact != null) {
                    return exact;
                }
            }
        }
        return unitKeysByClass.getOrDefault(className, className);
    }

    private static String formatStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        return sw.toString();
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
