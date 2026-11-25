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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.citrusframework.distribution.config.CitrusEndpointCollector;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.workspace.IsolatedTestLock;
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
import org.junit.platform.launcher.TestPlan;
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
        return executeLocally(delegate, units, additionalListeners, result -> {});
    }

    static LocalExecutionStats executeLocally(Launcher delegate, List<WorkUnitSpec> units,
                                               TestExecutionListener[] additionalListeners,
                                               Consumer<WorkerTestEventPayload> resultSink) {
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

        AtomicReference<String> activeDynamicUnitKey = new AtomicReference<>();
        return collectExecutions(units, additionalListeners, resultSink, (plan, identifier) -> {
            String className = resolveClassName(identifier, unitKeysByClass);
            return new ExecutionUnit(className, resolveWorkUnitKey(
                    identifier, className, activeDynamicUnitKey.get(), unitKeysByMethod, unitKeysByClass));
        }, merged -> {
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
                CitrusJupiterSupport.allowSources(new LinkedHashSet<>(Set.of(dynamicUnit.dynamicSourceName())));
                try {
                    LauncherDiscoveryRequest req = LauncherDiscoveryRequestBuilder.request()
                            .selectors(DiscoverySelectors.selectClass(dynamicUnit.className()))
                            .build();
                    delegate.execute(req, merged);
                } finally {
                    CitrusJupiterSupport.clearAllowedSources();
                    activeDynamicUnitKey.set(null);
                }
            }
        });
    }

    public static LocalExecutionStats executeTestPlanLocally(Launcher delegate,
                                                             TestPlan testPlan,
                                                             List<WorkUnitSpec> units,
                                                             TestExecutionListener[] additionalListeners) {
        return executeTestPlanLocally(delegate, testPlan, units, additionalListeners, result -> {});
    }

    static LocalExecutionStats executeTestPlanLocally(Launcher delegate, TestPlan testPlan, List<WorkUnitSpec> units,
                                                       TestExecutionListener[] additionalListeners,
                                                       Consumer<WorkerTestEventPayload> resultSink) {
        Map<String, List<WorkUnitSpec>> unitsByClass = units.stream()
                .collect(Collectors.groupingBy(WorkUnitSpec::className));
        LocalExecutionStats stats = collectExecutions(units, additionalListeners, resultSink,
                (plan, identifier) -> resolvePlanUnit(plan, identifier, unitsByClass),
                merged -> delegate.execute(testPlan, merged));

        String reportDir = System.getProperty(DistributionSettings.REPORT_DIR_PROPERTY);
        if (reportDir != null && !reportDir.isBlank()) {
            SurefireReportWriter writer = new SurefireReportWriter();
            stats.executions().forEach(writer::record);
            try {
                writer.writeReports(Path.of(reportDir));
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to write local test reports", e);
            }
        }
        return stats;
    }

    private record ExecutionUnit(String className, String key) {
    }

    private static ExecutionUnit resolvePlanUnit(TestPlan plan, TestIdentifier identifier,
                                                 Map<String, List<WorkUnitSpec>> unitsByClass) {
        List<String> classes = new ArrayList<>();
        MethodSource method = null;
        for (TestIdentifier current = identifier; current != null; current = plan.getParent(current).orElse(null)) {
            TestSource source = current.getSource().orElse(null);
            if (source instanceof ClassSource classSource) {
                classes.add(classSource.getClassName());
            } else if (classes.isEmpty() && method == null && source instanceof MethodSource methodSource) {
                method = methodSource;
            }
        }
        if (classes.isEmpty()) {
            return new ExecutionUnit(method != null ? method.getClassName() : "local", null);
        }

        String className = classes.getFirst();
        List<String> enclosing = classes.subList(1, classes.size()).reversed();
        String classKey = null;
        for (WorkUnitSpec unit : unitsByClass.getOrDefault(className, List.of())) {
            if (!unit.enclosingClassNames().equals(enclosing)) {
                continue;
            }
            if (unit.dynamicSourceName() != null && !unit.dynamicSourceName().isBlank()) {
                // Factory units use their leaf display name as the source name, scoped to the actual factory method.
                if (method != null && matchesMethod(unit, method) && identifier.isTest()
                        && unit.dynamicSourceName().equals(identifier.getDisplayName())) {
                    return new ExecutionUnit(className, unit.key());
                }
            } else if (unit.methodName() == null || unit.methodName().isBlank()) {
                classKey = unit.key();
            } else if (method != null && matchesMethod(unit, method)) {
                return new ExecutionUnit(className, unit.key());
            }
        }
        return new ExecutionUnit(className, classKey);
    }

    private static boolean matchesMethod(WorkUnitSpec unit, MethodSource method) {
        String parameters = method.getMethodParameterTypes();
        return (parameters.isBlank() && method.getMethodName().equals(unit.methodName()))
                || (method.getMethodName() + "(" + parameters + ")").equals(unit.methodName());
    }

    private static LocalExecutionStats collectExecutions(List<WorkUnitSpec> units,
                                                          TestExecutionListener[] additionalListeners,
                                                          Consumer<WorkerTestEventPayload> resultSink,
                                                          BiFunction<TestPlan, TestIdentifier, ExecutionUnit> resolver,
                                                          Consumer<TestExecutionListener[]> execute) {
        AtomicInteger passed = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger skipped = new AtomicInteger();
        List<WorkerTestEventPayload> executions = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<RuntimeException> deliveryFailure = new AtomicReference<>();
        Consumer<WorkerTestEventPayload> record = result -> {
            try {
                resultSink.accept(result);
                executions.add(result);
            } catch (RuntimeException e) {
                deliveryFailure.compareAndSet(null, e);
            }
        };
        Map<String, Long> testStartTimes = new ConcurrentHashMap<>();
        long startNs = System.nanoTime();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (IsolatedTestLock isolation = IsolatedTestLock.acquire(units, loader);
             CitrusEndpointCollector endpointCollector = CitrusEndpointCollector.install(loader)) {
            TestExecutionListener countingListener = new TestExecutionListener() {
                private volatile TestPlan plan;

                @Override
                public void testPlanExecutionStarted(TestPlan currentPlan) {
                    this.plan = currentPlan;
                }

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
                        ExecutionUnit unit = resolver.apply(plan, testIdentifier);
                        record.accept(new WorkerTestEventPayload(
                                "local",
                                "",
                                1,
                                unit.key(),
                                unit.className(),
                                testIdentifier.getDisplayName(),
                                TestOutcomeStatus.SKIPPED,
                                0L,
                                reason,
                                null,
                                List.of(), testIdentifier.getUniqueId()));
                    }
                }

                @Override
                public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult result) {
                    if (!testIdentifier.isTest()) {
                        if (result.getStatus() == TestExecutionResult.Status.FAILED) {
                            failed.incrementAndGet();
                            ExecutionUnit unit = resolver.apply(plan, testIdentifier);
                            record.accept(new WorkerTestEventPayload(
                                    "local",
                                    "",
                                    1,
                                    unit.key(),
                                    unit.className(),
                                    testIdentifier.getDisplayName(),
                                    TestOutcomeStatus.FAILED,
                                    0L,
                                    result.getThrowable().map(Throwable::getMessage).orElse("Container failed"),
                                    result.getThrowable().map(LocalFallbackExecutor::formatStackTrace).orElse(null),
                                    endpointCollector.drainEndpoints(), testIdentifier.getUniqueId()));
                        }
                        return;
                    }
                    Long startedAt = testStartTimes.remove(testIdentifier.getUniqueId());
                    long elapsed = startedAt != null ? Math.max(0L, System.currentTimeMillis() - startedAt) : 0L;
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
                    ExecutionUnit unit = resolver.apply(plan, testIdentifier);
                    record.accept(new WorkerTestEventPayload(
                            "local",
                            "",
                            1,
                            unit.key(),
                            unit.className(),
                            testIdentifier.getDisplayName(),
                            outcomeStatus,
                            elapsed,
                            result.getThrowable().map(Throwable::getMessage).orElse(null),
                            result.getThrowable().map(LocalFallbackExecutor::formatStackTrace).orElse(null),
                            endpointCollector.drainEndpoints(), testIdentifier.getUniqueId()));
                }
            };

            DistributionLauncherInterceptor.runLocally(mergeListeners(additionalListeners, countingListener), execute);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for isolated tests", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to lock isolated tests", e);
        }

        if (deliveryFailure.get() != null) {
            throw deliveryFailure.get();
        }
        long durationMs = Math.max(1L, (System.nanoTime() - startNs) / 1_000_000L);
        return new LocalExecutionStats(passed.get(), failed.get(), skipped.get(), durationMs, executions);
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
        List<String> enclosing = unit.enclosingClassNames();
        if (unit.methodName() != null && !unit.methodName().isBlank()) {
            if (enclosing.isEmpty()) {
                return DiscoverySelectors.selectMethod(unit.className() + "#" + unit.methodName());
            }
            String method = unit.methodName();
            int open = method.indexOf('(');
            return open > 0 && method.endsWith(")")
                    ? DiscoverySelectors.selectNestedMethod(enclosing, unit.className(), method.substring(0, open),
                            method.substring(open + 1, method.length() - 1))
                    : DiscoverySelectors.selectNestedMethod(enclosing, unit.className(), method);
        }
        // A @Nested class goes through the classes enclosing it in the plan (e.g. the subclass inheriting it).
        return enclosing.isEmpty()
                ? DiscoverySelectors.selectClass(unit.className())
                : DiscoverySelectors.selectNestedClass(enclosing, unit.className());
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
