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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Optional;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.citrusframework.distribution.config.CitrusEndpointCollector;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.workspace.IsolatedTestLock;
import org.junit.platform.engine.CancellationToken;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherExecutionRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit Platform discovery and execution of a partition's work units, shared by the in-process
 * {@link IsolatedPartitionRunner} and the forked partition JVM so both report identical test events.
 */
public final class PartitionJUnitLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionJUnitLauncher.class);

    /**
     * Outcome of executing a partition's work units. {@code success} is {@code false} only when execution itself
     * failed (for example a test class could not be loaded); failing tests are reported as test events.
     */
    public record ExecutionSummary(boolean success, String errorMessage) {
    }

    private PartitionJUnitLauncher() {
    }

    /**
     * Executes {@code workUnits} with {@code testClassLoader} as context class loader, streaming a
     * {@link WorkerTestEventPayload} for every test start, skip and finish.
     * <p>
     * All class and method units run in one JUnit execution, so the module's own configuration (for example
     * {@code junit.jupiter.execution.parallel.*} in its {@code junit-platform.properties}) applies across the whole
     * partition, as it does in a local Surefire run. Citrus factory source units run afterwards, one at a time, because
     * each needs its own source filter. Execution stops once {@code cancellationToken} is cancelled.
     */
    public static ExecutionSummary executeWorkUnits(String partitionId,
                                                    String sessionId,
                                                    int attempt,
                                                    List<WorkUnitSpec> workUnits,
                                                    ClassLoader testClassLoader,
                                                    CancellationToken cancellationToken,
                                                    Consumer<WorkerTestEventPayload> eventSink) {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(testClassLoader);
        try (IsolatedTestLock isolation = IsolatedTestLock.acquire(workUnits, testClassLoader);
             CitrusEndpointCollector endpointCollector = CitrusEndpointCollector.install(testClassLoader)) {
            resetCitrusExtensionIfPresent(testClassLoader);
            List<WorkUnitSpec> regular = new ArrayList<>();
            List<WorkUnitSpec> dynamic = new ArrayList<>();
            for (WorkUnitSpec unit : workUnits) {
                (unit.dynamicSourceName() != null && !unit.dynamicSourceName().isBlank() ? dynamic : regular).add(unit);
            }
            Set<String> reported = java.util.concurrent.ConcurrentHashMap.newKeySet();
            // A test class missing from the partition fails the partition, but only after every other unit has run.
            Set<String> missingClasses = new LinkedHashSet<>();
            List<WorkUnitSpec> loadable = new ArrayList<>();
            for (WorkUnitSpec unit : workUnits) {
                try {
                    Class.forName(unit.className(), false, testClassLoader);
                    loadable.add(unit);
                } catch (ClassNotFoundException e) {
                    missingClasses.add(unit.className());
                }
            }
            regular.retainAll(loadable);
            dynamic.retainAll(loadable);
            if (!regular.isEmpty() && !isCancelled(cancellationToken)) {
                executeTogether(partitionId, sessionId, attempt, regular, testClassLoader, endpointCollector,
                        cancellationToken, eventSink, reported);
            }
            for (WorkUnitSpec unit : dynamic) {
                if (isCancelled(cancellationToken)) {
                    break;
                }
                if (executeWorkUnit(new EventFactory(partitionId, sessionId, attempt, identifier -> unit),
                        testClassLoader, endpointCollector, cancellationToken, eventSink)) {
                    reported.add(unit.key());
                }
            }
            if (!missingClasses.isEmpty()) {
                return new ExecutionSummary(false, "Test class not found: " + String.join(", ", missingClasses));
            }
            List<String> unmatched = new ArrayList<>();
            if (!isCancelled(cancellationToken)) {
                for (WorkUnitSpec unit : loadable) {
                    if (!reported.contains(unit.key())) {
                        unmatched.add(unit.key() + " (" + (unit.methodName() != null ? unit.methodName() : unit.className()) + ")");
                    }
                }
            }
            if (!unmatched.isEmpty()) {
                return new ExecutionSummary(false, "Work units matched no tests on the worker: " + String.join(", ", unmatched));
            }
            return new ExecutionSummary(true, null);
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                thread.interrupt();
            }
            LOG.warn("Partition {} failed during execution", partitionId, t);
            String message = t.getMessage() != null ? t.getMessage() : t.getClass().getName();
            if (t instanceof ClassNotFoundException) {
                message = "Test class not found: " + message;
            }
            return new ExecutionSummary(false, message);
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    /** Selector for {@code unit} by class name; see {@link #selectorFor(WorkUnitSpec, ClassLoader)}. */
    static DiscoverySelector selectorFor(WorkUnitSpec unit) {
        try {
            return selectorFor(unit, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Test class not found: " + unit.className(), e);
        }
    }

    /**
     * Selector for {@code unit} by class name, so JUnit loads the test class itself through the context class loader,
     * as in a local Surefire run. Frameworks rely on that: Quarkus' launcher interceptor installs a class loader that
     * {@code @QuarkusTest} classes must be loaded with. {@code classLoader} only resolves bare method names to an
     * exact overload and checks that the class exists.
     *
     * @throws ClassNotFoundException when the partition does not contain the test class
     */
    static DiscoverySelector selectorFor(WorkUnitSpec unit, ClassLoader classLoader) throws ClassNotFoundException {
        String methodName = unit.methodName();
        List<String> enclosing = unit.enclosingClassNames();
        if (methodName == null || methodName.isBlank()) {
            Class.forName(unit.className(), false, classLoader);
            // A @Nested class goes through the classes enclosing it on the host, e.g. the subclass that inherits it.
            return enclosing.isEmpty()
                    ? DiscoverySelectors.selectClass(unit.className())
                    : DiscoverySelectors.selectNestedClass(enclosing, unit.className());
        }
        String name;
        String parameterTypes;
        int open = methodName.indexOf('(');
        if (open > 0 && methodName.endsWith(")")) {
            name = methodName.substring(0, open);
            parameterTypes = methodName.substring(open + 1, methodName.length() - 1);
        } else {
            Class<?> testClass = Class.forName(unit.className(), false, classLoader);
            DiscoverySelector resolved = methodSelector(testClass, methodName);
            if (resolved instanceof org.junit.platform.engine.discovery.MethodSelector ms && ms.getJavaMethod() != null) {
                name = ms.getJavaMethod().getName();
                parameterTypes = org.junit.platform.commons.util.ClassUtils.nullSafeToString(
                        ms.getJavaMethod().getParameterTypes());
            } else {
                name = methodName;
                parameterTypes = null;
            }
        }
        if (!enclosing.isEmpty()) {
            return DiscoverySelectors.selectNestedMethod(enclosing, unit.className(), name,
                    parameterTypes != null ? parameterTypes : "");
        }
        return parameterTypes != null
                ? DiscoverySelectors.selectMethod(unit.className(), name, parameterTypes)
                : DiscoverySelectors.selectMethod(unit.className(), name);
    }

    /**
     * Discovery selector for a work unit's method: {@code name(type1,type2)} and {@code name()} select exactly that
     * overload. A bare {@code name} (what the host sends for methods without parameters) selects the no-argument
     * method declared on the class or a superclass, or else the first method with that name declared on the class.
     */
    static DiscoverySelector methodSelector(Class<?> testClass, String methodName) {
        int open = methodName.indexOf('(');
        if (open > 0 && methodName.endsWith(")")) {
            return DiscoverySelectors.selectMethod(testClass, methodName.substring(0, open),
                    methodName.substring(open + 1, methodName.length() - 1));
        }
        for (Class<?> type = testClass; type != null && type != Object.class; type = type.getSuperclass()) {
            try {
                return DiscoverySelectors.selectMethod(testClass, type.getDeclaredMethod(methodName));
            } catch (NoSuchMethodException ignored) {
                // Not declared on this level; keep looking up the hierarchy.
            }
        }
        for (Method method : testClass.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                return DiscoverySelectors.selectMethod(testClass, method);
            }
        }
        return DiscoverySelectors.selectMethod(testClass, methodName);
    }

    private static boolean isCancelled(CancellationToken cancellationToken) {
        return cancellationToken != null && cancellationToken.isCancellationRequested();
    }

    /** Runs all {@code units} in one JUnit execution; adds the key of every unit that reported something. */
    private static void executeTogether(String partitionId, String sessionId, int attempt, List<WorkUnitSpec> units,
                                        ClassLoader classLoader, CitrusEndpointCollector endpointCollector,
                                        CancellationToken cancellationToken, Consumer<WorkerTestEventPayload> eventSink,
                                        Set<String> reported) throws ClassNotFoundException {
        List<DiscoverySelector> selectors = new ArrayList<>();
        for (WorkUnitSpec unit : units) {
            selectors.add(selectorFor(unit, classLoader));
        }
        UnitIndex index = new UnitIndex(units);
        setDynamicSourceFilterIfPresent(classLoader, null);
        try (LauncherSession session = LauncherFactory.openSession()) {
            LauncherDiscoveryRequest discoveryRequest = LauncherDiscoveryRequestBuilder.request()
                    .selectors(selectors)
                    .build();
            EventListener listener = new EventListener(
                    new EventFactory(partitionId, sessionId, attempt, null), endpointCollector, eventSink);
            listener.events = new EventFactory(partitionId, sessionId, attempt, listener::unitOf);
            listener.index = index;
            listener.reportedUnits = reported;
            LauncherExecutionRequestBuilder execution = LauncherExecutionRequestBuilder.request(discoveryRequest)
                    .listeners(listener);
            if (cancellationToken != null) {
                execution.cancellationToken(cancellationToken);
            }
            session.getLauncher().execute(execution.build());
        }
    }

    /**
     * Work unit of a test or container: the most specific method unit, else the closest enclosing class unit. Units
     * are keyed by the chain of classes enclosing them in the plan, so the same inherited {@code @Nested} class under
     * two subclasses maps to two units.
     */
    private static final class UnitIndex {
        private final List<WorkUnitSpec> units;
        private final Map<String, WorkUnitSpec> byMethod = new java.util.HashMap<>();
        private final Map<String, WorkUnitSpec> byClass = new java.util.HashMap<>();

        UnitIndex(List<WorkUnitSpec> units) {
            this.units = units;
            Map<String, List<WorkUnitSpec>> byBareName = new java.util.LinkedHashMap<>();
            for (WorkUnitSpec unit : units) {
                String classKey = classKey(unit.enclosingClassNames(), unit.className());
                if (unit.methodName() != null && !unit.methodName().isBlank()) {
                    int open = unit.methodName().indexOf('(');
                    String bare = open > 0 ? unit.methodName().substring(0, open) : unit.methodName();
                    // The host names a method without parameters by its bare name, so that name is also its exact key.
                    String exact = open > 0 ? unit.methodName() : bare + "()";
                    byMethod.putIfAbsent(classKey + "#" + exact, unit);
                    byBareName.computeIfAbsent(classKey + "#" + bare, k -> new ArrayList<>()).add(unit);
                } else {
                    byClass.putIfAbsent(classKey, unit);
                }
            }
            // A bare name only identifies a unit when no other overload in the partition shares it.
            byBareName.forEach((key, owners) -> {
                if (owners.size() == 1) {
                    byMethod.putIfAbsent(key, owners.get(0));
                }
            });
        }

        static String classKey(List<String> enclosing, String className) {
            return enclosing == null || enclosing.isEmpty() ? className : String.join("/", enclosing) + "/" + className;
        }

        WorkUnitSpec lookup(TestIdentifier identifier, org.junit.platform.launcher.TestPlan plan) {
            for (TestIdentifier cur = identifier; cur != null;
                 cur = plan != null ? plan.getParent(cur).orElse(null) : null) {
                TestSource source = cur.getSource().orElse(null);
                if (source instanceof MethodSource ms) {
                    String classKey = classKeyOf(classContainer(cur, plan), plan, ms.getClassName());
                    String params = ms.getMethodParameterTypes();
                    WorkUnitSpec unit = byMethod.get(classKey + "#" + ms.getMethodName() + "(" + (params != null ? params : "") + ")");
                    if (unit == null) {
                        unit = byMethod.get(classKey + "#" + ms.getMethodName());
                    }
                    if (unit == null) {
                        unit = byClass.get(classKey);
                    }
                    if (unit != null) {
                        return unit;
                    }
                } else if (source instanceof ClassSource cs) {
                    WorkUnitSpec unit = byClass.get(classKeyOf(cur, plan, cs.getClassName()));
                    if (unit != null) {
                        return unit;
                    }
                }
                if (plan == null) {
                    break;
                }
            }
            return null;
        }

        /** Every unit of the class container {@code identifier} and the classes nested in it. */
        List<WorkUnitSpec> unitsWithin(TestIdentifier identifier, org.junit.platform.launcher.TestPlan plan) {
            if (!(identifier.getSource().orElse(null) instanceof ClassSource cs)) {
                return List.of();
            }
            String classKey = classKeyOf(identifier, plan, cs.getClassName());
            return units.stream()
                    .filter(u -> {
                        String key = classKey(u.enclosingClassNames(), u.className());
                        return key.equals(classKey) || key.startsWith(classKey + "/");
                    })
                    .toList();
        }

        private static TestIdentifier classContainer(TestIdentifier node, org.junit.platform.launcher.TestPlan plan) {
            for (TestIdentifier cur = node; cur != null && plan != null; cur = plan.getParent(cur).orElse(null)) {
                if (cur.getSource().orElse(null) instanceof ClassSource) {
                    return cur;
                }
            }
            return null;
        }

        private static String classKeyOf(TestIdentifier classNode, org.junit.platform.launcher.TestPlan plan,
                                         String fallbackClassName) {
            if (classNode == null || plan == null || !(classNode.getSource().orElse(null) instanceof ClassSource cs)) {
                return fallbackClassName;
            }
            List<String> enclosing = new ArrayList<>();
            Optional<TestIdentifier> parent = plan.getParent(classNode);
            while (parent.isPresent()) {
                if (parent.get().getSource().orElse(null) instanceof ClassSource outer) {
                    enclosing.add(0, outer.getClassName());
                }
                parent = plan.getParent(parent.get());
            }
            return classKey(enclosing, cs.getClassName());
        }
    }

    /**
     * Runs one work unit and returns whether anything was reported for it (a test, a skipped or failed container).
     */
    private static boolean executeWorkUnit(EventFactory events,
                                           ClassLoader classLoader,
                                           CitrusEndpointCollector endpointCollector,
                                           CancellationToken cancellationToken,
                                           Consumer<WorkerTestEventPayload> eventSink) throws ClassNotFoundException {
        WorkUnitSpec unit = events.unit(null);
        DiscoverySelector selector = unit.dynamicSourceName() == null
                ? selectorFor(unit, classLoader)
                : selectorFor(new WorkUnitSpec(unit.key(), unit.className(), null, null, Map.of(), 0L,
                        unit.enclosingClassNames()), classLoader);

        boolean dynamicFilter = unit.dynamicSourceName() != null && !unit.dynamicSourceName().isBlank();
        setDynamicSourceFilterIfPresent(classLoader, dynamicFilter ? Set.of(unit.dynamicSourceName()) : null);
        try (LauncherSession session = LauncherFactory.openSession()) {
            LauncherDiscoveryRequest discoveryRequest = LauncherDiscoveryRequestBuilder.request()
                    .selectors(selector)
                    .build();
            EventListener listener = new EventListener(events, endpointCollector, eventSink);
            LauncherExecutionRequestBuilder execution = LauncherExecutionRequestBuilder.request(discoveryRequest)
                    .listeners(listener);
            if (cancellationToken != null) {
                execution.cancellationToken(cancellationToken);
            }
            session.getLauncher().execute(execution.build());
            return listener.reported;
        } finally {
            setDynamicSourceFilterIfPresent(classLoader, null);
        }
    }

    private static void resetCitrusExtensionIfPresent(ClassLoader classLoader) {
        try {
            Class<?> citrusExt = Class.forName("org.citrusframework.junit.jupiter.CitrusExtension", false, classLoader);
            Method resetMethod = citrusExt.getDeclaredMethod("reset");
            resetMethod.setAccessible(true);
            resetMethod.invoke(null);
        } catch (Throwable ignored) {
            // Optional if CitrusExtension is not on the partition classpath
        }
    }

    private static void setDynamicSourceFilterIfPresent(ClassLoader classLoader, Set<String> allowed) {
        try {
            Class<?> filterClass = Class.forName(
                    "org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter", false, classLoader);
            if (allowed == null) {
                filterClass.getMethod("clear").invoke(null);
            } else {
                filterClass.getMethod("setAllowedSources", Set.class).invoke(null, new LinkedHashSet<>(allowed));
            }
        } catch (Throwable ignored) {
            // Optional if CitrusDynamicSourceFilter is not on the partition classpath
        }
    }

    private record EventFactory(String partitionId, String sessionId, int attempt,
                                java.util.function.Function<TestIdentifier, WorkUnitSpec> unitOf) {

        WorkUnitSpec unit(TestIdentifier identifier) {
            return unitOf != null ? unitOf.apply(identifier) : null;
        }

        WorkerTestEventPayload event(TestIdentifier identifier, TestOutcomeStatus status, long durationMs,
                                     String errorMessage, String stackTrace, List<String> endpoints) {
            WorkUnitSpec unit = unit(identifier);
            return new WorkerTestEventPayload(partitionId, sessionId, attempt, unit != null ? unit.key() : null,
                    resolveClassName(identifier, unit != null ? unit.className() : null), identifier.getDisplayName(), status, durationMs,
                    errorMessage, stackTrace, endpoints != null ? endpoints : List.of(), identifier.getUniqueId());
        }

        private static String resolveClassName(TestIdentifier identifier, String fallback) {
            TestSource source = identifier.getSource().orElse(null);
            if (source instanceof MethodSource ms) {
                return ms.getClassName();
            }
            if (source instanceof ClassSource cs) {
                return cs.getClassName();
            }
            return fallback;
        }
    }

    /**
     * Streams test events. Containers (classes, engines) only produce events when they are skipped or fail, since
     * a failing {@code @BeforeAll} or engine otherwise leaves no trace: its tests never start.
     */
    private static final class EventListener implements TestExecutionListener {

        private EventFactory events;
        private final CitrusEndpointCollector endpointCollector;
        private final Consumer<WorkerTestEventPayload> eventSink;
        private final Map<String, Long> startTimes = new ConcurrentHashMap<>();
        private volatile boolean reported;
        /** Set for a multi-unit execution: resolves each event's unit and collects the units that reported. */
        private UnitIndex index;
        private Set<String> reportedUnits;
        private volatile org.junit.platform.launcher.TestPlan plan;

        @Override
        public void testPlanExecutionStarted(org.junit.platform.launcher.TestPlan testPlan) {
            this.plan = testPlan;
        }

        WorkUnitSpec unitOf(TestIdentifier identifier) {
            return unitForEvent(identifier);
        }

        private void markReported(TestIdentifier identifier) {
            reported = true;
            if (reportedUnits != null) {
                WorkUnitSpec unit = index != null ? index.lookup(identifier, plan) : events.unit(identifier);
                if (unit != null) {
                    reportedUnits.add(unit.key());
                } else if (index != null && identifier.isContainer()) {
                    // A skipped or failed class (e.g. @Disabled, failing @BeforeAll) that was split into method and
                    // nested-class units: none of its tests run, and that outcome covers all of them.
                    index.unitsWithin(identifier, plan).forEach(u -> reportedUnits.add(u.key()));
                }
            }
        }

        WorkUnitSpec unitForEvent(TestIdentifier identifier) {
            WorkUnitSpec unit = index != null ? index.lookup(identifier, plan) : null;
            if (unit == null && index != null && identifier.isContainer()) {
                List<WorkUnitSpec> within = index.unitsWithin(identifier, plan);
                return within.isEmpty() ? null : within.get(0);
            }
            return unit;
        }

        private EventListener(
                EventFactory events,
                CitrusEndpointCollector endpointCollector,
                Consumer<WorkerTestEventPayload> eventSink) {
            this.events = events;
            this.endpointCollector = endpointCollector;
            this.eventSink = eventSink;
        }

        @Override
        public void executionStarted(TestIdentifier testIdentifier) {
            if (testIdentifier.isTest()) {
                markReported(testIdentifier);
                endpointCollector.startTest();
                startTimes.put(testIdentifier.getUniqueId(), System.currentTimeMillis());
                eventSink.accept(events.event(testIdentifier, TestOutcomeStatus.STARTED, 0L, null, null, List.of()));
            }
        }

        @Override
        public void executionSkipped(TestIdentifier testIdentifier, String reason) {
            if (testIdentifier.isTest() || testIdentifier.getSource().isPresent()) {
                markReported(testIdentifier);
                eventSink.accept(events.event(testIdentifier, TestOutcomeStatus.SKIPPED, 0L, reason, null, List.of()));
            }
        }

        @Override
        public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult result) {
            if (!testIdentifier.isTest()) {
                if (result.getStatus() == TestExecutionResult.Status.FAILED) {
                    markReported(testIdentifier);
                    eventSink.accept(events.event(testIdentifier, TestOutcomeStatus.FAILED, 0L,
                            result.getThrowable().map(Throwable::getMessage).orElse("Container failed"),
                            result.getThrowable().map(EventListener::formatStackTrace).orElse(null),
                            endpointCollector.drainEndpoints()));
                }
                return;
            }
            long started = startTimes.getOrDefault(testIdentifier.getUniqueId(), System.currentTimeMillis());
            TestOutcomeStatus status = switch (result.getStatus()) {
                case SUCCESSFUL -> TestOutcomeStatus.PASSED;
                case ABORTED -> TestOutcomeStatus.SKIPPED;
                case FAILED -> TestOutcomeStatus.FAILED;
            };
            eventSink.accept(events.event(testIdentifier, status, Math.max(0L, System.currentTimeMillis() - started),
                    result.getThrowable().map(Throwable::getMessage).orElse(null),
                    result.getThrowable().map(EventListener::formatStackTrace).orElse(null),
                    endpointCollector.drainEndpoints()));
        }

        private static String formatStackTrace(Throwable throwable) {
            StringWriter sw = new StringWriter();
            throwable.printStackTrace(new PrintWriter(sw));
            return sw.toString();
        }
    }
}
