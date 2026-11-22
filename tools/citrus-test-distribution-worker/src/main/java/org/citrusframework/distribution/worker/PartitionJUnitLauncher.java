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
     * Executes {@code workUnits} one after another with {@code testClassLoader} as context class loader, streaming a
     * {@link WorkerTestEventPayload} for every test start, skip and finish. Stops before the next work unit once
     * {@code cancellationToken} is cancelled.
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
        try (CitrusEndpointCollector endpointCollector = CitrusEndpointCollector.install(testClassLoader)) {
            resetCitrusExtensionIfPresent(testClassLoader);
            List<String> unmatched = new ArrayList<>();
            for (WorkUnitSpec unit : workUnits) {
                if (cancellationToken != null && cancellationToken.isCancellationRequested()) {
                    break;
                }
                boolean reported = executeWorkUnit(
                        new EventFactory(partitionId, sessionId, attempt, unit),
                        testClassLoader,
                        endpointCollector,
                        cancellationToken,
                        eventSink);
                if (!reported && (cancellationToken == null || !cancellationToken.isCancellationRequested())) {
                    unmatched.add(unit.key() + " (" + (unit.methodName() != null ? unit.methodName() : unit.className()) + ")");
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

    /**
     * Runs one work unit and returns whether anything was reported for it (a test, a skipped or failed container).
     */
    private static boolean executeWorkUnit(EventFactory events,
                                           ClassLoader classLoader,
                                           CitrusEndpointCollector endpointCollector,
                                           CancellationToken cancellationToken,
                                           Consumer<WorkerTestEventPayload> eventSink) throws ClassNotFoundException {
        WorkUnitSpec unit = events.unit();
        Class<?> testClass = Class.forName(unit.className(), false, classLoader);
        DiscoverySelector selector = unit.methodName() != null && !unit.methodName().isBlank() && unit.dynamicSourceName() == null
                ? methodSelector(testClass, unit.methodName())
                : DiscoverySelectors.selectClass(testClass);

        boolean dynamicFilter = unit.dynamicSourceName() != null && !unit.dynamicSourceName().isBlank();
        setDynamicSourceFilterIfPresent(classLoader, dynamicFilter ? Set.of(unit.dynamicSourceName()) : null);
        try (LauncherSession session = LauncherFactory.openSession()) {
            LauncherDiscoveryRequest discoveryRequest = LauncherDiscoveryRequestBuilder.request()
                    .selectors(selector)
                    .configurationParameter("junit.platform.launcher.interceptors.enabled", "false")
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

    private record EventFactory(String partitionId, String sessionId, int attempt, WorkUnitSpec unit) {

        WorkerTestEventPayload event(TestIdentifier identifier, TestOutcomeStatus status, long durationMs,
                                     String errorMessage, String stackTrace, List<String> endpoints) {
            return new WorkerTestEventPayload(partitionId, sessionId, attempt, unit.key(),
                    resolveClassName(identifier, unit.className()), identifier.getDisplayName(), status, durationMs,
                    errorMessage, stackTrace, endpoints != null ? endpoints : List.of());
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

        private final EventFactory events;
        private final CitrusEndpointCollector endpointCollector;
        private final Consumer<WorkerTestEventPayload> eventSink;
        private final Map<String, Long> startTimes = new ConcurrentHashMap<>();
        private volatile boolean reported;

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
                reported = true;
                endpointCollector.startTest();
                startTimes.put(testIdentifier.getUniqueId(), System.currentTimeMillis());
                eventSink.accept(events.event(testIdentifier, TestOutcomeStatus.STARTED, 0L, null, null, List.of()));
            }
        }

        @Override
        public void executionSkipped(TestIdentifier testIdentifier, String reason) {
            if (testIdentifier.isTest() || testIdentifier.getSource().isPresent()) {
                reported = true;
                eventSink.accept(events.event(testIdentifier, TestOutcomeStatus.SKIPPED, 0L, reason, null, List.of()));
            }
        }

        @Override
        public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult result) {
            if (!testIdentifier.isTest()) {
                if (result.getStatus() == TestExecutionResult.Status.FAILED) {
                    reported = true;
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
