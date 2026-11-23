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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.AbstractTestDescriptor;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

/**
 * Replays remote worker test events to the host's JUnit listeners as if the tests had run locally.
 * <p>
 * Listeners such as Surefire's only count a test that belongs to the {@link TestPlan} they were given and that is
 * nested in a started class container (one test set per class). Events are therefore resolved to the plan's own
 * {@link TestIdentifier} by the JUnit unique id the worker reports, falling back to class and method name. Tests the
 * plan cannot contain up front (dynamic tests) get a synthetic identifier under their closest ancestor in the plan.
 * Ancestor containers are started before their first test and finished by {@link #finishStartedContainers()}.
 */
final class RemoteResultReplayer {

    private final TestPlan plan;
    private final List<TestExecutionListener> listeners;
    private final Set<String> startedContainers = new HashSet<>();
    private final Deque<TestIdentifier> containerStack = new ArrayDeque<>();
    private final Set<String> finishedContainers = new HashSet<>();
    private final Set<String> startedTests = new HashSet<>();
    private Map<String, List<TestIdentifier>> testsByClass;
    private final Map<String, Map<Integer, List<WorkerTestEventPayload>>> pending = new HashMap<>();
    /** Work unit key to test class, and the number of unreported units per class; empty when unknown. */
    private final Map<String, String> classByUnitKey = new HashMap<>();
    private final Map<String, Integer> remainingUnitsByClass = new HashMap<>();
    /**
     * Per class, how many classes directly nested in it are still open (have unreported units, or open classes nested
     * in them); a class closes only once this is zero.
     */
    private final Map<String, Integer> pendingNestedByClass = new HashMap<>();
    private Map<String, TestIdentifier> classContainers;

    RemoteResultReplayer(TestPlan plan, List<TestExecutionListener> listeners) {
        this(plan, listeners, List.of());
    }

    /**
     * @param expectedUnits every work unit whose results will be replayed (remote and local); a class test set is closed
     *                      as soon as all of its units have reported, otherwise by {@link #finishStartedContainers()}
     */
    RemoteResultReplayer(TestPlan plan, List<TestExecutionListener> listeners,
                         java.util.Collection<org.citrusframework.distribution.model.WorkUnitSpec> expectedUnits) {
        this.plan = plan;
        this.listeners = listeners;
        for (org.citrusframework.distribution.model.WorkUnitSpec unit : expectedUnits) {
            if (classByUnitKey.putIfAbsent(unit.key(), unit.className()) == null) {
                remainingUnitsByClass.merge(unit.className(), 1, Integer::sum);
            }
        }
        Set<String> open = new HashSet<>(remainingUnitsByClass.keySet());
        for (String className : remainingUnitsByClass.keySet()) {
            open.addAll(outerClassesOf(className));
        }
        for (String className : open) {
            directOuterClassOf(className).ifPresent(outer -> pendingNestedByClass.merge(outer, 1, Integer::sum));
        }
    }

    /** Holds a remote event until its partition attempt completes; see {@link #complete} and {@link #abandon}. */
    synchronized void buffer(WorkerTestEventPayload payload) {
        pending.computeIfAbsent(String.valueOf(payload.partitionId()), k -> new HashMap<>())
                .computeIfAbsent(payload.attempt(), k -> new ArrayList<>())
                .add(payload);
    }

    /**
     * Replays the events of the attempt that completed {@code partitionId} and forgets all its other attempts, so a
     * test re-run by a retry is reported once, with its final outcome.
     *
     * @return the replayed events, in arrival order
     */
    synchronized List<WorkerTestEventPayload> complete(String partitionId, int attempt) {
        return complete(partitionId, attempt, null);
    }

    /**
     * Like {@link #complete(String, int)}; {@code completedUnitKeys} are the work units the partition ran, whose class
     * test sets are closed once every unit of the class has reported ({@code null} when unknown).
     */
    synchronized List<WorkerTestEventPayload> complete(String partitionId, int attempt,
                                                       java.util.Collection<String> completedUnitKeys) {
        Map<Integer, List<WorkerTestEventPayload>> attempts = pending.remove(String.valueOf(partitionId));
        List<WorkerTestEventPayload> events = attempts != null ? attempts.getOrDefault(attempt, List.of()) : List.of();
        events.forEach(this::replay);
        unitsDone(completedUnitKeys);
        return events;
    }

    /**
     * Listener for work units executed in this JVM (pinned or lost units): forwards their events live, mapped onto the
     * host plan and its class test sets, so the host listeners see real durations, output, and outcomes. Call
     * {@link #localUnitsDone} once the units have run.
     */
    TestExecutionListener liveListener() {
        return new TestExecutionListener() {
            @Override
            public void executionStarted(TestIdentifier local) {
                synchronized (RemoteResultReplayer.this) {
                    TestIdentifier id = resolveLocal(local);
                    startAncestors(id);
                    if (id.isContainer()) {
                        startContainer(id);
                    } else {
                        start(id);
                    }
                }
            }

            @Override
            public void executionSkipped(TestIdentifier local, String reason) {
                synchronized (RemoteResultReplayer.this) {
                    TestIdentifier id = resolveLocal(local);
                    startAncestors(id);
                    if (id.isContainer()) {
                        if (startedContainers.add(id.getUniqueId()) && finishedContainers.add(id.getUniqueId())) {
                            listeners.forEach(l -> l.executionSkipped(id, reason));
                        }
                    } else {
                        listeners.forEach(l -> l.executionSkipped(id, reason));
                    }
                }
            }

            @Override
            public void executionFinished(TestIdentifier local, TestExecutionResult result) {
                synchronized (RemoteResultReplayer.this) {
                    TestIdentifier id = resolveLocal(local);
                    if (id.isContainer()) {
                        // Successful containers close with their class; a failure (e.g. @BeforeAll) is reported now.
                        if (result.getStatus() != TestExecutionResult.Status.SUCCESSFUL
                                && startedContainers.contains(id.getUniqueId())
                                && finishedContainers.add(id.getUniqueId())) {
                            containerStack.remove(id);
                            listeners.forEach(l -> l.executionFinished(id, result));
                        }
                        return;
                    }
                    start(id);
                    listeners.forEach(l -> l.executionFinished(id, result));
                    startedTests.remove(id.getUniqueId());
                }
            }

            @Override
            public void reportingEntryPublished(TestIdentifier local, org.junit.platform.engine.reporting.ReportEntry entry) {
                synchronized (RemoteResultReplayer.this) {
                    TestIdentifier id = resolveLocal(local);
                    listeners.forEach(l -> l.reportingEntryPublished(id, entry));
                }
            }
        };
    }

    /** Marks work units executed in this JVM as reported, closing class test sets that have nothing pending. */
    synchronized void localUnitsDone(java.util.Collection<String> unitKeys) {
        unitsDone(unitKeys);
    }

    private TestIdentifier resolveLocal(TestIdentifier local) {
        UniqueId uniqueId = UniqueId.parse(local.getUniqueId());
        Optional<TestIdentifier> inPlan = findInPlan(uniqueId);
        if (inPlan.isPresent()) {
            return inPlan.get();
        }
        String className = local.getSource().orElse(null) instanceof MethodSource ms ? ms.getClassName()
                : local.getSource().orElse(null) instanceof ClassSource cs ? cs.getClassName() : null;
        return synthesize(new WorkerTestEventPayload("local", null, 1, null, className, local.getDisplayName(),
                TestOutcomeStatus.STARTED, 0L, null, null, List.of(), local.getUniqueId()), uniqueId);
    }

    private void unitsDone(java.util.Collection<String> unitKeys) {
        if (unitKeys == null) {
            return;
        }
        for (String key : unitKeys) {
            String className = classByUnitKey.remove(key);
            if (className != null && remainingUnitsByClass.merge(className, -1, Integer::sum) <= 0) {
                remainingUnitsByClass.remove(className);
                finishClass(className);
            }
        }
    }

    /** Closes the test set of {@code className} and the containers nested in it, unless a nested class still runs. */
    private void finishClass(String className) {
        if (pendingNestedByClass.getOrDefault(className, 0) > 0) {
            return;
        }
        Optional<TestIdentifier> found = findClassContainer(className);
        if (found.isPresent() && startedContainers.contains(found.get().getUniqueId())) {
            TestIdentifier container = found.get();
            String prefix = container.getUniqueId() + "/";
            for (TestIdentifier inner : new ArrayList<>(containerStack)) {
                if (inner.getUniqueId().startsWith(prefix)) {
                    finishContainer(inner);
                }
            }
            finishContainer(container);
        }
        // This class is done: its directly enclosing class may have been waiting only for it.
        directOuterClassOf(className).ifPresent(outer -> {
            if (pendingNestedByClass.merge(outer, -1, Integer::sum) <= 0) {
                pendingNestedByClass.remove(outer);
                if (!remainingUnitsByClass.containsKey(outer)) {
                    finishClass(outer);
                }
            }
        });
    }

    private Optional<String> directOuterClassOf(String className) {
        List<String> outer = outerClassesOf(className);
        return outer.isEmpty() ? Optional.empty() : Optional.of(outer.get(0));
    }

    /** Enclosing class containers of {@code className}, innermost first. */
    private List<String> outerClassesOf(String className) {
        List<String> outer = new ArrayList<>();
        Optional<TestIdentifier> cur = findClassContainer(className).flatMap(plan::getParent);
        while (cur.isPresent()) {
            if (cur.get().getSource().orElse(null) instanceof ClassSource cs) {
                outer.add(cs.getClassName());
            }
            cur = plan.getParent(cur.get());
        }
        return outer;
    }

    private void finishContainer(TestIdentifier container) {
        containerStack.remove(container);
        if (finishedContainers.add(container.getUniqueId())) {
            for (TestExecutionListener l : listeners) {
                l.executionFinished(container, TestExecutionResult.successful());
            }
        }
    }

    /** Drops the buffered events of a requeued or lost partition; a retry or local re-run reports the tests again. */
    synchronized void abandon(String partitionId) {
        pending.remove(String.valueOf(partitionId));
    }

    private void replay(WorkerTestEventPayload payload) {
        TestIdentifier id = resolve(payload);
        startAncestors(id);

        if (id.isContainer()) {
            if (payload.status() == TestOutcomeStatus.SKIPPED) {
                skipContainer(id, payload);
            } else {
                replayContainer(id, payload);
            }
            return;
        }
        switch (payload.status()) {
            case STARTED -> start(id);
            case SKIPPED -> {
                String reason = payload.errorMessage() != null ? payload.errorMessage() : "skipped";
                if (startedTests.remove(id.getUniqueId())) {
                    // Workers report an aborted test (failed assumption) as SKIPPED after it STARTED.
                    TestExecutionResult aborted = TestExecutionResult.aborted(new org.opentest4j.TestAbortedException(reason));
                    for (TestExecutionListener l : listeners) {
                        l.executionFinished(id, aborted);
                    }
                } else {
                    for (TestExecutionListener l : listeners) {
                        l.executionSkipped(id, reason);
                    }
                }
            }
            default -> {
                start(id);
                TestExecutionResult result = payload.status() == TestOutcomeStatus.PASSED
                        ? TestExecutionResult.successful()
                        : TestExecutionResult.failed(remoteFailure(payload));
                for (TestExecutionListener l : listeners) {
                    l.executionFinished(id, result);
                }
                startedTests.remove(id.getUniqueId());
            }
        }
    }

    /** Finishes every container started by {@link #replay}, innermost first. */
    synchronized void finishStartedContainers() {
        pending.clear();
        while (!containerStack.isEmpty()) {
            finishContainer(containerStack.peek());
        }
    }

    private void skipContainer(TestIdentifier container, WorkerTestEventPayload payload) {
        if (startedContainers.add(container.getUniqueId()) && finishedContainers.add(container.getUniqueId())) {
            for (TestExecutionListener l : listeners) {
                l.executionSkipped(container, payload.errorMessage() != null ? payload.errorMessage() : "skipped");
            }
        }
    }

    private void replayContainer(TestIdentifier container, WorkerTestEventPayload payload) {
        startContainer(container);
        if (payload.status() == TestOutcomeStatus.FAILED && finishedContainers.add(container.getUniqueId())) {
            for (TestExecutionListener l : listeners) {
                l.executionFinished(container, TestExecutionResult.failed(remoteFailure(payload)));
            }
        }
    }

    private void start(TestIdentifier test) {
        if (startedTests.add(test.getUniqueId())) {
            for (TestExecutionListener l : listeners) {
                l.executionStarted(test);
            }
        }
    }

    private void startAncestors(TestIdentifier id) {
        List<TestIdentifier> chain = new ArrayList<>();
        Optional<TestIdentifier> parent = parentInPlan(id);
        while (parent.isPresent()) {
            chain.add(0, parent.get());
            parent = plan.getParent(parent.get());
        }
        chain.forEach(this::startContainer);
    }

    private void startContainer(TestIdentifier container) {
        if (startedContainers.add(container.getUniqueId())) {
            containerStack.push(container);
            for (TestExecutionListener l : listeners) {
                l.executionStarted(container);
            }
        }
    }

    private Optional<TestIdentifier> parentInPlan(TestIdentifier id) {
        return id.getParentIdObject().flatMap(this::findInPlan);
    }

    private TestIdentifier resolve(WorkerTestEventPayload payload) {
        UniqueId uniqueId = parseUniqueId(payload.uniqueId());
        if (uniqueId != null) {
            // A reported id missing from the plan is a runtime invocation (dynamic, parameterized, repeated):
            // matching it by name could hit its template container, so it goes under its plan ancestor instead.
            return findInPlan(uniqueId).orElseGet(() -> synthesize(payload, uniqueId));
        }
        Optional<TestIdentifier> byName = findByName(payload.className(), payload.testName());
        if (byName.isPresent()) {
            return byName.get();
        }
        return synthesize(payload, uniqueId);
    }

    private TestIdentifier synthesize(WorkerTestEventPayload payload, UniqueId uniqueId) {
        String className = payload.className() != null ? payload.className() : "DistributedTest";
        String testName = payload.testName() != null ? payload.testName() : payload.workUnitKey();
        Optional<TestIdentifier> parent = Optional.empty();
        if (uniqueId != null) {
            UniqueId cur = uniqueId;
            while (parent.isEmpty() && cur.getSegments().size() > 1) {
                cur = cur.removeLastSegment();
                parent = findInPlan(cur);
            }
        }
        if (parent.isEmpty()) {
            parent = findClassContainer(className);
        }
        UniqueId id = uniqueId != null ? uniqueId
                : parent.map(p -> UniqueId.parse(p.getUniqueId()))
                        .orElseGet(() -> UniqueId.forEngine("junit-jupiter").append("class", className))
                        .append("remote-test", testName);
        SyntheticDescriptor descriptor = new SyntheticDescriptor(id, testName, ClassSource.from(className),
                org.junit.platform.engine.TestDescriptor.Type.TEST);
        parent.ifPresent(p -> descriptor.setParent(new SyntheticDescriptor(UniqueId.parse(p.getUniqueId()),
                p.getDisplayName(), p.getSource().orElse(null), org.junit.platform.engine.TestDescriptor.Type.CONTAINER)));
        return TestIdentifier.from(descriptor);
    }

    private Optional<TestIdentifier> findByName(String className, String testName) {
        if (className == null || testName == null) {
            return Optional.empty();
        }
        for (TestIdentifier candidate : testsByClass().getOrDefault(className, List.of())) {
            if (candidate.isTest() && candidate.getSource().orElse(null) instanceof MethodSource ms
                    && (testName.equals(candidate.getDisplayName()) || testName.equals(ms.getMethodName() + "()")
                    || testName.equals(ms.getMethodName()))) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    private Optional<TestIdentifier> findClassContainer(String className) {
        if (classContainers == null) {
            // One pass over the plan; the first (outermost) container of a class wins, as in a breadth-first search.
            classContainers = new HashMap<>();
            Deque<TestIdentifier> queue = new ArrayDeque<>(plan.getRoots());
            while (!queue.isEmpty()) {
                TestIdentifier id = queue.poll();
                if (id.isContainer() && id.getSource().orElse(null) instanceof ClassSource cs) {
                    classContainers.putIfAbsent(cs.getClassName(), id);
                }
                queue.addAll(plan.getChildren(id));
            }
        }
        return Optional.ofNullable(classContainers.get(className));
    }

    private Map<String, List<TestIdentifier>> testsByClass() {
        if (testsByClass == null) {
            testsByClass = new HashMap<>();
            Deque<TestIdentifier> queue = new ArrayDeque<>(plan.getRoots());
            while (!queue.isEmpty()) {
                TestIdentifier id = queue.poll();
                queue.addAll(plan.getChildren(id));
                if (id.getSource().orElse(null) instanceof MethodSource ms) {
                    testsByClass.computeIfAbsent(ms.getClassName(), k -> new ArrayList<>()).add(id);
                }
            }
        }
        return testsByClass;
    }

    private Optional<TestIdentifier> findInPlan(UniqueId uniqueId) {
        try {
            return Optional.of(plan.getTestIdentifier(uniqueId));
        } catch (RuntimeException notInPlan) {
            return Optional.empty();
        }
    }

    private static UniqueId parseUniqueId(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return UniqueId.parse(value);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static AssertionError remoteFailure(WorkerTestEventPayload payload) {
        AssertionError error = new AssertionError(
                payload.errorMessage() != null ? payload.errorMessage() : "Remote test failed");
        if (payload.stackTrace() != null && !payload.stackTrace().isBlank()) {
            error.addSuppressed(new RemoteStackTrace(payload.stackTrace()));
        }
        return error;
    }

    /** Carries the worker's stack trace text so reports show where the remote test failed. */
    private static final class RemoteStackTrace extends RuntimeException {
        RemoteStackTrace(String stackTrace) {
            super("Remote stack trace:\n" + stackTrace, null, false, false);
        }
    }

    private static final class SyntheticDescriptor extends AbstractTestDescriptor {
        private final Type type;

        SyntheticDescriptor(UniqueId uniqueId, String displayName, TestSource source, Type type) {
            super(uniqueId, displayName, source);
            this.type = type;
        }

        @Override
        public Type getType() {
            return type;
        }
    }
}
