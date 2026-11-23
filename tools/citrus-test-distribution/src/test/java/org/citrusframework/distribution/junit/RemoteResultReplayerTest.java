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
import java.util.List;

import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteResultReplayerTest {

    @Test
    void shouldReportOnlyTheAttemptThatCompletedWhenAPartitionIsRetried() {
        TestPlan plan = discover(RetriedFixture.class);
        Recorder recorder = new Recorder();
        RemoteResultReplayer replayer = new RemoteResultReplayer(plan, List.of(recorder));
        String test = methodId(plan, "flaky");

        replayer.buffer(event("p1", 1, test, "flaky()", TestOutcomeStatus.STARTED));
        replayer.buffer(event("p1", 1, test, "flaky()", TestOutcomeStatus.FAILED));
        replayer.abandon("p1");
        replayer.buffer(event("p1", 2, test, "flaky()", TestOutcomeStatus.STARTED));
        replayer.buffer(event("p1", 2, test, "flaky()", TestOutcomeStatus.PASSED));
        List<WorkerTestEventPayload> replayed = replayer.complete("p1", 2);
        replayer.finishStartedContainers();

        assertThat(recorder.finishedTests).containsExactly(test + "=SUCCESSFUL");
        assertThat(replayed).extracting(WorkerTestEventPayload::attempt).containsOnly(2);
    }

    @Test
    void shouldNotReplayResultsOfALostPartition() {
        TestPlan plan = discover(RetriedFixture.class);
        Recorder recorder = new Recorder();
        RemoteResultReplayer replayer = new RemoteResultReplayer(plan, List.of(recorder));
        String test = methodId(plan, "flaky");

        replayer.buffer(event("p1", 1, test, "flaky()", TestOutcomeStatus.STARTED));
        replayer.buffer(event("p1", 1, test, "flaky()", TestOutcomeStatus.FAILED));
        replayer.abandon("p1");
        replayer.finishStartedContainers();

        assertThat(recorder.finishedTests).isEmpty();
        assertThat(recorder.started).isEmpty();
    }

    @Test
    void shouldReportEachRepetitionAsATestEvenWhenItsNameEqualsTheTemplate() {
        TestPlan plan = discover(RepeatedFixture.class);
        Recorder recorder = new Recorder();
        RemoteResultReplayer replayer = new RemoteResultReplayer(plan, List.of(recorder));
        String template = methodId(plan, "repeated");

        for (int i = 1; i <= 2; i++) {
            String invocation = template + "/[test-template-invocation:#" + i + "]";
            replayer.buffer(event("p1", 1, invocation, "repeated()", TestOutcomeStatus.STARTED));
            replayer.buffer(event("p1", 1, invocation, "repeated()",
                    i == 1 ? TestOutcomeStatus.FAILED : TestOutcomeStatus.PASSED));
        }
        replayer.complete("p1", 1);
        replayer.finishStartedContainers();

        assertThat(recorder.finishedTests).containsExactly(
                template + "/[test-template-invocation:#1]=FAILED",
                template + "/[test-template-invocation:#2]=SUCCESSFUL");
        assertThat(recorder.failedContainers).isEmpty();
    }

    @Test
    void shouldReplayAnAbortedTestAsAbortedRatherThanSkippedAfterStart() {
        TestPlan plan = discover(RetriedFixture.class);
        Recorder recorder = new Recorder();
        RemoteResultReplayer replayer = new RemoteResultReplayer(plan, List.of(recorder));
        String test = methodId(plan, "flaky");

        replayer.buffer(event("p1", 1, test, "flaky()", TestOutcomeStatus.STARTED));
        replayer.buffer(event("p1", 1, test, "flaky()", TestOutcomeStatus.SKIPPED));
        replayer.complete("p1", 1);

        assertThat(recorder.finishedTests).containsExactly(test + "=ABORTED");
        assertThat(recorder.skipped).isEmpty();
    }

    @Test
    void shouldCloseEachClassTestSetOnceAllItsWorkUnitsHaveReported() {
        TestPlan plan = discoverAll(RetriedFixture.class, OtherFixture.class);
        Recorder recorder = new Recorder();
        RemoteResultReplayer replayer = new RemoteResultReplayer(plan, List.of(recorder), List.of(
                unit(RetriedFixture.class), unit(OtherFixture.class)));

        replayer.buffer(event("p1", 1, methodId(plan, "flaky"), "flaky()", TestOutcomeStatus.PASSED));
        replayer.complete("p1", 1, List.of(RetriedFixture.class.getName()));
        recorder.log.add("-- p2 --");
        replayer.buffer(event("p2", 1, methodId(plan, "other"), "other()", TestOutcomeStatus.PASSED));
        replayer.complete("p2", 1, List.of(OtherFixture.class.getName()));

        assertThat(recorder.log).containsSubsequence(
                "finish " + RetriedFixture.class.getName(), "-- p2 --", "finish " + OtherFixture.class.getName());
    }

    @Test
    void shouldReplayLocallyExecutedResultsIntoTheSameClassTestSet() {
        TestPlan plan = discover(RetriedFixture.class);
        Recorder recorder = new Recorder();
        RemoteResultReplayer replayer = new RemoteResultReplayer(plan, List.of(recorder), List.of(
                unit(RetriedFixture.class)));
        String test = methodId(plan, "flaky");

        LauncherFactory.create().execute(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(RetriedFixture.class)).build(), replayer.liveListener());
        replayer.localUnitsDone(List.of(RetriedFixture.class.getName()));

        assertThat(recorder.finishedTests).containsExactly(test + "=SUCCESSFUL");
        assertThat(recorder.log).containsOnlyOnce("start " + RetriedFixture.class.getName())
                .contains("finish " + RetriedFixture.class.getName());
    }

    @Test
    void launcherShouldDescribeCyclicCausesWithoutLooping() {
        RuntimeException first = new RuntimeException((String) null);
        IllegalStateException second = new IllegalStateException((String) null);
        first.initCause(second);
        second.initCause(first);

        assertThat(org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(2),
                () -> DistributedLauncher.describe(first))).isEqualTo(RuntimeException.class.getName());
    }

    @Test
    void shouldCloseThousandsOfClassTestSetsWithoutRescanningThePlan() {
        int classes = 3000;
        org.junit.platform.engine.support.descriptor.EngineDescriptor engine =
                new org.junit.platform.engine.support.descriptor.EngineDescriptor(
                        org.junit.platform.engine.UniqueId.forEngine("junit-jupiter"), "JUnit Jupiter");
        List<org.citrusframework.distribution.model.WorkUnitSpec> units = new ArrayList<>();
        for (int i = 0; i < classes; i++) {
            String className = "com.example.Generated" + i + "Test";
            Node type = new Node(engine.getUniqueId().append("class", className), className,
                    org.junit.platform.engine.support.descriptor.ClassSource.from(className),
                    org.junit.platform.engine.TestDescriptor.Type.CONTAINER);
            type.addChild(new Node(type.getUniqueId().append("method", "test()"), "test()",
                    org.junit.platform.engine.support.descriptor.MethodSource.from(className, "test"),
                    org.junit.platform.engine.TestDescriptor.Type.TEST));
            engine.addChild(type);
            units.add(new org.citrusframework.distribution.model.WorkUnitSpec(className, className, null, null,
                    java.util.Map.of(), 0L));
        }
        TestPlan plan = TestPlan.from(true, List.of(engine), new EmptyConfiguration(), null);
        Recorder recorder = new Recorder();
        RemoteResultReplayer replayer = new RemoteResultReplayer(plan, List.of(recorder), units);

        org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(10), () -> {
            for (org.citrusframework.distribution.model.WorkUnitSpec unit : units) {
                String uid = "[engine:junit-jupiter]/[class:" + unit.className() + "]/[method:test()]";
                replayer.buffer(new WorkerTestEventPayload("p-" + unit.key(), "s1", 1, unit.key(), unit.className(),
                        "test()", TestOutcomeStatus.PASSED, 1L, null, null, List.of(), uid));
                replayer.complete("p-" + unit.key(), 1, List.of(unit.key()));
            }
        });

        assertThat(recorder.finishedTests).hasSize(classes);
        assertThat(recorder.log.stream().filter(line -> line.startsWith("finish "))).hasSize(classes);
    }

    @Test
    void shouldCloseOuterClassesOnlyAfterEveryNestedLevelReported() {
        TestPlan plan = discover(Outer.class);
        Recorder recorder = new Recorder();
        RemoteResultReplayer replayer = new RemoteResultReplayer(plan, List.of(recorder), List.of(
                unit(Outer.class), unit(Outer.Middle.class), unit(Outer.Middle.Inner.class)));

        replayer.buffer(event("p1", 1, methodId(plan, "outer"), "outer()", TestOutcomeStatus.PASSED));
        replayer.complete("p1", 1, List.of(Outer.class.getName()));
        replayer.buffer(event("p2", 1, methodId(plan, "middle"), "middle()", TestOutcomeStatus.PASSED));
        replayer.complete("p2", 1, List.of(Outer.Middle.class.getName()));
        recorder.log.add("-- inner --");
        replayer.buffer(event("p3", 1, methodId(plan, "inner"), "inner()", TestOutcomeStatus.PASSED));
        replayer.complete("p3", 1, List.of(Outer.Middle.Inner.class.getName()));

        assertThat(recorder.log.subList(recorder.log.indexOf("-- inner --"), recorder.log.size())
                .stream().filter(line -> !line.startsWith("start ")).toList()).containsExactly(
                "-- inner --",
                "finish " + Outer.Middle.Inner.class.getName(),
                "finish " + Outer.Middle.class.getName(),
                "finish " + Outer.class.getName());
    }

    static class Outer {
        @Test
        void outer() {
        }

        @org.junit.jupiter.api.Nested
        class Middle {
            @Test
            void middle() {
            }

            @org.junit.jupiter.api.Nested
            class Inner {
                @Test
                void inner() {
                }
            }
        }
    }

    private static final class Node extends org.junit.platform.engine.support.descriptor.AbstractTestDescriptor {
        private final Type type;

        Node(org.junit.platform.engine.UniqueId id, String name, org.junit.platform.engine.TestSource source, Type type) {
            super(id, name, source);
            this.type = type;
        }

        @Override
        public Type getType() {
            return type;
        }
    }

    private static final class EmptyConfiguration implements org.junit.platform.engine.ConfigurationParameters {
        @Override
        public java.util.Optional<String> get(String key) {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<Boolean> getBoolean(String key) {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Set<String> keySet() {
            return java.util.Set.of();
        }
    }

    private static org.citrusframework.distribution.model.WorkUnitSpec unit(Class<?> type) {
        return new org.citrusframework.distribution.model.WorkUnitSpec(type.getName(), type.getName(), null, null,
                java.util.Map.of(), 0L);
    }

    private static TestPlan discoverAll(Class<?>... fixtures) {
        return LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(java.util.Arrays.stream(fixtures).map(DiscoverySelectors::selectClass).toList())
                .build());
    }

    private static WorkerTestEventPayload event(String partition, int attempt, String uniqueId, String name,
                                                TestOutcomeStatus status) {
        String className = uniqueId.replaceAll(".*\\[class:([^\\]]+)\\].*", "$1");
        return new WorkerTestEventPayload(partition, "s1", attempt, "unit", className, name,
                status, 1L, status == TestOutcomeStatus.FAILED ? "boom" : null, null, List.of(), uniqueId);
    }

    private static TestPlan discover(Class<?> fixture) {
        return LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(fixture))
                .build());
    }

    private static String methodId(TestPlan plan, String method) {
        List<TestIdentifier> queue = new ArrayList<>(plan.getRoots());
        while (!queue.isEmpty()) {
            TestIdentifier id = queue.removeFirst();
            queue.addAll(plan.getChildren(id));
            if (id.getSource().orElse(null) instanceof org.junit.platform.engine.support.descriptor.MethodSource ms
                    && ms.getMethodName().equals(method)) {
                return id.getUniqueId();
            }
        }
        throw new AssertionError("no " + method + " in plan");
    }

    private static final class Recorder implements TestExecutionListener {
        final List<String> started = new ArrayList<>();
        final List<String> finishedTests = new ArrayList<>();
        final List<String> failedContainers = new ArrayList<>();
        final List<String> skipped = new ArrayList<>();
        final List<String> log = new ArrayList<>();

        @Override
        public void executionStarted(TestIdentifier id) {
            if (id.isTest()) {
                started.add(id.getUniqueId());
            } else if (id.getSource().orElse(null) instanceof org.junit.platform.engine.support.descriptor.ClassSource cs) {
                log.add("start " + cs.getClassName());
            }
        }

        @Override
        public void executionSkipped(TestIdentifier id, String reason) {
            skipped.add(id.getUniqueId());
        }

        @Override
        public void executionFinished(TestIdentifier id, TestExecutionResult result) {
            if (id.getSource().orElse(null) instanceof org.junit.platform.engine.support.descriptor.ClassSource cs) {
                log.add("finish " + cs.getClassName());
            }
            if (id.isTest()) {
                finishedTests.add(id.getUniqueId() + "=" + result.getStatus());
            } else if (result.getStatus() == TestExecutionResult.Status.FAILED) {
                failedContainers.add(id.getUniqueId());
            }
        }
    }

    static class RetriedFixture {
        @Test
        void flaky() {
        }
    }

    static class OtherFixture {
        @Test
        void other() {
        }
    }

    static class RepeatedFixture {
        @RepeatedTest(value = 2, name = "{displayName}")
        void repeated() {
        }
    }
}
