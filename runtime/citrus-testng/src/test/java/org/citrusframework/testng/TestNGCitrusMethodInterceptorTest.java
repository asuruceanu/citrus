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

package org.citrusframework.testng;

import java.util.ArrayList;
import java.util.List;

import org.citrusframework.CitrusContext;
import org.citrusframework.TestCase;
import org.citrusframework.report.AbstractTestListener;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;

public class TestNGCitrusMethodInterceptorTest {

    @Test
    public void reportsEachPlainSourceOnce() {
        assertInvocations(PlainSourceFixture.class, "runSources", PlainSourceFixture.SOURCES, "EchoTest", "SecondEchoTest");
    }

    @Test
    public void reportsEachSpringSourceOnce() {
        assertInvocations(SpringSourceFixture.class, "runSources", SpringSourceFixture.SOURCES, "EchoActionIT", "FailActionIT");
    }

    @Test
    public void retainsExplicitInvocationsPerSource() {
        assertInvocations(PlainSourceFixture.class, "repeatSources", PlainSourceFixture.SOURCES,
                "EchoTest", "SecondEchoTest", "EchoTest", "SecondEchoTest");
    }

    private static void assertInvocations(Class<?> fixture, String method, List<String> sources, String... expected) {
        sources.clear();
        var summaryListener = new SummaryGeneratingListener();
        List<String> invocationIds = new ArrayList<>();
        LauncherFactory.create().execute(LauncherDiscoveryRequestBuilder.request()
                .selectors(selectMethod(fixture, method))
                .build(), summaryListener, new TestExecutionListener() {
                    @Override
                    public void dynamicTestRegistered(TestIdentifier test) {
                        invocationIds.add(test.getUniqueId());
                    }
                });

        var summary = summaryListener.getSummary();
        assertThat(summary.getFailures()).isEmpty();
        assertThat(summary.getTestsSucceededCount()).isEqualTo(expected.length);
        assertThat(sources).containsExactlyInAnyOrder(expected);
        assertThat(invocationIds).hasSize(expected.length).doesNotHaveDuplicates();
    }

    static void recordSources(CitrusContext context, List<String> sources) {
        context.addTestListener(new AbstractTestListener() {
            @Override
            public void onTestSuccess(TestCase test) {
                sources.add(test.getName());
            }
        });
    }
}
