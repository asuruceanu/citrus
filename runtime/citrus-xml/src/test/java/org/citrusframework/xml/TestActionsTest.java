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

package org.citrusframework.xml;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.DocumentBuilderFactory;

import jakarta.xml.bind.annotation.XmlAttribute;
import org.citrusframework.TestAction;
import org.citrusframework.TestActionBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

public class TestActionsTest {

    @Test
    public void shouldUnmarshalCustomActionsConcurrently() throws Exception {
        int threadCount = 16;
        int iterations = 250;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int thread = 0; thread < threadCount; thread++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();

                    for (int i = 0; i < iterations; i++) {
                        String value = "value-" + i;
                        List<TestActionBuilder<?>> builders = createTestActions(value).getActionBuilders();

                        Assertions.assertEquals(1, builders.size());
                        ConcurrentActionBuilder builder = (ConcurrentActionBuilder) builders.getFirst();
                        Assertions.assertEquals(value, builder.getValue());
                    }

                    return null;
                }));
            }

            Assertions.assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();

            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private TestActions createTestActions(String value) throws Exception {
        Element action = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .newDocument()
                .createElementNS(XmlTestLoader.TEST_NS, "concurrent-action");
        action.setAttribute("value", value);

        TestActions testActions = new TestActions();
        Field actions = TestActions.class.getDeclaredField("actions");
        actions.setAccessible(true);
        actions.set(testActions, List.of(action));
        return testActions;
    }

    public static class ConcurrentActionBuilder implements TestActionBuilder<TestAction> {

        @XmlAttribute
        private String value;

        public String getValue() {
            return value;
        }

        @Override
        public TestAction build() {
            return null;
        }
    }
}
