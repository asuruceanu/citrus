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

package org.citrusframework.distribution.config;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CitrusEndpointCollectorTest {

    @Test
    void shouldKeepEndpointsOfTestsRunningInParallelApart() throws Exception {
        try (CitrusEndpointCollector collector = CitrusEndpointCollector.install(getClass().getClassLoader())) {
            CountDownLatch bothStarted = new CountDownLatch(2);
            CountDownLatch bothRecorded = new CountDownLatch(2);

            CompletableFuture<List<String>> first = CompletableFuture.supplyAsync(
                    () -> runTest(collector, "httpClient", bothStarted, bothRecorded));
            CompletableFuture<List<String>> second = CompletableFuture.supplyAsync(
                    () -> runTest(collector, "kafkaEndpoint", bothStarted, bothRecorded));

            assertThat(first.get()).containsExactly("httpClient");
            assertThat(second.get()).containsExactly("kafkaEndpoint");
        }
    }

    private static List<String> runTest(CitrusEndpointCollector collector, String endpoint,
                                        CountDownLatch bothStarted, CountDownLatch bothRecorded) {
        try {
            collector.startTest();
            bothStarted.countDown();
            bothStarted.await();
            collector.record(endpoint);
            bothRecorded.countDown();
            bothRecorded.await();
            return collector.drainEndpoints();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
