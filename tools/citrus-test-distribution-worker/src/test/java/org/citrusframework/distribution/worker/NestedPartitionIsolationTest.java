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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.workspace.IsolatedTestLock;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.engine.CancellationToken;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NestedPartitionIsolationTest {

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void nestedSelectionWaitsForItsEnclosingClassLock(@TempDir Path home) throws Exception {
        String previousHome = System.setProperty("user.home", home.toString());
        ClassLoader loader = getClass().getClassLoader();
        String outer = IsolatedSuite.class.getName();
        List<WorkUnitSpec> holderUnits = List.of(new WorkUnitSpec("outer", outer, null, null, Map.of(), 1_000));
        List<WorkUnitSpec> nestedUnits = List.of(new WorkUnitSpec("nested", IsolatedSuite.NestedSuite.class.getName(),
                null, null, Map.of(), 1_000, List.of(outer)));
        var events = new ArrayList<WorkerTestEventPayload>();
        var started = new CountDownLatch(1);

        try (var executor = Executors.newSingleThreadExecutor()) {
            try (var nestedLock = IsolatedTestLock.acquire(nestedUnits, loader)) {
                assertThat(nestedLock).isNotNull();
            }
            Future<PartitionJUnitLauncher.ExecutionSummary> pending;
            try (var holder = IsolatedTestLock.acquire(holderUnits, loader)) {
                assertThat(holder).isNotNull();
                pending = executor.submit(() -> {
                    started.countDown();
                    return PartitionJUnitLauncher.executeWorkUnits("partition", "session", 1,
                            nestedUnits, loader, CancellationToken.create(), events::add);
                });
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> pending.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            }
            assertThat(pending.get(5, TimeUnit.SECONDS).success()).isTrue();
            assertThat(events).extracting(WorkerTestEventPayload::status)
                    .containsExactly(TestOutcomeStatus.STARTED, TestOutcomeStatus.PASSED);
        } finally {
            if (previousHome == null) {
                System.clearProperty("user.home");
            } else {
                System.setProperty("user.home", previousHome);
            }
        }
    }

    @Isolated
    static class IsolatedSuite {

        @Nested
        class NestedSuite {
            @Test
            void nestedTest() {
            }
        }
    }
}
