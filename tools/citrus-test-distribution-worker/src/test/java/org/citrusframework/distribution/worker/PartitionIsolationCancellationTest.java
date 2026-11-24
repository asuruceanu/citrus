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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.workspace.IsolatedTestLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.engine.CancellationToken;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionIsolationCancellationTest {

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void cancelledAssignmentFinishesWhileAnotherPartitionStillHoldsTheLock(@TempDir Path home) throws Exception {
        String previousHome = System.getProperty("user.home");
        System.setProperty("user.home", home.toString());
        ClassLoader loader = getClass().getClassLoader();
        List<WorkUnitSpec> units = List.of(new WorkUnitSpec("fixture", Fixture.class.getName(), null, null, Map.of(), 1_000));
        CancellationToken token = CancellationToken.create();
        try (var executor = Executors.newSingleThreadExecutor();
             var holder = IsolatedTestLock.acquire(units, loader)) {
            assertThat(holder).isNotNull();
            var pending = executor.submit(() -> PartitionJUnitLauncher.executeWorkUnits(
                    "partition", "session", 1, units, loader, token,
                    event -> { throw new AssertionError("Cancelled test must not execute"); }));

            assertThatThrownBy(() -> pending.get(250, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            token.cancel();

            var result = pending.get(5, TimeUnit.SECONDS);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("cancelled");
        } finally {
            if (previousHome == null) {
                System.clearProperty("user.home");
            } else {
                System.setProperty("user.home", previousHome);
            }
        }
    }

    @Isolated
    static class Fixture {
        @Test
        void shouldNeverRun() {
            throw new AssertionError("Cancelled test must not execute");
        }
    }
}
