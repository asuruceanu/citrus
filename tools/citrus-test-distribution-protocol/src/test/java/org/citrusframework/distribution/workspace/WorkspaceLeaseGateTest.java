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

package org.citrusframework.distribution.workspace;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkspaceLeaseGateTest {

    @TempDir
    Path tempDir;

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void shouldEitherAdmitTheHelperOrClearTheTicketForCleanup() throws Exception {
        Path parent = tempDir.toRealPath();
        Path workspace = Files.createDirectory(parent.resolve("workspace"));
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int attempt = 0; attempt < 25; attempt++) {
                // Give each race a different ticket. A retired ticket is never reusable.
                Path offer = parent.resolve(".lease-%032x".formatted(attempt));
                WorkspaceLeaseGate.create(offer, workspace);
                CountDownLatch start = new CountDownLatch(1);
                var incoming = executor.submit(() -> {
                    start.await();
                    try {
                        return WorkspaceLease.acquire(offer);
                    } catch (IOException e) {
                        return null;
                    }
                });
                start.countDown();
                boolean cleared = WorkspaceLeaseGate.tryRetire(offer, true);
                try (WorkspaceLease admitted = incoming.get(10, TimeUnit.SECONDS)) {
                    if (admitted != null) {
                        assertThat(cleared).isFalse();
                        assertThat(admitted.workspace()).isEqualTo(workspace);
                    }
                    assertThatThrownBy(() -> {
                        try (WorkspaceLease late = WorkspaceLease.acquire(offer)) {
                            late.workspace();
                        }
                    }).isInstanceOf(IOException.class);
                }
                assertThat(WorkspaceLeaseGate.tryRetire(offer, true)).isTrue();
                WorkspaceLeaseGate.removeRetired(offer);
            }
        }
    }

    @Test
    void shouldFailClosedWhenARecordedTicketIsMissing() throws IOException {
        Path ticket = tempDir.resolve(".lease-" + "1".repeat(32));

        assertThat(WorkspaceLeaseGate.tryRetire(ticket, true)).isFalse();
        assertThat(WorkspaceLeaseGate.tryRetire(ticket, false)).isTrue();
    }
}
