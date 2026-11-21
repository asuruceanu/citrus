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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ActiveAssignmentTrackerTest {

    @Test
    void shouldKeepNewerAttemptOfSamePartitionWhenOlderAttemptFinishes() {
        ActiveAssignmentTracker tracker = new ActiveAssignmentTracker();
        ActiveAssignmentTracker.Assignment first = tracker.start("p1", 1);
        assertThat(tracker.cancel("p1", 1)).isTrue();
        assertThat(first.token().isCancellationRequested()).isTrue();

        ActiveAssignmentTracker.Assignment second = tracker.start("p1", 2);
        tracker.finish(first);

        assertThat(tracker.activePartitionId()).as("retry stays active, so heartbeats keep naming it").isEqualTo("p1");
        assertThat(second.token().isCancellationRequested()).isFalse();

        tracker.finish(second);
        assertThat(tracker.activePartitionId()).isNull();
    }

    @Test
    void shouldIgnoreCancelForAnotherPartitionOrAttempt() {
        ActiveAssignmentTracker tracker = new ActiveAssignmentTracker();
        ActiveAssignmentTracker.Assignment active = tracker.start("p1", 2);

        assertThat(tracker.cancel("p2", 2)).isFalse();
        assertThat(tracker.cancel("p1", 1)).isFalse();
        assertThat(active.token().isCancellationRequested()).isFalse();

        assertThat(tracker.cancel(null, 0)).as("legacy cancel without ids cancels the active attempt").isTrue();
        assertThat(active.token().isCancellationRequested()).isTrue();
        assertThat(new ActiveAssignmentTracker().cancel("p1", 1)).isFalse();
    }

    @Test
    void shouldAcceptNewAssignmentOnlyWhenIdleOrTheActiveOneIsCancelled() {
        ActiveAssignmentTracker tracker = new ActiveAssignmentTracker();
        assertThat(tracker.canAccept()).isTrue();
        ActiveAssignmentTracker.Assignment running = tracker.start("p1", 1);
        assertThat(tracker.canAccept()).as("a live run keeps the slot").isFalse();
        tracker.cancel(null, 0);
        assertThat(tracker.canAccept()).as("a cancelled run is winding down; the next one queues behind it").isTrue();
        tracker.finish(running);
        assertThat(tracker.canAccept()).isTrue();
    }

    @Test
    void shouldReportOnlyTheExecutingAssignmentForHeartbeats() {
        ActiveAssignmentTracker tracker = new ActiveAssignmentTracker();
        ActiveAssignmentTracker.Assignment winding = tracker.start("p1", 1);
        tracker.markExecuting(winding);
        tracker.cancel(null, 0);
        ActiveAssignmentTracker.Assignment queued = tracker.start("p2", 1);
        assertThat(tracker.executingPartitionId()).as("queued p2 is not running yet").isEqualTo("p1");

        tracker.finish(winding);
        assertThat(tracker.executingPartitionId()).isNull();
        tracker.markExecuting(queued);
        assertThat(tracker.executingPartitionId()).isEqualTo("p2");
        tracker.finish(queued);
        assertThat(tracker.executingPartitionId()).isNull();
    }
}
