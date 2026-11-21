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

import java.util.concurrent.atomic.AtomicReference;

import org.junit.platform.engine.CancellationToken;

/**
 * Tracks the partition attempt a worker currently holds.
 * <p>
 * Each assignment is tracked by identity, not by partition id: when a cancelled attempt finishes after the
 * coordinator has already handed the same partition back to this worker as a new attempt, finishing the old
 * attempt must not clear the new one (heartbeats keep reporting it, which keeps its lease alive).
 */
final class ActiveAssignmentTracker {

    /**
     * One partition attempt and its cancellation token.
     */
    record Assignment(String partitionId, int attempt, CancellationToken token) {
    }

    private final AtomicReference<Assignment> active = new AtomicReference<>();

    /**
     * Records a newly received assignment as the active one and returns it.
     */
    Assignment start(String partitionId, int attempt) {
        Assignment assignment = new Assignment(partitionId, attempt, CancellationToken.create());
        active.set(assignment);
        return assignment;
    }

    /**
     * Clears {@code assignment} if it is still the active one; a newer assignment is left untouched.
     */
    void finish(Assignment assignment) {
        active.compareAndSet(assignment, null);
    }

    /**
     * Partition id of the active assignment, or {@code null}.
     */
    String activePartitionId() {
        Assignment assignment = active.get();
        return assignment != null ? assignment.partitionId() : null;
    }

    /**
     * Cancels the active assignment if it matches {@code partitionId} (when given) and {@code attempt} (when
     * greater than zero), so a late cancel for an older attempt cannot stop a newer one.
     *
     * @return whether an assignment was cancelled
     */
    boolean cancel(String partitionId, int attempt) {
        Assignment assignment = active.get();
        if (assignment == null
                || (partitionId != null && !partitionId.equals(assignment.partitionId()))
                || (attempt > 0 && attempt != assignment.attempt())) {
            return false;
        }
        assignment.token().cancel();
        return true;
    }
}
