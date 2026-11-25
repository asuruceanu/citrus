/*
 * Copyright the original author or authors.
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
import java.util.Map;

import org.citrusframework.distribution.model.ResultJournal;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerResultOutboxTest {

    @TempDir
    Path cache;

    @Test
    void shouldReplayUnacknowledgedResultsAndCompletionInOrderFromDisk() {
        var first = new ResultJournal(cache);
        var result = first.persist(WorkerWsMessage.TEST_EVENT, Map.of("partitionId", "partition"));
        var completion = first.persist(WorkerWsMessage.PARTITION_COMPLETED, Map.of("partitionId", "partition"));
        var restarted = new ResultJournal(cache);
        var replayed = new ArrayList<WorkerWsMessage>();
        restarted.replay(replayed::add);
        assertThat(replayed).containsExactly(result, completion);
        restarted.acknowledge((String) result.payload().get(WorkerWsMessage.DELIVERY_ID));
        replayed.clear();
        new ResultJournal(cache).replay(replayed::add);
        assertThat(replayed).containsExactly(completion);
    }
}
