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

import java.util.function.Consumer;

import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.junit.platform.engine.CancellationToken;

/**
 * Executes one partition attempt on a worker.
 */
interface PartitionRunner {

    /**
     * Runs {@code assignment}, streaming test events to {@code eventSink}, and stops early once
     * {@code cancellationToken} is cancelled.
     */
    IsolatedPartitionRunner.PartitionExecutionResult execute(PartitionAssignPayload assignment,
                                                             CancellationToken cancellationToken,
                                                             Consumer<WorkerTestEventPayload> eventSink);
}
