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

package org.citrusframework.distribution.model;

import java.util.Map;

public record WorkerWsMessage(
        String type,
        Map<String, Object> payload
) {
    public static final String WORKER_REGISTER = "worker.register";
    public static final String WORKER_REGISTERED = "worker.registered";
    public static final String WORKER_HEARTBEAT = "worker.heartbeat";
    public static final String WORKER_DRAINING = "worker.draining";
    public static final String PARTITION_ASSIGN = "partition.assign";
    public static final String PARTITION_STARTED = "partition.started";
    public static final String TEST_EVENT = "test.event";
    public static final String PARTITION_COMPLETED = "partition.completed";
    public static final String PARTITION_CANCEL = "partition.cancel";
}
