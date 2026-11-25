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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record WorkerRecord(
        String workerId,
        String host,
        String version,
        String poolId,
        String poolDisplayName,
        Map<String, String> capabilities,
        WorkerState state,
        String activePartitionId,
        String activeTestDisplay,
        long stateUpdatedAt,
        long lastHeartbeatAt,
        long registeredAt,
        WorkerCacheStats cacheStats,
        List<WorkerWarning> warnings
) {

    public WorkerRecord {
        capabilities = capabilities == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(capabilities));
        warnings = warnings == null ? List.of() : List.copyOf(warnings);
    }

    public WorkerRecord(String workerId, String host, Map<String, String> capabilities, WorkerState state,
                        String activePartitionId, long lastHeartbeatAt, long registeredAt, WorkerCacheStats cacheStats) {
        this(workerId, host, null, null, null, capabilities, state, activePartitionId, null, 0L, lastHeartbeatAt,
                registeredAt, cacheStats, List.of());
    }

    public WorkerRecord(String workerId, String host, Map<String, String> capabilities, WorkerState state,
                        String activePartitionId, long lastHeartbeatAt, long registeredAt) {
        this(workerId, host, capabilities, state, activePartitionId, lastHeartbeatAt, registeredAt, null);
    }
}
