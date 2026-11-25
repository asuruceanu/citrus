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

public record AgentPoolSummary(
        String poolId,
        String displayName,
        Map<String, String> capabilities,
        int priorityOrder,
        int connectedWorkers,
        int busyWorkers,
        int idleWorkers,
        int activeKeyCount,
        long createdAt,
        long updatedAt
) {

    public AgentPoolSummary {
        capabilities = capabilities == null ? Map.of() : Map.copyOf(capabilities);
    }
}
