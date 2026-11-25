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

import java.util.List;
import java.util.Map;

public record UsageSampleRecord(
        long sampledAt,
        List<JobDemandBucket> activeJobs,
        List<PoolSampleBucket> pools,
        List<UnpooledWorkerBucket> unpooledWorkers
) {

    public UsageSampleRecord {
        activeJobs = activeJobs == null ? List.of() : List.copyOf(activeJobs);
        pools = pools == null ? List.of() : List.copyOf(pools);
        unpooledWorkers = unpooledWorkers == null ? List.of() : List.copyOf(unpooledWorkers);
    }

    public record JobDemandBucket(
            Map<String, String> requiredCapabilities,
            int activeSessions,
            int activePartitions,
            int pendingPartitions,
            List<String> activeSessionIds
    ) {

        public JobDemandBucket {
            activeSessionIds = activeSessionIds == null ? List.of() : List.copyOf(activeSessionIds);
            requiredCapabilities = requiredCapabilities == null ? Map.of() : Map.copyOf(requiredCapabilities);
        }

        public JobDemandBucket(Map<String, String> requiredCapabilities, int activeSessions, int activePartitions,
                               int pendingPartitions) {
            this(requiredCapabilities, activeSessions, activePartitions, pendingPartitions, List.of());
        }
    }

    public record PoolSampleBucket(
            String poolId,
            String displayName,
            int priorityOrder,
            Map<String, String> capabilities,
            int connected,
            int utilized,
            boolean deleted
    ) {

        public PoolSampleBucket {
            capabilities = capabilities == null ? Map.of() : Map.copyOf(capabilities);
        }

        public PoolSampleBucket(String poolId, String displayName, int priorityOrder, Map<String, String> capabilities,
                                int connected, int utilized) {
            this(poolId, displayName, priorityOrder, capabilities, connected, utilized, false);
        }
    }

    public record UnpooledWorkerBucket(Map<String, String> capabilities, int connected, int utilized) {

        public UnpooledWorkerBucket {
            capabilities = capabilities == null ? Map.of() : Map.copyOf(capabilities);
        }
    }
}
