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

package org.citrusframework.distribution.coordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.citrusframework.distribution.model.AgentPoolRecord;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.UsageSampleRecord;
import org.citrusframework.distribution.model.UsageSampleRecord.JobDemandBucket;
import org.citrusframework.distribution.model.UsageSampleRecord.PoolSampleBucket;
import org.citrusframework.distribution.model.UsageSampleRecord.UnpooledWorkerBucket;

/** Groups the live coordinator snapshot into compact buckets before writing it to the store. */
final class UsageSampleBuilder {
    private static final class Workers {
        int connected;
        int utilized;

        void add(boolean busy) {
            connected++;
            if (busy) {
                utilized++;
            }
        }
    }

    private static final class Demand {
        final TreeSet<String> sessions = new TreeSet<>();
        int active;
        int pending;
    }

    private final Map<String, AgentPoolRecord> current = new LinkedHashMap<>();
    private final Map<String, AgentPoolRecord> all = new LinkedHashMap<>();
    private final Map<String, Workers> pooled = new HashMap<>();
    private final Map<Map<String, String>, Workers> unpooled = new HashMap<>();
    private final Map<Map<String, String>, Demand> jobs = new HashMap<>();

    UsageSampleBuilder(List<AgentPoolRecord> currentPools) {
        currentPools.forEach(p -> current.put(p.poolId(), p));
        all.putAll(current);
    }

    void addWorker(AgentPoolRecord pool, Map<String, String> capabilities, boolean busy) {
        if (pool == null) {
            unpooled.computeIfAbsent(capabilities, key -> new Workers()).add(busy);
        } else {
            all.putIfAbsent(pool.poolId(), pool);
            pooled.computeIfAbsent(pool.poolId(), id -> new Workers()).add(busy);
        }
    }

    void addPartition(PartitionRecord partition) {
        boolean pending = partition.state() == PartitionState.PENDING;
        if (!pending && partition.state() != PartitionState.ASSIGNED && partition.state() != PartitionState.RUNNING) {
            return;
        }
        var demand = jobs.computeIfAbsent(partition.requiredCapabilities(), key -> new Demand());
        demand.sessions.add(partition.sessionId());
        if (pending) {
            demand.pending++;
        } else {
            demand.active++;
        }
    }

    UsageSampleRecord build(long now) {
        List<PoolSampleBucket> poolBuckets = new ArrayList<>();
        for (AgentPoolRecord p : all.values()) {
            var counts = pooled.get(p.poolId());
            poolBuckets.add(new PoolSampleBucket(p.poolId(), p.displayName(), p.priorityOrder(), p.capabilities(),
                    counts == null ? 0 : counts.connected, counts == null ? 0 : counts.utilized,
                    !current.containsKey(p.poolId())));
        }
        List<JobDemandBucket> jobBuckets = new ArrayList<>();
        jobs.forEach((caps, d) -> jobBuckets.add(new JobDemandBucket(caps, d.sessions.size(), d.active, d.pending,
                List.copyOf(d.sessions))));
        List<UnpooledWorkerBucket> unpooledBuckets = new ArrayList<>();
        unpooled.forEach((caps, counts) -> unpooledBuckets.add(new UnpooledWorkerBucket(caps, counts.connected, counts.utilized)));
        return new UsageSampleRecord(now, jobBuckets, poolBuckets, unpooledBuckets);
    }
}
