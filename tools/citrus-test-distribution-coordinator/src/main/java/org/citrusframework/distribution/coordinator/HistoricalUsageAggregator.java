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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.citrusframework.distribution.model.AgentPoolRecord;
import org.citrusframework.distribution.model.HistoricalUsageResponse;
import org.citrusframework.distribution.model.PoolUsageSeries;
import org.citrusframework.distribution.model.UsagePoint;
import org.citrusframework.distribution.model.UsageSampleRecord;
import org.citrusframework.distribution.model.UsageSampleRecord.JobDemandBucket;
import org.citrusframework.distribution.model.UsageSampleRecord.PoolSampleBucket;
import org.citrusframework.distribution.scheduler.CapabilityMatcher;

/** Resolves each snapshot against the capabilities and priorities in effect at collection time. */
final class HistoricalUsageAggregator {
    private static final Comparator<PoolSampleBucket> PRIORITY = Comparator.comparingInt(PoolSampleBucket::priorityOrder)
            .thenComparing(PoolSampleBucket::poolId);

    record Window(int count, long bucketMs) {
        long from(long now) {
            return now - count * bucketMs;
        }

        List<UsagePoint> emptySeries(long from) {
            List<UsagePoint> points = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                points.add(new UsagePoint(from + i * bucketMs, 0, 0, 0, 0, 0));
            }
            return points;
        }
    }

    private record Demand(JobDemandBucket job, List<PoolSampleBucket> compatible, String requirements) {
    }

    private HistoricalUsageAggregator() {
    }

    static Window window(String name) {
        return switch (name) {
            case "1h" -> new Window(60, 60_000L);
            case "24h" -> new Window(96, 900_000L);
            case "7d" -> new Window(84, 7_200_000L);
            default -> throw new IllegalArgumentException("Unsupported usage window: " + name);
        };
    }

    static HistoricalUsageResponse aggregate(String window, long nowMs, Map<String, String> filter,
                                             List<UsageSampleRecord> samples, List<AgentPoolRecord> currentPools) {
        Window span = window(window);
        long from = span.from(nowMs);
        List<UsagePoint> global = span.emptySeries(from);
        Map<String, List<UsagePoint>> poolPoints = new HashMap<>();
        Map<String, PoolSampleBucket> metadata = new HashMap<>();

        for (UsageSampleRecord sample : samples.stream().sorted(Comparator.comparingLong(UsageSampleRecord::sampledAt)).toList()) {
            if (sample.sampledAt() < from || sample.sampledAt() > nowMs) {
                continue;
            }
            int index = Math.min(span.count() - 1, (int) ((sample.sampledAt() - from) / span.bucketMs()));
            List<PoolSampleBucket> pools = sample.pools().stream()
                    .filter(p -> CapabilityMatcher.matches(filter, p.capabilities())).sorted(PRIORITY).toList();
            List<JobDemandBucket> jobs = sample.activeJobs().stream()
                    .filter(j -> CapabilityMatcher.matches(filter, j.requiredCapabilities())).toList();
            Map<String, Integer> desired = allocate(jobs, pools);
            int connected = 0;
            int utilized = 0;
            for (PoolSampleBucket pool : pools) {
                metadata.put(pool.poolId(), pool);
                List<UsagePoint> series = poolPoints.computeIfAbsent(pool.poolId(), id -> span.emptySeries(from));
                takeMax(series, index, 0, 0, pool.utilized(), desired.getOrDefault(pool.poolId(), 0), pool.connected());
                connected += pool.connected();
                utilized += pool.utilized();
            }
            for (var workers : sample.unpooledWorkers()) {
                if (CapabilityMatcher.matches(filter, workers.capabilities())) {
                    connected += workers.connected();
                    utilized += workers.utilized();
                }
            }
            var sessions = new HashSet<String>();
            int anonymousSessions = 0;
            int active = 0;
            int pending = 0;
            for (JobDemandBucket job : jobs) {
                sessions.addAll(job.activeSessionIds());
                // Older snapshots carry counts only. Their session identities cannot be recovered.
                if (job.activeSessionIds().isEmpty()) {
                    anonymousSessions += job.activeSessions();
                }
                active += job.activePartitions();
                pending += job.pendingPartitions();
            }
            takeMax(global, index, sessions.size() + anonymousSessions, active, utilized, active + pending, connected);
        }
        for (AgentPoolRecord pool : currentPools) {
            if (CapabilityMatcher.matches(filter, pool.capabilities())) {
                metadata.put(pool.poolId(), new PoolSampleBucket(pool.poolId(), pool.displayName(), pool.priorityOrder(),
                        pool.capabilities(), 0, 0));
            }
        }
        List<PoolUsageSeries> series = metadata.values().stream().sorted(PRIORITY)
                .map(p -> new PoolUsageSeries(p.poolId(), p.displayName(), p.capabilities(),
                        poolPoints.computeIfAbsent(p.poolId(), id -> span.emptySeries(from)))).toList();
        return new HistoricalUsageResponse(window, from, nowMs, span.bucketMs(), filter, global, series);
    }

    private static Map<String, Integer> allocate(List<JobDemandBucket> jobs, List<PoolSampleBucket> pools) {
        List<Demand> demands = new ArrayList<>();
        for (JobDemandBucket job : jobs) {
            List<PoolSampleBucket> compatible = pools.stream().filter(p -> !p.deleted())
                    .filter(p -> CapabilityMatcher.matches(job.requiredCapabilities(), p.capabilities())).toList();
            if (!compatible.isEmpty()) {
                demands.add(new Demand(job, compatible, new TreeMap<>(job.requiredCapabilities()).toString()));
            }
        }
        demands.sort(Comparator.comparingInt((Demand d) -> d.compatible().size()).thenComparing(Demand::requirements));
        Map<String, Integer> capacity = new HashMap<>();
        Map<String, Integer> desired = new HashMap<>();
        for (PoolSampleBucket pool : pools) {
            capacity.put(pool.poolId(), pool.connected());
        }
        for (Demand demand : demands) {
            int remaining = demand.job().activePartitions() + demand.job().pendingPartitions();
            for (PoolSampleBucket pool : demand.compatible()) {
                int available = capacity.get(pool.poolId());
                int allocated = Math.min(available, remaining);
                capacity.put(pool.poolId(), available - allocated);
                desired.merge(pool.poolId(), allocated, Integer::sum);
                remaining -= allocated;
                if (remaining == 0) {
                    break;
                }
            }
            if (remaining > 0) {
                desired.merge(demand.compatible().getFirst().poolId(), remaining, Integer::sum);
            }
        }
        return desired;
    }

    private static void takeMax(List<UsagePoint> points, int index, int jobs, int active, int utilized, int desired,
                                int connected) {
        UsagePoint old = points.get(index);
        points.set(index, new UsagePoint(old.timestamp(), Math.max(jobs, old.activeJobs()),
                Math.max(active, old.activePartitions()), Math.max(utilized, old.utilized()),
                Math.max(desired, old.desired()), Math.max(connected, old.connected())));
    }
}
