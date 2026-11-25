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

import java.util.List;
import java.util.Map;

import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.HistoricalUsageResponse;
import org.citrusframework.distribution.model.UsageSampleRecord;
import org.citrusframework.distribution.model.UsageSampleRecord.JobDemandBucket;
import org.citrusframework.distribution.model.UsageSampleRecord.PoolSampleBucket;
import org.citrusframework.distribution.model.UsageSampleRecord.UnpooledWorkerBucket;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HistoricalUsageAggregatorTest {
    private static final long NOW = 1_000_000_000L;
    private static final Map<String, String> JDK = Map.of("jdk", "21");
    private static final Map<String, String> DB = Map.of("postgres", "18");
    private static final Map<String, String> BOTH = Map.of("jdk", "21", "postgres", "18");

    @Test
    void sizesWindowsAndRejectsUnknownWindow() {
        for (var entry : Map.of("1h", 60, "24h", 96, "7d", 84).entrySet()) {
            var usage = HistoricalUsageAggregator.aggregate(entry.getKey(), NOW, Map.of(), List.of(), List.of());
            assertThat(usage.series()).hasSize(entry.getValue());
            assertThat(usage.bucketMs()).isEqualTo(Map.of("1h", 60_000L, "24h", 900_000L, "7d", 7_200_000L).get(entry.getKey()));
        }
        assertThatThrownBy(() -> HistoricalUsageAggregator.aggregate("1y", NOW, Map.of(), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void attributesDemandByPriorityAndPlacesUnmetDemandInFirstPool() {
        assertDesired(query(List.of(job(JDK, 3)), pools(BOTH, BOTH)), 3, 2, 1);
        assertDesired(query(List.of(job(JDK, 8)), pools(BOTH, BOTH)), 8, 4, 4);
        assertDesired(query(List.of(job(JDK, 2), job(DB, 2)), pools(BOTH, BOTH)), 4, 2, 2);
    }

    @Test
    void assignsMostConstrainedJobsFirstRegardlessOfInputOrder() {
        var jobs = List.of(job(JDK, 3), job(DB, 2));
        assertDesired(query(jobs, pools(BOTH, JDK)), 5, 2, 3);
        assertDesired(query(jobs.reversed(), pools(BOTH, JDK).reversed()), 5, 2, 3);
    }

    @Test
    void breaksTiesBetweenEquallyConstrainedDemandBySortedRequirements() {
        var jobs = List.of(job(DB, 2), job(JDK, 2));
        var pools = List.of(new PoolSampleBucket("a", "Shared", 0, BOTH, 2, 0),
                new PoolSampleBucket("b", "Database", 1, DB, 2, 0),
                new PoolSampleBucket("c", "Java", 2, JDK, 2, 0));
        for (var order : List.of(jobs, jobs.reversed())) {
            var usage = query(order, pools.reversed());
            assertThat(usage.pools()).extracting(p -> p.points().getLast().desired()).containsExactly(2, 2, 0);
        }
    }

    @Test
    void scopesSamplesAndCountsDistinctSessionsAcrossMatchingBuckets() {
        var jobs = List.of(new JobDemandBucket(DB, 1, 1, 2, List.of("shared")),
                new JobDemandBucket(BOTH, 2, 1, 0, List.of("shared", "other")), job(JDK, 5));
        var sample = new UsageSampleRecord(NOW, jobs, pools(BOTH, JDK), List.of(
                new UnpooledWorkerBucket(DB, 2, 1), new UnpooledWorkerBucket(JDK, 3, 3)));
        var usage = HistoricalUsageAggregator.aggregate("1h", NOW, DB, List.of(sample), List.of());
        assertThat(usage.pools()).extracting(p -> p.poolId()).containsExactly("a");
        var point = usage.series().getLast();
        assertThat(point.connected()).isEqualTo(4);
        assertThat(point.utilized()).isEqualTo(2);
        assertThat(point.desired()).isEqualTo(4);
        assertThat(point.activeJobs()).isEqualTo(2);
        assertThat(query(jobs, pools(BOTH, JDK)).series().getLast().activeJobs()).isEqualTo(3);
    }

    @Test
    void keepsEachMetricsPeakAcrossSamplesInOneBucket() {
        var older = new UsageSampleRecord(NOW - 1000, List.of(job(JDK, 8)), List.of(), List.of());
        var newer = new UsageSampleRecord(NOW, List.of(), pools(BOTH, JDK), List.of());
        var result = HistoricalUsageAggregator.aggregate("1h", NOW, Map.of(), List.of(older, newer), List.of());
        var point = result.series().getLast();
        assertThat(point.desired()).isEqualTo(8);
        assertThat(point.connected()).isEqualTo(6);
    }

    @Test
    void readsCountOnlySnapshotsFromBeforeSessionIdsAndDeletedPoolMarkers() {
        var snapshot = DistributionJson.fromJson("""
                {"sampledAt":1000000000,
                 "activeJobs":[{"requiredCapabilities":{},"activeSessions":2,"activePartitions":2,"pendingPartitions":0}],
                 "pools":[{"poolId":"a","displayName":"A","priorityOrder":0,"capabilities":{},"connected":1,"utilized":1}]}
                """, UsageSampleRecord.class);
        var usage = HistoricalUsageAggregator.aggregate("1h", NOW, Map.of(), List.of(snapshot), List.of());
        assertThat(usage.series().getLast().activeJobs()).isEqualTo(2);
        assertThat(usage.pools().getFirst().points().getLast().desired()).isEqualTo(2);
    }

    private static JobDemandBucket job(Map<String, String> caps, int demand) {
        return new JobDemandBucket(caps, 1, Math.min(2, demand), Math.max(0, demand - 2));
    }

    private static List<PoolSampleBucket> pools(Map<String, String> first, Map<String, String> second) {
        return List.of(new PoolSampleBucket("a", "First", 0, first, 2, 1),
                new PoolSampleBucket("b", "Second", 1, second, 4, 1));
    }

    private static HistoricalUsageResponse query(List<JobDemandBucket> jobs, List<PoolSampleBucket> pools) {
        return HistoricalUsageAggregator.aggregate("1h", NOW, Map.of(),
                List.of(new UsageSampleRecord(NOW, jobs, pools, List.of())), List.of());
    }

    private static void assertDesired(HistoricalUsageResponse usage, int total, int first, int second) {
        assertThat(usage.series().getLast().desired()).isEqualTo(total);
        assertThat(usage.pools()).extracting(p -> p.points().getLast().desired()).containsExactly(first, second);
    }
}
