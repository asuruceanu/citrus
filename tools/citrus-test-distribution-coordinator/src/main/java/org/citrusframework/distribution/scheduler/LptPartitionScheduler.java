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

package org.citrusframework.distribution.scheduler;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.ToIntFunction;

import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.WorkUnitSpec;

/**
 * Splits submitted work units into partitions.
 * <ul>
 *     <li>{@link PartitionPolicy#LPT} (default): grouped by capability constraints; within a group, units are taken
 *     longest historical duration first and each is placed in the least loaded partition that still has room.
 *     Each group starts with {@code ceil(n / maxPartitionSize)} partitions, which optional duration and wave
 *     limits can consolidate.</li>
 *     <li>{@link PartitionPolicy#SINGLE}: all units in exactly one partition, in submission order, requiring the
 *     union of their capabilities. Values for one key must be equal ignoring case; different thresholds such as
 *     {@code >=2048} and {@code >=4096} count as a conflict rather than being merged.</li>
 * </ul>
 */
public class LptPartitionScheduler {

    public List<PartitionRecord> schedule(
            String sessionId,
            List<WorkUnitSpec> units,
            Map<String, Long> historicalDurationsMs,
            long defaultDurationMs,
            int maxPartitionSize) {
        return schedule(sessionId, units, historicalDurationsMs, defaultDurationMs, maxPartitionSize, PartitionPolicy.LPT);
    }

    /**
     * Schedules {@code units} with the given {@code policy} ({@code null} means {@link PartitionPolicy#LPT}).
     *
     * @throws IllegalArgumentException under {@link PartitionPolicy#SINGLE} when two units require different values
     *                                  for the same capability key
     */
    public List<PartitionRecord> schedule(
            String sessionId,
            List<WorkUnitSpec> units,
            Map<String, Long> historicalDurationsMs,
            long defaultDurationMs,
            int maxPartitionSize,
            PartitionPolicy policy) {
        return schedule(sessionId, units, historicalDurationsMs, defaultDurationMs, maxPartitionSize, policy, 0L, 0,
                null);
    }

    /**
     * Schedules units with optional duration and worker-wave limits on each LPT capability group.
     * A non-positive duration or wave limit disables that limit.
     */
    public List<PartitionRecord> schedule(
            String sessionId,
            List<WorkUnitSpec> units,
            Map<String, Long> historicalDurationsMs,
            long defaultDurationMs,
            int maxPartitionSize,
            PartitionPolicy policy,
            long minPartitionDurationMs,
            int maxWavesPerWorker,
            ToIntFunction<Map<String, String>> eligibleWorkerCounter) {

        if (units == null || units.isEmpty()) {
            return List.of();
        }

        long fallbackDuration = Math.max(1L, defaultDurationMs);
        Map<String, Long> history = historicalDurationsMs != null ? historicalDurationsMs : Map.of();
        List<WorkUnitSpec> enriched = units.stream()
                .map(unit -> withResolvedDuration(unit, history, fallbackDuration))
                .toList();

        if (policy == PartitionPolicy.SINGLE) {
            return List.of(singlePartition(sessionId, enriched));
        }
        return lptPartitions(sessionId, enriched, Math.max(1, maxPartitionSize), minPartitionDurationMs,
                maxWavesPerWorker, eligibleWorkerCounter);
    }

    private static WorkUnitSpec withResolvedDuration(WorkUnitSpec raw, Map<String, Long> history, long fallbackDuration) {
        long defaultDuration = raw.estimatedDurationMs() > 0 ? raw.estimatedDurationMs() : fallbackDuration;
        Long historicalDuration = history.get(raw.key());
        long resolvedDuration = historicalDuration != null && historicalDuration >= 0
                ? historicalDuration : defaultDuration;
        Map<String, String> normalizedCaps = raw.requiredCapabilities() == null || raw.requiredCapabilities().isEmpty()
                ? Map.of()
                : new TreeMap<>(raw.requiredCapabilities());
        return new WorkUnitSpec(
                raw.key(),
                raw.className(),
                raw.methodName(),
                raw.dynamicSourceName(),
                normalizedCaps,
                resolvedDuration,
                raw.enclosingClassNames()
        );
    }

    private static PartitionRecord singlePartition(String sessionId, List<WorkUnitSpec> units) {
        Map<String, String> capabilities = new TreeMap<>();
        for (WorkUnitSpec unit : units) {
            for (Map.Entry<String, String> required : unit.requiredCapabilities().entrySet()) {
                String existing = capabilities.putIfAbsent(required.getKey(), required.getValue());
                if (existing != null && !existing.equalsIgnoreCase(required.getValue())) {
                    throw new IllegalArgumentException("partitionPolicy=SINGLE cannot co-schedule work units requiring "
                            + "different values for capability '" + required.getKey() + "': '" + existing + "' and '"
                            + required.getValue() + "' (" + unit.key() + ")");
                }
            }
        }
        long sumDuration = units.stream().mapToLong(WorkUnitSpec::estimatedDurationMs)
                .reduce(0L, LptPartitionScheduler::addDurations);
        return newPartition(sessionId, capabilities, List.copyOf(units), sumDuration);
    }

    private static List<PartitionRecord> lptPartitions(String sessionId, List<WorkUnitSpec> units, int capSize,
            long minPartitionDurationMs, int maxWavesPerWorker,
            ToIntFunction<Map<String, String>> eligibleWorkerCounter) {
        Map<Map<String, String>, List<WorkUnitSpec>> byCapabilities = new LinkedHashMap<>();
        for (WorkUnitSpec unit : units) {
            byCapabilities.computeIfAbsent(unit.requiredCapabilities(), k -> new ArrayList<>()).add(unit);
        }

        List<PartitionRecord> partitions = new ArrayList<>();
        for (Map.Entry<Map<String, String>, List<WorkUnitSpec>> group : byCapabilities.entrySet()) {
            List<WorkUnitSpec> groupUnits = group.getValue();
            groupUnits.sort(Comparator.comparingLong(WorkUnitSpec::estimatedDurationMs).reversed()
                    .thenComparing(WorkUnitSpec::key));

            int maxBinsFromSize = Math.ceilDiv(groupUnits.size(), capSize);
            int binCount = maxBinsFromSize;
            if (minPartitionDurationMs > 0) {
                binCount = limitBinsByDuration(groupUnits, binCount, minPartitionDurationMs);
            }
            if (maxWavesPerWorker > 0 && eligibleWorkerCounter != null) {
                int eligibleWorkers = eligibleWorkerCounter.applyAsInt(group.getKey());
                if (eligibleWorkers > 0) {
                    binCount = (int) Math.min(binCount, (long) eligibleWorkers * maxWavesPerWorker);
                }
            }

            boolean packedAdaptively = binCount < maxBinsFromSize;
            int effectiveCapSize = packedAdaptively ? Math.ceilDiv(groupUnits.size(), binCount) : capSize;
            // Prefer an empty bin on a tie when packing adaptively, so zero-duration units use all planned bins.
            List<List<WorkUnitSpec>> bins = new ArrayList<>();
            long[] binDurations = new long[binCount];
            for (int i = 0; i < binCount; i++) {
                bins.add(new ArrayList<>());
            }
            for (WorkUnitSpec unit : groupUnits) {
                int target = -1;
                for (int i = 0; i < binCount; i++) {
                    List<WorkUnitSpec> bin = bins.get(i);
                    if (bin.size() < effectiveCapSize && (target < 0 || binDurations[i] < binDurations[target]
                            || (packedAdaptively && binDurations[i] == binDurations[target]
                            && bin.isEmpty() && !bins.get(target).isEmpty()))) {
                        target = i;
                    }
                }
                bins.get(target).add(unit);
                binDurations[target] = addDurations(binDurations[target], unit.estimatedDurationMs());
            }
            for (int i = 0; i < binCount; i++) {
                partitions.add(newPartition(sessionId, group.getKey(), List.copyOf(bins.get(i)), binDurations[i]));
            }
        }

        partitions.sort(Comparator.comparingLong(PartitionRecord::estimatedDurationMs).reversed()
                .thenComparing(p -> p.workUnits().get(0).key()));
        return partitions;
    }

    private static int limitBinsByDuration(List<WorkUnitSpec> units, int binCount, long minPartitionDurationMs) {
        try {
            long total = units.stream().mapToLong(WorkUnitSpec::estimatedDurationMs).reduce(0L, Math::addExact);
            return (int) Math.min(binCount, Math.ceilDiv(Math.max(1L, total), minPartitionDurationMs));
        } catch (ArithmeticException overflow) {
            // A saturated total can underestimate the bin count when the minimum duration is also large.
            BigInteger total = units.stream()
                    .map(unit -> BigInteger.valueOf(unit.estimatedDurationMs()))
                    .reduce(BigInteger.ZERO, BigInteger::add);
            BigInteger durationBins = total.subtract(BigInteger.ONE)
                    .divide(BigInteger.valueOf(minPartitionDurationMs))
                    .add(BigInteger.ONE);
            return durationBins.min(BigInteger.valueOf(binCount)).intValueExact();
        }
    }

    /** Adds two non-negative durations while keeping the projected partition duration representable. */
    private static long addDurations(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static PartitionRecord newPartition(String sessionId, Map<String, String> capabilities,
                                                List<WorkUnitSpec> units, long estimatedDurationMs) {
        return new PartitionRecord(
                "part-" + UUID.randomUUID().toString().substring(0, 12),
                sessionId,
                null,
                PartitionState.PENDING,
                1,
                estimatedDurationMs,
                0L,
                0L,
                capabilities,
                units
        );
    }
}
