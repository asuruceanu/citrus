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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.WorkUnitSpec;

/**
 * Splits submitted work units into partitions.
 * <ul>
 *     <li>{@link PartitionPolicy#LPT} (default): Longest-Processing-Time ordering by historical duration, grouped by
 *     capability constraints and chunked into partitions of at most {@code maxPartitionSize} units.</li>
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
        return lptPartitions(sessionId, enriched, Math.max(1, maxPartitionSize));
    }

    private static WorkUnitSpec withResolvedDuration(WorkUnitSpec raw, Map<String, Long> history, long fallbackDuration) {
        long resolvedDuration = history.getOrDefault(
                raw.key(),
                raw.estimatedDurationMs() > 0 ? raw.estimatedDurationMs() : fallbackDuration
        );
        Map<String, String> normalizedCaps = raw.requiredCapabilities() == null || raw.requiredCapabilities().isEmpty()
                ? Map.of()
                : new TreeMap<>(raw.requiredCapabilities());
        return new WorkUnitSpec(
                raw.key(),
                raw.className(),
                raw.methodName(),
                raw.dynamicSourceName(),
                normalizedCaps,
                resolvedDuration
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
        long sumDuration = units.stream().mapToLong(WorkUnitSpec::estimatedDurationMs).sum();
        return newPartition(sessionId, capabilities, List.copyOf(units), sumDuration);
    }

    private static List<PartitionRecord> lptPartitions(String sessionId, List<WorkUnitSpec> units, int capSize) {
        Map<Map<String, String>, List<WorkUnitSpec>> byCapabilities = new LinkedHashMap<>();
        for (WorkUnitSpec unit : units) {
            byCapabilities.computeIfAbsent(unit.requiredCapabilities(), k -> new ArrayList<>()).add(unit);
        }

        List<PartitionRecord> partitions = new ArrayList<>();
        for (Map.Entry<Map<String, String>, List<WorkUnitSpec>> group : byCapabilities.entrySet()) {
            List<WorkUnitSpec> groupUnits = group.getValue();
            groupUnits.sort(Comparator.comparingLong(WorkUnitSpec::estimatedDurationMs).reversed()
                    .thenComparing(WorkUnitSpec::key));

            for (int i = 0; i < groupUnits.size(); i += capSize) {
                List<WorkUnitSpec> chunk = List.copyOf(groupUnits.subList(i, Math.min(i + capSize, groupUnits.size())));
                long sumDuration = chunk.stream().mapToLong(WorkUnitSpec::estimatedDurationMs).sum();
                partitions.add(newPartition(sessionId, group.getKey(), chunk, sumDuration));
            }
        }

        partitions.sort(Comparator.comparingLong(PartitionRecord::estimatedDurationMs).reversed()
                .thenComparing(p -> p.workUnits().get(0).key()));
        return partitions;
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
