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

import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.WorkUnitSpec;

/**
 * Longest-Processing-Time (LPT) scheduler that orders work units descending by historical duration,
 * groups by capability constraints, and chunks into work-stealing partitions.
 */
public class LptPartitionScheduler {

    public List<PartitionRecord> schedule(
            String sessionId,
            List<WorkUnitSpec> units,
            Map<String, Long> historicalDurationsMs,
            long defaultDurationMs,
            int maxPartitionSize) {

        if (units == null || units.isEmpty()) {
            return List.of();
        }

        int capSize = Math.max(1, maxPartitionSize);
        long fallbackDuration = Math.max(1L, defaultDurationMs);
        Map<String, Long> history = historicalDurationsMs != null ? historicalDurationsMs : Map.of();

        Map<Map<String, String>, List<WorkUnitSpec>> byCapabilities = new LinkedHashMap<>();
        for (WorkUnitSpec raw : units) {
            long resolvedDuration = history.getOrDefault(
                    raw.key(),
                    raw.estimatedDurationMs() > 0 ? raw.estimatedDurationMs() : fallbackDuration
            );
            Map<String, String> normalizedCaps = raw.requiredCapabilities() == null || raw.requiredCapabilities().isEmpty()
                    ? Map.of()
                    : new TreeMap<>(raw.requiredCapabilities());
            WorkUnitSpec enriched = new WorkUnitSpec(
                    raw.key(),
                    raw.className(),
                    raw.methodName(),
                    raw.dynamicSourceName(),
                    normalizedCaps,
                    resolvedDuration
            );
            byCapabilities.computeIfAbsent(normalizedCaps, k -> new ArrayList<>()).add(enriched);
        }

        List<PartitionRecord> partitions = new ArrayList<>();
        for (Map.Entry<Map<String, String>, List<WorkUnitSpec>> group : byCapabilities.entrySet()) {
            List<WorkUnitSpec> groupUnits = group.getValue();
            groupUnits.sort(Comparator.comparingLong(WorkUnitSpec::estimatedDurationMs).reversed()
                    .thenComparing(WorkUnitSpec::key));

            for (int i = 0; i < groupUnits.size(); i += capSize) {
                List<WorkUnitSpec> chunk = List.copyOf(groupUnits.subList(i, Math.min(i + capSize, groupUnits.size())));
                long sumDuration = chunk.stream().mapToLong(WorkUnitSpec::estimatedDurationMs).sum();
                String partitionId = "part-" + UUID.randomUUID().toString().substring(0, 12);
                partitions.add(new PartitionRecord(
                        partitionId,
                        sessionId,
                        null,
                        PartitionState.PENDING,
                        1,
                        sumDuration,
                        0L,
                        0L,
                        group.getKey(),
                        chunk
                ));
            }
        }

        partitions.sort(Comparator.comparingLong(PartitionRecord::estimatedDurationMs).reversed()
                .thenComparing(p -> p.workUnits().get(0).key()));
        return partitions;
    }
}
