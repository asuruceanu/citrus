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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LptPartitionSchedulerTest {

    @Test
    void shouldOrderSlowestWorkUnitsFirstAndRespectMaxPartitionSize() {
        List<WorkUnitSpec> units = List.of(
                new WorkUnitSpec("Fast1IT", "com.example.Fast1IT", null, null, Map.of(), 0L),
                new WorkUnitSpec("SlowIT", "com.example.SlowIT", null, null, Map.of(), 0L),
                new WorkUnitSpec("Fast2IT", "com.example.Fast2IT", null, null, Map.of(), 0L),
                new WorkUnitSpec("UnseenIT", "com.example.UnseenIT", null, null, Map.of(), 0L)
        );
        Map<String, Long> history = Map.of(
                "SlowIT", 4000L,
                "Fast1IT", 200L,
                "Fast2IT", 150L
        );

        LptPartitionScheduler scheduler = new LptPartitionScheduler();
        List<PartitionRecord> partitions = scheduler.schedule("session-1", units, history, 1000L, 1);

        assertThat(partitions).hasSize(4);
        assertThat(partitions.get(0).workUnits().get(0).key()).isEqualTo("SlowIT");
        assertThat(partitions.get(0).estimatedDurationMs()).isEqualTo(4000L);
        assertThat(partitions.get(1).workUnits().get(0).key()).isEqualTo("UnseenIT");
        assertThat(partitions.get(1).estimatedDurationMs()).isEqualTo(1000L);
        assertThat(partitions.get(2).workUnits().get(0).key()).isEqualTo("Fast1IT");
        assertThat(partitions.get(3).workUnits().get(0).key()).isEqualTo("Fast2IT");
    }

    @Test
    void shouldChunkByMaxPartitionSizeAndKeepDistinctCapabilityGroupsSeparate() {
        List<WorkUnitSpec> units = List.of(
                new WorkUnitSpec("Plain1IT", "com.example.Plain1IT", null, null, Map.of(), 500L),
                new WorkUnitSpec("Plain2IT", "com.example.Plain2IT", null, null, Map.of(), 400L),
                new WorkUnitSpec("Plain3IT", "com.example.Plain3IT", null, null, Map.of(), 300L),
                new WorkUnitSpec("DockerIT", "com.example.DockerIT", null, null, Map.of("docker", "true"), 2500L)
        );

        LptPartitionScheduler scheduler = new LptPartitionScheduler();
        List<PartitionRecord> partitions = scheduler.schedule("session-2", units, Map.of(), 1000L, 2);

        assertThat(partitions).hasSize(3);
        // Slowest partition (DockerIT: 2500ms) comes first and is not merged with plain tests
        assertThat(partitions.get(0).workUnits()).extracting(WorkUnitSpec::key).containsExactly("DockerIT");
        assertThat(partitions.get(0).requiredCapabilities()).containsEntry("docker", "true");

        // Plain tests balanced over ceil(3 / 2) = 2 partitions: 500 | 400 + 300
        assertThat(partitions.get(1).workUnits()).extracting(WorkUnitSpec::key).containsExactly("Plain2IT", "Plain3IT");
        assertThat(partitions.get(1).estimatedDurationMs()).isEqualTo(700L);
        assertThat(partitions.get(2).workUnits()).extracting(WorkUnitSpec::key).containsExactly("Plain1IT");
    }

    @Test
    void shouldSpreadTheSlowestUnitsAcrossPartitionsInsteadOfGroupingThem() {
        List<WorkUnitSpec> units = new ArrayList<>();
        for (int i = 20; i >= 1; i--) {
            units.add(new WorkUnitSpec(String.format("U%02d", i), "com.example.U" + i, null, null, Map.of(), i * 1000L));
        }

        List<PartitionRecord> partitions = new LptPartitionScheduler().schedule("session-4", units, Map.of(), 1000L, 10);

        assertThat(partitions).hasSize(2);
        assertThat(partitions).allSatisfy(p -> assertThat(p.workUnits()).hasSize(10));
        assertThat(partitions).extracting(PartitionRecord::estimatedDurationMs).containsOnly(105_000L);
        assertThat(partitions).as("the two slowest units run in parallel").allSatisfy(p ->
                assertThat(p.workUnits()).extracting(WorkUnitSpec::key).containsAnyOf("U20", "U19")
                        .doesNotContain(p.workUnits().stream().anyMatch(u -> u.key().equals("U20")) ? "U19" : "U20"));
    }

    @Test
    void shouldPackFastModuleIntoFewerPartitionsWhenMinPartitionDurationFloorApplies() {
        List<WorkUnitSpec> units = units("Fast", 19, Map.of(), 150L);

        List<PartitionRecord> partitions = new LptPartitionScheduler()
                .schedule("s1", units, Map.of(), 1_000L, 5, PartitionPolicy.LPT, 5_000L, 2, caps -> 6);

        assertThat(partitions).singleElement().satisfies(partition -> assertThat(partition.workUnits()).hasSize(19));
    }

    @Test
    void shouldCapColdMultiWaveExplosionUsingMaxWavesPerWorker() {
        List<WorkUnitSpec> units = units("Cold", 185, Map.of(), 0L);

        List<PartitionRecord> partitions = new LptPartitionScheduler()
                .schedule("s1", units, Map.of(), 1_000L, 5, PartitionPolicy.LPT, 5_000L, 2, caps -> 6);

        assertThat(partitions).hasSize(12)
                .allSatisfy(partition -> assertThat(partition.workUnits()).hasSizeLessThanOrEqualTo(16));
        assertThat(partitions.stream().flatMap(partition -> partition.workUnits().stream()).map(WorkUnitSpec::key))
                .containsExactlyInAnyOrderElementsOf(units.stream().map(WorkUnitSpec::key).toList());
    }

    @Test
    void shouldEvaluateEligibleWorkersPerCapabilityGroupIndependently() {
        Map<String, String> requiresDocker = Map.of("docker", "true");
        List<WorkUnitSpec> units = new ArrayList<>(units("Plain", 20, Map.of(), 1_000L));
        units.addAll(units("Docker", 20, requiresDocker, 1_000L));

        List<PartitionRecord> partitions = new LptPartitionScheduler()
                .schedule("s1", units, Map.of(), 1_000L, 5, PartitionPolicy.LPT, 5_000L, 2,
                        caps -> caps.isEmpty() ? 6 : 1);

        assertThat(partitions).filteredOn(partition -> partition.requiredCapabilities().isEmpty()).hasSize(4);
        assertThat(partitions).filteredOn(partition -> partition.requiredCapabilities().equals(requiresDocker)).hasSize(2);
    }

    @Test
    void shouldPreserveLegacyBehaviorWhenAdaptiveParametersAreDisabled() {
        List<WorkUnitSpec> units = units("Fast", 7, Map.of(), 150L);
        List<WorkUnitSpec> skewed = new ArrayList<>(units);
        skewed.add(new WorkUnitSpec("Slow", "pkg.Slow", null, null, Map.of(), 10_000L));
        LptPartitionScheduler scheduler = new LptPartitionScheduler();

        List<PartitionRecord> partitions = scheduler
                .schedule("s1", skewed, Map.of(), 1_000L, 7, PartitionPolicy.LPT, 0L, 0, caps -> 0);

        assertThat(partitions).extracting(partition -> partition.workUnits().size()).containsExactly(1, 7);
        assertThat(partitions).extracting(PartitionRecord::workUnits).containsExactlyElementsOf(
                scheduler.schedule("s1", skewed, Map.of(), 1_000L, 7).stream().map(PartitionRecord::workUnits).toList());
    }

    @Test
    void shouldFillAdaptivePartitionsEvenWithZeroDurationHistory() {
        List<WorkUnitSpec> units = units("Test", 25, Map.of(), 0L);
        Map<String, Long> history = new HashMap<>();
        units.forEach(unit -> history.put(unit.key(), 0L));
        history.put("Test0", 120L);

        List<PartitionRecord> partitions = new LptPartitionScheduler()
                .schedule("s1", units, history, 1_000L, 1, PartitionPolicy.LPT, 10L, 0, null);

        assertThat(partitions).hasSize(12)
                .allSatisfy(partition -> assertThat(partition.workUnits()).isNotEmpty());
    }

    @Test
    void shouldSizeAdaptivePartitionsWhenGroupDurationExceedsLongRange() {
        List<WorkUnitSpec> units = units("Slow", 2, Map.of(), Long.MAX_VALUE);
        LptPartitionScheduler scheduler = new LptPartitionScheduler();

        for (long minPartitionDurationMs : List.of(5_000L, Long.MAX_VALUE)) {
            List<PartitionRecord> partitions = scheduler.schedule("s1", units, Map.of(), 1_000L, 1, PartitionPolicy.LPT,
                    minPartitionDurationMs, 0, null);

            assertThat(partitions).extracting(PartitionRecord::estimatedDurationMs)
                    .containsExactly(Long.MAX_VALUE, Long.MAX_VALUE);
        }
    }

    @Test
    void shouldSaturatePartitionDurationWhenItsUnitsExceedLongRange() {
        List<WorkUnitSpec> units = units("Slow", 2, Map.of(), Long.MAX_VALUE);
        LptPartitionScheduler scheduler = new LptPartitionScheduler();

        for (PartitionPolicy policy : List.of(PartitionPolicy.LPT, PartitionPolicy.SINGLE)) {
            assertThat(scheduler.schedule("s1", units, Map.of(), 1_000L, 2, policy))
                    .extracting(PartitionRecord::estimatedDurationMs).containsExactly(Long.MAX_VALUE);
        }
    }

    @Test
    void shouldFallBackForNegativeHistoryAndKeepZeroDurationHistory() {
        List<WorkUnitSpec> units = new ArrayList<>(units("Test", 25, Map.of(), 0L));
        units.set(1, new WorkUnitSpec("Test1", "pkg.Test1", null, null, Map.of(), 275L));
        Map<String, Long> history = new HashMap<>();
        units.forEach(unit -> history.put(unit.key(), -1L));
        history.put("Test0", 0L);

        List<PartitionRecord> partitions = new LptPartitionScheduler()
                .schedule("s1", units, history, 1_000L, 1, PartitionPolicy.LPT, 0L, 12, caps -> 1);

        assertThat(partitions).hasSize(12).allSatisfy(partition -> assertThat(partition.workUnits()).isNotEmpty());
        assertThat(partitions.stream().flatMap(partition -> partition.workUnits().stream()))
                .extracting(WorkUnitSpec::estimatedDurationMs)
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, 25).mapToObj(i -> i == 0 ? 0L : i == 1 ? 275L : 1_000L).toList());
    }

    private static List<WorkUnitSpec> units(String prefix, int count, Map<String, String> capabilities, long durationMs) {
        return IntStream.range(0, count)
                .mapToObj(i -> new WorkUnitSpec(prefix + i, "pkg." + prefix + i, null, null, capabilities, durationMs))
                .toList();
    }

    @Test
    void shouldCoScheduleAllUnitsUnderSinglePolicyAndRejectConflictingCapabilities() {
        LptPartitionScheduler scheduler = new LptPartitionScheduler();
        List<WorkUnitSpec> units = IntStream.range(0, 25)
                .mapToObj(i -> new WorkUnitSpec("u" + i, "pkg.Test" + i, "m", null,
                        i % 2 == 0 ? Map.of("os", "linux") : Map.of("docker", "true"), 0L))
                .toList();
        List<PartitionRecord> single = scheduler.schedule("s1", units, Map.of("u3", 700L), 1000L, 5, PartitionPolicy.SINGLE);
        assertThat(single).hasSize(1);
        assertThat(single.getFirst().workUnits()).hasSize(25);
        assertThat(single.getFirst().workUnits()).extracting(WorkUnitSpec::key)
                .as("submission order is kept so tests sharing one workspace run in a stable order")
                .containsExactlyElementsOf(units.stream().map(WorkUnitSpec::key).toList());
        assertThat(single.getFirst().requiredCapabilities())
                .containsEntry("os", "linux")
                .containsEntry("docker", "true");
        assertThat(single.getFirst().estimatedDurationMs()).isEqualTo(24 * 1000L + 700L);
        assertThat(single.getFirst().workUnits().get(3).estimatedDurationMs()).isEqualTo(700L);
        assertThat(single.getFirst().attempt()).isEqualTo(1);

        List<WorkUnitSpec> conflicting = List.of(
                new WorkUnitSpec("u1", "pkg.A", "m", null, Map.of("os", "linux"), 0L),
                new WorkUnitSpec("u2", "pkg.B", "m", null, Map.of("os", "windows"), 0L));
        assertThatThrownBy(() -> scheduler.schedule("s1", conflicting, Map.of(), 1000L, 5, PartitionPolicy.SINGLE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("os");

        List<WorkUnitSpec> differentCase = List.of(
                new WorkUnitSpec("u1", "pkg.A", "m", null, Map.of("os", "Linux"), 0L),
                new WorkUnitSpec("u2", "pkg.B", "m", null, Map.of("os", "linux"), 0L));
        assertThat(scheduler.schedule("s1", differentCase, Map.of(), 1000L, 5, PartitionPolicy.SINGLE))
                .as("capability values match case-insensitively, like CapabilityMatcher")
                .hasSize(1);
    }

    @Test
    void shouldTreatNullPolicyAsLptAndReturnNothingForNoUnits() {
        LptPartitionScheduler scheduler = new LptPartitionScheduler();
        List<WorkUnitSpec> units = List.of(
                new WorkUnitSpec("a", "pkg.A", null, null, Map.of(), 100L),
                new WorkUnitSpec("b", "pkg.B", null, null, Map.of(), 100L));
        assertThat(scheduler.schedule("s1", units, Map.of(), 1000L, 1, null)).hasSize(2);
        assertThat(scheduler.schedule("s1", List.of(), Map.of(), 1000L, 1, PartitionPolicy.SINGLE)).isEmpty();
    }
}
