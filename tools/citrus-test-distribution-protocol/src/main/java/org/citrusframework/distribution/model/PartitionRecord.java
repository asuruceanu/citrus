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

public record PartitionRecord(
        String partitionId,
        String sessionId,
        String workerId,
        PartitionState state,
        int attempt,
        long estimatedDurationMs,
        long startedAt,
        long finishedAt,
        Map<String, String> requiredCapabilities,
        List<WorkUnitSpec> workUnits,
        String moduleName,
        boolean forkWorkspace,
        String workingDirectory,
        List<String> inputPaths,
        List<String> outputPaths,
        List<String> producedOutputs,
        String errorMessage,
        InputPreparation inputPreparation
) {

    public PartitionRecord(String partitionId,
                           String sessionId,
                           String workerId,
                           PartitionState state,
                           int attempt,
                           long estimatedDurationMs,
                           long startedAt,
                           long finishedAt,
                           Map<String, String> requiredCapabilities,
                           List<WorkUnitSpec> workUnits,
                           String moduleName,
                           boolean forkWorkspace,
                           String workingDirectory,
                           List<String> inputPaths,
                           List<String> outputPaths,
                           List<String> producedOutputs,
                           String errorMessage) {
        this(partitionId, sessionId, workerId, state, attempt, estimatedDurationMs, startedAt, finishedAt,
                requiredCapabilities, workUnits, moduleName, forkWorkspace, workingDirectory, inputPaths,
                outputPaths, producedOutputs, errorMessage, null);
    }

    public PartitionRecord {
        requiredCapabilities = requiredCapabilities == null ? Map.of() : Map.copyOf(requiredCapabilities);
        workUnits = workUnits == null ? List.of() : List.copyOf(workUnits);
        workingDirectory = workingDirectory == null ? "" : workingDirectory;
        inputPaths = inputPaths == null ? List.of() : List.copyOf(inputPaths);
        outputPaths = outputPaths == null ? List.of() : List.copyOf(outputPaths);
        producedOutputs = producedOutputs == null ? List.of() : List.copyOf(producedOutputs);
    }

    public PartitionRecord(String partitionId,
                           String sessionId,
                           String workerId,
                           PartitionState state,
                           int attempt,
                           long estimatedDurationMs,
                           long startedAt,
                           long finishedAt,
                           Map<String, String> requiredCapabilities,
                           List<WorkUnitSpec> workUnits) {
        this(
                partitionId,
                sessionId,
                workerId,
                state,
                attempt,
                estimatedDurationMs,
                startedAt,
                finishedAt,
                requiredCapabilities,
                workUnits,
                null,
                false,
                "",
                List.of(),
                List.of(),
                List.of(),
                null
        );
    }
}
