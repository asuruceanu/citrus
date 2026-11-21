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

public record SubmitWorkRequest(
        String submissionId,
        String moduleName,
        List<ArtifactRef> classpathArtifacts,
        Map<String, String> systemProperties,
        List<WorkUnitSpec> workUnits,
        int cachedBlobs,
        int uploadedBlobs,
        List<WorkspaceFileSet> inputArtifacts,
        List<String> outputPaths,
        boolean forkWorkspace,
        PartitionPolicy partitionPolicy
) {

    public SubmitWorkRequest {
        inputArtifacts = inputArtifacts == null ? List.of() : List.copyOf(inputArtifacts);
        outputPaths = outputPaths == null ? List.of() : List.copyOf(outputPaths);
        partitionPolicy = partitionPolicy == null ? PartitionPolicy.LPT : partitionPolicy;
    }

    public SubmitWorkRequest(String submissionId,
                             String moduleName,
                             List<ArtifactRef> classpathArtifacts,
                             Map<String, String> systemProperties,
                             List<WorkUnitSpec> workUnits,
                             int cachedBlobs,
                             int uploadedBlobs) {
        this(submissionId, moduleName, classpathArtifacts, systemProperties, workUnits, cachedBlobs, uploadedBlobs,
                List.of(), List.of(), false, PartitionPolicy.LPT);
    }
}
