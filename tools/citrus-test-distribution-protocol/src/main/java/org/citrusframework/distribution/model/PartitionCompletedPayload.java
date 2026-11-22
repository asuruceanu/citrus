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

/**
 * Reports the outcome of one partition attempt. {@code attempt == 0} means the sender did not report an
 * attempt (legacy worker); negative values are normalized to {@code 0}.
 */
public record PartitionCompletedPayload(
        String partitionId,
        String sessionId,
        int attempt,
        boolean success,
        String errorMessage,
        List<WorkspaceFileSet> outputArtifacts
) {

    public PartitionCompletedPayload {
        attempt = Math.max(0, attempt);
        outputArtifacts = outputArtifacts == null ? List.of() : List.copyOf(outputArtifacts);
    }

    public PartitionCompletedPayload(String partitionId, String sessionId, boolean success, String errorMessage) {
        this(partitionId, sessionId, 0, success, errorMessage, List.of());
    }
}
