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

public record BuildSessionRecord(
        String sessionId,
        String projectName,
        String gitBranch,
        String gitCommit,
        SessionStatus status,
        long startedAt,
        long finishedAt,
        int totalTests,
        int passedTests,
        int failedTests,
        int skippedTests,
        int totalBlobs,
        int cachedBlobs,
        long serialDurationMs,
        long wallDurationMs,
        String failureReason,
        java.util.List<String> modules,
        String runGroup
) {

    /** Modules (Maven artifact ids) that submitted work, in the order they first started. */
    public BuildSessionRecord {
        modules = modules != null ? java.util.List.copyOf(modules) : java.util.List.of();
    }

    public BuildSessionRecord(
            String sessionId,
            String projectName,
            String gitBranch,
            String gitCommit,
            SessionStatus status,
            long startedAt,
            long finishedAt,
            int totalTests,
            int passedTests,
            int failedTests,
            int skippedTests,
            int totalBlobs,
            int cachedBlobs,
            long serialDurationMs,
            long wallDurationMs,
            String failureReason) {
        this(sessionId, projectName, gitBranch, gitCommit, status, startedAt, finishedAt, totalTests, passedTests,
                failedTests, skippedTests, totalBlobs, cachedBlobs, serialDurationMs, wallDurationMs, failureReason,
                java.util.List.of(), null);
    }

    public BuildSessionRecord(String sessionId, String projectName, String gitBranch, String gitCommit,
                              SessionStatus status, long startedAt, long finishedAt, int totalTests,
                              int passedTests, int failedTests, int skippedTests, int totalBlobs, int cachedBlobs,
                              long serialDurationMs, long wallDurationMs, String failureReason, java.util.List<String> modules) {
        this(sessionId, projectName, gitBranch, gitCommit, status, startedAt, finishedAt, totalTests, passedTests,
                failedTests, skippedTests, totalBlobs, cachedBlobs, serialDurationMs, wallDurationMs, failureReason,
                modules, null);
    }

    /** Copy listing {@code modules}. */
    public BuildSessionRecord withModules(java.util.List<String> modules) {
        return new BuildSessionRecord(sessionId, projectName, gitBranch, gitCommit, status, startedAt, finishedAt,
                totalTests, passedTests, failedTests, skippedTests, totalBlobs, cachedBlobs, serialDurationMs,
                wallDurationMs, failureReason, modules, runGroup);
    }

    /** Session without a failure reason (running or passed sessions). */
    public BuildSessionRecord(
            String sessionId,
            String projectName,
            String gitBranch,
            String gitCommit,
            SessionStatus status,
            long startedAt,
            long finishedAt,
            int totalTests,
            int passedTests,
            int failedTests,
            int skippedTests,
            int totalBlobs,
            int cachedBlobs,
            long serialDurationMs,
            long wallDurationMs) {
        this(sessionId, projectName, gitBranch, gitCommit, status, startedAt, finishedAt, totalTests, passedTests,
                failedTests, skippedTests, totalBlobs, cachedBlobs, serialDurationMs, wallDurationMs, null);
    }
}
