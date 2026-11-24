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

public record TestHistorySummary(
        String workUnitKey,
        String className,
        String testName,
        long avgDurationMs,
        long maxDurationMs,
        int totalRuns,
        int passedRuns,
        int failedRuns,
        int flakyRuns,
        TestOutcomeStatus lastStatus,
        String projectScope,
        String projectName
) {

    public TestHistorySummary(String workUnitKey, String className, String testName,
                              long avgDurationMs, long maxDurationMs, int totalRuns, int passedRuns,
                              int failedRuns, int flakyRuns, TestOutcomeStatus lastStatus) {
        this(workUnitKey, className, testName, avgDurationMs, maxDurationMs, totalRuns, passedRuns,
                failedRuns, flakyRuns, lastStatus, null, null);
    }

    public TestHistorySummary(String workUnitKey,
                              String className,
                              String testName,
                              long avgDurationMs,
                              int totalRuns,
                              int failedRuns,
                              int flakyRuns) {
        this(
                workUnitKey,
                className,
                testName,
                avgDurationMs,
                avgDurationMs,
                totalRuns,
                Math.max(0, totalRuns - failedRuns),
                failedRuns,
                flakyRuns,
                failedRuns > 0 ? TestOutcomeStatus.FAILED : TestOutcomeStatus.PASSED
        );
    }
}
