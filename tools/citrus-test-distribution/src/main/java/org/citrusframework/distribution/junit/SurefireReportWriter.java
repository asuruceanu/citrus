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

package org.citrusframework.distribution.junit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkerTestEventPayload;

/**
 * Writes standard Maven Surefire / Failsafe {@code TEST-<className>.xml} report files from remote worker events.
 */
public final class SurefireReportWriter {

    private final Map<String, List<WorkerTestEventPayload>> byClass = new LinkedHashMap<>();

    public synchronized void record(WorkerTestEventPayload event) {
        if (event == null || event.status() == TestOutcomeStatus.STARTED) {
            return;
        }
        String cls = event.className() != null && !event.className().isBlank()
                ? event.className()
                : "org.citrusframework.DistributedTests";
        byClass.computeIfAbsent(cls, k -> new ArrayList<>()).add(event);
    }

    public synchronized void writeReports(Path reportDir) throws IOException {
        if (reportDir == null || byClass.isEmpty()) {
            return;
        }
        Files.createDirectories(reportDir);
        for (Map.Entry<String, List<WorkerTestEventPayload>> entry : byClass.entrySet()) {
            String className = entry.getKey();
            List<WorkerTestEventPayload> events = entry.getValue();
            int tests = events.size();
            int failures = 0;
            int skipped = 0;
            long totalMs = 0L;
            for (WorkerTestEventPayload ev : events) {
                totalMs += Math.max(0L, ev.durationMs());
                if (ev.status() == TestOutcomeStatus.FAILED) {
                    failures++;
                } else if (ev.status() == TestOutcomeStatus.SKIPPED) {
                    skipped++;
                }
            }

            StringBuilder xml = new StringBuilder(512);
            xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            xml.append(String.format(Locale.ROOT,
                    "<testsuite name=\"%s\" time=\"%.3f\" tests=\"%d\" errors=\"0\" skipped=\"%d\" failures=\"%d\">\n",
                    escapeXml(className), totalMs / 1000.0, tests, skipped, failures));
            for (WorkerTestEventPayload ev : events) {
                String testName = ev.testName() != null ? ev.testName() : ev.workUnitKey();
                xml.append(String.format(Locale.ROOT,
                        "  <testcase name=\"%s\" classname=\"%s\" time=\"%.3f\"",
                        escapeXml(testName), escapeXml(className), Math.max(0L, ev.durationMs()) / 1000.0));
                if (ev.status() == TestOutcomeStatus.PASSED) {
                    xml.append("/>\n");
                } else if (ev.status() == TestOutcomeStatus.SKIPPED) {
                    xml.append(">\n    <skipped");
                    if (ev.errorMessage() != null && !ev.errorMessage().isBlank()) {
                        xml.append(" message=\"").append(escapeXml(ev.errorMessage())).append("\"");
                    }
                    xml.append("/>\n  </testcase>\n");
                } else {
                    xml.append(">\n    <failure");
                    if (ev.errorMessage() != null && !ev.errorMessage().isBlank()) {
                        xml.append(" message=\"").append(escapeXml(ev.errorMessage())).append("\"");
                    }
                    xml.append(" type=\"java.lang.AssertionError\">");
                    if (ev.stackTrace() != null) {
                        xml.append(escapeXml(ev.stackTrace()));
                    }
                    xml.append("</failure>\n  </testcase>\n");
                }
            }
            xml.append("</testsuite>\n");

            Path target = reportDir.resolve("TEST-" + className + ".xml");
            Files.writeString(target, xml.toString(), StandardCharsets.UTF_8);
        }
    }

    private static String escapeXml(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }
}
