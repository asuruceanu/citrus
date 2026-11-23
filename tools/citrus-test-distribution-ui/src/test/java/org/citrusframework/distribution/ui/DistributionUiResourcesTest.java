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

package org.citrusframework.distribution.ui;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DistributionUiResourcesTest {

    @Test
    void shouldPackageDistUiStaticAssetsWithFixedTemplateEvaluator() throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        try (InputStream index = cl.getResourceAsStream("META-INF/resources/dist-ui/index.html");
             InputStream styles = cl.getResourceAsStream("META-INF/resources/dist-ui/styles.css");
             InputStream app = cl.getResourceAsStream("META-INF/resources/dist-ui/app.js");
             InputStream htm = cl.getResourceAsStream("META-INF/resources/dist-ui/vendor/htm-mini.js")) {

            assertThat(index).isNotNull();
            assertThat(styles).isNotNull();
            assertThat(app).isNotNull();
            assertThat(htm).isNotNull();

            String stylesText = new String(styles.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(stylesText)
                    .contains("--citrus-orange: #f5921d")
                    .contains(".btn-danger")
                    .contains(".session-failure-banner")
                    .contains(".pager {")
                    .contains(".worker-cache-panel")
                    .containsPattern("(?s)\\.overview-worker-badges\\s*\\{[^}]*display:\\s*flex;[^}]*align-items:\\s*center;");

            String appText = new String(app.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(appText)
                    .contains("renderWorkerCacheTelemetry")
                    .contains("lowDiskSpace")
                    .contains("freeSpaceBufferBytes")
                    .contains("evictedBlobsTotal")
                    .contains("if (state.workerStateFilter === 'LOW_DISK') {", "if (!isLowDisk(w)) return false;")
                    .contains("state.workerStateFilter = st", "st === 'LOW_DISK' ? 'LOW DISK' : st")
                    .contains("if (!Number.isFinite(buffer) || buffer <= 0) return 100;")
                    .containsPattern("usableLabel = Number\\(c\\.usableDiskBytes\\) < 0 [^\\n]+\\s*\\? 'N/A'")
                    .containsPattern("(?s)<div class=\"overview-worker-badges\">.*?isLowDisk\\(w\\).*?statusBadge\\(w\\.state\\)")
                    .contains("w.state !== 'OFFLINE' && w.cacheStats && w.cacheStats.lowDiskSpace")
                    .contains("const lowDisk = isLowDisk(w);")
                    .contains("worker-cache-panel ${lowDisk ? 'low-disk' : ''}",
                            "worker-cache-bar-fill ${lowDisk ? 'low-disk' : ''}",
                            "${lowDisk ? html` <span class=\"badge badge-failed\">LOW DISK</span>` : null}")
                    .containsPattern("(?s)else if \\(shouldPollWorkers\\(\\)\\) \\{[^}]*pollWorkers\\(\\);")
                    .contains("return state.sseConnected && !state.authError && (state.view === 'overview' || state.view === 'workers');")
                    .containsPattern("(?s)function loadWorkers\\(\\).*?apiGet\\('/api/v1/workers'\\).*?state\\.workers")
                    .containsPattern("(?s)function pollWorkers\\(\\).*?if \\(await loadWorkers\\(\\) && shouldPollWorkers\\(\\)\\) \\{\\s+renderApp\\(\\);")
                    .contains("worker.offline")
                    .contains("session.aborted")
                    .contains("/abort")
                    .contains("POLL_INTERVAL_MS")
                    .contains("EventSource.CLOSED")
                    .contains("state.authTokenDraft = state.authToken;", "value=${state.authTokenDraft}")
                    .contains("onInput=${e => { state.authTokenDraft = e.target.value; }}")
                    .contains("onClick=${() => commitAuthToken('')}",
                            "onClick=${() => commitAuthToken(state.authTokenDraft.trim())}")
                    .contains("forkWorkspace")
                    .contains("inputPaths")
                    .contains("producedOutputs")
                    .contains("errorMessage")
                    .contains("safeDecodeSegment")
                    .contains("collectLatestTestFailures")
                    .contains("session.failureReason")
                    .contains("session-failure-banner")
                    .contains("refreshInFlight")
                    .contains("DETAIL_MIN_INTERVAL_MS")
                    .contains("detailRequestSeq")
                    .contains("detailTimer")
                    .contains("moduleColor")
                    .contains("session-modules")
                    .contains("<title>")
                    .contains("fitBarLabel")
                    .contains("partitionTiming")
                    .contains("function paginate(")
                    .contains("function loadHistory(")
                    .contains("HISTORY_MIN_INTERVAL_MS")
                    // A table returns to its first page when the session or a filter it shows changes.
                    .contains("function paginate(key, items, pageSize = 25, context = '')")
                    .contains("paginate('sessions', filtered, 25, `${state.sessionStatusFilter}|${q}`)")
                    .contains("paginate('partitions', partitions, 25, state.selectedSessionId)")
                    .contains("paginate('tests', filteredTests, 50, `${state.selectedSessionId}|${state.testStatusFilter}|${tq}`)")
                    .contains("paginate('history', filtered, 25, `${state.historyFlakyOnly}|${q}`)")
                    .contains("class=\"pager\"");

            String htmText = new String(htm.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(htmText)
                    .contains("export function html")
                    .contains("fields[built[i++] - 1]");
        }
    }
}
