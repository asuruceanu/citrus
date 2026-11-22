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

package org.citrusframework.distribution.broker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class DistributionBrokerUiTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldServeOfflineCapableWebUiWithWaterfallAndAllViews() throws Exception {
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .port(0)
                .authToken("ui-token")
                .cacheDir(tempDir.resolve("ui-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:broker_ui_" + System.nanoTime())
                .build();

        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        try (DistributionBrokerServer server = new DistributionBrokerServer(config)) {
            server.start();

            HttpResponse<String> rootResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(rootResp.statusCode()).isEqualTo(200);
            assertThat(rootResp.body())
                    .contains("Citrus Test Distribution")
                    .contains("type=\"module\"")
                    .contains("app.js");

            HttpResponse<String> cssResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/ui/styles.css")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(cssResp.statusCode()).isEqualTo(200);
            assertThat(cssResp.body()).contains("--citrus-orange: #f5921d");

            HttpResponse<String> jsResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/ui/app.js")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(jsResp.statusCode()).isEqualTo(200);
            assertThat(jsResp.body())
                    .contains("renderWaterfallSvg")
                    .contains("/api/v1/events/stream")
                    .contains("/api/v1/workers")
                    .contains("/api/v1/tests/history");

            HttpResponse<String> htmResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/ui/vendor/htm-mini.js")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(htmResp.statusCode()).isEqualTo(200);
            assertThat(htmResp.body())
                    .contains("export function html")
                    .contains("export function h");

            HttpResponse<String> seedResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/demo/seed"))
                            .header("Authorization", "Bearer ui-token")
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(seedResp.statusCode()).isEqualTo(200);
            assertThat(seedResp.body()).contains("\"seededSessions\":6");

            HttpResponse<String> detailResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/sessions/sess-citrus-main-942"))
                            .header("Authorization", "Bearer ui-token")
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(detailResp.statusCode()).isEqualTo(200);
            assertThat(detailResp.body())
                    .contains("\"partitionId\":\"part-942-01\"")
                    .contains("httpOrderServer");

            HttpResponse<String> prDetailResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/sessions/sess-citrus-pr-2108"))
                            .header("Authorization", "Bearer ui-token")
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(prDetailResp.statusCode()).isEqualTo(200);
            assertThat(prDetailResp.body())
                    .contains("\"partitionId\":\"part-902\"")
                    .contains("\"forkWorkspace\":true")
                    .contains("\"producedOutputs\":[\"target/citrus-reports\"]")
                    .contains("Worker disconnected during execution");

            HttpResponse<String> historyResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/tests/history?limit=25"))
                            .header("Authorization", "Bearer ui-token")
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(historyResp.statusCode()).isEqualTo(200);
            assertThat(historyResp.body())
                    .contains("\"passedRuns\":")
                    .contains("\"maxDurationMs\":")
                    .contains("\"lastStatus\":\"PASSED\"");
        }
    }
}
