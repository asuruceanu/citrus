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

package org.citrusframework.distribution.coordinator;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class DistributionCoordinatorUiTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldServeOfflineCapableWebUiWithWaterfallAndAllViews() throws Exception {
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .authToken("ui-token")
                .usageSampleIntervalMs(150L)
                .cacheDir(tempDir.resolve("ui-cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("coordinator_ui_" + System.nanoTime()) + ";shutdown=true")
                .build();

        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        try (DistributionCoordinatorServer server = new DistributionCoordinatorServer(config)) {
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
                    .contains("Inspect distributed build sessions, artifact cache hit rates, and test outcomes.")
                    .doesNotContain("Inspect distributed Maven & JBang reactor runs, classpath cache hit rates, and test outcomes.")
                    .contains("data-view=${id}", "navItem('usage', 'Historical Usage'", "navItem('pools', 'Agent Pools'")
                    .doesNotContainIgnoringCase("Connecting Builds")
                    .doesNotContainIgnoringCase("project groups")
                    .doesNotContainIgnoringCase("access control")
                    .doesNotContain("minSize", "maxSize")
                    .contains("renderAgentPools", "renderHistoricalUsage", "apiPut", "apiDelete")
                    .contains("/api/v1/agent-pools", "/api/v1/agent-pools/priority", "/api/v1/usage")
                    .contains("POOL_CAPABILITY_MISMATCH", "POOL_DELETED", "KEY_REVOKED")
                    .contains("Waiting for test execution requests", "activeTestDisplay")
                    .contains("renderWaterfallSvg")
                    .contains("/api/v1/events/stream")
                    .contains("/api/v1/workers")
                    .contains("/api/v1/tests/history");

            HttpResponse<String> chartResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/ui/usage-chart.js")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(chartResp.statusCode()).isEqualTo(200);
            assertThat(chartResp.body()).contains("export function renderUsageChart", "<svg", "usage-tooltip");

            HttpResponse<String> htmResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/ui/vendor/htm-mini.js")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(htmResp.statusCode()).isEqualTo(200);
            assertThat(htmResp.body())
                    .contains("export function html")
                    .contains("export function h");

            HttpResponse<String> usageResp = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/usage"))
                            .header("Authorization", "Bearer ui-token")
                            .header("Origin", "http://dashboard.example")
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(usageResp.statusCode()).isEqualTo(200);
            assertThat(usageResp.headers().firstValue("X-Citrus-Usage-Sample-Interval-Ms")).hasValue("150");
            assertThat(usageResp.headers().firstValue("Access-Control-Expose-Headers"))
                    .hasValueSatisfying(value -> assertThat(value).containsIgnoringCase("X-Citrus-Usage-Sample-Interval-Ms"));
            assertThat(usageResp.body()).contains("\"window\":\"1h\"", "\"series\"");

            assertThat(jsResp.body()).doesNotContain("Seed Demo Data");
            HttpResponse<String> removedSeedRoute = client.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/demo/seed"))
                            .header("Authorization", "Bearer ui-token")
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(removedSeedRoute.statusCode()).isIn(404, 405);
        }
    }
}
