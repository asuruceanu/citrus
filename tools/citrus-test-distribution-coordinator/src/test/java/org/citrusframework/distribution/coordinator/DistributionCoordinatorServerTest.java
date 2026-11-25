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
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.AgentPoolSummary;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CoordinatorEvent;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.GeneratedRegistrationKey;
import org.citrusframework.distribution.model.HistoricalDurationsRequest;
import org.citrusframework.distribution.model.HistoricalDurationsResponse;
import org.citrusframework.distribution.model.HistoricalUsageResponse;
import org.citrusframework.distribution.model.MissingDigestsRequest;
import org.citrusframework.distribution.model.MissingDigestsResponse;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionCompletedPayload;
import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.RegistrationKeyRecord;
import org.citrusframework.distribution.model.ReportLocalTestsRequest;
import org.citrusframework.distribution.model.SessionDetailResponse;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.TestExecutionRecord;
import org.citrusframework.distribution.model.TestHistorySummary;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DistributionCoordinatorServerTest {

    @TempDir
    Path tempDir;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void shouldReadCoordinatorSettingsAndDefaultCachePath() {
        String durationKey = "citrus.test.distribution.coordinator.minPartitionDurationMs";
        String wavesKey = "citrus.test.distribution.coordinator.maxWavesPerWorker";
        String previousDuration = System.setProperty(durationKey, "7250");
        String previousWaves = System.setProperty(wavesKey, "4");
        try {
            DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder().build();
            assertThat(config.cacheDir()).isEqualTo(
                    Path.of(System.getProperty("user.home"), ".citrus", "dist-coordinator-cache"));
            assertThat(config.minPartitionDurationMs()).isEqualTo(7_250L);
            assertThat(config.maxWavesPerWorker()).isEqualTo(4);
        } finally {
            restoreProperty(durationKey, previousDuration);
            restoreProperty(wavesKey, previousWaves);
        }
    }

    private static void restoreProperty(String key, String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }

    @Test
    void shouldManagePoolsAndReturnSecretsOnlyOnCreation() throws Exception {
        var config = poolConfig().authToken("top-secret-token").build();
        try (var server = new DistributionCoordinatorServer(config)) {
            server.start();
            String pools = server.baseUrl() + "/api/v1/agent-pools";
            assertThat(postJson(pools, Map.of("displayName", "Unauthorized")).statusCode()).isEqualTo(401);
            assertThat(httpClient.send(HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/usage")).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(401);

            var created = api("POST", pools, Map.of("displayName", "Linux", "capabilities", Map.of("os", "linux"),
                    "generateRegistrationKey", true, "keyDescription", "Bootstrap"));
            assertThat(created.statusCode()).isEqualTo(201);
            assertThat(created.headers().firstValue("Cache-Control")).contains("no-store");
            var result = DistributionJson.fromJson(created.body(), SessionCoordinator.CreatedAgentPool.class);
            String poolId = result.pool().poolId();
            String pool = pools + "/" + poolId;
            var preflight = httpClient.send(HttpRequest.newBuilder(URI.create(pool))
                    .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).header("Origin", "https://example.org")
                    .header("Access-Control-Request-Method", "DELETE").build(), HttpResponse.BodyHandlers.discarding());
            assertThat(preflight.statusCode()).isEqualTo(204);
            assertThat(preflight.headers().firstValue("Access-Control-Allow-Methods").orElseThrow()).contains("DELETE");
            var first = result.generatedKey();
            assertThat(first.secretKey()).isNotBlank();
            assertThat(first.metadata().description()).isEqualTo("Bootstrap");

            var detail = api("GET", pool, null);
            assertThat(detail.statusCode()).isEqualTo(200);
            assertThat(detail.body()).doesNotContain("secretKey", "keyHash", first.secretKey());
            assertThat(DistributionJson.fromJson(detail.body(), SessionCoordinator.AgentPoolDetail.class).registrationKeys())
                    .containsExactly(first.metadata());
            assertThat(api("PUT", pool, Map.of()).statusCode()).isEqualTo(400);
            var replaced = api("PUT", pool, Map.of("displayName", "Renamed"));
            assertThat(replaced.statusCode()).isEqualTo(200);
            var updated = DistributionJson.fromJson(replaced.body(), AgentPoolSummary.class);
            assertThat(updated.displayName()).isEqualTo("Renamed");
            assertThat(updated.capabilities()).isEmpty();

            var nextKey = api("POST", pool + "/keys", null);
            assertThat(nextKey.statusCode()).isEqualTo(201);
            assertThat(nextKey.headers().firstValue("Cache-Control")).contains("no-store");
            var second = DistributionJson.fromJson(nextKey.body(), GeneratedRegistrationKey.class);
            assertThat(second.secretKey()).isNotBlank().isNotEqualTo(first.secretKey());
            var revoke = api("POST", pool + "/keys/" + first.metadata().keyId() + "/revoke", null);
            assertThat(revoke.statusCode()).isEqualTo(200);
            var revoked = DistributionJson.fromJson(revoke.body(), RegistrationKeyRecord.class);
            assertThat(revoked.revokedAt()).isNotNull();
            var keys = api("GET", pool + "/keys", null);
            assertThat(keys.statusCode()).isEqualTo(200);
            assertThat(keys.body()).doesNotContain("secretKey", "keyHash", first.secretKey(), second.secretKey());
            assertThat(DistributionJson.fromJson(keys.body(), RegistrationKeyRecord[].class))
                    .containsExactlyInAnyOrder(revoked, second.metadata());

            var deleted = api("DELETE", pool, null);
            assertThat(deleted.statusCode()).isEqualTo(200);
            assertThat(DistributionJson.mapper().readTree(deleted.body()))
                    .isEqualTo(DistributionJson.mapper().valueToTree(Map.of("deleted", true, "poolId", poolId)));
            assertThat(api("GET", pool, null).statusCode()).isEqualTo(404);
            assertThat(api("PUT", pool, Map.of("displayName", "Gone")).statusCode()).isEqualTo(404);
            assertThat(api("DELETE", pool, null).statusCode()).isEqualTo(404);
            assertThat(api("GET", pool + "/keys", null).statusCode()).isEqualTo(404);
            assertThat(api("POST", pool + "/keys", Map.of("description", "Gone")).statusCode()).isEqualTo(404);
            assertThat(api("POST", pool + "/keys/missing/revoke", null).statusCode()).isEqualTo(404);
            assertThat(api("POST", pools, null).statusCode()).isEqualTo(400);
            assertThat(api("POST", pools, Map.of("displayName", " ")).statusCode()).isEqualTo(400);
            for (var invalid : List.of(Map.of("displayName", 123), Map.of("displayName", "Bad", "capabilities", List.of("linux")),
                    Map.of("displayName", "Bad", "capabilities", Map.of("jdk", 21)),
                    Map.of("displayName", "Bad", "generateRegistrationKey", "true"))) {
                assertThat(api("POST", pools, invalid).statusCode()).isEqualTo(400);
            }
            var withoutKey = api("POST", pools, Map.of("displayName", "No credentials"));
            assertThat(withoutKey.statusCode()).isEqualTo(201);
            assertThat(DistributionJson.fromJson(withoutKey.body(), SessionCoordinator.CreatedAgentPool.class).generatedKey())
                    .isNull();
        }
    }

    @Test
    void shouldReorderPoolsThroughPriorityRoute() throws Exception {
        try (var server = new DistributionCoordinatorServer(poolConfig().build())) {
            server.start();
            String pools = server.baseUrl() + "/api/v1/agent-pools";
            String first = createPool(pools, "First", Map.of()).pool().poolId();
            String second = createPool(pools, "Second", Map.of()).pool().poolId();
            var result = api("PUT", pools + "/priority", Map.of("poolIds", List.of(second, first)));
            assertThat(result.statusCode()).isEqualTo(200);
            assertThat(DistributionJson.fromJson(result.body(), AgentPoolSummary[].class))
                    .extracting(AgentPoolSummary::poolId).containsExactly(second, first);
            for (var invalid : List.of(Map.of(), Map.of("poolIds", List.of(second)),
                    Map.of("poolIds", List.of(first, first)), Map.of("poolIds", List.of(second, "unknown")))) {
                assertThat(api("PUT", pools + "/priority", invalid).statusCode()).isEqualTo(400);
            }
            var listed = api("GET", pools, null);
            assertThat(listed.statusCode()).isEqualTo(200);
            assertThat(DistributionJson.fromJson(listed.body(), AgentPoolSummary[].class))
                    .extracting(AgentPoolSummary::poolId).containsExactly(second, first);
        }
    }

    @Test
    void shouldQueryAndSampleFilteredHistoricalUsage() throws Exception {
        try (var server = new DistributionCoordinatorServer(poolConfig().usageSampleIntervalMs(100).build())) {
            server.start();
            String usage = server.baseUrl() + "/api/v1/usage";
            var defaultWindow = api("GET", usage, null);
            assertThat(defaultWindow.statusCode()).isEqualTo(200);
            assertThat(DistributionJson.fromJson(defaultWindow.body(), HistoricalUsageResponse.class).window()).isEqualTo("1h");
            assertThat(api("GET", usage + "?window=bad", null).statusCode()).isEqualTo(400);
            assertThat(api("GET", usage + "?window=1h&filter=a=b=c", null).statusCode()).isEqualTo(400);

            String pools = server.baseUrl() + "/api/v1/agent-pools";
            var matching = createPool(pools, "JDK 21", Map.of("jdk", "21"));
            createPool(pools, "JDK 17", Map.of("jdk", "17"));
            var collector = new WsCollector();
            var worker = connectWorker(server.wsUrl(), collector, new WorkerRegisterPayload("worker", "host", null,
                    matching.pool().poolId(), matching.generatedKey().secretKey(), Map.of("jdk", "21"), null));
            try {
                assertThat(collector.nextMessage().type()).isEqualTo(WorkerWsMessage.WORKER_REGISTERED);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                HistoricalUsageResponse response;
                do {
                    var filtered = api("GET", usage + "?window=1h&filter=jdk=21", null);
                    assertThat(filtered.statusCode()).isEqualTo(200);
                    response = DistributionJson.fromJson(filtered.body(), HistoricalUsageResponse.class);
                    if (response.series().stream().anyMatch(point -> point.connected() == 1)) {
                        break;
                    }
                    Thread.sleep(25);
                } while (System.nanoTime() < deadline);
                assertThat(response.series()).hasSize(60).anyMatch(point -> point.connected() == 1);
                assertThat(response.pools()).singleElement().satisfies(pool -> {
                    assertThat(pool.poolId()).isEqualTo(matching.pool().poolId());
                    assertThat(pool.points()).hasSize(60).anyMatch(point -> point.connected() == 1);
                });
            } finally {
                worker.abort();
            }
        }
    }

    @Test
    void shouldRecordUsageOnStartupBeforeTheFirstPeriodicSample() throws Exception {
        try (var server = new DistributionCoordinatorServer(poolConfig().usageSampleIntervalMs(60_000).build())) {
            server.start();
            assertThat(server.store().listUsageSamples(0, System.currentTimeMillis())).hasSize(1);
        }
    }

    @Test
    void shouldCloseAfterRejectingTheInitialWorkerRegistration() throws Exception {
        try (var server = new DistributionCoordinatorServer(poolConfig().requireAgentPool(true).build())) {
            server.start();
            var collector = new WsCollector();
            var worker = connectWorker(server.wsUrl(), collector, new WorkerRegisterPayload("worker", "host", Map.of()));
            try {
                var rejection = collector.nextMessage();
                assertThat(rejection.type()).isEqualTo(WorkerWsMessage.WORKER_REJECTED);
                assertThat(rejection.payload()).containsEntry("code", "POOL_REQUIRED");
                assertThat(collector.closeCode.get(5, TimeUnit.SECONDS)).isEqualTo(4003);
            } finally {
                worker.abort();
            }
        }
    }

    @Test
    void shouldSendSafePoolAndWarningEventsAndRejectWorkerBeforeClosing() throws Exception {
        try (var server = new DistributionCoordinatorServer(poolConfig().build())) {
            server.start();
            String pools = server.baseUrl() + "/api/v1/agent-pools";
            var created = createPool(pools, "Builders", Map.of());
            String poolId = created.pool().poolId();
            String pool = pools + "/" + poolId;
            var key = created.generatedKey();
            String hash = server.store().findRegistrationKey(key.metadata().keyId()).orElseThrow().keyHash();
            var collector = new WsCollector();
            var worker = connectWorker(server.wsUrl(), collector, new WorkerRegisterPayload("worker", "host", null,
                    poolId, key.secretKey(), Map.of(), null));
            try {
                var acknowledgement = collector.nextMessage();
                assertThat(acknowledgement.type()).isEqualTo(WorkerWsMessage.WORKER_REGISTERED);
                assertThat(acknowledgement.payload()).containsExactlyInAnyOrderEntriesOf(Map.of("workerId", "worker", "poolId", poolId));
                var stream = httpClient.send(HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/events/stream"))
                        .build(), HttpResponse.BodyHandlers.ofLines());
                assertThat(stream.statusCode()).isEqualTo(200);
                try (var lines = stream.body()) {
                    var delivered = CompletableFuture.supplyAsync(() -> collectUntilPoolDeleted(lines));
                    assertThat(api("POST", pool + "/keys/" + key.metadata().keyId() + "/revoke", null).statusCode())
                            .isEqualTo(200);
                    assertThat(api("GET", server.baseUrl() + "/api/v1/workers", null).body()).contains("KEY_REVOKED");
                    sendWs(worker, WorkerWsMessage.WORKER_RENEW, Map.of("workerId", "worker", "poolId", poolId,
                            "registrationKey", key.secretKey()));
                    var rejection = collector.nextMessage();
                    assertThat(rejection.type()).isEqualTo(WorkerWsMessage.WORKER_REJECTED);
                    assertThat(rejection.payload()).containsEntry("code", "REVOKED_REGISTRATION_KEY");
                    assertThat(DistributionJson.toJson(rejection)).doesNotContain(key.secretKey(), hash, "keyHash");
                    assertThat(collector.closeCode.get(5, TimeUnit.SECONDS)).isEqualTo(4003);
                    assertThat(collector.closeReason).doesNotContain(key.secretKey(), hash);
                    assertThat(api("DELETE", pool, null).statusCode()).isEqualTo(200);
                    var events = delivered.get(5, TimeUnit.SECONDS);
                    assertThat(events).extracting(CoordinatorEvent::type).containsSubsequence("pool.updated",
                            "worker.registered", "worker.updated", "pool.updated", "worker.rejected", "pool.deleted");
                    assertThat(events).filteredOn(event -> event.type().equals("worker.rejected")).singleElement()
                            .satisfies(event -> assertThat(event.data()).containsEntry("code", "REVOKED_REGISTRATION_KEY"));
                    assertThat(DistributionJson.toJson(events)).doesNotContain(key.secretKey(), hash, "secretKey", "keyHash");
                }
            } finally {
                worker.abort();
            }
        }
    }

    private static List<CoordinatorEvent> collectUntilPoolDeleted(Stream<String> lines) {
        var events = new ArrayList<CoordinatorEvent>();
        var data = lines.filter(line -> line.startsWith("data: ")).iterator();
        while (data.hasNext()) {
            var event = DistributionJson.fromJson(data.next().substring("data: ".length()), CoordinatorEvent.class);
            events.add(event);
            if ("pool.deleted".equals(event.type())) {
                break;
            }
        }
        return events;
    }

    private DistributionCoordinatorConfig.Builder poolConfig() {
        return DistributionCoordinatorConfig.builder().port(0).cacheDir(tempDir.resolve("pool-cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("coordinator_pools") + ";shutdown=true");
    }

    private SessionCoordinator.CreatedAgentPool createPool(String url, String name, Map<String, String> capabilities)
            throws Exception {
        var response = api("POST", url, Map.of("displayName", name, "capabilities", capabilities,
                "generateRegistrationKey", true));
        assertThat(response.statusCode()).isEqualTo(201);
        return DistributionJson.fromJson(response.body(), SessionCoordinator.CreatedAgentPool.class);
    }

    private HttpResponse<String> api(String method, String url, Object body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5))
                .header("Authorization", "Bearer top-secret-token").header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(DistributionJson.toJson(body)));
        return httpClient.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void shouldServeHistoricalDurationsEndpointAndValidateBounds() throws Exception {
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .authToken("top-secret-token")
                .cacheDir(tempDir.resolve("cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("coordinator_duration_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer server = new DistributionCoordinatorServer(config)) {
            server.start();
            String key = "pkg.SampleTest";
            server.store().saveTestExecution(new TestExecutionRecord("test-duration", "history", "part", "worker",
                    key, key, "test", TestOutcomeStatus.PASSED, 250L, 1, false, null, null, List.of(), 1L));
            String url = server.baseUrl() + "/api/v1/history/durations";

            assertThat(postJson(url, new HistoricalDurationsRequest(List.of(key), 10)).statusCode()).isEqualTo(401);
            HttpResponse<String> known = postHistoricalDurations(url,
                    DistributionJson.toJson(new HistoricalDurationsRequest(Arrays.asList(key, "pkg.Unknown", "", null, key), 10)));
            assertThat(known.statusCode()).isEqualTo(200);
            assertThat(DistributionJson.fromJson(known.body(), HistoricalDurationsResponse.class).durationsMs())
                    .isEqualTo(Map.of(key, 250L));

            HttpResponse<String> empty = postHistoricalDurations(url, "{\"testKeys\":[],\"limit\":10}");
            assertThat(empty.statusCode()).isEqualTo(200);
            assertThat(DistributionJson.fromJson(empty.body(), HistoricalDurationsResponse.class).durationsMs()).isEmpty();
            assertThat(postHistoricalDurations(url, DistributionJson.toJson(
                    new HistoricalDurationsRequest(Collections.nCopies(10_001, key), 10))).statusCode()).isEqualTo(400);
            for (String invalid : List.of("", "{", "null", "{} {}", "{\"testKeys\":{}}")) {
                assertThat(postHistoricalDurations(url, invalid).statusCode()).isEqualTo(400);
            }
        }
    }

    private HttpResponse<String> postHistoricalDurations(String url, String body) throws Exception {
        return httpClient.send(
                HttpRequest.newBuilder(URI.create(url))
                        .header("Authorization", "Bearer top-secret-token")
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void shouldEnforceBearerAuthExceptHealthEndpoint() throws Exception {
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .authToken("top-secret-token")
                .cacheDir(tempDir.resolve("cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("coordinator_auth_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer server = new DistributionCoordinatorServer(config)) {
            server.start();

            HttpResponse<String> health = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/health")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(health.statusCode()).isEqualTo(200);
            assertThat(health.body()).contains("UP");

            HttpResponse<String> unauth = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/sessions")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(unauth.statusCode()).isEqualTo(401);

            HttpResponse<String> wrongAuth = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/sessions"))
                            .header("Authorization", "Bearer wrong")
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(wrongAuth.statusCode()).isEqualTo(401);

            HttpResponse<String> validAuth = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/sessions"))
                            .header("Authorization", "Bearer top-secret-token")
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(validAuth.statusCode()).isEqualTo(200);
        }
    }

    @Test
    void shouldRefuseAWorkerOwnedCache() throws Exception {
        Path cacheDir = tempDir.resolve("worker-owned");
        BlobCacheDirectory.recordOwnerIfAbsent(cacheDir, BlobCacheDirectory.OWNER_WORKER);
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder().port(0).cacheDir(cacheDir).build();
        try (DistributionCoordinatorServer server = new DistributionCoordinatorServer(config)) {
            assertThatThrownBy(server::start)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContainingAll(cacheDir.toString(), "owned by worker", "--cache-dir");
        }
    }

    @Test
    void shouldDeduplicateAndValidateContentAddressedBlobs() throws Exception {
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("blob-cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("coordinator_blobs_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer server = new DistributionCoordinatorServer(config)) {
            server.start();

            byte[] payload = "citrus-test-jar-bytes".getBytes(StandardCharsets.UTF_8);
            String sha256 = ClasspathPackager.sha256Hex(payload);
            String otherSha = "a".repeat(64);

            HttpResponse<String> missingBefore = postJson(
                    server.baseUrl() + "/api/v1/cache/missing",
                    new MissingDigestsRequest(List.of(sha256, otherSha)));
            assertThat(missingBefore.statusCode()).isEqualTo(200);
            MissingDigestsResponse beforeBody = DistributionJson.fromJson(missingBefore.body(), MissingDigestsResponse.class);
            assertThat(beforeBody.missing()).containsExactly(sha256, otherSha);

            HttpResponse<String> badDigest = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/cache/blobs/not-a-sha256"))
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(badDigest.statusCode()).isEqualTo(400);

            HttpResponse<String> mismatchDigest = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/cache/blobs/" + otherSha))
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(mismatchDigest.statusCode()).isEqualTo(400);

            HttpResponse<String> uploadOk = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/cache/blobs/" + sha256))
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(uploadOk.statusCode()).isEqualTo(201);

            HttpResponse<String> missingAfter = postJson(
                    server.baseUrl() + "/api/v1/cache/missing",
                    new MissingDigestsRequest(List.of(sha256, otherSha)));
            MissingDigestsResponse afterBody = DistributionJson.fromJson(missingAfter.body(), MissingDigestsResponse.class);
            assertThat(afterBody.missing()).containsExactly(otherSha);

            HttpResponse<byte[]> downloaded = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/cache/blobs/" + sha256)).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(downloaded.statusCode()).isEqualTo(200);
            assertThat(downloaded.body()).isEqualTo(payload);
        }
    }

    @Test
    void shouldDispatchPartitionsMatchCapabilitiesRequeueOnDisconnectAndReplaySse() throws Exception {
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("dispatch-cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("coordinator_dispatch_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer server = new DistributionCoordinatorServer(config)) {
            server.start();

            HttpResponse<String> createdResp = postJson(
                    server.baseUrl() + "/api/v1/sessions",
                    new CreateSessionRequest("citrus", "feature/dist", "abc1234", List.of("pom.xml"), 2, 1));
            assertThat(createdResp.statusCode()).isEqualTo(201);
            BuildSessionResponse session = DistributionJson.fromJson(createdResp.body(), BuildSessionResponse.class);

            WorkUnitSpec dockerUnit = new WorkUnitSpec(
                    "org.example.DockerIT",
                    "org.example.DockerIT",
                    null,
                    null,
                    Map.of("docker", "true"),
                    2_000L);
            ArtifactRef artifact = new ArtifactRef("test.jar", "b".repeat(64), ArtifactKind.JAR, 128L);

            SubmitWorkRequest submitReq = new SubmitWorkRequest(
                    "sub-1",
                    "citrus-http",
                    List.of(artifact),
                    Map.of("env", "ci"),
                    List.of(dockerUnit),
                    1,
                    0);
            HttpResponse<String> submitResp = postJson(
                    server.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/work",
                    submitReq);
            assertThat(submitResp.statusCode()).isEqualTo(200);
            SubmitWorkResponse submitBody = DistributionJson.fromJson(submitResp.body(), SubmitWorkResponse.class);
            assertThat(submitBody.partitionsCreated()).isEqualTo(1);

            // Idempotent submission check
            HttpResponse<String> duplicateSubmit = postJson(
                    server.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/work",
                    submitReq);
            SubmitWorkResponse dupBody = DistributionJson.fromJson(duplicateSubmit.body(), SubmitWorkResponse.class);
            assertThat(dupBody.partitionIds()).isEqualTo(submitBody.partitionIds());

            // Connect non-docker worker: should NOT receive docker=true partition
            WsCollector nonDockerCollector = new WsCollector();
            WebSocket nonDockerWs = connectWorker(server.wsUrl(), nonDockerCollector,
                    new WorkerRegisterPayload("worker-nodocker", "host-a", Map.of("docker", "false")));
            WorkerWsMessage regAckA = nonDockerCollector.nextMessage();
            assertThat(regAckA.type()).isEqualTo(WorkerWsMessage.WORKER_REGISTERED);
            assertThat(nonDockerCollector.pollMessage(200)).isNull();

            // Connect docker worker-1: receives partition, then abruptly disconnects mid-partition!
            WsCollector dockerCollector1 = new WsCollector();
            WebSocket dockerWs1 = connectWorker(server.wsUrl(), dockerCollector1,
                    new WorkerRegisterPayload("worker-docker-1", "host-b", Map.of("docker", "true")));
            assertThat(dockerCollector1.nextMessage().type()).isEqualTo(WorkerWsMessage.WORKER_REGISTERED);
            WorkerWsMessage assign1 = dockerCollector1.nextMessage();
            assertThat(assign1.type()).isEqualTo(WorkerWsMessage.PARTITION_ASSIGN);
            PartitionAssignPayload assignPayload1 = DistributionJson.convert(assign1.payload(), PartitionAssignPayload.class);
            assertThat(assignPayload1.attempt()).isEqualTo(1);

            // Abruptly close worker-docker-1 without completing partition
            dockerWs1.abort();

            // Connect docker worker-2: receives requeued partition with attempt=2 and completes it!
            WsCollector dockerCollector2 = new WsCollector();
            WebSocket dockerWs2 = connectWorker(server.wsUrl(), dockerCollector2,
                    new WorkerRegisterPayload("worker-docker-2", "host-c", Map.of("docker", "true")));
            assertThat(dockerCollector2.nextMessage().type()).isEqualTo(WorkerWsMessage.WORKER_REGISTERED);
            WorkerWsMessage assign2 = dockerCollector2.nextMessage();
            assertThat(assign2.type()).isEqualTo(WorkerWsMessage.PARTITION_ASSIGN);
            PartitionAssignPayload assignPayload2 = DistributionJson.convert(assign2.payload(), PartitionAssignPayload.class);
            assertThat(assignPayload2.partitionId()).isEqualTo(assignPayload1.partitionId());
            assertThat(assignPayload2.attempt()).isEqualTo(2);

            sendWs(dockerWs2, WorkerWsMessage.PARTITION_STARTED, Map.of(
                    "partitionId", assignPayload2.partitionId(),
                    "sessionId", session.sessionId()));

            @SuppressWarnings("unchecked")
            Map<String, Object> testEventMap = DistributionJson.convert(new WorkerTestEventPayload(
                    assignPayload2.partitionId(),
                    session.sessionId(),
                    2,
                    dockerUnit.key(),
                    dockerUnit.className(),
                    "shouldRunInDocker",
                    TestOutcomeStatus.PASSED,
                    420L,
                    null,
                    null,
                    List.of("httpClient")), Map.class);
            sendWs(dockerWs2, WorkerWsMessage.TEST_EVENT, testEventMap);

            @SuppressWarnings("unchecked")
            Map<String, Object> doneMap = DistributionJson.convert(
                    new PartitionCompletedPayload(assignPayload2.partitionId(), session.sessionId(),
                            assignPayload2.attempt(), true, null, List.of()),
                    Map.class);
            sendWs(dockerWs2, WorkerWsMessage.PARTITION_COMPLETED, doneMap);

            HttpResponse<String> completeResp = postJson(
                    server.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/complete",
                    new CompleteSessionRequest(1, 0, 0, 100L));
            assertThat(completeResp.statusCode()).isEqualTo(200);

            HttpResponse<String> detailResp = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/sessions/" + session.sessionId()))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            SessionDetailResponse detail = DistributionJson.fromJson(detailResp.body(), SessionDetailResponse.class);
            assertThat(detail.session().status()).isEqualTo(SessionStatus.PASSED);
            assertThat(detail.session().passedTests()).isEqualTo(2);
            assertThat(detail.partitions()).hasSize(2);
            assertThat(detail.partitions().get(0).attempt()).isEqualTo(1);
            assertThat(detail.partitions().get(0).state()).isEqualTo(PartitionState.LOST);
            assertThat(detail.partitions().get(0).workerId()).isEqualTo("worker-docker-1");
            assertThat(detail.partitions().get(1).attempt()).isEqualTo(2);
            assertThat(detail.partitions().get(1).state()).isEqualTo(PartitionState.COMPLETED);
            assertThat(detail.partitions().get(1).workerId()).isEqualTo("worker-docker-2");

            // Replay SSE history from sequence 0 (completed session closes SSE stream cleanly after replay)
            HttpResponse<String> sseReplay = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/events?afterSeq=0"))
                            .timeout(Duration.ofSeconds(5))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(sseReplay.statusCode()).isEqualTo(200);
            assertThat(sseReplay.body())
                    .contains("event: partition.requeued")
                    .contains("event: test.event")
                    .contains("event: partition.completed")
                    .contains("event: session.completed");

            nonDockerWs.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
            dockerWs2.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
        }
    }

    @Test
    void shouldRejectConflictingCapabilitiesUnderSinglePolicyWithBadRequest() throws Exception {
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("single-cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("coordinator_single_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer server = new DistributionCoordinatorServer(config)) {
            server.start();
            BuildSessionResponse session = DistributionJson.fromJson(postJson(server.baseUrl() + "/api/v1/sessions",
                    new CreateSessionRequest("citrus", "main", "abc", List.of(), 5, 1)).body(), BuildSessionResponse.class);

            SubmitWorkRequest conflicting = new SubmitWorkRequest("sub-single", "citrus-http", List.of(), Map.of(),
                    List.of(new WorkUnitSpec("u1", "pkg.A", "m", null, Map.of("os", "linux"), 0L),
                            new WorkUnitSpec("u2", "pkg.B", "m", null, Map.of("os", "windows"), 0L)),
                    0, 0, List.of(), List.of(), false, PartitionPolicy.SINGLE);
            HttpResponse<String> response = postJson(
                    server.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/work", conflicting);

            assertThat(response.statusCode()).isEqualTo(400);
            assertThat(response.body()).contains("\"error\"").contains("os");
        }
    }

    @Test
    void shouldDeduplicateWorkSubmissionAcrossCoordinatorRestart() throws Exception {
        String jdbcUrl = "jdbc:hsqldb:file:" + tempDir.resolve("restart-db/coordinator");
        Path sharedCache = tempDir.resolve("restart-cache");

        WorkUnitSpec unit = new WorkUnitSpec(
                "org.example.RestartIT#shouldRunOnce",
                "org.example.RestartIT",
                "shouldRunOnce",
                null,
                Map.of(),
                1_000L);
        ArtifactRef artifact = new ArtifactRef("restart-test.jar", "c".repeat(64), ArtifactKind.JAR, 256L);
        SubmitWorkRequest submitReq = new SubmitWorkRequest(
                "sub-restart-1",
                "citrus-http",
                List.of(artifact),
                Map.of("env", "ci"),
                List.of(unit),
                1,
                0);

        String sessionId;
        SubmitWorkResponse firstSubmit;

        DistributionCoordinatorConfig config1 = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(sharedCache)
                .jdbcUrl(jdbcUrl)
                .build();
        try (DistributionCoordinatorServer server1 = new DistributionCoordinatorServer(config1)) {
            server1.start();

            HttpResponse<String> createdResp = postJson(
                    server1.baseUrl() + "/api/v1/sessions",
                    new CreateSessionRequest("citrus", "main", "def5678", List.of("pom.xml"), 2, 1));
            assertThat(createdResp.statusCode()).isEqualTo(201);
            BuildSessionResponse session = DistributionJson.fromJson(createdResp.body(), BuildSessionResponse.class);
            sessionId = session.sessionId();

            HttpResponse<String> submitResp1 = postJson(
                    server1.baseUrl() + "/api/v1/sessions/" + sessionId + "/work",
                    submitReq);
            assertThat(submitResp1.statusCode()).isEqualTo(200);
            firstSubmit = DistributionJson.fromJson(submitResp1.body(), SubmitWorkResponse.class);
            assertThat(firstSubmit.partitionIds()).hasSize(1);
        }

        DistributionCoordinatorConfig config2 = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(sharedCache)
                .jdbcUrl(jdbcUrl)
                .build();
        try (DistributionCoordinatorServer server2 = new DistributionCoordinatorServer(config2)) {
            server2.start();

            HttpResponse<String> submitResp2 = postJson(
                    server2.baseUrl() + "/api/v1/sessions/" + sessionId + "/work",
                    submitReq);
            assertThat(submitResp2.statusCode()).isEqualTo(200);
            SubmitWorkResponse secondSubmit = DistributionJson.fromJson(submitResp2.body(), SubmitWorkResponse.class);
            assertThat(secondSubmit.partitionIds()).isEqualTo(firstSubmit.partitionIds());

            HttpResponse<String> detailResp = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server2.baseUrl() + "/api/v1/sessions/" + sessionId))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(detailResp.statusCode()).isEqualTo(200);
            SessionDetailResponse detail = DistributionJson.fromJson(detailResp.body(), SessionDetailResponse.class);
            assertThat(detail.partitions()).hasSize(1);
        }
    }

    @Test
    void shouldSupportLocalTestsReportingHistoryFieldsAndAbortEndpointOverHttp() throws Exception {
        DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("abort-cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("coordinator_abort_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer server = new DistributionCoordinatorServer(config)) {
            server.start();

            BuildSessionResponse session = DistributionJson.fromJson(postJson(
                    server.baseUrl() + "/api/v1/sessions",
                    new CreateSessionRequest("citrus", "feature/ui", "1234567", List.of("README.md"), 4, 1)).body(),
                    BuildSessionResponse.class);

            HttpResponse<String> localResp = postJson(
                    server.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/local-tests",
                    new ReportLocalTestsRequest("maven-host", List.of(
                            new WorkerTestEventPayload("local", session.sessionId(), 1,
                                    "org.example.LocalIT#shouldVerifyLocal", "org.example.LocalIT", "shouldVerifyLocal",
                                    TestOutcomeStatus.PASSED, 180L, null, null, List.of("localEndpoint")))));
            assertThat(localResp.statusCode()).isEqualTo(200);
            assertThat(localResp.body()).contains("\"recorded\":1");

            HttpResponse<String> historyResp = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/tests/history?limit=10")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(historyResp.statusCode()).isEqualTo(200);
            TestHistorySummary[] history = DistributionJson.fromJson(historyResp.body(), TestHistorySummary[].class);
            assertThat(history).hasSize(1);
            assertThat(history[0].passedRuns()).isEqualTo(1);
            assertThat(history[0].maxDurationMs()).isEqualTo(180L);
            assertThat(history[0].lastStatus()).isEqualTo(TestOutcomeStatus.PASSED);

            HttpResponse<String> abortResp = postJson(
                    server.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/abort",
                    Map.of("reason", "Manual abort from UI"));
            assertThat(abortResp.statusCode()).isEqualTo(200);
            assertThat(abortResp.body()).contains("\"status\":\"ABORTED\"");

            HttpResponse<String> sseReplay = httpClient.send(
                    HttpRequest.newBuilder(URI.create(server.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/events?afterSeq=0"))
                            .timeout(Duration.ofSeconds(5))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(sseReplay.statusCode()).isEqualTo(200);
            assertThat(sseReplay.body()).contains("event: session.aborted");
        }
    }

    private HttpResponse<String> postJson(String url, Object body) throws Exception {
        return httpClient.send(
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(DistributionJson.toJson(body)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private WebSocket connectWorker(String wsUrl, WsCollector collector, WorkerRegisterPayload registerPayload) {
        WebSocket ws = httpClient.newWebSocketBuilder()
                .buildAsync(URI.create(wsUrl), collector)
                .join();
        @SuppressWarnings("unchecked")
        Map<String, Object> payloadMap = DistributionJson.convert(registerPayload, Map.class);
        sendWs(ws, WorkerWsMessage.WORKER_REGISTER, payloadMap);
        return ws;
    }

    private void sendWs(WebSocket ws, String type, Map<String, Object> payload) {
        ws.sendText(DistributionJson.toJson(new WorkerWsMessage(type, payload)), true).join();
    }

    private static final class WsCollector implements WebSocket.Listener {
        private final CompletableFuture<Integer> closeCode = new CompletableFuture<>();
        private String closeReason;
        private final StringBuilder buffer = new StringBuilder();
        private final BlockingQueue<WorkerWsMessage> messages = new LinkedBlockingQueue<>();

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String json = buffer.toString();
                buffer.setLength(0);
                messages.offer(DistributionJson.fromJson(json, WorkerWsMessage.class));
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            closeReason = reason;
            closeCode.complete(statusCode);
            return null;
        }

        WorkerWsMessage nextMessage() throws InterruptedException {
            WorkerWsMessage msg = messages.poll(5, TimeUnit.SECONDS);
            assertThat(msg).as("Expected WebSocket message within 5s").isNotNull();
            return msg;
        }

        WorkerWsMessage pollMessage(long timeoutMs) throws InterruptedException {
            return messages.poll(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldMarkItsBlobCacheAsCoordinatorOwnedButKeepAnExistingOwner() throws Exception {
        Path fresh = tempDir.resolve("owned-cache");
        Path adopted = tempDir.resolve("adopted-cache");
        BlobCacheDirectory.recordOwnerIfAbsent(adopted, BlobCacheDirectory.OWNER_LAUNCHER);

        for (Path cacheDir : List.of(fresh, adopted)) {
            DistributionCoordinatorConfig config = DistributionCoordinatorConfig.builder()
                    .port(0)
                    .cacheDir(cacheDir)
                    .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("coordinator_owner_" + System.nanoTime()) + ";shutdown=true")
                    .build();
            try (DistributionCoordinatorServer server = new DistributionCoordinatorServer(config)) {
                server.start();
            }
        }

        assertThat(BlobCacheDirectory.readOwner(fresh)).contains(BlobCacheDirectory.OWNER_COORDINATOR);
        assertThat(BlobCacheDirectory.readOwner(adopted)).contains(BlobCacheDirectory.OWNER_LAUNCHER);
    }

    @Test
    void shouldBindLoopbackWhenEphemeralPortRequestedWithWildcardHost() {
        DistributionCoordinatorConfig ephemeralWildcard = DistributionCoordinatorConfig.builder()
            .host("0.0.0.0")
            .port(0)
            .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("ephemeral_host_test") + ";shutdown=true")
            .cacheDir(tempDir.resolve("ephemeral-cache"))
            .build();
        DistributionCoordinatorConfig explicitPortWildcard = DistributionCoordinatorConfig.builder()
            .host("0.0.0.0")
            .port(8765)
            .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("explicit_host_test") + ";shutdown=true")
            .cacheDir(tempDir.resolve("explicit-cache"))
            .build();

        assertThat(DistributionCoordinatorVerticle.resolveListenHost(ephemeralWildcard)).isEqualTo("127.0.0.1");
        assertThat(DistributionCoordinatorVerticle.resolveListenHost(explicitPortWildcard)).isEqualTo("0.0.0.0");
    }
}
