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
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.MissingDigestsRequest;
import org.citrusframework.distribution.model.MissingDigestsResponse;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionCompletedPayload;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.SessionDetailResponse;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class DistributionBrokerServerTest {

    @TempDir
    Path tempDir;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Test
    void shouldEnforceBearerAuthExceptHealthEndpoint() throws Exception {
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .port(0)
                .authToken("top-secret-token")
                .cacheDir(tempDir.resolve("cache"))
                .jdbcUrl("jdbc:hsqldb:mem:broker_auth_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer server = new DistributionBrokerServer(config)) {
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
    void shouldDeduplicateAndValidateContentAddressedBlobs() throws Exception {
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("blob-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:broker_blobs_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer server = new DistributionBrokerServer(config)) {
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
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("dispatch-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:broker_dispatch_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer server = new DistributionBrokerServer(config)) {
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

            sendWs(dockerWs2, WorkerWsMessage.TEST_EVENT, DistributionJson.convert(new WorkerTestEventPayload(
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
                    List.of("httpClient")), Map.class));

            sendWs(dockerWs2, WorkerWsMessage.PARTITION_COMPLETED, DistributionJson.convert(
                    new PartitionCompletedPayload(assignPayload2.partitionId(), session.sessionId(), true, null),
                    Map.class));

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
            assertThat(detail.partitions()).hasSize(1);
            assertThat(detail.partitions().get(0).state()).isEqualTo(PartitionState.COMPLETED);
            assertThat(detail.partitions().get(0).workerId()).isEqualTo("worker-docker-2");

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

        WorkerWsMessage nextMessage() throws InterruptedException {
            WorkerWsMessage msg = messages.poll(5, TimeUnit.SECONDS);
            assertThat(msg).as("Expected WebSocket message within 5s").isNotNull();
            return msg;
        }

        WorkerWsMessage pollMessage(long timeoutMs) throws InterruptedException {
            return messages.poll(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }
}
