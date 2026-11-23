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

package org.citrusframework.distribution.worker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.SessionDetailResponse;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DistributionWorkerClientTest {

    @TempDir
    Path tempDir;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public static class SampleRemoteTest {
        @Test
        void shouldPassOnWorker() {
            assertEquals("worker", System.getProperty("citrus.test.distribution.role"));
            assertEquals("hello-remote", System.getProperty("sample.custom.prop", "hello-remote"));
        }
    }

    public static class AlphaTenantRemoteTest {
        @Test
        void shouldIsolateAlphaTenantProperty() throws InterruptedException {
            assertEquals("worker", System.getProperty("citrus.test.distribution.role"));
            assertEquals("alpha", System.getProperty("custom.tenant.id"));
            AtomicReference<String> inherited = new AtomicReference<>();
            Thread child = new Thread(() -> inherited.set(System.getProperty("custom.tenant.id")));
            child.start();
            child.join(2_000L);
            assertEquals("alpha", inherited.get());
            Thread.sleep(250L);
            assertEquals("alpha", System.getProperty("custom.tenant.id"));
        }
    }

    public static class BetaTenantRemoteTest {
        @Test
        void shouldIsolateBetaTenantProperty() throws InterruptedException {
            assertEquals("worker", System.getProperty("citrus.test.distribution.role"));
            assertEquals("beta", System.getProperty("custom.tenant.id"));
            Thread.sleep(250L);
            assertEquals("beta", System.getProperty("custom.tenant.id"));
        }
    }

    @Test
    void shouldDownloadClasspathBlobExecuteJUnitTestAndStreamResultsToBroker() throws Exception {
        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .authToken("worker-secret")
                .cacheDir(tempDir.resolve("broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:worker_it_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();

            DistributionWorkerConfig workerConfig = DistributionWorkerConfig.builder()
                    .workerId("worker-1")
                    .brokerUrl(broker.baseUrl())
                    .authToken("worker-secret")
                    .cacheDir(tempDir.resolve("worker-cache"))
                    .forkMode(DistributionSettings.WorkerForkMode.AUTO) // exercises the shared worker JVM
                    .addCapabilitiesCsv("zone=eu-central")
                    .build();

            try (DistributionWorkerClient worker = new DistributionWorkerClient(workerConfig)) {
                worker.start();
                assertThat(worker.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();

                // Package test-classes directory as DIRECTORY_ZIP and upload to broker cache
                Path testClassesDir = Paths.get(SampleRemoteTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(testClassesDir);

                HttpResponse<String> uploadResp = httpClient.send(
                        HttpRequest.newBuilder(URI.create(broker.baseUrl() + "/api/v1/cache/blobs/" + packaged.ref().sha256()))
                                .header("Authorization", "Bearer worker-secret")
                                .PUT(HttpRequest.BodyPublishers.ofByteArray(packaged.bytes()))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(uploadResp.statusCode()).isEqualTo(201);

                HttpResponse<String> createResp = postJson(
                        broker.baseUrl() + "/api/v1/sessions",
                        "worker-secret",
                        new CreateSessionRequest("citrus", "main", "1234567", List.of(), 4, 1));
                BuildSessionResponse session = DistributionJson.fromJson(createResp.body(), BuildSessionResponse.class);

                WorkUnitSpec unit = new WorkUnitSpec(
                        SampleRemoteTest.class.getName() + "#shouldPassOnWorker",
                        SampleRemoteTest.class.getName(),
                        "shouldPassOnWorker",
                        null,
                        Map.of("zone", "eu-central"),
                        500L);

                SubmitWorkRequest submitReq = new SubmitWorkRequest(
                        "sub-worker-1",
                        "worker-test",
                        List.of(packaged.ref()),
                        Map.of("sample.custom.prop", "hello-remote"),
                        List.of(unit),
                        0,
                        1);
                HttpResponse<String> submitResp = postJson(
                        broker.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/work",
                        "worker-secret",
                        submitReq);
                assertThat(submitResp.statusCode()).isEqualTo(200);

                // Poll session detail until partition completes
                SessionDetailResponse detail = awaitPartitionCompletion(broker.baseUrl(), "worker-secret", session.sessionId());
                assertThat(detail.partitions()).hasSize(1);
                assertThat(detail.partitions().get(0).state()).isEqualTo(PartitionState.COMPLETED);
                assertThat(detail.tests()).anySatisfy(rec -> {
                    assertThat(rec.className()).isEqualTo(SampleRemoteTest.class.getName());
                    assertThat(rec.status()).isEqualTo(TestOutcomeStatus.PASSED);
                });

                postJson(
                        broker.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/complete",
                        "worker-secret",
                        new CompleteSessionRequest(0, 0, 0, 0L));

                SessionDetailResponse finalDetail = fetchDetail(broker.baseUrl(), "worker-secret", session.sessionId());
                assertThat(finalDetail.session().status()).isEqualTo(SessionStatus.PASSED);
                assertThat(finalDetail.session().passedTests()).isEqualTo(1);

                worker.drain(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void shouldIsolatePartitionSystemPropertiesAcrossConcurrentWorkersInSameJvm() throws Exception {
        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .authToken("worker-secret")
                .cacheDir(tempDir.resolve("broker-concurrent-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:worker_concurrent_" + System.nanoTime())
                .defaultMaxPartitionSize(1)
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();

            DistributionWorkerConfig alphaConfig = DistributionWorkerConfig.builder()
                    .workerId("worker-alpha")
                    .brokerUrl(broker.baseUrl())
                    .authToken("worker-secret")
                    .cacheDir(tempDir.resolve("worker-alpha-cache"))
                    .forkMode(DistributionSettings.WorkerForkMode.AUTO) // exercises the shared worker JVM
                    .build();
            DistributionWorkerConfig betaConfig = DistributionWorkerConfig.builder()
                    .workerId("worker-beta")
                    .brokerUrl(broker.baseUrl())
                    .authToken("worker-secret")
                    .cacheDir(tempDir.resolve("worker-beta-cache"))
                    .forkMode(DistributionSettings.WorkerForkMode.AUTO) // exercises the shared worker JVM
                    .build();

            try (DistributionWorkerClient workerAlpha = new DistributionWorkerClient(alphaConfig);
                 DistributionWorkerClient workerBeta = new DistributionWorkerClient(betaConfig)) {
                workerAlpha.start();
                workerBeta.start();
                assertThat(workerAlpha.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();
                assertThat(workerBeta.awaitRegistration(5, TimeUnit.SECONDS)).isTrue();

                Path testClassesDir = Paths.get(AlphaTenantRemoteTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(testClassesDir);

                HttpResponse<String> uploadResp = httpClient.send(
                        HttpRequest.newBuilder(URI.create(broker.baseUrl() + "/api/v1/cache/blobs/" + packaged.ref().sha256()))
                                .header("Authorization", "Bearer worker-secret")
                                .PUT(HttpRequest.BodyPublishers.ofByteArray(packaged.bytes()))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(uploadResp.statusCode()).isEqualTo(201);

                HttpResponse<String> createResp = postJson(
                        broker.baseUrl() + "/api/v1/sessions",
                        "worker-secret",
                        new CreateSessionRequest("citrus", "main", "1234567", List.of(), 1, 0));
                BuildSessionResponse session = DistributionJson.fromJson(createResp.body(), BuildSessionResponse.class);

                WorkUnitSpec alphaUnit = new WorkUnitSpec(
                        AlphaTenantRemoteTest.class.getName() + "#shouldIsolateAlphaTenantProperty",
                        AlphaTenantRemoteTest.class.getName(),
                        "shouldIsolateAlphaTenantProperty",
                        null,
                        Map.of(),
                        500L);
                WorkUnitSpec betaUnit = new WorkUnitSpec(
                        BetaTenantRemoteTest.class.getName() + "#shouldIsolateBetaTenantProperty",
                        BetaTenantRemoteTest.class.getName(),
                        "shouldIsolateBetaTenantProperty",
                        null,
                        Map.of(),
                        500L);

                HttpResponse<String> submitAlpha = postJson(
                        broker.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/work",
                        "worker-secret",
                        new SubmitWorkRequest(
                                "sub-alpha",
                                "module-alpha",
                                List.of(packaged.ref()),
                                Map.of("custom.tenant.id", "alpha"),
                                List.of(alphaUnit),
                                0,
                                1));
                HttpResponse<String> submitBeta = postJson(
                        broker.baseUrl() + "/api/v1/sessions/" + session.sessionId() + "/work",
                        "worker-secret",
                        new SubmitWorkRequest(
                                "sub-beta",
                                "module-beta",
                                List.of(packaged.ref()),
                                Map.of("custom.tenant.id", "beta"),
                                List.of(betaUnit),
                                1,
                                0));
                assertThat(submitAlpha.statusCode()).isEqualTo(200);
                assertThat(submitBeta.statusCode()).isEqualTo(200);

                SessionDetailResponse detail = awaitPartitionsCompletion(
                        broker.baseUrl(), "worker-secret", session.sessionId(), 2);
                assertThat(detail.partitions()).hasSize(2);
                assertThat(detail.partitions()).allSatisfy(p ->
                        assertThat(p.state()).isEqualTo(PartitionState.COMPLETED));
                assertThat(detail.tests()).hasSize(2);
                assertThat(detail.tests()).allSatisfy(rec ->
                        assertThat(rec.status()).as("Test %s failed: %s", rec.className(), rec.errorMessage())
                                .isEqualTo(TestOutcomeStatus.PASSED));
                assertThat(System.getProperty("custom.tenant.id")).isNull();

                workerAlpha.drain(5, TimeUnit.SECONDS);
                workerBeta.drain(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void shouldNormalizeWebSocketBrokerUrlsToHttpBaseUrl() {
        assertThat(DistributionWorkerConfig.builder()
                .brokerUrl("ws://127.0.0.1:18080/ws/v1/worker")
                .build()
                .brokerUrl()).isEqualTo("http://127.0.0.1:18080");
        assertThat(DistributionWorkerConfig.builder()
                .brokerUrl("wss://broker.example.org/ws/v1/worker/")
                .build()
                .brokerUrl()).isEqualTo("https://broker.example.org");
    }

    private SessionDetailResponse awaitPartitionsCompletion(String baseUrl, String token, String sessionId, int expectedPartitions) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            SessionDetailResponse detail = fetchDetail(baseUrl, token, sessionId);
            if (detail.partitions().size() >= expectedPartitions
                    && detail.partitions().stream().allMatch(p ->
                    p.state() == PartitionState.COMPLETED || p.state() == PartitionState.FAILED || p.state() == PartitionState.LOST)) {
                return detail;
            }
            Thread.sleep(50L);
        }
        return fetchDetail(baseUrl, token, sessionId);
    }

    private SessionDetailResponse awaitPartitionCompletion(String baseUrl, String token, String sessionId) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            SessionDetailResponse detail = fetchDetail(baseUrl, token, sessionId);
            if (!detail.partitions().isEmpty()
                    && detail.partitions().get(0).state() == PartitionState.COMPLETED) {
                return detail;
            }
            Thread.sleep(100L);
        }
        return fetchDetail(baseUrl, token, sessionId);
    }

    private SessionDetailResponse fetchDetail(String baseUrl, String token, String sessionId) throws Exception {
        HttpResponse<String> resp = httpClient.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/sessions/" + sessionId))
                        .header("Authorization", "Bearer " + token)
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        return DistributionJson.fromJson(resp.body(), SessionDetailResponse.class);
    }

    private HttpResponse<String> postJson(String url, String token, Object payload) throws Exception {
        return httpClient.send(
                HttpRequest.newBuilder(URI.create(url))
                        .header("Authorization", "Bearer " + token)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(DistributionJson.toJson(payload)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
