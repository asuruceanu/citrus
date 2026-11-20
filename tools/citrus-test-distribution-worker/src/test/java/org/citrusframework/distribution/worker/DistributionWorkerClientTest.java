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

import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
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
