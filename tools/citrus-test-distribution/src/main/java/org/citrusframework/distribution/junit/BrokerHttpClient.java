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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;

import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.BrokerEvent;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.MissingDigestsRequest;
import org.citrusframework.distribution.model.MissingDigestsResponse;
import org.citrusframework.distribution.model.ReportLocalTestsRequest;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;

/**
 * HTTP/SSE client used by the JUnit Platform coordinator and Maven extension to communicate with the broker.
 */
public class BrokerHttpClient {

    private final String baseUrl;
    private final String authToken;
    private final HttpClient httpClient;

    public BrokerHttpClient(String baseUrl, String authToken, long connectTimeoutMs) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.authToken = authToken;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(Math.max(500L, connectTimeoutMs)))
                .build();
    }

    public BuildSessionResponse createSession(CreateSessionRequest request) throws IOException, InterruptedException {
        HttpResponse<String> resp = sendJsonPost("/api/v1/sessions", request);
        ensureSuccess(resp, "createSession");
        return DistributionJson.fromJson(resp.body(), BuildSessionResponse.class);
    }

    public List<String> findMissingDigests(List<String> digests) throws IOException, InterruptedException {
        if (digests.isEmpty()) {
            return List.of();
        }
        HttpResponse<String> resp = sendJsonPost("/api/v1/cache/missing", new MissingDigestsRequest(digests));
        ensureSuccess(resp, "findMissingDigests");
        MissingDigestsResponse body = DistributionJson.fromJson(resp.body(), MissingDigestsResponse.class);
        return body.missing() != null ? body.missing() : List.of();
    }

    public void uploadArtifact(ClasspathPackager.PackagedArtifact artifact) throws IOException, InterruptedException {
        HttpRequest.BodyPublisher publisher = artifact.sourceFile() != null
                ? HttpRequest.BodyPublishers.ofFile(artifact.sourceFile())
                : HttpRequest.BodyPublishers.ofByteArray(artifact.bytes());
        HttpRequest.Builder req = requestBuilder("/api/v1/cache/blobs/" + artifact.ref().sha256())
                .header("Content-Type", "application/octet-stream")
                .PUT(publisher);
        HttpResponse<String> resp = httpClient.send(req.build(), HttpResponse.BodyHandlers.ofString());
        ensureSuccess(resp, "uploadBlob(" + artifact.ref().sha256() + ")");
    }

    /**
     * Streams a blob from the coordinator into {@code target} (created or truncated), without holding it in memory.
     */
    public void downloadBlobToFile(String sha256, Path target) throws IOException, InterruptedException {
        HttpResponse<InputStream> resp = httpClient.send(requestBuilder("/api/v1/cache/blobs/" + sha256)
                        .timeout(Duration.ofMinutes(5))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream body = resp.body()) {
            if (resp.statusCode() != 200) {
                throw new IOException("Broker HTTP " + resp.statusCode() + " during downloadBlob " + sha256);
            }
            long limit = ClasspathPackager.MAX_BLOB_BYTES;
            if (resp.headers().firstValueAsLong("Content-Length").orElse(0L) > limit) {
                throw new IOException("Blob " + sha256 + " exceeds the maximum of " + limit + " bytes");
            }
            // Bounded while streaming: a misbehaving coordinator cannot fill the disk.
            try (java.io.OutputStream out = Files.newOutputStream(target)) {
                byte[] buffer = new byte[64 * 1024];
                long total = 0;
                int read;
                while ((read = body.read(buffer)) != -1) {
                    total += read;
                    if (total > limit) {
                        throw new IOException("Blob " + sha256 + " exceeds the maximum of " + limit + " bytes");
                    }
                    out.write(buffer, 0, read);
                }
            }
        }
    }

    public SubmitWorkResponse submitWork(String sessionId, SubmitWorkRequest request)
            throws IOException, InterruptedException {
        HttpResponse<String> resp = sendJsonPost("/api/v1/sessions/" + sessionId + "/work", request);
        ensureSuccess(resp, "submitWork");
        return DistributionJson.fromJson(resp.body(), SubmitWorkResponse.class);
    }

    public BuildSessionResponse completeSession(String sessionId, CompleteSessionRequest request)
            throws IOException, InterruptedException {
        HttpResponse<String> resp = sendJsonPost("/api/v1/sessions/" + sessionId + "/complete", request);
        ensureSuccess(resp, "completeSession");
        return DistributionJson.fromJson(resp.body(), BuildSessionResponse.class);
    }

    public void reportLocalTests(String sessionId, ReportLocalTestsRequest request)
            throws IOException, InterruptedException {
        if (request == null || request.executions() == null || request.executions().isEmpty()) {
            return;
        }
        HttpResponse<String> resp = sendJsonPost("/api/v1/sessions/" + sessionId + "/local-tests", request);
        ensureSuccess(resp, "reportLocalTests");
    }

    public void abortSession(String sessionId, String reason) throws IOException, InterruptedException {
        HttpResponse<String> resp = sendJsonPost(
                "/api/v1/sessions/" + sessionId + "/abort",
                java.util.Map.of("reason", reason != null ? reason : "Aborted"));
        ensureSuccess(resp, "abortSession");
    }

    /**
     * Streams SSE events from {@code /api/v1/sessions/:sessionId/events}, reconnecting with
     * {@code Last-Event-ID} on transient drop until {@code shouldStop} returns {@code true} or timeout expires.
     */
    public long streamSessionEvents(String sessionId,
                                    long initialAfterSeq,
                                    long timeoutMs,
                                    Predicate<BrokerEvent> shouldStop) throws IOException, InterruptedException {
        long lastSeq = initialAfterSeq;
        long deadline = System.currentTimeMillis() + Math.max(2_000L, timeoutMs);

        while (System.currentTimeMillis() < deadline) {
            long remainingMs = deadline - System.currentTimeMillis();
            if (remainingMs <= 0) {
                break;
            }
            HttpRequest.Builder req = requestBuilder(
                    "/api/v1/sessions/" + sessionId + "/events?afterSeq=" + lastSeq)
                    .header("Accept", "text/event-stream")
                    .header("Last-Event-ID", Long.toString(lastSeq))
                    .timeout(Duration.ofMillis(remainingMs))
                    .GET();
            try {
                HttpResponse<InputStream> resp = httpClient.send(
                        req.build(), HttpResponse.BodyHandlers.ofInputStream());
                ensureSuccess(resp, "streamSessionEvents");
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
                    StringBuilder dataLines = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("data:")) {
                            if (!dataLines.isEmpty()) {
                                dataLines.append('\n');
                            }
                            dataLines.append(line.substring(5).trim());
                        } else if (line.isEmpty() && !dataLines.isEmpty()) {
                            String json = dataLines.toString();
                            dataLines.setLength(0);
                            BrokerEvent event = DistributionJson.fromJson(json, BrokerEvent.class);
                            if (event.sequence() > lastSeq) {
                                lastSeq = event.sequence();
                            }
                            if (shouldStop.test(event)) {
                                return lastSeq;
                            }
                        }
                    }
                }
            } catch (IOException e) {
                if (System.currentTimeMillis() >= deadline) {
                    throw e;
                }
                Thread.sleep(200L);
            }
        }
        throw new IOException("Timed out waiting for distributed partitions in session " + sessionId);
    }

    private HttpResponse<String> sendJsonPost(String path, Object body) throws IOException, InterruptedException {
        HttpRequest req = requestBuilder(path)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(DistributionJson.toJson(body)))
                .build();
        return httpClient.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private HttpRequest.Builder requestBuilder(String path) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(baseUrl + path));
        if (authToken != null && !authToken.isBlank()) {
            builder.header("Authorization", "Bearer " + authToken);
        }
        return builder;
    }

    private static void ensureSuccess(HttpResponse<?> resp, String op) throws IOException {
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IOException("Broker HTTP " + resp.statusCode() + " during " + op + ": " + resp.body());
        }
    }
}
