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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.MissingDigestsRequest;
import org.citrusframework.distribution.model.MissingDigestsResponse;
import org.citrusframework.distribution.model.SessionDetailResponse;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vert.x 5 HTTP/REST, SSE, Static Web UI, and Worker WebSocket verticle for the Test Distribution Broker.
 */
public final class DistributionBrokerVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionBrokerVerticle.class);
    private static final long MAX_BODY_BYTES = 256L * 1024L * 1024L;

    private final DistributionBrokerConfig config;
    private final BlobCacheDirectory blobCache;
    private final BrokerEventStreamHub eventHub;
    private final BrokerSessionCoordinator coordinator;

    private HttpServer httpServer;
    private long leaseTimerId = -1L;

    public DistributionBrokerVerticle(DistributionBrokerConfig config,
                                      BlobCacheDirectory blobCache,
                                      BrokerEventStreamHub eventHub,
                                      BrokerSessionCoordinator coordinator) {
        this.config = config;
        this.blobCache = blobCache;
        this.eventHub = eventHub;
        this.coordinator = coordinator;
    }

    public int actualPort() {
        return httpServer != null ? httpServer.actualPort() : config.port();
    }

    @Override
    public void start(Promise<Void> startPromise) {
        Router router = Router.router(vertx);

        router.route().handler(CorsHandler.create()
                .allowedMethod(HttpMethod.GET)
                .allowedMethod(HttpMethod.POST)
                .allowedMethod(HttpMethod.PUT)
                .allowedHeader("Authorization")
                .allowedHeader("Content-Type")
                .allowedHeader("Last-Event-ID"));

        router.get("/health").handler(ctx -> writeJson(ctx, 200, Map.of("status", "UP")));
        router.get("/").handler(ctx -> ctx.redirect("/ui/index.html"));
        router.get("/ui").handler(ctx -> ctx.redirect("/ui/index.html"));
        router.route("/ui/*").handler(StaticHandler.create("META-INF/resources/dist-ui").setCachingEnabled(false));

        // Mount WebSocket upgrade route BEFORE BodyHandler so HTTP upgrade stream is untouched
        router.get("/ws/v1/worker").handler(this::handleWorkerWebSocket);

        // Auth + BodyHandler on /api/*
        router.route("/api/*").handler(this::enforceAuth);
        router.post("/api/*").handler(BodyHandler.create().setBodyLimit(MAX_BODY_BYTES));
        router.put("/api/*").handler(BodyHandler.create().setBodyLimit(MAX_BODY_BYTES));

        router.post("/api/v1/sessions").handler(this::handleCreateSession);
        router.get("/api/v1/sessions").handler(this::handleListSessions);
        router.get("/api/v1/sessions/:sessionId").handler(this::handleGetSession);
        router.post("/api/v1/sessions/:sessionId/work").handler(this::handleSubmitWork);
        router.post("/api/v1/sessions/:sessionId/complete").handler(this::handleCompleteSession);
        router.get("/api/v1/sessions/:sessionId/events").handler(this::handleSessionEvents);
        router.get("/api/v1/events/stream").handler(this::handleGlobalEvents);

        router.post("/api/v1/cache/missing").handler(this::handleMissingDigests);
        router.put("/api/v1/cache/blobs/:sha256").handler(this::handlePutBlob);
        router.get("/api/v1/cache/blobs/:sha256").handler(this::handleGetBlob);

        router.get("/api/v1/workers").handler(ctx -> writeJson(ctx, 200, coordinator.listWorkers()));
        router.get("/api/v1/tests/history").handler(this::handleTestHistory);
        router.post("/api/v1/demo/seed").handler(this::handleSeedDemo);

        leaseTimerId = vertx.setPeriodic(5_000L, id -> coordinator.sweepExpiredLeases());

        httpServer = vertx.createHttpServer();
        httpServer.requestHandler(router)
                .listen(config.port(), config.host())
                .<Void>mapEmpty()
                .onComplete(startPromise);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (leaseTimerId != -1L) {
            vertx.cancelTimer(leaseTimerId);
        }
        if (httpServer != null) {
            httpServer.close().onComplete(stopPromise);
        } else {
            stopPromise.complete();
        }
    }

    private void enforceAuth(RoutingContext ctx) {
        if (!isAuthorized(ctx)) {
            writeJson(ctx, 401, Map.of("error", "Unauthorized"));
            return;
        }
        ctx.next();
    }

    private boolean isAuthorized(RoutingContext ctx) {
        String expected = config.authToken();
        if (expected == null || expected.isBlank()) {
            return true;
        }
        String candidate = null;
        String authHeader = ctx.request().getHeader("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            candidate = authHeader.substring("Bearer ".length()).trim();
        }
        if (candidate == null || candidate.isEmpty()) {
            candidate = ctx.request().getParam("token");
        }
        if (candidate == null) {
            return false;
        }
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                candidate.getBytes(StandardCharsets.UTF_8));
    }

    private void handleCreateSession(RoutingContext ctx) {
        vertx.executeBlocking(() -> {
            CreateSessionRequest req = DistributionJson.fromJson(ctx.body().asString(), CreateSessionRequest.class);
            return coordinator.createSession(req);
        }).onSuccess((BuildSessionResponse res) -> writeJson(ctx, 201, res))
          .onFailure(err -> writeError(ctx, err));
    }

    private void handleListSessions(RoutingContext ctx) {
        int limit = parseIntParam(ctx.request().getParam("limit"), 50);
        vertx.executeBlocking(() -> coordinator.listSessions(limit))
                .onSuccess(list -> writeJson(ctx, 200, list))
                .onFailure(err -> writeError(ctx, err));
    }

    private void handleGetSession(RoutingContext ctx) {
        String sessionId = ctx.pathParam("sessionId");
        vertx.executeBlocking(() -> coordinator.getSessionDetail(sessionId))
                .onSuccess((Optional<SessionDetailResponse> detail) -> {
                    if (detail.isPresent()) {
                        writeJson(ctx, 200, detail.get());
                    } else {
                        writeJson(ctx, 404, Map.of("error", "Session not found: " + sessionId));
                    }
                })
                .onFailure(err -> writeError(ctx, err));
    }

    private void handleSubmitWork(RoutingContext ctx) {
        String sessionId = ctx.pathParam("sessionId");
        vertx.executeBlocking(() -> {
            SubmitWorkRequest req = DistributionJson.fromJson(ctx.body().asString(), SubmitWorkRequest.class);
            return coordinator.submitWork(sessionId, req);
        }).onSuccess((SubmitWorkResponse res) -> writeJson(ctx, 200, res))
          .onFailure(err -> writeError(ctx, err));
    }

    private void handleCompleteSession(RoutingContext ctx) {
        String sessionId = ctx.pathParam("sessionId");
        vertx.executeBlocking(() -> {
            String raw = ctx.body().asString();
            CompleteSessionRequest req = (raw != null && !raw.isBlank())
                    ? DistributionJson.fromJson(raw, CompleteSessionRequest.class)
                    : new CompleteSessionRequest(0, 0, 0, 0L);
            return coordinator.completeSession(sessionId, req);
        }).onSuccess((BuildSessionRecord res) -> writeJson(ctx, 200, res))
          .onFailure(err -> writeError(ctx, err));
    }

    private void handleSessionEvents(RoutingContext ctx) {
        String sessionId = ctx.pathParam("sessionId");
        long afterSeq = resolveAfterSequence(ctx);
        eventHub.subscribeSession(sessionId, afterSeq, ctx.response());
    }

    private void handleGlobalEvents(RoutingContext ctx) {
        long afterSeq = resolveAfterSequence(ctx);
        eventHub.subscribeGlobal(afterSeq, ctx.response());
    }

    private void handleMissingDigests(RoutingContext ctx) {
        vertx.executeBlocking(() -> {
            MissingDigestsRequest req = DistributionJson.fromJson(ctx.body().asString(), MissingDigestsRequest.class);
            List<String> missing = new ArrayList<>();
            if (req.digests() != null) {
                for (String digest : req.digests()) {
                    BlobCacheDirectory.validateDigest(digest);
                    if (!blobCache.contains(digest)) {
                        missing.add(digest);
                    }
                }
            }
            return new MissingDigestsResponse(missing);
        }).onSuccess(res -> writeJson(ctx, 200, res))
          .onFailure(err -> writeError(ctx, err));
    }

    private void handlePutBlob(RoutingContext ctx) {
        String sha256 = ctx.pathParam("sha256");
        Buffer body = ctx.body().buffer();
        byte[] bytes = body != null ? body.getBytes() : new byte[0];
        vertx.executeBlocking(() -> {
            blobCache.put(sha256, bytes);
            return sha256;
        }).onSuccess(saved -> writeJson(ctx, 201, Map.of("sha256", saved, "sizeBytes", bytes.length)))
          .onFailure(err -> writeError(ctx, err));
    }

    private void handleGetBlob(RoutingContext ctx) {
        String sha256 = ctx.pathParam("sha256");
        vertx.executeBlocking(() -> {
            BlobCacheDirectory.validateDigest(sha256);
            if (!blobCache.contains(sha256)) {
                return null;
            }
            return blobCache.readBytes(sha256);
        }).onSuccess(bytes -> {
            if (bytes == null) {
                writeJson(ctx, 404, Map.of("error", "Blob not found: " + sha256));
            } else {
                ctx.response()
                        .setStatusCode(200)
                        .putHeader("Content-Type", "application/octet-stream")
                        .end(Buffer.buffer(bytes));
            }
        }).onFailure(err -> writeError(ctx, err));
    }

    private void handleTestHistory(RoutingContext ctx) {
        int limit = parseIntParam(ctx.request().getParam("limit"), 100);
        vertx.executeBlocking(() -> coordinator.listTestHistory(limit))
                .onSuccess(list -> writeJson(ctx, 200, list))
                .onFailure(err -> writeError(ctx, err));
    }

    private void handleSeedDemo(RoutingContext ctx) {
        vertx.executeBlocking(coordinator::seedDemoData)
                .onSuccess(count -> writeJson(ctx, 200, Map.of("seededSessions", count, "status", "OK")))
                .onFailure(err -> writeError(ctx, err));
    }

    private void handleWorkerWebSocket(RoutingContext ctx) {
        if (!isAuthorized(ctx)) {
            writeJson(ctx, 401, Map.of("error", "Unauthorized"));
            return;
        }
        Future<ServerWebSocket> wsFuture = ctx.request().toWebSocket();
        wsFuture.onSuccess(ws -> {
            String connectionId = UUID.randomUUID().toString();
            ws.textMessageHandler(text -> vertx.executeBlocking(() -> {
                WorkerWsMessage msg = DistributionJson.fromJson(text, WorkerWsMessage.class);
                if (WorkerWsMessage.WORKER_REGISTER.equals(msg.type())) {
                    WorkerRegisterPayload reg = DistributionJson.convert(msg.payload(), WorkerRegisterPayload.class);
                    coordinator.registerWorker(connectionId, reg, outbound ->
                            ws.writeTextMessage(DistributionJson.toJson(outbound)));
                } else {
                    coordinator.handleWorkerMessage(connectionId, msg);
                }
                return null;
            }).onFailure(err -> LOG.warn("Error handling worker WebSocket message", err)));

            ws.closeHandler(v -> vertx.executeBlocking(() -> {
                coordinator.handleWorkerDisconnect(connectionId);
                return null;
            }));
            ws.exceptionHandler(err -> LOG.debug("Worker WebSocket error ({}): {}", connectionId, err.getMessage()));
        }).onFailure(err -> LOG.warn("Failed to upgrade worker WebSocket", err));
    }

    private long resolveAfterSequence(RoutingContext ctx) {
        String lastEventId = ctx.request().getHeader("Last-Event-ID");
        if (lastEventId != null && !lastEventId.isBlank()) {
            try {
                return Long.parseLong(lastEventId.trim());
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        String param = ctx.request().getParam("afterSeq");
        if (param != null && !param.isBlank()) {
            try {
                return Long.parseLong(param.trim());
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        return 0L;
    }

    private int parseIntParam(String raw, int fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private void writeJson(RoutingContext ctx, int statusCode, Object body) {
        ctx.response()
                .setStatusCode(statusCode)
                .putHeader("Content-Type", "application/json;charset=UTF-8")
                .end(DistributionJson.toJson(body));
    }

    private void writeError(RoutingContext ctx, Throwable err) {
        int code = (err instanceof IllegalArgumentException) ? 400 : 500;
        writeJson(ctx, code, Map.of("error", err.getMessage() != null ? err.getMessage() : err.getClass().getSimpleName()));
    }
}
