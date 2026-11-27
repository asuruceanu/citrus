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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;

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
import org.citrusframework.distribution.model.HistoricalDurationsRequest;
import org.citrusframework.distribution.model.HistoricalDurationsResponse;
import org.citrusframework.distribution.model.MissingDigestsRequest;
import org.citrusframework.distribution.model.MissingDigestsResponse;
import org.citrusframework.distribution.model.ReportLocalTestsRequest;
import org.citrusframework.distribution.model.SessionDetailResponse;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.cfg.CoercionAction;
import tools.jackson.databind.cfg.CoercionInputShape;
import tools.jackson.databind.type.LogicalType;

/**
 * Vert.x 5 HTTP/REST, SSE, Static Web UI, and Worker WebSocket verticle for the Test Distribution Coordinator.
 */
public final class DistributionCoordinatorVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionCoordinatorVerticle.class);
    private static final String USAGE_INTERVAL_HEADER = "X-Citrus-Usage-Sample-Interval-Ms";
    private static final int MAX_HISTORY_TEST_KEYS = 10_000;
    private static final ObjectReader HISTORY_REQUEST_READER = DistributionJson.mapper()
            .readerFor(HistoricalDurationsRequest.class)
            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final ObjectReader POOL_REQUEST_READER = DistributionJson.mapper().rebuild()
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .withCoercionConfig(LogicalType.Textual, coercion -> coercion
                    .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
                    .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                    .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail))
            .build().reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final long MAX_BODY_BYTES = 256L * 1024L * 1024L;

    private final DistributionCoordinatorConfig config;
    private final BlobCacheDirectory blobCache;
    private final CoordinatorEventStreamHub eventHub;
    private final SessionCoordinator coordinator;

    private HttpServer httpServer;
    private long dashboardTimerId = -1L;
    private long leaseTimerId = -1L;
    private long usageTimerId = -1L;
    private Future<Void> leaseSweep = Future.succeededFuture();
    private Future<Void> usageSample = Future.succeededFuture();

    public DistributionCoordinatorVerticle(DistributionCoordinatorConfig config,
                                      BlobCacheDirectory blobCache,
                                      CoordinatorEventStreamHub eventHub,
                                      SessionCoordinator coordinator) {
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
                .allowedMethod(HttpMethod.DELETE)
                .allowedHeader("Authorization")
                .allowedHeader("Content-Type")
                .allowedHeader("Last-Event-ID")
                .exposedHeader(USAGE_INTERVAL_HEADER));

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
        router.delete("/api/v1/sessions/:sessionId").handler(ctx -> deleteRecord(ctx,
                () -> coordinator.deleteSession(ctx.pathParam("sessionId")), "Only finished sessions can be deleted"));
        router.delete("/api/v1/workers/:workerId").handler(ctx -> deleteRecord(ctx,
                () -> coordinator.deleteWorker(ctx.pathParam("workerId")), "Only offline workers can be deleted"));
        router.post("/api/v1/sessions/:sessionId/work").handler(this::handleSubmitWork);
        router.post("/api/v1/sessions/:sessionId/local-tests").handler(this::handleReportLocalTests);
        router.post("/api/v1/sessions/:sessionId/complete").handler(this::handleCompleteSession);
        router.post("/api/v1/sessions/:sessionId/abort").handler(this::handleAbortSession);
        router.get("/api/v1/sessions/:sessionId/partitions/:partitionId/peak").handler(ctx -> {
            String session = ctx.pathParam("sessionId");
            String partition = ctx.pathParam("partitionId");
            int attempt = parseIntParam(ctx.request().getParam("attempt"), 1);
            vertx.executeBlocking(() -> coordinator.observedWorkUnitPeak(session, partition, attempt))
                    .onSuccess(peak -> writeJson(ctx, 200, Map.of("peak", peak)))
                    .onFailure(err -> writeError(ctx, err));
        });
        router.get("/api/v1/sessions/:sessionId/events").handler(this::handleSessionEvents);
        router.get("/api/v1/events/stream").handler(this::handleGlobalEvents);

        router.post("/api/v1/cache/missing").handler(this::handleMissingDigests);
        router.put("/api/v1/cache/blobs/:sha256").handler(this::handlePutBlob);
        router.get("/api/v1/cache/blobs/:sha256").handler(this::handleGetBlob);

        registerAgentPoolRoutes(router);
        router.get("/api/v1/usage").handler(ctx -> {
            ctx.response().putHeader(USAGE_INTERVAL_HEADER, Long.toString(config.usageSampleIntervalMs()));
            respond(ctx, 200, () ->
                    coordinator.queryHistoricalUsage(ctx.request().getParam("window"), ctx.request().getParam("filter")));
        });
        router.get("/api/v1/workers").handler(ctx -> vertx.executeBlocking(coordinator::listWorkers, false)
                .onSuccess(workers -> writeJson(ctx, 200, workers)).onFailure(err -> writeError(ctx, err)));
        router.get("/api/v1/tests/history").handler(this::handleTestHistory);
        router.get("/api/v1/tests/history/runs").handler(this::handleTestHistoryRuns);
        router.post("/api/v1/history/durations").handler(this::handleHistoricalDurations);

        httpServer = vertx.createHttpServer().requestHandler(router);
        sampleUsage().onComplete(initial -> httpServer.listen(config.port(), resolveListenHost(config))
                .onSuccess(server -> {
                    dashboardTimerId = vertx.setPeriodic(1_000L, id -> eventHub.flushDashboardChanges());
                    leaseTimerId = vertx.setPeriodic(5_000L, id -> leaseSweep =
                            runMaintenance(leaseSweep, coordinator::sweepExpiredLeases, "Lease sweep"));
                    usageTimerId = vertx.setPeriodic(config.usageSampleIntervalMs(), id -> sampleUsage());
                })
                .<Void>mapEmpty()
                .onComplete(startPromise));
    }

    private Future<Void> sampleUsage() {
        usageSample = runMaintenance(usageSample, coordinator::sampleUsageNow, "Usage sample");
        return usageSample;
    }

    private Future<Void> runMaintenance(Future<Void> previous, Runnable work, String name) {
        if (!previous.isComplete()) {
            return previous;
        }
        return vertx.<Void>executeBlocking(() -> {
            work.run();
            return null;
        }, false).onFailure(err -> LOG.warn("{} failed", name, err));
    }

    static String resolveListenHost(DistributionCoordinatorConfig config) {
        if (config.port() == 0 && "0.0.0.0".equals(config.host())) {
            return "127.0.0.1";
        }
        return config.host();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (dashboardTimerId != -1L) {
            vertx.cancelTimer(dashboardTimerId);
        }
        if (leaseTimerId != -1L) {
            vertx.cancelTimer(leaseTimerId);
        }
        if (usageTimerId != -1L) {
            vertx.cancelTimer(usageTimerId);
        }
        if (httpServer != null) {
            Future.join(leaseSweep, usageSample).onComplete(done -> httpServer.close().onComplete(stopPromise));
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

    private record PoolRequest(String displayName, Map<String, String> capabilities, Boolean generateRegistrationKey,
                               String keyDescription) {
    }

    private record PoolPriorityRequest(List<String> poolIds) {
    }

    private record KeyRequest(String description) {
    }

    private void registerAgentPoolRoutes(Router router) {
        String pools = "/api/v1/agent-pools";
        String pool = pools + "/:poolId";
        router.get(pools).handler(ctx -> respond(ctx, 200, coordinator::listAgentPools));
        router.post(pools).handler(ctx -> {
            ctx.response().putHeader("Cache-Control", "no-store");
            respond(ctx, 201, () -> {
                var request = readPoolRequest(ctx, PoolRequest.class);
                return coordinator.createAgentPool(request.displayName(), request.capabilities(),
                        Boolean.TRUE.equals(request.generateRegistrationKey()), request.keyDescription());
            });
        });
        router.put(pools + "/priority").handler(ctx -> respond(ctx, 200, () ->
                coordinator.reorderAgentPools(readPoolRequest(ctx, PoolPriorityRequest.class).poolIds())));
        router.get(pool).handler(ctx -> respond(ctx, 200, () ->
                coordinator.getAgentPoolDetail(ctx.pathParam("poolId")).orElse(null)));
        router.put(pool).handler(ctx -> respond(ctx, 200, () -> {
            var request = readPoolRequest(ctx, PoolRequest.class);
            return coordinator.updateAgentPool(ctx.pathParam("poolId"), request.displayName(), request.capabilities())
                    .orElse(null);
        }));
        router.delete(pool).handler(ctx -> respond(ctx, 200, () -> {
            String poolId = ctx.pathParam("poolId");
            return coordinator.deleteAgentPool(poolId) ? Map.of("deleted", true, "poolId", poolId) : null;
        }));
        router.get(pool + "/keys").handler(ctx -> respond(ctx, 200, () ->
                coordinator.listRegistrationKeys(ctx.pathParam("poolId")).orElse(null)));
        router.post(pool + "/keys").handler(ctx -> {
            ctx.response().putHeader("Cache-Control", "no-store");
            respond(ctx, 201, () -> {
                String description = ctx.body().length() > 0 ? readPoolRequest(ctx, KeyRequest.class).description() : null;
                return coordinator.generateRegistrationKey(ctx.pathParam("poolId"), description).orElse(null);
            });
        });
        router.post(pool + "/keys/:keyId/revoke").handler(ctx -> respond(ctx, 200, () ->
                coordinator.revokeRegistrationKey(ctx.pathParam("poolId"), ctx.pathParam("keyId")).orElse(null)));
    }

    private <T> T readPoolRequest(RoutingContext ctx, Class<T> type) {
        String body = ctx.body().asString();
        if (body != null && !body.isBlank()) {
            try {
                T request = POOL_REQUEST_READER.forType(type).readValue(body);
                if (request != null) {
                    return request;
                }
            } catch (JacksonException e) {
                throw new IllegalArgumentException("Invalid agent pool request", e);
            }
        }
        throw new IllegalArgumentException("Missing agent pool request");
    }

    private void respond(RoutingContext ctx, int statusCode, Callable<?> action) {
        vertx.executeBlocking(action).onSuccess(result -> {
            if (result == null) {
                writeJson(ctx, 404, Map.of("error", "Agent pool or registration key not found"));
            } else {
                writeJson(ctx, statusCode, result);
            }
        }).onFailure(err -> writeError(ctx, err));
    }

    private void deleteRecord(RoutingContext ctx, Callable<SessionCoordinator.Deletion> action, String activeMessage) {
        vertx.executeBlocking(action).onSuccess(result -> {
            switch (result) {
                case DELETED -> writeJson(ctx, 200, Map.of("deleted", true));
                case MISSING -> writeJson(ctx, 404, Map.of("error", "Record not found"));
                case ACTIVE -> writeJson(ctx, 409, Map.of("error", activeMessage));
            }
        }).onFailure(err -> writeError(ctx, err));
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

    private void handleReportLocalTests(RoutingContext ctx) {
        String sessionId = ctx.pathParam("sessionId");
        vertx.executeBlocking(() -> {
            String raw = ctx.body().asString();
            ReportLocalTestsRequest req = (raw != null && !raw.isBlank())
                    ? DistributionJson.fromJson(raw, ReportLocalTestsRequest.class)
                    : new ReportLocalTestsRequest(null, List.of());
            return coordinator.reportLocalTests(sessionId, req);
        }).onSuccess((Integer count) -> writeJson(ctx, 200, Map.of("recorded", count)))
          .onFailure(err -> writeError(ctx, err));
    }

    private void handleAbortSession(RoutingContext ctx) {
        String sessionId = ctx.pathParam("sessionId");
        vertx.executeBlocking(() -> {
            String raw = ctx.body().asString();
            String reason = null;
            if (raw != null && !raw.isBlank()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> body = DistributionJson.fromJson(raw, Map.class);
                Object r = body != null ? body.get("reason") : null;
                reason = r != null ? r.toString() : null;
            }
            return coordinator.abortSession(sessionId, reason);
        }).onSuccess((BuildSessionRecord res) -> writeJson(ctx, 200, res))
          .onFailure(err -> writeError(ctx, err));
    }

    private void handleSessionEvents(RoutingContext ctx) {
        String sessionId = ctx.pathParam("sessionId");
        long afterSeq = resolveAfterSequence(ctx);
        eventHub.subscribeSession(sessionId, afterSeq, ctx.response());
    }

    private void handleGlobalEvents(RoutingContext ctx) {
        if ("dashboard".equals(ctx.request().getParam("mode"))) {
            eventHub.subscribeDashboard(ctx.response());
            return;
        }
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
            return blobCache.resolveBlobPathForAccess(sha256);
        }).onSuccess(path -> {
            if (path == null) {
                writeJson(ctx, 404, Map.of("error", "Blob not found: " + sha256));
            } else {
                ctx.response()
                        .setStatusCode(200)
                        .putHeader("Content-Type", "application/octet-stream")
                        .sendFile(path.toString())
                        .onFailure(ctx::fail);
            }
        }).onFailure(err -> writeError(ctx, err));
    }

    private void handleTestHistory(RoutingContext ctx) {
        int limit = parseIntParam(ctx.request().getParam("limit"), 100);
        vertx.executeBlocking(() -> coordinator.listTestHistory(limit))
                .onSuccess(list -> writeJson(ctx, 200, list))
                .onFailure(err -> writeError(ctx, err));
    }

    private void handleTestHistoryRuns(RoutingContext ctx) {
        String scope = ctx.request().getParam("projectScope");
        String workUnitKey = ctx.request().getParam("workUnitKey");
        String className = ctx.request().getParam("className");
        String testName = ctx.request().getParam("testName");
        int limit = parseIntParam(ctx.request().getParam("limit"), 30);
        if (scope == null || scope.isBlank()) {
            writeError(ctx, new IllegalArgumentException("Missing projectScope"));
            return;
        }
        vertx.executeBlocking(() -> coordinator.listTestHistoryRuns(scope, workUnitKey, className, testName, limit))
                .onSuccess(list -> writeJson(ctx, 200, list))
                .onFailure(err -> writeError(ctx, err));
    }

    private void handleHistoricalDurations(RoutingContext ctx) {
        vertx.executeBlocking(() -> {
            String body = ctx.body().asString();
            HistoricalDurationsRequest request;
            try {
                request = body == null ? null : HISTORY_REQUEST_READER.readValue(body);
            } catch (JacksonException e) {
                throw new IllegalArgumentException("Invalid historical durations request", e);
            }
            if (request == null) {
                throw new IllegalArgumentException("Missing historical durations request");
            }
            if (request.testKeys().size() > MAX_HISTORY_TEST_KEYS) {
                throw new IllegalArgumentException("At most " + MAX_HISTORY_TEST_KEYS + " test keys are allowed");
            }
            return new HistoricalDurationsResponse(coordinator.findHistoricalDurations(request.testKeys(), request.limit(), request.historyScope()));
        }).onSuccess(response -> writeJson(ctx, 200, response))
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
            var transport = new WorkerSocket(ws);
            ws.textMessageHandler(text -> vertx.executeBlocking(() -> {
                WorkerWsMessage msg = DistributionJson.fromJson(text, WorkerWsMessage.class);
                if (WorkerWsMessage.WORKER_REGISTER.equals(msg.type())) {
                    WorkerRegisterPayload reg = DistributionJson.convert(msg.payload(), WorkerRegisterPayload.class);
                    // An expiry sweep can reject this socket while another registration frame is queued.
                    synchronized (coordinator) {
                        if (!transport.closing) {
                            coordinator.registerWorker(connectionId, reg, transport::send, transport::closeRejected);
                        }
                    }
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

    private static final class WorkerSocket {
        private final ServerWebSocket socket;
        // The coordinator serializes calls to send and closeRejected, including from maintenance sweeps.
        private Future<Void> rejectionWrite = Future.succeededFuture();
        private boolean closing;

        private WorkerSocket(ServerWebSocket socket) {
            this.socket = socket;
        }

        private void send(WorkerWsMessage outbound) {
            var written = socket.writeTextMessage(DistributionJson.toJson(outbound));
            if (WorkerWsMessage.WORKER_REJECTED.equals(outbound.type())) {
                rejectionWrite = written;
            }
        }

        private void closeRejected() {
            closing = true;
            rejectionWrite.onComplete(written -> socket.close((short) 4003, "Worker rejected"));
        }
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
