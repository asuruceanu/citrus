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
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionCompletedPayload;
import org.citrusframework.distribution.model.ResultJournal;
import org.citrusframework.distribution.model.WorkerCacheStats;
import org.citrusframework.distribution.model.WorkerHeartbeatPayload;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerRejectedPayload;
import org.citrusframework.distribution.model.WorkerRenewPayload;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.junit.platform.engine.CancellationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistent WebSocket worker client that registers host capabilities with the Citrus Test
 * Distribution Coordinator, executes assigned partitions, and supports graceful SIGTERM draining.
 * <p>
 * The client owns the worker's exclusive {@link WorkerFileCache} and one {@link CoordinatorBlobClient}, shared by the
 * in-process and forked runners. {@link #start()} locks the cache directory before connecting (failing fast when
 * another worker holds it); {@link #close()} releases it after in-flight partitions have ended. Cache statistics are
 * attached to registration and heartbeats.
 */
public final class DistributionWorkerClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionWorkerClient.class);

    private final DistributionWorkerConfig config;
    private final HttpClient httpClient;
    private final ExecutorService partitionExecutor;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean draining = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean cacheStatsFailureActive = new AtomicBoolean();
    private final AtomicReference<WebSocket> activeWebSocket = new AtomicReference<>();
    private final CompletableFuture<Void> rejectionNotification = new CompletableFuture<>();
    private volatile WorkerRejectedPayload rejectionReason;
    // Guarded by this client's monitor from the handshake through the end of the connection.
    private WorkerWsListener connection;
    private ScheduledFuture<?> renewal;
    private volatile ResultJournal resultOutbox;
    private final ActiveAssignmentTracker assignments = new ActiveAssignmentTracker();
    private volatile CountDownLatch registeredLatch = new CountDownLatch(1);
    private volatile CompletableFuture<Void> currentExecution = CompletableFuture.completedFuture(null);

    // Opened by the first start(), released by close(); partitions only run while they are set.
    private volatile WorkerFileCache workerCache;
    private volatile CoordinatorBlobClient blobClient;
    private volatile IsolatedPartitionRunner inProcessRunner;
    private volatile ForkedPartitionRunner forkedRunner;

    public DistributionWorkerClient(DistributionWorkerConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.partitionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "citrus-dist-worker-exec-" + config.workerId());
            t.setDaemon(true);
            return t;
        });
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "citrus-dist-worker-hb-" + config.workerId());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Locks the worker cache (on the first call), connects to the coordinator and starts heartbeats.
     *
     * @throws IllegalStateException if the cache directory cannot be locked, for example because another worker uses
     *                               it, or if this client is closed
     */
    public void start() {
        if (rejectionReason != null || !running.compareAndSet(false, true)) {
            return;
        }
        try {
            openCache();
        } catch (RuntimeException | Error e) {
            running.set(false);
            throw e;
        }
        connectInternal();
        scheduler.scheduleAtFixedRate(
                this::sendHeartbeatSafe,
                config.heartbeatIntervalMs(),
                config.heartbeatIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    /** Synchronized with {@link #closeResources()}, so a cache is never opened after it released the last one. */
    private synchronized void openCache() {
        if (closed.get()) {
            throw new IllegalStateException("Worker " + config.workerId() + " is closed");
        }
        if (workerCache != null) {
            return;
        }
        WorkerFileCache cache = WorkerFileCache.open(config.cacheDir(), config.cacheCleanup(),
                config.cacheFreeSpaceBufferBytes());
        try {
            CoordinatorBlobClient blobs = new CoordinatorBlobClient(config, cache);
            inProcessRunner = new IsolatedPartitionRunner(config, cache, blobs);
            forkedRunner = new ForkedPartitionRunner(config, cache, blobs, (assignment, preparation) ->
                    sendMessageSafe(WorkerWsMessage.PARTITION_INPUTS_READY, Map.of(
                            "partitionId", assignment.partitionId(), "attempt", assignment.attempt(),
                            "inputPreparation", preparation)));
            String resultScope = java.util.UUID.nameUUIDFromBytes((config.coordinatorUrl() + "\n" + config.workerId())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
            resultOutbox = new ResultJournal(config.cacheDir().resolve("result-outbox").resolve(resultScope));
            blobClient = blobs;
            workerCache = cache;
        } catch (RuntimeException | Error e) {
            cache.close();
            throw e;
        }
    }

    /** The worker cache, or {@code null} before the first {@link #start()}. */
    WorkerFileCache workerCache() {
        return workerCache;
    }

    /** The shared blob client, or {@code null} before the first {@link #start()}. */
    CoordinatorBlobClient blobClient() {
        return blobClient;
    }

    public boolean awaitRegistration(long timeout, TimeUnit unit) throws InterruptedException {
        return registeredLatch.await(timeout, unit) && rejectionReason == null;
    }

    public Optional<WorkerRejectedPayload> rejectionReason() {
        return Optional.ofNullable(rejectionReason);
    }

    /** Runs the notification once when registration or renewal is rejected, including a prior rejection. */
    public void onRejection(Runnable notification) {
        rejectionNotification.thenRun(notification);
    }

    public boolean isConnected() {
        WebSocket ws = activeWebSocket.get();
        return ws != null && !ws.isOutputClosed() && !ws.isInputClosed();
    }

    /**
     * Gracefully drains this worker: notifies the coordinator to stop assigning new partitions and
     * waits for any in-flight partition execution to finish.
     */
    public void drain(long timeout, TimeUnit unit) {
        if (draining.compareAndSet(false, true)) {
            sendMessageSafe(WorkerWsMessage.WORKER_DRAINING, Map.of("workerId", config.workerId()));
        }
        try {
            currentExecution.get(timeout, unit);
        } catch (Exception e) {
            LOG.debug("Timed out or interrupted while draining worker {}", config.workerId(), e);
        }
    }

    /**
     * Abruptly drops the WebSocket connection (primarily for testing disconnect failover).
     */
    public synchronized void abortConnection() {
        assignments.cancel(null, 0);
        running.set(false);
        connection = null;
        cancelRenewal();
        WebSocket ws = activeWebSocket.getAndSet(null);
        if (ws != null) {
            ws.abort();
        }
    }

    /**
     * Cancels in-flight work and aborts blob transfers before draining. Disconnects, waits for the partition thread
     * and kills any remaining forked JVMs before closing the cache. Idempotent.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            assignments.cancel(null, 0);
            CoordinatorBlobClient blobs = blobClient;
            if (blobs != null) {
                blobs.close();
            }
            drain(10, TimeUnit.SECONDS);
            running.set(false);
            WebSocket ws;
            synchronized (this) {
                cancelRenewal();
                connection = null;
                ws = activeWebSocket.getAndSet(null);
            }
            if (ws != null) {
                try {
                    ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown").get(2, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    ws.abort();
                }
            }
            scheduler.shutdownNow();
            partitionExecutor.shutdownNow();
            try {
                if (!partitionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn("Partition thread of worker {} did not stop within 5 seconds; killing its forked partition "
                            + "JVMs and releasing its cache anyway", config.workerId());
                }
                scheduler.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } finally {
            try {
                ForkedPartitionRunner forked = forkedRunner;
                if (!partitionExecutor.isTerminated() && forked != null) {
                    // No child may keep reading or writing the cache once another worker can own it.
                    forked.killActiveForks();
                }
            } finally {
                closeResources();
            }
        }
    }

    /**
     * Captures the resources under this client's monitor (after which {@link #openCache()} sees {@code closed} and
     * opens nothing new), then closes them outside it: {@link WorkerFileCache#close()} may wait for an in-flight
     * eviction, which must not block other users of the monitor such as {@link #sendMessageSafe}.
     */
    private void closeResources() {
        CoordinatorBlobClient blobs;
        WorkerFileCache cache;
        synchronized (this) {
            blobs = blobClient;
            cache = workerCache;
        }
        try {
            if (blobs != null) {
                blobs.close();
            }
        } finally {
            try {
                httpClient.shutdownNow();
            } finally {
                if (cache != null) {
                    cache.close();
                }
            }
        }
    }

    private synchronized void connectInternal() {
        if (!running.get() || draining.get() || closed.get() || rejectionReason != null || connection != null) {
            return;
        }
        if (registeredLatch.getCount() == 0) {
            registeredLatch = new CountDownLatch(1);
        }
        WebSocket.Builder builder = httpClient.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10));
        if (config.authToken() != null && !config.authToken().isBlank()) {
            builder.header("Authorization", "Bearer " + config.authToken());
        }

        WorkerWsListener listener = new WorkerWsListener();
        connection = listener;
        builder.buildAsync(buildWsUri(), listener).whenComplete((ws, err) -> {
            if (err != null) {
                synchronized (this) {
                    if (connection == listener) {
                        connection = null;
                        LOG.warn("Worker {} failed to connect to {}", config.workerId(), config.coordinatorUrl());
                        scheduleReconnect();
                    }
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    private synchronized void register(WorkerWsListener listener, WebSocket ws) {
        if (connection != listener || closed.get() || !running.get() || rejectionReason != null) {
            ws.abort();
            return;
        }
        activeWebSocket.set(ws);
        Map<String, String> caps = new LinkedHashMap<>(config.capabilities());
        caps.put("citrus-worker", "true");
        WorkerRegisterPayload register = new WorkerRegisterPayload(config.workerId(), config.hostName(),
                config.version(), config.poolId(), config.registrationKey(), caps, cacheStats(),
                Boolean.getBoolean("citrus.test.distribution.worker.overflowSlot"));
        sendMessageSafe(WorkerWsMessage.WORKER_REGISTER, DistributionJson.convert(register, Map.class));
        ws.request(1);
    }

    private URI buildWsUri() {
        String base = config.coordinatorUrl();
        String wsBase;
        if (base.startsWith("https://")) {
            wsBase = "wss://" + base.substring("https://".length());
        } else if (base.startsWith("http://")) {
            wsBase = "ws://" + base.substring("http://".length());
        } else {
            wsBase = base;
        }
        String url = wsBase + "/ws/v1/worker";
        if (config.authToken() != null && !config.authToken().isBlank()) {
            url += "?token=" + URLEncoder.encode(config.authToken(), StandardCharsets.UTF_8);
        }
        return URI.create(url);
    }

    private void scheduleReconnect() {
        if (!running.get() || draining.get() || closed.get() || rejectionReason != null) {
            return;
        }
        scheduler.schedule(this::connectInternal, config.reconnectDelayMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * Whether {@code assign} runs in a forked JVM: always with {@code forkMode=ALWAYS}, otherwise when it needs a
     * workspace (fork requested, inputs or outputs declared).
     */
    static boolean requiresFork(DistributionWorkerConfig config, PartitionAssignPayload assign) {
        return config.forkMode() == DistributionSettings.WorkerForkMode.ALWAYS
                || assign.forkWorkspace()
                || !assign.inputArtifacts().isEmpty()
                || !assign.outputPaths().isEmpty();
    }

    private void sendHeartbeatSafe() {
        if (!running.get() || !isConnected()) {
            return;
        }
        WorkerHeartbeatPayload hb = new WorkerHeartbeatPayload(config.workerId(), assignments.executingPartitionId(),
                cacheStats());
        @SuppressWarnings("unchecked")
        Map<String, Object> map = DistributionJson.convert(hb, Map.class);
        sendMessageSafe(WorkerWsMessage.WORKER_HEARTBEAT, map);
        replayPendingResults();
    }

    /** Lock-free cache statistics, or {@code null} before the cache is open. */
    private WorkerCacheStats cacheStats() {
        WorkerFileCache cache = workerCache;
        if (cache == null) {
            return null;
        }
        try {
            WorkerCacheStats stats = cache.statsSnapshot();
            cacheStatsFailureActive.set(false);
            return stats;
        } catch (RuntimeException e) {
            if (cacheStatsFailureActive.compareAndSet(false, true)) {
                LOG.warn("Could not read cache statistics of worker {}: {}", config.workerId(), e.toString());
            } else {
                LOG.debug("Could not read cache statistics of worker {}", config.workerId(), e);
            }
            return null;
        }
    }

    /**
     * Evicts down to the free space buffer now that the partition's pins are released, and reports the new cache
     * statistics. Runs on the single partition thread after the partition ended; pinning and eviction also exclude each
     * other through the cache's eviction lock, so this holds even if another thread pins or evicts.
     */
    private void afterPartition() {
        WorkerFileCache cache = workerCache;
        if (cache != null && !closed.get()) {
            try {
                // Also a no-op once the cache is closed, e.g. when close() gave up waiting for this thread.
                cache.evictAfterPartition();
            } catch (RuntimeException e) {
                LOG.warn("Post-partition cache eviction failed on worker {}", config.workerId(), e);
            }
        }
        sendHeartbeatSafe();
    }

    private synchronized void sendMessageSafe(String type, Map<String, Object> payload) {
        WebSocket ws = activeWebSocket.get();
        if (ws == null || ws.isOutputClosed()) {
            return;
        }
        try {
            String json = DistributionJson.toJson(new WorkerWsMessage(type, payload));
            ws.sendText(json, true).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.debug("Failed sending WS message {} from worker {}", type, config.workerId(), e);
        }
    }

    private void replayPendingResults() {
        ResultJournal outbox = resultOutbox;
        if (outbox == null || !isConnected()) {
            return;
        }
        try {
            outbox.replay(message -> sendMessageSafe(message.type(), message.payload()));
        } catch (RuntimeException e) {
            LOG.warn("Worker {} could not replay pending test results", config.workerId(), e);
        }
    }

    private synchronized void handleWsMessage(WebSocket ws, WorkerWsMessage message) {
        if (ws != activeWebSocket.get() || message == null || message.type() == null) {
            return;
        }
        switch (message.type()) {
            case WorkerWsMessage.WORKER_REGISTERED -> {
                if (!closed.get() && registeredLatch.getCount() > 0) {
                    if (requiresPoolConfirmation()) {
                        long interval = config.registrationRenewalIntervalMs();
                        renewal = scheduler.scheduleWithFixedDelay(() -> renewRegistration(ws),
                                interval, interval, TimeUnit.MILLISECONDS);
                    }
                    registeredLatch.countDown();
                    scheduler.execute(this::replayPendingResults);
                }
            }
            case WorkerWsMessage.WORKER_RENEWED -> {
                // The next renewal is already scheduled for this connection.
            }
            case WorkerWsMessage.RESULT_ACK -> resultOutbox.acknowledge(
                    (String) message.payload().get(WorkerWsMessage.DELIVERY_ID));
            case WorkerWsMessage.PARTITION_ASSIGN -> {
                PartitionAssignPayload assign = DistributionJson.convert(message.payload(), PartitionAssignPayload.class);
                if (closed.get()) {
                    return;
                }
                if (!assignments.canAccept()) {
                    // One live partition at a time. A cancelled run that is still winding down does not block: the
                    // new assignment queues behind it on the single-threaded executor.
                    LOG.warn("Worker {} rejects partition {} attempt {}: still running partition {}",
                            config.workerId(), assign.partitionId(), assign.attempt(), assignments.activePartitionId());
                    return;
                }
                ActiveAssignmentTracker.Assignment assignment = assignments.start(assign.partitionId(), assign.attempt());
                CancellationToken token = assignment.token();
                PartitionRunner runner = requiresFork(config, assign) ? forkedRunner : inProcessRunner;
                if (runner == null) {
                    LOG.warn("Worker {} rejects partition {}: not started", config.workerId(), assign.partitionId());
                    assignments.finish(assignment);
                    return;
                }
                currentExecution = CompletableFuture.runAsync(() -> {
                    boolean finished = false;
                    assignments.markExecuting(assignment);
                    try {
                        sendMessageSafe(WorkerWsMessage.PARTITION_STARTED, Map.of(
                                "partitionId", assign.partitionId(),
                                "sessionId", assign.sessionId(),
                                "attempt", assign.attempt()));
                        IsolatedPartitionRunner.PartitionExecutionResult result = token.isCancellationRequested()
                                ? new IsolatedPartitionRunner.PartitionExecutionResult(false, "Partition cancelled before it started")
                                : runner.execute(assign, token, event -> {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> eventMap = DistributionJson.convert(event, Map.class);
                                    var messageToSend = resultOutbox.persist(WorkerWsMessage.TEST_EVENT, eventMap);
                                    sendMessageSafe(messageToSend.type(), messageToSend.payload());
                                });
                        PartitionCompletedPayload completed = new PartitionCompletedPayload(assign.partitionId(),
                                assign.sessionId(), assign.attempt(), result.success(), result.errorMessage(),
                                result.outputArtifacts(), result.inputPreparation());
                        @SuppressWarnings("unchecked")
                        Map<String, Object> completedMap = DistributionJson.convert(completed, Map.class);
                        var durableCompletion = resultOutbox.persist(WorkerWsMessage.PARTITION_COMPLETED, completedMap);
                        // Free the slot before reporting: the coordinator may answer with the next assignment at once.
                        assignments.finish(assignment);
                        finished = true;
                        sendMessageSafe(durableCompletion.type(), durableCompletion.payload());
                    } finally {
                        if (!finished) {
                            assignments.finish(assignment);
                        }
                        afterPartition();
                    }
                }, partitionExecutor);
            }
            case WorkerWsMessage.PARTITION_CANCEL -> {
                Map<String, Object> payload = message.payload() != null ? message.payload() : Map.of();
                Object partitionId = payload.get("partitionId");
                Object attempt = payload.get("attempt");
                assignments.cancel(partitionId != null ? partitionId.toString() : null,
                        attempt instanceof Number number ? number.intValue() : 0);
            }
            default -> LOG.debug("Ignoring unknown message type {} on worker {}", message.type(), config.workerId());
        }
    }

    @SuppressWarnings("unchecked")
    private synchronized void renewRegistration(WebSocket ws) {
        if (ws == activeWebSocket.get() && !closed.get()) {
            WorkerRenewPayload payload = new WorkerRenewPayload(config.workerId(), config.poolId(), config.registrationKey());
            sendMessageSafe(WorkerWsMessage.WORKER_RENEW, DistributionJson.convert(payload, Map.class));
        }
    }

    private void cancelRenewal() {
        if (renewal != null) {
            renewal.cancel(false);
            renewal = null;
        }
    }

    private boolean requiresPoolConfirmation() {
        return config.poolId() != null && !config.poolId().isBlank();
    }

    private WorkerRejectedPayload registrationFailure(WorkerWsMessage message) {
        if (message == null) {
            return null;
        }
        if (WorkerWsMessage.WORKER_REJECTED.equals(message.type())) {
            var reason = DistributionJson.convert(message.payload(), WorkerRejectedPayload.class);
            return reason == null ? genericRejection() : new WorkerRejectedPayload(
                    sanitizeReason(reason.code()), sanitizeReason(reason.message()));
        }
        if (requiresPoolConfirmation()) {
            boolean unconfirmed = WorkerWsMessage.WORKER_REGISTERED.equals(message.type())
                    && (message.payload() == null || !config.poolId().equals(message.payload().get("poolId")));
            boolean prematureAssignment = WorkerWsMessage.PARTITION_ASSIGN.equals(message.type()) && registeredLatch.getCount() > 0;
            if (unconfirmed || prematureAssignment) {
                return new WorkerRejectedPayload("POOL_NOT_CONFIRMED", "The coordinator did not confirm the configured agent pool");
            }
        }
        return null;
    }

    private String sanitizeReason(String value) {
        if (value == null) {
            return null;
        }
        for (String credential : new String[] {config.registrationKey(), config.authToken()}) {
            if (credential != null && !credential.isEmpty()) {
                value = value.replace(credential, "[redacted]");
            }
        }
        return value.replace('\r', ' ').replace('\n', ' ');
    }

    private static WorkerRejectedPayload genericRejection() {
        return new WorkerRejectedPayload("WORKER_REJECTED", "The coordinator rejected the worker registration");
    }

    private void disconnected(WebSocket ws, WorkerRejectedPayload reason) {
        synchronized (this) {
            if (!activeWebSocket.compareAndSet(ws, null)) {
                return;
            }
            connection = null;
            cancelRenewal();
            // The coordinator requeues the partition of a lost connection.
            assignments.cancel(null, 0);
            if (reason == null) {
                if (registeredLatch.getCount() == 0) {
                    registeredLatch = new CountDownLatch(1);
                }
                scheduleReconnect();
                return;
            }
            rejectionReason = reason;
            registeredLatch.countDown();
        }
        if (!ws.isInputClosed()) {
            ws.abort();
        }
        // Application callbacks may wait for registration or close the client.
        rejectionNotification.complete(null);
    }

    private final class WorkerWsListener implements WebSocket.Listener {
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            register(this, webSocket);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String json = buffer.toString();
                buffer.setLength(0);
                try {
                    WorkerWsMessage msg = DistributionJson.fromJson(json, WorkerWsMessage.class);
                    var reason = registrationFailure(msg);
                    if (reason != null) {
                        disconnected(webSocket, reason);
                    } else {
                        handleWsMessage(webSocket, msg);
                    }
                } catch (Exception e) {
                    LOG.warn("Invalid WebSocket message on worker {}", config.workerId());
                }
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            disconnected(webSocket, statusCode == 4003 ? genericRejection() : null);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            disconnected(webSocket, null);
        }
    }
}
