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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionCompletedPayload;
import org.citrusframework.distribution.model.WorkerHeartbeatPayload;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerWsMessage;
import org.junit.platform.engine.CancellationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistent WebSocket worker client that registers host capabilities with the Citrus Test
 * Distribution Broker, executes assigned partitions, and supports graceful SIGTERM draining.
 */
public final class DistributionWorkerClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionWorkerClient.class);

    private final DistributionWorkerConfig config;
    private final IsolatedPartitionRunner runner;
    private final HttpClient httpClient;
    private final ExecutorService partitionExecutor;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean draining = new AtomicBoolean(false);
    private final AtomicReference<WebSocket> activeWebSocket = new AtomicReference<>();
    private final ActiveAssignmentTracker assignments = new ActiveAssignmentTracker();
    private volatile CountDownLatch registeredLatch = new CountDownLatch(1);
    private volatile CompletableFuture<Void> currentExecution = CompletableFuture.completedFuture(null);

    public DistributionWorkerClient(DistributionWorkerConfig config) {
        this.config = config;
        this.runner = new IsolatedPartitionRunner(config);
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

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        connectInternal();
        scheduler.scheduleAtFixedRate(
                this::sendHeartbeatSafe,
                config.heartbeatIntervalMs(),
                config.heartbeatIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    public boolean awaitRegistration(long timeout, TimeUnit unit) throws InterruptedException {
        return registeredLatch.await(timeout, unit);
    }

    public boolean isConnected() {
        WebSocket ws = activeWebSocket.get();
        return ws != null && !ws.isOutputClosed() && !ws.isInputClosed();
    }

    /**
     * Gracefully drains this worker: notifies the broker to stop assigning new partitions and
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
    public void abortConnection() {
        running.set(false);
        WebSocket ws = activeWebSocket.getAndSet(null);
        if (ws != null) {
            ws.abort();
        }
    }

    @Override
    public void close() {
        drain(10, TimeUnit.SECONDS);
        running.set(false);
        WebSocket ws = activeWebSocket.getAndSet(null);
        if (ws != null) {
            try {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown").get(2, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                ws.abort();
            }
        }
        scheduler.shutdownNow();
        partitionExecutor.shutdownNow();
    }

    private void connectInternal() {
        if (!running.get()) {
            return;
        }
        registeredLatch = new CountDownLatch(1);
        URI wsUri = buildWsUri();
        WebSocket.Builder builder = httpClient.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10));
        if (config.authToken() != null && !config.authToken().isBlank()) {
            builder.header("Authorization", "Bearer " + config.authToken());
        }

        builder.buildAsync(wsUri, new WorkerWsListener())
                .whenComplete((ws, err) -> {
                    if (err != null) {
                        LOG.warn("Worker {} failed to connect to {}: {}", config.workerId(), wsUri, err.getMessage());
                        scheduleReconnect();
                        return;
                    }
                    activeWebSocket.set(ws);
                    Map<String, String> caps = new LinkedHashMap<>(config.capabilities());
                    caps.put("citrus-worker", "true");
                    WorkerRegisterPayload registerPayload = new WorkerRegisterPayload(
                            config.workerId(), config.hostName(), caps);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> payloadMap = DistributionJson.convert(registerPayload, Map.class);
                    sendMessageSafe(WorkerWsMessage.WORKER_REGISTER, payloadMap);
                });
    }

    private URI buildWsUri() {
        String base = config.brokerUrl();
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
        if (!running.get() || draining.get()) {
            return;
        }
        scheduler.schedule(this::connectInternal, config.reconnectDelayMs(), TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeatSafe() {
        if (!running.get() || !isConnected()) {
            return;
        }
        WorkerHeartbeatPayload hb = new WorkerHeartbeatPayload(config.workerId(), assignments.activePartitionId());
        @SuppressWarnings("unchecked")
        Map<String, Object> map = DistributionJson.convert(hb, Map.class);
        sendMessageSafe(WorkerWsMessage.WORKER_HEARTBEAT, map);
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

    private void handleWsMessage(WorkerWsMessage message) {
        if (message == null || message.type() == null) {
            return;
        }
        switch (message.type()) {
            case WorkerWsMessage.WORKER_REGISTERED -> registeredLatch.countDown();
            case WorkerWsMessage.PARTITION_ASSIGN -> {
                PartitionAssignPayload assign = DistributionJson.convert(message.payload(), PartitionAssignPayload.class);
                ActiveAssignmentTracker.Assignment assignment = assignments.start(assign.partitionId(), assign.attempt());
                CancellationToken token = assignment.token();
                currentExecution = CompletableFuture.runAsync(() -> {
                    try {
                        sendMessageSafe(WorkerWsMessage.PARTITION_STARTED, Map.of(
                                "partitionId", assign.partitionId(),
                                "sessionId", assign.sessionId(),
                                "attempt", assign.attempt()));
                        PartitionCompletedPayload completed = runner.execute(assign, token, event -> {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> eventMap = DistributionJson.convert(event, Map.class);
                            sendMessageSafe(WorkerWsMessage.TEST_EVENT, eventMap);
                        });
                        @SuppressWarnings("unchecked")
                        Map<String, Object> completedMap = DistributionJson.convert(completed, Map.class);
                        sendMessageSafe(WorkerWsMessage.PARTITION_COMPLETED, completedMap);
                    } finally {
                        assignments.finish(assignment);
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

    private final class WorkerWsListener implements WebSocket.Listener {
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String json = buffer.toString();
                buffer.setLength(0);
                try {
                    WorkerWsMessage msg = DistributionJson.fromJson(json, WorkerWsMessage.class);
                    handleWsMessage(msg);
                } catch (Exception e) {
                    LOG.warn("Invalid WebSocket message on worker {}: {}", config.workerId(), e.getMessage());
                }
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            activeWebSocket.compareAndSet(webSocket, null);
            scheduleReconnect();
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            activeWebSocket.compareAndSet(webSocket, null);
            scheduleReconnect();
        }
    }
}
