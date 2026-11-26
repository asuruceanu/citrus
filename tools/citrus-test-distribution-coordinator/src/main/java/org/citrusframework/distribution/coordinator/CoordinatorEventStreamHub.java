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

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import io.vertx.core.http.HttpServerResponse;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.CoordinatorEvent;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SessionSummary;
import org.citrusframework.distribution.model.TestExecutionRecord;
import org.citrusframework.distribution.store.DistributionStore;
import org.citrusframework.distribution.store.DistributionStore.RecoverableWork;

/**
 * Publishes monotonically sequenced SSE events per session and globally, supporting Last-Event-ID replay.
 */
public final class CoordinatorEventStreamHub {

    private static final int MAX_GLOBAL_HISTORY = 2_000;

    private final DistributionStore store;
    private final Map<String, Set<HttpServerResponse>> sessionSubscribers = new ConcurrentHashMap<>();
    private final Set<HttpServerResponse> globalSubscribers = new CopyOnWriteArraySet<>();

    private final Set<HttpServerResponse> dashboardSubscribers = new CopyOnWriteArraySet<>();
    private final Set<String> dashboardChanges = new HashSet<>();

    // The dashboard reads snapshots. It only needs to know which kinds of data changed.
    void subscribeDashboard(HttpServerResponse response) {
        prepareSseHeaders(response);
        response.closeHandler(v -> dashboardSubscribers.remove(response));
        response.exceptionHandler(err -> dashboardSubscribers.remove(response));
        dashboardSubscribers.add(response);
    }

    void flushDashboardChanges() {
        Set<String> changes;
        synchronized (dashboardChanges) {
            if (dashboardChanges.isEmpty()) {
                return;
            }
            changes = Set.copyOf(dashboardChanges);
            dashboardChanges.clear();
        }
        String frame = "event: dashboard.updated\ndata: " + DistributionJson.toJson(changes) + "\n\n";
        for (HttpServerResponse response : dashboardSubscribers) {
            if (response.writeQueueFull()) {
                // EventSource reconnects and the UI fetches a fresh snapshot.
                dashboardSubscribers.remove(response);
                closeQuietly(response);
            } else {
                response.write(frame).onFailure(err -> dashboardSubscribers.remove(response));
            }
        }
    }

    public CoordinatorEventStreamHub(DistributionStore store) {
        this.store = store;
    }

    public CoordinatorEvent publish(String type, String sessionId, String partitionId, String workerId, Map<String, Object> data) {
        return publish(type, sessionId, partitionId, workerId, data, null, null, List.of());
    }

    synchronized CoordinatorEvent publish(String type, String sessionId, String partitionId, String workerId,
                                          Map<String, Object> data, String deliveryId, TestExecutionRecord result, List<PartitionRecord> partitions) {
        long timestamp = System.currentTimeMillis();
        Map<String, Object> eventData = data == null ? Map.of() : data;
        long sequence = store.appendEvent(new CoordinatorEvent(0, type, sessionId, partitionId, workerId, timestamp, eventData),
                deliveryId, result, partitions);
        CoordinatorEvent event = new CoordinatorEvent(sequence, type, sessionId, partitionId, workerId, timestamp, eventData);

        broadcastEvent(event);
        return event;
    }

    synchronized void publishSubmission(RecoverableWork work, List<PartitionRecord> partitions, Map<String, Object> data) {
        long timestamp = System.currentTimeMillis();
        var event = new CoordinatorEvent(0, "work.submitted", work.sessionId(), null, null, timestamp, data);
        long sequence = store.saveWorkSubmission(work, partitions, event);
        broadcastEvent(new CoordinatorEvent(sequence, event.type(), event.sessionId(), null, null, timestamp, data));
    }

    synchronized void finishSession(String type, String sessionId, long finishedAt, SessionStatus status,
                                    SessionSummary summary, String reason, Map<String, Object> data) {
        var event = new CoordinatorEvent(0, type, sessionId, null, null, finishedAt, data);
        long sequence = store.finishSession(event, status, summary, reason);
        broadcastEvent(new CoordinatorEvent(sequence, type, sessionId, null, null, finishedAt, data));
    }

    private void broadcastEvent(CoordinatorEvent event) {
        String sessionId = event.sessionId();
        String type = event.type();
        if (!dashboardSubscribers.isEmpty()) {
            synchronized (dashboardChanges) {
                dashboardChanges.add(type);
            }
        }
        if (globalSubscribers.isEmpty() && (sessionId == null || !sessionSubscribers.containsKey(sessionId))) {
            return;
        }
        String formatted = formatSse(event);
        broadcast(globalSubscribers, formatted);
        if (sessionId != null) {
            Set<HttpServerResponse> subs = sessionSubscribers.get(sessionId);
            if (subs != null) {
                broadcast(subs, formatted);
            }
            if ("session.completed".equals(type) || "session.aborted".equals(type)) {
                if (subs != null) {
                    for (HttpServerResponse resp : subs) {
                        closeQuietly(resp);
                    }
                    sessionSubscribers.remove(sessionId);
                }
            }
        }
    }

    public synchronized void subscribeSession(String sessionId, long afterSequence, HttpServerResponse response) {
        prepareSseHeaders(response);
        store.replayEvents(sessionId, afterSequence, 0, event -> response.write(formatSse(event)));
        if (store.findSession(sessionId).map(record -> record.status() != SessionStatus.RUNNING).orElse(false)) {
            closeQuietly(response);
            return;
        }
        Set<HttpServerResponse> subs = sessionSubscribers.computeIfAbsent(sessionId, ignored -> new CopyOnWriteArraySet<>());
        subs.add(response);
        response.closeHandler(v -> subs.remove(response));
        response.exceptionHandler(err -> subs.remove(response));
    }

    public synchronized void subscribeGlobal(long afterSequence, HttpServerResponse response) {
        prepareSseHeaders(response);
        store.replayEvents(null, afterSequence, MAX_GLOBAL_HISTORY, event -> response.write(formatSse(event)));
        globalSubscribers.add(response);
        response.closeHandler(v -> globalSubscribers.remove(response));
        response.exceptionHandler(err -> globalSubscribers.remove(response));
    }

    private void prepareSseHeaders(HttpServerResponse response) {
        response.setChunked(true);
        response.putHeader("Content-Type", "text/event-stream;charset=UTF-8");
        response.putHeader("Cache-Control", "no-cache");
        response.write(": connected\n\n");
    }

    private String formatSse(CoordinatorEvent event) {
        return "id: " + event.sequence() + "\n"
                + "event: " + event.type() + "\n"
                + "data: " + DistributionJson.toJson(event) + "\n\n";
    }

    private void broadcast(Set<HttpServerResponse> subscribers, String frame) {
        for (HttpServerResponse response : subscribers) {
            if (response.closed() || response.ended()) {
                subscribers.remove(response);
                continue;
            }
            response.write(frame).onFailure(err -> subscribers.remove(response));
        }
    }

    private void closeQuietly(HttpServerResponse response) {
        try {
            if (!response.ended() && !response.closed()) {
                response.end();
            }
        } catch (Exception ignored) {
            // Ignored
        }
    }
}
