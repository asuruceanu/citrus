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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

import io.vertx.core.http.HttpServerResponse;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.BrokerEvent;

/**
 * Publishes monotonically sequenced SSE events per session and globally, supporting Last-Event-ID replay.
 */
public final class BrokerEventStreamHub {

    private static final int MAX_GLOBAL_HISTORY = 2_000;

    private final AtomicLong sequenceGenerator = new AtomicLong(0);
    private final Map<String, List<BrokerEvent>> sessionHistory = new ConcurrentHashMap<>();
    private final Set<String> terminalSessions = new CopyOnWriteArraySet<>();
    private final List<BrokerEvent> globalHistory = new CopyOnWriteArrayList<>();
    private final Map<String, Set<HttpServerResponse>> sessionSubscribers = new ConcurrentHashMap<>();
    private final Set<HttpServerResponse> globalSubscribers = new CopyOnWriteArraySet<>();

    public BrokerEvent publish(String type, String sessionId, String partitionId, String workerId, Map<String, Object> data) {
        BrokerEvent event = new BrokerEvent(
                sequenceGenerator.incrementAndGet(),
                type,
                sessionId,
                partitionId,
                workerId,
                System.currentTimeMillis(),
                data != null ? data : Map.of()
        );

        globalHistory.add(event);
        while (globalHistory.size() > MAX_GLOBAL_HISTORY) {
            globalHistory.remove(0);
        }

        if (sessionId != null) {
            sessionHistory.computeIfAbsent(sessionId, ignored -> new CopyOnWriteArrayList<>()).add(event);
        }

        String formatted = formatSse(event);
        broadcast(globalSubscribers, formatted);
        if (sessionId != null) {
            Set<HttpServerResponse> subs = sessionSubscribers.get(sessionId);
            if (subs != null) {
                broadcast(subs, formatted);
            }
            if ("session.completed".equals(type) || "session.aborted".equals(type)) {
                terminalSessions.add(sessionId);
                if (subs != null) {
                    for (HttpServerResponse resp : subs) {
                        closeQuietly(resp);
                    }
                    subs.clear();
                }
            }
        }
        return event;
    }

    public void subscribeSession(String sessionId, long afterSequence, HttpServerResponse response) {
        prepareSseHeaders(response);
        List<BrokerEvent> replay = new ArrayList<>(sessionHistory.getOrDefault(sessionId, List.of()));
        for (BrokerEvent event : replay) {
            if (event.sequence() > afterSequence) {
                response.write(formatSse(event));
            }
        }
        if (terminalSessions.contains(sessionId)) {
            closeQuietly(response);
            return;
        }
        Set<HttpServerResponse> subs = sessionSubscribers.computeIfAbsent(sessionId, ignored -> new CopyOnWriteArraySet<>());
        subs.add(response);
        response.closeHandler(v -> subs.remove(response));
        response.exceptionHandler(err -> subs.remove(response));
    }

    public void subscribeGlobal(long afterSequence, HttpServerResponse response) {
        prepareSseHeaders(response);
        for (BrokerEvent event : globalHistory) {
            if (event.sequence() > afterSequence) {
                response.write(formatSse(event));
            }
        }
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

    private String formatSse(BrokerEvent event) {
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
