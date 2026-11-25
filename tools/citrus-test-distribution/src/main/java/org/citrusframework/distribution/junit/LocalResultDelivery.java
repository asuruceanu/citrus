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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.ReportLocalTestsRequest;
import org.citrusframework.distribution.model.ResultJournal;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkerWsMessage;

public final class LocalResultDelivery implements Consumer<WorkerTestEventPayload> {
    private static final System.Logger LOG = System.getLogger(LocalResultDelivery.class.getName());
    private static final String LOCAL_WORKER_ID = "localWorkerId";
    private final CoordinatorHttpClient client;
    private final String sessionId;
    private final String workerId;
    private final ResultJournal journal;

    LocalResultDelivery(CoordinatorHttpClient client, String sessionId, String workerId) {
        this.client = client;
        this.sessionId = sessionId;
        this.workerId = workerId;
        String execution = System.getProperty(DistributionSettings.MAVEN_TEST_EXECUTION_PROPERTY, System.getProperty("user.dir"));
        this.journal = sessionId == null || sessionId.isBlank() ? null : new ResultJournal(
                sessionDirectory(client, sessionId).resolve(key(workerId + "\n" + execution)));
    }

    @Override
    public void accept(WorkerTestEventPayload event) {
        if (journal != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> data = DistributionJson.convert(event, Map.class);
            data.put(LOCAL_WORKER_ID, workerId);
            journal.persist(WorkerWsMessage.TEST_EVENT, data);
        }
    }

    void flush() {
        if (journal == null) {
            return;
        }
        try {
            deliver(client, sessionId, journal);
        } catch (UncheckedIOException e) {
            // Keep the existing local fallback policy. Maven will retry the journal before closing the session.
            LOG.log(System.Logger.Level.WARNING, "Local results for session {0} are on disk awaiting delivery: {1}",
                    sessionId, e.getCause().getMessage());
        }
    }

    /** Delivers all Maven-side results before the lifecycle participant marks the session terminal. */
    public static void flushSession(CoordinatorHttpClient client, String sessionId) throws IOException {
        Path parent = sessionDirectory(client, sessionId);
        if (!Files.isDirectory(parent)) {
            return;
        }
        try (var children = Files.list(parent)) {
            for (Path path : children.filter(Files::isDirectory).toList()) {
                deliver(client, sessionId, new ResultJournal(path));
            }
        }
    }

    private static void deliver(CoordinatorHttpClient client, String sessionId, ResultJournal journal) {
        journal.replay(message -> {
            String id = (String) message.payload().get(WorkerWsMessage.DELIVERY_ID);
            String worker = (String) message.payload().get(LOCAL_WORKER_ID);
            var result = DistributionJson.convert(message.payload(), WorkerTestEventPayload.class);
            try {
                client.reportLocalTestsWithDeadline(sessionId, new ReportLocalTestsRequest(worker, List.of(result), id));
                journal.acknowledge(id);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while sending recorded local results", e);
            }
        });
    }

    private static Path sessionDirectory(CoordinatorHttpClient client, String sessionId) {
        return Path.of(System.getProperty("user.home"), ".citrus", "distribution-local-results",
                key(client.baseUrl() + "\n" + sessionId));
    }

    private static String key(String value) {
        return UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
