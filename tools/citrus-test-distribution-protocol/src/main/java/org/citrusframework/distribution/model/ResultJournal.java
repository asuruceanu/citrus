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

package org.citrusframework.distribution.model;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.citrusframework.distribution.config.DistributionJson;

/** Test results remain on disk until the coordinator acknowledges their database transaction. */
public final class ResultJournal {
    private final Path directory;
    private long lastOrder;

    public ResultJournal(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
            try (var files = Files.list(directory)) {
                lastOrder = files.map(path -> path.getFileName().toString()).filter(ResultJournal::isMessage)
                        .mapToLong(name -> Long.parseLong(name.substring(0, 19))).max().orElse(0);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open the test result journal", e);
        }
    }

    public synchronized WorkerWsMessage persist(String type, Map<String, Object> payload) {
        lastOrder = Math.max(lastOrder + 1, System.currentTimeMillis() * 1_000L);
        String id = String.format(Locale.ROOT, "%019d-%s", lastOrder, UUID.randomUUID());
        var data = new LinkedHashMap<>(payload);
        data.put(WorkerWsMessage.DELIVERY_ID, id);
        WorkerWsMessage message = new WorkerWsMessage(type, data);
        Path temporary = directory.resolve(id + ".tmp");
        try {
            ByteBuffer bytes = ByteBuffer.wrap(DistributionJson.toJson(message).getBytes(StandardCharsets.UTF_8));
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                while (bytes.hasRemaining()) {
                    channel.write(bytes);
                }
                channel.force(true);
            }
            Files.move(temporary, directory.resolve(id + ".json"), StandardCopyOption.ATOMIC_MOVE);
            return message;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot persist a test result", e);
        }
    }

    public void replay(Consumer<WorkerWsMessage> sender) {
        try (var files = Files.list(directory)) {
            for (Path path : files.filter(file -> isMessage(file.getFileName().toString())).sorted().toList()) {
                try {
                    sender.accept(DistributionJson.fromJson(Files.readString(path), WorkerWsMessage.class));
                } catch (java.nio.file.NoSuchFileException ignored) {
                    // The coordinator acknowledged it while replay was starting.
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot replay the test result journal", e);
        }
    }

    public void acknowledge(String id) {
        if (id == null || !isMessage(id + ".json")) {
            return;
        }
        try {
            Files.deleteIfExists(directory.resolve(id + ".json"));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot acknowledge a test result", e);
        }
    }

    private static boolean isMessage(String name) {
        return name.matches("[0-9]{19}-[a-f0-9-]{36}[.]json");
    }
}
