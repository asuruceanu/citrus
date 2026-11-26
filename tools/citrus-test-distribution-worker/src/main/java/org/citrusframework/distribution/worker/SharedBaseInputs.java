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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;

import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.InputPreparation;
import org.citrusframework.distribution.model.WorkspaceFileSet;

/** One bounded directory snapshot per Compose worker. Forks receive independent writable files. */
final class SharedBaseInputs {
    private SharedBaseInputs() {
    }

    static boolean supports(List<ArtifactRef> entries) {
        var directories = entries.stream().filter(entry -> entry.kind() == ArtifactKind.DIRECTORY_ZIP)
                .map(ArtifactRef::sha256).toList();
        return !directories.isEmpty() && directories.stream().distinct().count() == directories.size();
    }

    static synchronized InputPreparation materialize(Path root, String sessionId, List<ArtifactRef> entries,
            WorkerFileCache cache, CoordinatorBlobClient blobs, Path privateInputs) throws IOException, InterruptedException {
        long requested = System.currentTimeMillis();
        Files.createDirectories(root);
        try (var channel = FileChannel.open(root.resolve("lock"), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
             var lock = channel.lock()) {
            var directories = entries.stream().filter(entry -> entry.kind() == ArtifactKind.DIRECTORY_ZIP).distinct().toList();
            if (directories.isEmpty()) {
                return null;
            }
            String key = sessionId + ":" + directories.stream().map(ArtifactRef::sha256).toList();
            Path marker = root.resolve("ready");
            Path snapshot = root.resolve("snapshot");
            List<String> saved = Files.exists(marker) ? Files.readAllLines(marker) : List.of();
            long started;
            long ready;
            if (saved.size() == 3 && saved.get(0).equals(key)) {
                started = Long.parseLong(saved.get(1));
                ready = Long.parseLong(saved.get(2));
            } else {
                started = System.currentTimeMillis();
                Files.deleteIfExists(marker);
                if (Files.exists(snapshot)) {
                    try (var paths = Files.walk(snapshot)) {
                        for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                            Files.delete(path);
                        }
                    }
                }
                for (ArtifactRef directory : directories) {
                    var verified = blobs.ensureCached(directory);
                    cache.materializeIntoDirectory(new WorkspaceFileSet(directory.sha256(), directory), verified, snapshot);
                }
                ready = System.currentTimeMillis();
                Files.write(root.resolve("publishing"), List.of(key, Long.toString(started), Long.toString(ready)));
                Files.move(root.resolve("publishing"), marker, StandardCopyOption.ATOMIC_MOVE);
            }
            // The lock also protects the snapshot from replacement while another session is copying it.
            try (var paths = Files.walk(snapshot)) {
                var iterator = paths.iterator();
                while (iterator.hasNext()) {
                    Path source = iterator.next();
                    Path dest = privateInputs.resolve(snapshot.relativize(source));
                    if (Files.isDirectory(source)) {
                        Files.createDirectories(dest);
                    } else {
                        Files.copy(source, dest);
                    }
                }
            }
            return new InputPreparation(started, ready, Math.max(0, ready - requested));
        }
    }
}
