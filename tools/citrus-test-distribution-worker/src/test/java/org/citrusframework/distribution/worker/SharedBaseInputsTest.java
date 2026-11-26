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

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class SharedBaseInputsTest {

    @TempDir
    Path temp;

    @Test
    void secondSlotReusesVerifiedInputsAndReceivesIndependentFiles() throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry("resource.txt"));
            zip.write(42);
        }
        byte[] archive = bytes.toByteArray();
        String digest = ClasspathPackager.sha256Hex(archive);
        var entries = List.of(new ArtifactRef("test-classes", digest, ArtifactKind.DIRECTORY_ZIP, archive.length));
        Path root = temp.resolve("shared");
        Path firstSlot = temp.resolve("slot1");
        new BlobCacheDirectory(firstSlot).put(digest, archive);
        var config = DistributionWorkerConfig.builder().build();
        try (var cache = WorkerFileCache.open(firstSlot, false, 0);
             var blobs = new CoordinatorBlobClient(config, cache)) {
            var first = SharedBaseInputs.materialize(root, "session", entries, cache, blobs, temp.resolve("first"));
            Files.writeString(temp.resolve("first").resolve(digest).resolve("resource.txt"), "modified");
            try (var other = WorkerFileCache.open(temp.resolve("slot2"), false, 0);
                 var otherBlobs = new CoordinatorBlobClient(config, other)) {
                var second = SharedBaseInputs.materialize(root, "session", entries, other, otherBlobs, temp.resolve("second"));
                assertThat(second.readyAt()).isEqualTo(first.readyAt());
                assertThat(second.startedAt()).isEqualTo(first.startedAt());
                assertThat(second.waitMs()).isZero();
                assertThat(Files.readAllBytes(temp.resolve("second").resolve(digest).resolve("resource.txt")))
                        .containsExactly((byte) 42);
            }
        }
    }
}
