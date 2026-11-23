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

package org.citrusframework.distribution.cache;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.WorkerCacheStats;
import org.citrusframework.distribution.model.WorkerHeartbeatPayload;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerRegisterPayload;
import org.citrusframework.distribution.model.WorkerState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class BlobCacheDirectoryTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldPutJarsOnTheClasspathUnderTheirOriginalJarFileName() throws Exception {
        // Libraries look at classpath file names: Testcontainers' MountableFile only extracts a resource from an
        // archive when its URL contains ".jar!", and copies the whole archive otherwise.
        byte[] jar = jarWith("testcontainers/entrypoint-tc.sh", "#!/usr/bin/env bash\n");
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(jar));
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        cache.put(sha, jar);
        ArtifactRef ref = new ArtifactRef("testcontainers-redpanda-2.0.5.jar", sha, ArtifactKind.JAR, jar.length);

        Path onClasspath = cache.materializeForClasspath(ref, Files.createDirectories(tempDir.resolve("partition")));

        assertThat(onClasspath.getFileName().toString()).isEqualTo("testcontainers-redpanda-2.0.5.jar");
        assertThat(onClasspath).hasBinaryContent(jar);
        assertThat(cache.materializeForClasspath(ref, tempDir.resolve("other-partition")))
                .as("shared by every partition of the worker").isEqualTo(onClasspath);
    }

    @Test
    void shouldKeepAJarExtensionAndAPlainFileNameForAnyArtifactName() throws Exception {
        byte[] jar = jarWith("a.txt", "a");
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(jar));
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        cache.put(sha, jar);

        Path onClasspath = cache.materializeForClasspath(
                new ArtifactRef("../../evil name", sha, ArtifactKind.JAR, jar.length), tempDir);

        assertThat(onClasspath.getFileName().toString()).endsWith(".jar").doesNotContain("/").doesNotContain("..");
        assertThat(onClasspath.startsWith(cache.getRootDir())).isTrue();
    }

    @Test
    void shouldListStoredBlobsIgnoringStrayFilesAndSymlinks() throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        byte[] first = "first".getBytes();
        byte[] second = "second".getBytes();
        String firstSha = sha(first);
        String secondSha = sha(second);
        Path firstPath = cache.put(firstSha, first);
        Path secondPath = cache.put(secondSha, second);

        Path blobs = cache.getRootDir().resolve("blobs");
        Files.writeString(blobs.resolve(".DS_Store"), "x");
        Path prefixDir = Files.createDirectories(blobs.resolve("aa"));
        Files.writeString(prefixDir.resolve("not-a-digest.txt"), "x");
        // a digest-named file under the wrong prefix is not a blob either
        Path wrongPrefix = Files.createDirectories(blobs.resolve("bb"));
        Files.writeString(wrongPrefix.resolve(firstSha), "x");
        boolean posix = cache.getRootDir().getFileSystem().supportedFileAttributeViews().contains("posix");
        if (posix) {
            String linkSha = sha("link".getBytes());
            Path linkDir = Files.createDirectories(blobs.resolve(linkSha.substring(0, 2)));
            Files.createSymbolicLink(linkDir.resolve(linkSha), firstPath);
        }

        List<BlobCacheDirectory.BlobEntry> entries = cache.listBlobs();

        assertThat(entries).containsExactlyInAnyOrder(
                entry(firstSha, firstPath, first.length),
                entry(secondSha, secondPath, second.length));
    }

    @Test
    void shouldListNothingForAnEmptyCache() throws Exception {
        assertThat(new BlobCacheDirectory(tempDir.resolve("empty")).listBlobs()).isEmpty();
    }

    @Test
    void shouldDeleteExistingBlobAndReturnFalseWhenMissing() throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        byte[] bytes = "payload".getBytes();
        String sha = sha(bytes);
        Path path = cache.put(sha, bytes);
        assertThat(path).isEqualTo(cache.getRootDir().resolve("blobs").resolve(sha.substring(0, 2)).resolve(sha));

        assertThat(cache.delete(sha)).isTrue();
        assertThat(path).doesNotExist();
        assertThat(cache.delete(sha)).isFalse();

        assertThatThrownBy(() -> cache.delete("../etc")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.delete(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.delete(sha.toUpperCase())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldPruneEmptyBlobShardDirectoryOnDeleteButKeepSharedShards() throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        byte[] first = "first".getBytes();
        byte[] sibling = null;
        for (int i = 0; sibling == null; i++) {
            byte[] candidate = ("sibling " + i).getBytes();
            if (sha(candidate).substring(0, 2).equals(sha(first).substring(0, 2))) {
                sibling = candidate;
            }
        }
        Path firstBlob = cache.put(sha(first), first);
        Path siblingBlob = cache.put(sha(sibling), sibling);
        Path shard = firstBlob.getParent();

        assertThat(cache.delete(sha(first))).isTrue();
        assertThat(shard).as("shard still holds the sibling").isDirectory();
        assertThat(siblingBlob).exists();

        assertThat(cache.delete(sha(sibling))).isTrue();
        assertThat(shard).as("empty shard pruned").doesNotExist();
        assertThat(cache.getRootDir().resolve("blobs")).isDirectory();
        assertThat(cache.delete(sha(sibling))).isFalse();
    }

    @Test
    void shouldKeepBlobWhenItsNamedJarDirectoryCannotBeDeleted() throws Exception {
        assumeFalse("root".equals(System.getProperty("user.name")), "root ignores directory permissions");
        byte[] jar = jarWith("b.txt", "b");
        String sha = sha(jar);
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path blob = cache.put(sha, jar);
        Path named = cache.materializeForClasspath(new ArtifactRef("lib.jar", sha, ArtifactKind.JAR, jar.length),
                tempDir.resolve("partition"));
        Path namedDir = named.getParent();
        assumeTrue(namedDir.toFile().setWritable(false), "file system supports read-only directories");
        try {
            assertThatThrownBy(() -> cache.delete(sha)).isInstanceOf(java.io.IOException.class);
            assertThat(blob).as("blob kept so callers' bookkeeping stays accurate").exists();
        } finally {
            namedDir.toFile().setWritable(true);
        }
        assertThat(cache.delete(sha)).isTrue();
        assertThat(blob).doesNotExist();
        assertThat(namedDir).doesNotExist();
    }

    @Test
    void shouldDeleteNamedJarDirectoryWithBlobAndSweepOrphanStaging() throws Exception {
        byte[] jar = jarWith("a.txt", "a");
        String sha = sha(jar);
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path blob = cache.put(sha, jar);
        Path named = cache.materializeForClasspath(new ArtifactRef("lib.jar", sha, ArtifactKind.JAR, jar.length),
                tempDir.resolve("partition"));
        assertThat(named).exists();

        Path otherDir = Files.createDirectories(cache.getRootDir().resolve("classpath").resolve("0".repeat(64)));
        Path orphan = Files.writeString(otherDir.resolve(".staging-x.jar"), "partial");
        Path published = Files.writeString(otherDir.resolve("published.jar"), "ok");

        boolean unix = cache.getRootDir().getFileSystem().supportedFileAttributeViews().contains("unix");
        // a probe link outside the cache lets us observe the inode's link count after both cache links are gone
        Path probe = tempDir.resolve("probe");
        if (unix) {
            Files.createLink(probe, blob);
            assertThat(((Number) Files.getAttribute(probe, "unix:nlink")).intValue()).isEqualTo(3);
        }

        assertThat(cache.delete(sha)).isTrue();

        assertThat(blob).doesNotExist();
        assertThat(named).doesNotExist();
        assertThat(named.getParent()).doesNotExist();
        if (unix) {
            assertThat(((Number) Files.getAttribute(probe, "unix:nlink")).intValue())
                    .as("no surviving hard link inside the cache").isEqualTo(1);
        }

        assertThat(cache.sweepClasspathStaging()).isEqualTo(1);
        assertThat(orphan).doesNotExist();
        assertThat(published).exists();
        assertThat(cache.sweepClasspathStaging()).isZero();
    }

    @Test
    void shouldSweepTempFilesButKeepBlobsClasspathAndLock() throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        byte[] bytes = "kept".getBytes();
        Path blob = cache.put(sha(bytes), bytes);
        Path tmpDir = Files.createDirectories(cache.getRootDir().resolve("tmp"));
        Path download = cache.newTempFile("dl-");
        Path staged = cache.newTempFile(sha(bytes).substring(0, 8) + "-");
        Path classpath = Files.createDirectories(cache.getRootDir().resolve("classpath").resolve("c"));
        Path jar = Files.writeString(classpath.resolve("x.jar"), "x");
        Path lock = Files.writeString(cache.getRootDir().resolve(".lock"), "");

        assertThat(cache.sweepTempFiles()).isEqualTo(2);

        assertThat(tmpDir).isDirectory();
        assertThat(download).doesNotExist();
        assertThat(staged).doesNotExist();
        assertThat(blob).exists();
        assertThat(jar).exists();
        assertThat(lock).exists();
        assertThat(cache.sweepTempFiles()).isZero();
        assertThat(new BlobCacheDirectory(tempDir.resolve("fresh")).sweepTempFiles()).isZero();
    }

    @Test
    void shouldOnlySweepCacheCreatedTempFilesAndLeaveForeignEntriesAlone() throws Exception {
        // --cache-dir may point at an existing directory whose tmp/ belongs to someone else.
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path tmpDir = Files.createDirectories(cache.getRootDir().resolve("tmp"));
        Path cacheMade = cache.newTempFile("ws-");
        Path unrelated = Files.writeString(tmpDir.resolve("notes.txt"), "keep");
        Path unrelatedTmp = Files.writeString(tmpDir.resolve("session.tmp"), "keep");
        Path subdir = Files.createDirectories(tmpDir.resolve("dl-1.tmp"));
        Path subdirContent = Files.writeString(subdir.resolve("inner-1.tmp"), "keep");
        Path outside = Files.writeString(tempDir.resolve("outside-target.txt"), "keep");
        Path link = tmpDir.resolve("dl-2.tmp");
        boolean symlinks = createSymlink(link, outside);

        assertThat(cache.sweepTempFiles()).isEqualTo(1);

        assertThat(cacheMade).doesNotExist();
        assertThat(unrelated).hasContent("keep");
        assertThat(unrelatedTmp).hasContent("keep");
        assertThat(subdir).isDirectory();
        assertThat(subdirContent).hasContent("keep");
        assertThat(outside).hasContent("keep");
        if (symlinks) {
            assertThat(Files.isSymbolicLink(link)).isTrue();
        }
    }

    @Test
    void shouldWarnAndContinueWhenATempFileCannotBeDeleted() throws Exception {
        assumeFalse("root".equals(System.getProperty("user.name")), "root ignores directory permissions");
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path first = cache.newTempFile("dl-");
        Path second = cache.newTempFile("ws-");
        Path tmpDir = first.getParent();
        assumeTrue(tmpDir.toFile().setWritable(false), "file system supports read-only directories");
        try {
            assertThat(cache.sweepTempFiles()).isZero();
            assertThat(first).exists();
            assertThat(second).exists();
        } finally {
            tmpDir.toFile().setWritable(true);
        }
        assertThat(cache.sweepTempFiles()).isEqualTo(2);
    }

    private static boolean createSymlink(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void shouldRoundTripWorkerCacheStatsAndLegacyPayloadsWithoutCacheStats() {
        WorkerCacheStats stats = new WorkerCacheStats(true, 2_147_483_648L, 3, 1234L, 99_999L, 7L, true);
        Map<String, String> caps = Map.of("docker", "true");

        WorkerRegisterPayload register = new WorkerRegisterPayload("w1", "h", caps, stats);
        assertThat(DistributionJson.fromJson(DistributionJson.toJson(register), WorkerRegisterPayload.class))
                .isEqualTo(register);
        WorkerHeartbeatPayload heartbeat = new WorkerHeartbeatPayload("w1", "p1", stats);
        assertThat(DistributionJson.fromJson(DistributionJson.toJson(heartbeat), WorkerHeartbeatPayload.class))
                .isEqualTo(heartbeat);
        WorkerRecord record = new WorkerRecord("w1", "h", caps, WorkerState.IDLE, null, 5L, 4L, stats);
        assertThat(DistributionJson.fromJson(DistributionJson.toJson(record), WorkerRecord.class)).isEqualTo(record);

        WorkerRegisterPayload legacyRegister = DistributionJson.fromJson(
                "{\"workerId\":\"w1\",\"host\":\"h\",\"capabilities\":{\"docker\":\"true\"}}",
                WorkerRegisterPayload.class);
        assertThat(legacyRegister.cacheStats()).isNull();
        assertThat(legacyRegister).isEqualTo(new WorkerRegisterPayload("w1", "h", caps));
        WorkerHeartbeatPayload legacyHeartbeat = DistributionJson.fromJson(
                "{\"workerId\":\"w1\",\"activePartitionId\":\"p1\"}", WorkerHeartbeatPayload.class);
        assertThat(legacyHeartbeat.cacheStats()).isNull();
        assertThat(legacyHeartbeat).isEqualTo(new WorkerHeartbeatPayload("w1", "p1"));
        WorkerRecord legacyRecord = DistributionJson.fromJson(
                "{\"workerId\":\"w1\",\"host\":\"h\",\"capabilities\":{},\"state\":\"IDLE\","
                        + "\"lastHeartbeatAt\":5,\"registeredAt\":4}", WorkerRecord.class);
        assertThat(legacyRecord.cacheStats()).isNull();
        assertThat(legacyRecord.registeredAt()).isEqualTo(4L);
    }

    private static BlobCacheDirectory.BlobEntry entry(String sha, Path path, long size) throws Exception {
        return new BlobCacheDirectory.BlobEntry(sha, path, size,
                Files.getLastModifiedTime(path, LinkOption.NOFOLLOW_LINKS).toMillis());
    }

    private static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static byte[] jarWith(String entry, String content) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry(entry));
            zip.write(content.getBytes());
            zip.closeEntry();
        }
        return bytes.toByteArray();
    }
}
