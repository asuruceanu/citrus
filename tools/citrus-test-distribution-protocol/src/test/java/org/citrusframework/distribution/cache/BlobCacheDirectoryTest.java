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
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    void shouldRepairAnIndependentlyReplacedNamedJarThroughEitherMaterializationMethod() throws Exception {
        byte[] jar = jarWith("a.txt", "a");
        String sha = sha(jar);
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path blob = cache.put(sha, jar);
        ArtifactRef ref = new ArtifactRef("library.jar", sha, ArtifactKind.JAR, jar.length);
        Path named = cache.materializeForClasspath(ref, blob, tempDir);
        FileTime originalTime = Files.getLastModifiedTime(named);

        Files.move(named, tempDir.resolve("original-name.jar"));
        Files.write(named, new byte[jar.length]);
        Files.setLastModifiedTime(named, originalTime);

        assertThat(cache.materializeForClasspath(ref, blob, tempDir)).hasBinaryContent(jar);
        assertThat(blob).hasBinaryContent(jar);

        Files.move(named, tempDir.resolve("repaired-name.jar"));
        Files.write(named, new byte[jar.length]);
        Files.setLastModifiedTime(named, originalTime);

        assertThat(cache.materializeForClasspath(ref, tempDir)).hasBinaryContent(jar);
        assertThat(blob).hasBinaryContent(jar);
    }

    @Test
    void shouldReplaceANamedJarSymlinkEvenWhenItTargetsTheCanonicalBlob() throws Exception {
        byte[] jar = jarWith("a.txt", "a");
        String sha = sha(jar);
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path blob = cache.put(sha, jar);
        ArtifactRef ref = new ArtifactRef("library.jar", sha, ArtifactKind.JAR, jar.length);
        Path named = cache.materializeForClasspath(ref, blob, tempDir);
        Files.delete(named);
        assumeTrue(createSymlink(named, blob), "file system supports symbolic links");

        assertThat(cache.materializeForClasspath(ref, blob, tempDir)).hasBinaryContent(jar);
        assertThat(Files.isSymbolicLink(named)).isFalse();
        assertThat(blob).hasBinaryContent(jar);
    }

    @Test
    void shouldReuseUnchangedNamedJarCopiesAndRefreshChangedCopiesWhenLinksAreUnsupported() throws Exception {
        byte[] jar = jarWith("a.txt", "a");
        String sha = sha(jar);
        AtomicInteger linkAttempts = new AtomicInteger();
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"), (link, target) -> {
            linkAttempts.incrementAndGet();
            throw new UnsupportedOperationException("simulated");
        });
        Path blob = cache.put(sha, jar);
        ArtifactRef ref = new ArtifactRef("library.jar", sha, ArtifactKind.JAR, jar.length);

        Path named = cache.materializeForClasspath(ref, blob, tempDir);
        assertThat(named).hasBinaryContent(jar);
        assertThat(Files.isSameFile(named, blob)).isFalse();
        FileTime blobTime = Files.getLastModifiedTime(blob);
        Files.setLastModifiedTime(blob, FileTime.fromMillis(blobTime.toMillis() + 60_000));
        assertThat(cache.materializeForClasspath(ref, tempDir)).isEqualTo(named);
        assertThat(linkAttempts).hasValue(1);

        FileTime originalTime = Files.getLastModifiedTime(named);
        Files.write(named, new byte[jar.length]);
        Files.setLastModifiedTime(named, FileTime.fromMillis(originalTime.toMillis() + 60_000));

        assertThat(cache.materializeForClasspath(ref, blob, tempDir)).hasBinaryContent(jar);
        assertThat(cache.materializeForClasspath(ref, tempDir)).isEqualTo(named);
        assertThat(linkAttempts).hasValue(2);

        Path savedCopy = Files.move(named, tempDir.resolve("saved-copy.jar"));
        cache.deleteNamedJars(sha);
        Files.createDirectories(named.getParent());
        Files.move(savedCopy, named);

        assertThat(cache.materializeForClasspath(ref, tempDir)).hasBinaryContent(jar);
        assertThat(linkAttempts).hasValue(3);
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
    void shouldRefuseToDeleteThroughRedirectedCacheDirectories() throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        byte[] content = "cached blob".getBytes();
        String sha = sha(content);
        Path blob = cache.put(sha, content);
        Path outside = Files.createDirectory(tempDir.resolve("outside"));
        Path outsideJar = Files.writeString(Files.createDirectory(outside.resolve(sha)).resolve("library.jar"), "keep");
        Path classpath = cache.getRootDir().resolve("classpath");
        assumeTrue(createSymlink(classpath, outside), "file system supports symbolic links");

        assertThatThrownBy(() -> cache.delete(sha))
                .isInstanceOf(IOException.class).hasMessageContaining(classpath.toString());
        assertThat(outsideJar).hasContent("keep");
        assertThat(blob).hasBinaryContent(content);

        Files.delete(classpath);
        Path shard = blob.getParent();
        Path outsideShard = Files.move(shard, outside.resolve("shard"));
        assumeTrue(createSymlink(shard, outsideShard), "file system supports symbolic links");
        assertThatThrownBy(() -> cache.delete(sha))
                .isInstanceOf(IOException.class).hasMessageContaining(shard.toString());
        assertThat(outsideShard.resolve(sha)).hasBinaryContent(content);
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
        byte[] jar = jarWith("b.txt", "b");
        String sha = sha(jar);
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path blob = cache.put(sha, jar);
        Path named = cache.materializeForClasspath(new ArtifactRef("lib.jar", sha, ArtifactKind.JAR, jar.length),
                tempDir.resolve("partition"));
        Path namedDir = named.getParent();
        assumeTrue(namedDir.toFile().setWritable(false), "file system supports read-only directories");
        try {
            assumeTrue(!Files.isWritable(namedDir), "directory permissions are enforced for this user");
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
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path first = cache.newTempFile("dl-");
        Path second = cache.newTempFile("ws-");
        Path tmpDir = first.getParent();
        assumeTrue(tmpDir.toFile().setWritable(false), "file system supports read-only directories");
        try {
            assumeTrue(!Files.isWritable(tmpDir), "directory permissions are enforced for this user");
            assertThat(cache.sweepTempFiles()).isZero();
            assertThat(first).exists();
            assertThat(second).exists();
        } finally {
            tmpDir.toFile().setWritable(true);
        }
        assertThat(cache.sweepTempFiles()).isEqualTo(2);
    }

    @Test
    void shouldSweepOnlyTheTemporaryFilePrefixesTheCacheCreates() throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        Path tmpDir = Files.createDirectories(cache.getRootDir().resolve("tmp"));
        List<Path> cacheMade = List.of(
                cache.newTempFile("0a1b2c3d-"),
                cache.newTempFile("dl-"),
                cache.newTempFile("ws-"),
                cache.newTempFile("output-"));
        List<Path> foreign = List.of(
                Files.writeString(tmpDir.resolve("backup-2024.tmp"), "keep"),
                Files.writeString(tmpDir.resolve("report-1.tmp"), "keep"),
                Files.writeString(tmpDir.resolve("abcdefgh-1.tmp"), "keep"),
                Files.writeString(tmpDir.resolve("0A1B2C3D-1.tmp"), "keep"),
                Files.writeString(tmpDir.resolve("0a1b2c3-1.tmp"), "keep"),
                Files.writeString(tmpDir.resolve("dl-.tmp"), "keep"),
                Files.writeString(tmpDir.resolve("dl-12x.tmp"), "keep"));

        assertThat(cache.sweepTempFiles()).isEqualTo(cacheMade.size());

        assertThat(cacheMade).allSatisfy(path -> assertThat(path).doesNotExist());
        assertThat(foreign).allSatisfy(path -> assertThat(path).hasContent("keep"));
        assertThatThrownBy(() -> cache.newTempFile("backup-"))
                .as("every temporary file prefix must be one the sweep recognizes")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRemoveAPartialTargetWhenTheNonAtomicFallbackMoveFails() throws Exception {
        BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
        byte[] bytes = "moved without atomic rename".getBytes();
        String sha = sha(bytes);
        cache.fileMover((source, target, options) -> {
            if (List.of(options).contains(StandardCopyOption.ATOMIC_MOVE)) {
                throw new AtomicMoveNotSupportedException(source.toString(), target.toString(), "simulated");
            }
            Files.writeString(target, "partial");
            throw new IOException("simulated copy failure");
        });

        assertThatThrownBy(() -> cache.put(sha, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("simulated copy failure");
        assertThat(cache.resolveBlobPath(sha)).as("partial target removed").doesNotExist();
        assertThat(tmpFiles(cache)).isEmpty();

        cache.fileMover((source, target, options) -> {
            if (List.of(options).contains(StandardCopyOption.ATOMIC_MOVE)) {
                throw new AtomicMoveNotSupportedException(source.toString(), target.toString(), "simulated");
            }
            Files.move(source, target, options);
        });
        assertThat(cache.put(sha, bytes)).as("non-atomic fallback").hasBinaryContent(bytes);
        assertThat(tmpFiles(cache)).isEmpty();
    }

    @Test
    void shouldPublishOneCompleteOwnerAcrossConcurrentClaims() throws Exception {
        Path root = tempDir.resolve("concurrent-owner");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var start = new CountDownLatch(1);
            var coordinator = executor.submit(() -> {
                start.await();
                return BlobCacheDirectory.recordOwnerIfAbsent(root, BlobCacheDirectory.OWNER_COORDINATOR);
            });
            var worker = executor.submit(() -> {
                start.await();
                return BlobCacheDirectory.recordOwnerIfAbsent(root, BlobCacheDirectory.OWNER_WORKER);
            });
            start.countDown();
            String owner = coordinator.get(5, TimeUnit.SECONDS);
            assertThat(worker.get(5, TimeUnit.SECONDS)).isEqualTo(owner);
            assertThat(BlobCacheDirectory.readOwner(root)).contains(owner);
        }
        try (var members = Files.list(root)) {
            assertThat(members).containsExactly(root.resolve(BlobCacheDirectory.OWNER_MARKER_FILE));
        }
    }

    @Test
    void shouldRecordTheFirstCacheOwnerAndNeverOverwriteIt() throws Exception {
        Path root = tempDir.resolve("owned");
        assertThat(BlobCacheDirectory.readOwner(root)).isEmpty();

        assertThat(BlobCacheDirectory.recordOwnerIfAbsent(root, BlobCacheDirectory.OWNER_COORDINATOR))
                .isEqualTo(BlobCacheDirectory.OWNER_COORDINATOR);
        assertThat(root.resolve(BlobCacheDirectory.OWNER_MARKER_FILE)).hasContent(BlobCacheDirectory.OWNER_COORDINATOR);
        assertThat(BlobCacheDirectory.recordOwnerIfAbsent(root, BlobCacheDirectory.OWNER_WORKER))
                .as("an existing owner is kept").isEqualTo(BlobCacheDirectory.OWNER_COORDINATOR);
        assertThat(BlobCacheDirectory.readOwner(root)).contains(BlobCacheDirectory.OWNER_COORDINATOR);

        Path blank = Files.createDirectories(tempDir.resolve("blank"));
        Files.writeString(blank.resolve(BlobCacheDirectory.OWNER_MARKER_FILE), " \n");
        assertThatThrownBy(() -> BlobCacheDirectory.recordOwnerIfAbsent(blank, BlobCacheDirectory.OWNER_LAUNCHER))
                .isInstanceOf(IOException.class).hasMessageContaining("owner marker");
        assertThat(blank.resolve(BlobCacheDirectory.OWNER_MARKER_FILE)).hasContent(" ");
    }

    private static List<Path> tmpFiles(BlobCacheDirectory cache) throws IOException {
        Path tmp = cache.getRootDir().resolve("tmp");
        if (!Files.isDirectory(tmp)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(tmp)) {
            return files.toList();
        }
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
