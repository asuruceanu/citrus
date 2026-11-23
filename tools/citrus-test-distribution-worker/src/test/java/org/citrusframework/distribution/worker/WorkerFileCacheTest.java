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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.BlobCacheDirectory.BlobEntry;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.WorkerCacheStats;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class WorkerFileCacheTest {

    private static final long GIB = 1024L * 1024 * 1024;
    private static final long MIB = 1024L * 1024;
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    private static final WorkerFileCache.DiskSpaceProbe PLENTY = p -> 1024 * GIB;

    @TempDir
    Path tempDir;

    @Test
    void shouldDefaultWorkerConfigCacheSettingsFromDistributionSettingsAndRejectNegativeBuffer() {
        DistributionWorkerConfig config = DistributionWorkerConfig.builder().build();

        assertThat(config.cacheDir()).isEqualTo(DistributionSettings.getWorkerCacheDir());
        assertThat(config.cacheCleanup()).isTrue();
        assertThat(config.cacheFreeSpaceBufferBytes()).isEqualTo(2 * GIB);

        DistributionWorkerConfig custom = DistributionWorkerConfig.builder()
                .cacheCleanup(false)
                .cacheFreeSpaceBufferBytes(0L)
                .build();
        assertThat(custom.cacheCleanup()).isFalse();
        assertThat(custom.cacheFreeSpaceBufferBytes()).isZero();

        assertThatThrownBy(() -> DistributionWorkerConfig.builder().cacheFreeSpaceBufferBytes(-1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectSecondWorkerOnSameCacheDirInSameJvmAndReleaseOnClose() throws Exception {
        Path canonical = tempDir.toRealPath();
        WorkerFileCache first = WorkerFileCache.open(tempDir, true, 0L, PLENTY, FIXED_CLOCK);
        try {
            assertThat(first.rootDir()).isEqualTo(canonical);
            assertThatThrownBy(() -> WorkerFileCache.open(tempDir, true, 0L, PLENTY, FIXED_CLOCK))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(canonical.toString())
                    .hasMessageContaining("--cache-dir");
        } finally {
            first.close();
        }
        first.close(); // idempotent

        assertThat(canonical.resolve(".lock")).exists();
        try (WorkerFileCache third = WorkerFileCache.open(tempDir, true, 0L, PLENTY, FIXED_CLOCK)) {
            assertThat(third.rootDir()).isEqualTo(canonical);
        }
    }

    @Test
    void shouldRejectSecondJvmLockAndRecoverCleanlyAfterForcibleChildCrash() throws Exception {
        Path dir = tempDir.resolve("shared");
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        Process child = new ProcessBuilder(java.toString(), "-cp", System.getProperty("java.class.path"),
                WorkerFileCacheLockHolderMain.class.getName(), dir.toString())
                .redirectErrorStream(true)
                .start();
        boolean childLocked;
        Throwable secondOpen = null;
        Path canonical = null;
        boolean jvmLockRolledBack = false;
        boolean leftoverKept = false;
        boolean childGone;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8));
            CompletableFuture<Boolean> locked = CompletableFuture.supplyAsync(() -> {
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (WorkerFileCacheLockHolderMain.LOCKED_MARKER.equals(line.trim())) {
                            return true;
                        }
                    }
                    return false;
                } catch (IOException e) {
                    return false;
                }
            });
            childLocked = locked.get(60, TimeUnit.SECONDS);
            if (childLocked) {
                canonical = dir.toRealPath();
                secondOpen = catchThrowable(() -> WorkerFileCache.open(dir, true, 0L, PLENTY, FIXED_CLOCK).close());
                jvmLockRolledBack = !WorkerFileCache.ACTIVE_JVM_LOCKS.contains(canonical);
                leftoverKept = Files.exists(canonical.resolve("tmp").resolve("dl-4711.tmp"));
            }
        } finally {
            childGone = child.destroyForcibly().waitFor(30, TimeUnit.SECONDS);
        }

        assertThat(childLocked).as("child JVM acquired the cache lock").isTrue();
        assertThat(secondOpen)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(canonical.toString())
                .hasMessageContaining("--cache-dir");
        assertThat(jvmLockRolledBack).isTrue();
        assertThat(leftoverKept).as("a locked cache is not swept").isTrue();
        assertThat(childGone).isTrue();

        try (WorkerFileCache cache = WorkerFileCache.open(dir, true, 0L, PLENTY, FIXED_CLOCK)) {
            assertThat(cache.rootDir().resolve("tmp").resolve("dl-4711.tmp")).doesNotExist();
            assertThat(cache.rootDir().resolve(".lock")).exists();
        }
    }

    @Test
    void shouldRollBackJvmLockSetWhenFileLockThrowsIoException() throws Exception {
        Path canonical = tempDir.toRealPath();
        assertThatThrownBy(() -> WorkerFileCache.open(tempDir, true, 0L, PLENTY, FIXED_CLOCK, channel -> {
            throw new IOException("simulated lockless mount");
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(canonical.resolve(".lock").toString());
        assertThat(WorkerFileCache.ACTIVE_JVM_LOCKS).doesNotContain(canonical);

        assertThatThrownBy(() -> WorkerFileCache.open(tempDir, true, 0L, PLENTY, FIXED_CLOCK, channel -> {
            throw new IllegalStateException("simulated unexpected failure");
        })).isInstanceOf(IllegalStateException.class).hasMessage("simulated unexpected failure");
        assertThat(WorkerFileCache.ACTIVE_JVM_LOCKS).doesNotContain(canonical);

        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, true, 0L, PLENTY, FIXED_CLOCK)) {
            assertThat(WorkerFileCache.ACTIVE_JVM_LOCKS).contains(canonical);
        }
        assertThat(WorkerFileCache.ACTIVE_JVM_LOCKS).doesNotContain(canonical);
    }

    @Test
    void shouldAlwaysSweepOrphanTmpFilesEvenWhenCacheCleanupDisabled() throws Exception {
        byte[] content = "valid blob".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(content);
        Path blob = new BlobCacheDirectory(tempDir).put(sha, content);
        Path stale = Files.createDirectories(tempDir.resolve("tmp")).resolve("dl-4712.tmp");
        Files.writeString(stale, "partial");

        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, false, 2 * GIB, p -> 0L, FIXED_CLOCK)) {
            assertThat(stale).doesNotExist();
            assertThat(tempDir.resolve("tmp")).isDirectory();
            assertThat(blob).exists();
            assertThat(cache.verifyOrInvalidate(sha)).isNotNull();
            cache.ensureFreeSpaceForDownload(1024);
            cache.evictAfterPartition();
            assertThat(blob).as("no eviction without cache cleanup").exists();
        }
    }

    @Test
    void shouldVerifyAnExistingTargetBeforeReturningADownload() throws Exception {
        byte[] content = "freshly downloaded".getBytes(StandardCharsets.UTF_8);
        String digest = ClasspathPackager.sha256Hex(content);
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, true, 0L, PLENTY, FIXED_CLOCK)) {
            Path target = store(cache).resolveBlobPath(digest);
            Files.createDirectories(target.getParent());
            Files.writeString(target, "corrupt file that appeared during download");

            assertThat(promote(cache, content)).hasBinaryContent(content);
            assertThat(cache.statsSnapshot().cachedBytes()).isEqualTo(content.length);
        }
    }

    @Test
    void shouldDetectStampMismatchOnExternalSizeMtimeOrInodeMutation() throws Exception {
        byte[] content = "stamped blob content".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(content);
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, true, 0L, PLENTY, FIXED_CLOCK)) {
            Path blob = promote(cache, content);
            assertThat(cache.verifyOrInvalidate(sha)).isEqualTo(blob);
            assertThat(cache.verifyOrInvalidate(sha)).as("warm hit").isEqualTo(blob);

            // (a) size change
            Files.write(blob, "different size".getBytes(StandardCharsets.UTF_8));
            assertThat(cache.verifyOrInvalidate(sha)).isNull();
            assertThat(blob).doesNotExist();

            // (b) same length, corrupt bytes, bumped mtime
            promote(cache, content);
            Files.write(blob, corrupt(content));
            Files.setLastModifiedTime(blob, FileTime.from(FIXED_CLOCK.instant().plusSeconds(5)));
            assertThat(cache.verifyOrInvalidate(sha)).isNull();
            assertThat(blob).doesNotExist();

            // (c) move-over: same length, same mtime, new inode
            promote(cache, content);
            FileTime mtime = Files.getLastModifiedTime(blob);
            Path replacement = blob.resolveSibling("replacement");
            Files.write(replacement, corrupt(content));
            Files.setLastModifiedTime(replacement, mtime);
            Files.move(replacement, blob, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            assertThat(Files.size(blob)).isEqualTo(content.length);
            assertThat(Files.getLastModifiedTime(blob)).isEqualTo(mtime);
            assertThat(cache.verifyOrInvalidate(sha)).isNull();
            assertThat(blob).doesNotExist();

            assertThat(cache.verifyOrInvalidate(sha)).as("absent blob").isNull();
        }
    }

    @Test
    void shouldReverifyAFileReplacedWhileTouchingAWarmBlob() throws Exception {
        byte[] content = "verified blob".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(content);
        AtomicReference<Runnable> onTouch = new AtomicReference<>();
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, false, 0L, PLENTY, clockWithTouchHook(onTouch))) {
            Path blob = promote(cache, content);
            Path replacement = Files.write(blob.resolveSibling("replacement"), corrupt(content));
            Files.setLastModifiedTime(replacement, Files.getLastModifiedTime(blob));
            onTouch.set(() -> {
                try {
                    Files.move(replacement, blob, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            assertThat(cache.verifyOrInvalidate(sha)).isNull();
            assertThat(blob).doesNotExist();
            assertThat(promote(cache, content)).hasBinaryContent(content);
            assertThat(cache.verifyOrInvalidate(sha)).isEqualTo(blob);
        }
    }

    @Test
    void shouldRestoreABlobReplacedWithASymbolicLinkWithoutTouchingItsTarget() throws Exception {
        byte[] content = "verified blob".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(content);
        Path external = Files.writeString(tempDir.resolve("outside"), "keep me");
        FileTime externalTime = Files.getLastModifiedTime(external);
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir.resolve("cache"), false, 0L, PLENTY, FIXED_CLOCK,
                FileChannel::tryLock, root -> new BlobCacheDirectory(root) {
                    @Override
                    public boolean isIntact(String sha256) throws IOException {
                        assertThat(Files.isSymbolicLink(resolveBlobPath(sha256))).isFalse();
                        return super.isIntact(sha256);
                    }
                })) {
            Path blob = promote(cache, content);
            Files.delete(blob);
            try {
                Files.createSymbolicLink(blob, external);
            } catch (IOException | UnsupportedOperationException e) {
                assumeTrue(false, "file system supports symbolic links");
            }

            assertThat(cache.verifyOrInvalidate(sha)).isNull();
            assertThat(Files.isSymbolicLink(blob)).isFalse();
            assertThat(promote(cache, content)).hasBinaryContent(content);
            assertThat(external).hasContent("keep me");
            assertThat(Files.getLastModifiedTime(external)).isEqualTo(externalTime);
        }
    }

    @Test
    void shouldRebuildTheNamedJarWhenRedownloadingAMissingCanonicalBlob() throws Exception {
        byte[] content = "classpath jar contents".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(content);
        ArtifactRef artifact = new ArtifactRef("library.jar", sha, ArtifactKind.JAR, content.length);
        Path dir = tempDir.resolve("cache");
        Path workspace = tempDir.resolve("workspace");
        try (WorkerFileCache cache = WorkerFileCache.open(dir, false, 0L, PLENTY, FIXED_CLOCK);
             WorkerFileCache.PinLease lease = cache.pinAll(List.of(sha))) {
            Path blob = promote(cache, content);
            Path named = cache.materializeForClasspath(artifact, blob, workspace);
            Files.write(named, corrupt(content));
            Files.delete(blob);
        }

        try (WorkerFileCache cache = WorkerFileCache.open(dir, false, 0L, PLENTY, FIXED_CLOCK);
             WorkerFileCache.PinLease lease = cache.pinAll(List.of(sha))) {
            assertThat(cache.verifyOrInvalidate(sha)).isNull();
            Path blob = promote(cache, content);
            assertThat(cache.materializeForClasspath(artifact, blob, workspace)).hasBinaryContent(content);
        }
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldFreeNamedJarFootprintOnEvictionAndSweepStagingOnOpen() throws Exception {
        byte[] jarBytes = "jar bytes".getBytes(StandardCharsets.UTF_8);
        String jarSha = ClasspathPackager.sha256Hex(jarBytes);
        Path namedJar;
        Path jarBlob;
        List<Path> others = new ArrayList<>();
        AtomicReference<WorkerFileCache.DiskSpaceProbe> probe = new AtomicReference<>(PLENTY);
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, true, GIB, p -> probe.get().usableBytes(p), FIXED_CLOCK)) {
            jarBlob = promote(cache, jarBytes);
            namedJar = cache.materializeForClasspath(new ArtifactRef("lib-1.0.jar", jarSha, ArtifactKind.JAR, jarBytes.length),
                    cache.verifyOrInvalidate(jarSha), tempDir.resolve("ws"));
            assertThat(namedJar.getParent().getFileName().toString()).isEqualTo(jarSha);
            assertThat((Integer) Files.getAttribute(jarBlob, "unix:nlink")).isEqualTo(2);
            for (int i = 0; i < 3; i++) {
                others.add(promote(cache, ("other " + i).getBytes(StandardCharsets.UTF_8)));
            }

            // Space is reported as freed only once the jar inode is gone (no remaining links).
            probe.set(p -> Files.exists(jarBlob) || Files.exists(namedJar) ? GIB - 1 : GIB);
            cache.evictAfterPartition();

            assertThat(jarBlob).doesNotExist();
            assertThat(namedJar.getParent()).doesNotExist();
            assertThat(others).allSatisfy(o -> assertThat(o).exists());
            assertThat(cache.statsSnapshot().evictedBlobsTotal()).isEqualTo(1);

            // A pinned digest keeps its named jar even when every other blob is evicted.
            promote(cache, jarBytes);
            Path pinnedJar = cache.materializeForClasspath(new ArtifactRef("lib-1.0.jar", jarSha, ArtifactKind.JAR, jarBytes.length),
                    cache.verifyOrInvalidate(jarSha), tempDir.resolve("ws"));
            try (WorkerFileCache.PinLease lease = cache.pinAll(List.of(jarSha))) {
                probe.set(p -> 0L);
                cache.evictAfterPartition();
                assertThat(others).allSatisfy(o -> assertThat(o).doesNotExist());
                assertThat(pinnedJar).exists();
                assertThat(jarBlob).exists();
            }
            probe.set(PLENTY);
        }

        Path staging = tempDir.resolve("classpath").resolve(jarSha).resolve(".staging-123.jar");
        Files.writeString(staging, "orphan");
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, false, GIB, p -> 0L, FIXED_CLOCK)) {
            assertThat(staging).doesNotExist();
            assertThat(tempDir.resolve("classpath").resolve(jarSha).resolve("lib-1.0.jar")).exists();
        }
    }

    @Test
    void shouldEvictInExactSessionAccessOrderAndDeterministicRestartOrderWhilePreservingPinnedBlobs() throws Exception {
        long unit = 1000L;
        Path dir = tempDir.resolve("session");
        List<byte[]> contents = new ArrayList<>();
        List<String> shas = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            byte[] bytes = sized("blob-" + i, (int) unit);
            contents.add(bytes);
            shas.add(ClasspathPackager.sha256Hex(bytes));
        }
        List<Path> paths = shas.stream().map(s -> new BlobCacheDirectory(dir).resolveBlobPath(s)).toList();
        WorkerFileCache.DiskSpaceProbe probe = missingCountProbe(paths, unit);

        try (WorkerFileCache cache = WorkerFileCache.open(dir, true, GIB, probe, FIXED_CLOCK)) {
            for (byte[] bytes : contents) {
                promote(cache, bytes);
            }
            assertThat(cache.verifyOrInvalidate(shas.get(0))).isNotNull();
            assertThat(cache.verifyOrInvalidate(shas.get(1))).isNotNull();

            WorkerFileCache.PinLease lease = cache.pinAll(List.of(shas.get(2), shas.get(2)));
            WorkerFileCache.PinLease otherLease = cache.pinAll(List.of(shas.get(2)));
            assertThat(cache.isPinned(shas.get(2))).isTrue();

            cache.ensureFreeSpaceForDownload(unit);          // deficit = one blob
            assertThat(existing(paths)).containsExactly(true, true, true, false);
            cache.ensureFreeSpaceForDownload(2 * unit);      // one more
            assertThat(existing(paths)).containsExactly(false, true, true, false);
            cache.ensureFreeSpaceForDownload(100 * unit);    // everything unpinned
            assertThat(existing(paths)).containsExactly(false, false, true, false);

            lease.close();
            assertThat(cache.isPinned(shas.get(2))).as("still pinned by the other lease").isTrue();
            lease.close();
            assertThat(cache.isPinned(shas.get(2))).as("second close is a no-op").isTrue();
            otherLease.close();
            assertThat(cache.isPinned(shas.get(2))).isFalse();
            cache.ensureFreeSpaceForDownload(100 * unit);
            assertThat(existing(paths)).containsExactly(false, false, false, false);
        }

        // Restart: every mtime ties and there is no session access order, so eviction follows sha256 ascending.
        Path restartDir = tempDir.resolve("restart");
        List<String> restartShas = new ArrayList<>();
        try (WorkerFileCache cache = WorkerFileCache.open(restartDir, false, GIB, PLENTY, FIXED_CLOCK)) {
            for (int i = 0; i < 5; i++) {
                byte[] bytes = sized("restart-" + i, (int) unit);
                promote(cache, bytes);
                restartShas.add(ClasspathPackager.sha256Hex(bytes));
            }
            List<String> descending = restartShas.stream().sorted((a, b) -> b.compareTo(a)).toList();
            for (String sha : descending) {
                assertThat(cache.verifyOrInvalidate(sha)).isNotNull();
            }
        }
        List<String> ascending = restartShas.stream().sorted().toList();
        List<Path> restartPaths = ascending.stream().map(s -> new BlobCacheDirectory(restartDir).resolveBlobPath(s)).toList();
        try (WorkerFileCache cache = WorkerFileCache.open(restartDir, true, GIB, missingCountProbe(restartPaths, unit), FIXED_CLOCK)) {
            for (int k = 1; k <= ascending.size(); k++) {
                cache.ensureFreeSpaceForDownload(k * unit);
                for (int i = 0; i < ascending.size(); i++) {
                    assertThat(Files.exists(restartPaths.get(i))).as("blob %d after %d evictions", i, k).isEqualTo(i >= k);
                }
            }
        }
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldContinueEvictionWhenSingleBlobDeleteFailsAndRateLimitTightDiskWarning() throws Exception {
        byte[] first = null;
        byte[] second = null;
        for (int i = 0; first == null || second == null; i++) {
            byte[] candidate = ("candidate " + i).getBytes(StandardCharsets.UTF_8);
            if (first == null) {
                first = candidate;
            } else if (!ClasspathPackager.sha256Hex(candidate).substring(0, 2)
                    .equals(ClasspathPackager.sha256Hex(first).substring(0, 2))) {
                second = candidate;
            }
        }
        String firstSha = ClasspathPackager.sha256Hex(first);
        AtomicReference<WorkerFileCache.DiskSpaceProbe> probe = new AtomicReference<>(PLENTY);
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, true, 2 * GIB, p -> probe.get().usableBytes(p), FIXED_CLOCK)) {
            Path firstBlob = promote(cache, first);
            Path secondBlob = promote(cache, second);
            Path firstParent = firstBlob.getParent();
            Files.setPosixFilePermissions(firstParent, PosixFilePermissions.fromString("r-xr-xr-x"));
            try {
                assumeTrue(!Files.isWritable(firstParent), "directory permissions are enforced for this user");
                probe.set(p -> Files.exists(secondBlob) ? GIB : 3 * GIB);
                cache.evictAfterPartition();
                assertThat(firstBlob).as("delete failed, blob survives").exists();
                assertThat(secondBlob).as("eviction continued with the next candidate").doesNotExist();
                assertThat(cache.evictionDeleteFailureCount()).as("the failed delete was warned about").isEqualTo(1);
            } finally {
                Files.setPosixFilePermissions(firstParent, PosixFilePermissions.fromString("rwxr-xr-x"));
            }
            assertThat(cache.verifyOrInvalidate(firstSha)).isEqualTo(firstBlob);

            try (WorkerFileCache.PinLease lease = cache.pinAll(List.of(firstSha))) {
                probe.set(p -> 500 * MIB);
                assertThatNoException().isThrownBy(() -> cache.ensureFreeSpaceForDownload(1024));
                cache.ensureFreeSpaceForDownload(1024);
                cache.evictAfterPartition();
                assertThat(cache.lowDiskWarningActive()).isTrue();
                assertThat(cache.lowDiskWarningCount()).isEqualTo(1);
                assertThat(cache.statsSnapshot().lowDiskSpace()).isTrue();

                probe.set(p -> 2 * GIB);
                cache.evictAfterPartition();
                assertThat(cache.lowDiskWarningActive()).isFalse();
                assertThat(cache.statsSnapshot().lowDiskSpace()).isFalse();

                probe.set(p -> 500 * MIB);
                cache.ensureFreeSpaceForDownload(1024);
                assertThat(cache.lowDiskWarningCount()).as("new shortage episode").isEqualTo(2);
            }
        }
    }

    @Test
    void shouldSaturateAdditionOnNearMaxBufferAndEvict100kSyntheticEntriesUnderFiveSeconds() throws Exception {
        assertThat(WorkerFileCache.deficit(Long.MAX_VALUE - 10, GIB, 1024)).isPositive();
        assertThat(WorkerFileCache.deficit(Long.MAX_VALUE, 0L, Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
        assertThat(WorkerFileCache.deficit(0L, Long.MAX_VALUE, 0L)).isNegative();

        Path nearMaxDir = tempDir.resolve("near-max");
        try (WorkerFileCache cache = WorkerFileCache.open(nearMaxDir, true, Long.MAX_VALUE - 10, p -> GIB, FIXED_CLOCK)) {
            Path blob = promote(cache, "unpinned".getBytes(StandardCharsets.UTF_8));
            assertThatNoException().isThrownBy(() -> cache.ensureFreeSpaceForDownload(1024));
            assertThat(blob).as("a saturated, positive deficit still evicts").doesNotExist();
        }

        int total = 100_000;
        int toEvict = 10_000;
        long base = 10 * GIB;
        List<BlobEntry> synthetic = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            synthetic.add(new BlobEntry(String.format("%064x", i), tempDir.resolve("synthetic"), 1L, 1_000L + i));
        }
        AtomicLong deleted = new AtomicLong();
        AtomicInteger probes = new AtomicInteger();
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir.resolve("synthetic"), true, base,
                p -> {
                    probes.incrementAndGet();
                    return base + deleted.get();
                }, FIXED_CLOCK)) {
            cache.blobIndex(new WorkerFileCache.BlobIndex() {
                @Override
                public List<BlobEntry> listBlobs() {
                    return synthetic;
                }

                @Override
                public boolean delete(String sha256) {
                    deleted.incrementAndGet();
                    return true;
                }
            });
            probes.set(0);
            long start = System.nanoTime();
            cache.ensureFreeSpaceForDownload(toEvict);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertThat(deleted.get()).isEqualTo(toEvict);
            assertThat(elapsedMs).isLessThan(5_000L);
            assertThat(probes.get()).isLessThanOrEqualTo((toEvict / 64) + 3);
            assertThat(cache.statsSnapshot().evictedBlobsTotal()).isEqualTo(toEvict);
        }
    }

    @Test
    void shouldMaintainAccurateIncrementalStatsSnapshotAcrossStartupPromoteCorruptAndEvict() throws Exception {
        byte[] seeded = "seeded blob".getBytes(StandardCharsets.UTF_8);
        String seededSha = ClasspathPackager.sha256Hex(seeded);
        Path seededBlob = new BlobCacheDirectory(tempDir).put(seededSha, seeded);
        byte[] promoted = "promoted blob content".getBytes(StandardCharsets.UTF_8);

        AtomicReference<WorkerFileCache.DiskSpaceProbe> probe = new AtomicReference<>(p -> 5 * GIB);
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, true, 2 * GIB, p -> probe.get().usableBytes(p), FIXED_CLOCK)) {
            WorkerCacheStats initial = cache.statsSnapshot();
            assertThat(initial.cleanupEnabled()).isTrue();
            assertThat(initial.freeSpaceBufferBytes()).isEqualTo(2 * GIB);
            assertThat(initial.blobCount()).isEqualTo(1);
            assertThat(initial.cachedBytes()).isEqualTo(seeded.length);
            assertThat(initial.usableDiskBytes()).isEqualTo(5 * GIB);
            assertThat(initial.evictedBlobsTotal()).isZero();
            assertThat(initial.lowDiskSpace()).isFalse();

            Path promotedBlob = promote(cache, promoted);
            assertThat(cache.statsSnapshot().blobCount()).isEqualTo(2);
            promote(cache, promoted);
            assertThat(cache.statsSnapshot().blobCount()).as("duplicate promote").isEqualTo(2);
            assertThat(cache.statsSnapshot().cachedBytes()).isEqualTo(seeded.length + promoted.length);

            Files.write(seededBlob, corrupt(seeded));
            assertThat(cache.verifyOrInvalidate(seededSha)).isNull();
            assertThat(cache.statsSnapshot().blobCount()).isEqualTo(1);
            assertThat(cache.statsSnapshot().cachedBytes()).isEqualTo(promoted.length);
            assertThat(cache.statsSnapshot().evictedBlobsTotal()).as("corruption is not eviction").isZero();

            probe.set(p -> Files.exists(promotedBlob) ? GIB : 3 * GIB);
            cache.evictAfterPartition();
            WorkerCacheStats evicted = cache.statsSnapshot();
            assertThat(evicted.blobCount()).isZero();
            assertThat(evicted.cachedBytes()).isZero();
            assertThat(evicted.evictedBlobsTotal()).isEqualTo(1);

            probe.set(p -> {
                throw new IOException("probe failure");
            });
            WorkerCacheStats unknown = cache.statsSnapshot();
            assertThat(unknown.usableDiskBytes()).isEqualTo(-1L);
            assertThat(unknown.lowDiskSpace()).isFalse();
        }

        try (WorkerFileCache cache = WorkerFileCache.open(tempDir.resolve("zero"), true, 0L, p -> 10L, FIXED_CLOCK)) {
            WorkerCacheStats zeroBuffer = cache.statsSnapshot();
            assertThat(zeroBuffer.usableDiskBytes()).isEqualTo(10L);
            assertThat(zeroBuffer.lowDiskSpace()).isFalse();
        }
    }

    @Test
    void shouldNeitherEvictNorStoreNorInvalidateAfterClose() throws Exception {
        Path dir = tempDir.resolve("closed-cache");
        byte[] old = "older blob".getBytes(StandardCharsets.UTF_8);
        String oldSha = ClasspathPackager.sha256Hex(old);
        Path oldBlob = new BlobCacheDirectory(dir).put(oldSha, old);
        byte[] fresh = "fresh download".getBytes(StandardCharsets.UTF_8);
        String freshSha = ClasspathPackager.sha256Hex(fresh);
        Path workspace = Files.createDirectories(tempDir.resolve("closed-ws/out"));
        Files.writeString(workspace.resolve("report.txt"), "output");

        AtomicLong usable = new AtomicLong(1024 * GIB);
        WorkerFileCache cache = WorkerFileCache.open(dir, true, 10 * GIB, p -> usable.get(), FIXED_CLOCK);
        Path tmp = cache.newDownloadTempFile();
        Files.write(tmp, fresh);
        cache.close();

        // A partition thread that outlived close() must not touch a directory another worker may own by now.
        usable.set(0L);
        cache.evictAfterPartition();
        cache.ensureFreeSpaceForDownload(GIB);
        assertThat(oldBlob).as("no eviction after close").exists();

        assertThatThrownBy(() -> cache.promoteDownload(freshSha, tmp)).isInstanceOf(IllegalStateException.class);
        assertThat(store(cache).resolveBlobPath(freshSha)).doesNotExist();
        assertThat(tmp).as("the caller still owns its temporary file").exists();
        assertThatThrownBy(() -> cache.stageAndPinOutput(workspace.getParent(), "out", Map.of(), sha -> { }))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store(cache).listBlobs()).extracting(BlobEntry::sha256).containsExactly(oldSha);
        assertThatThrownBy(cache::newDownloadTempFile).isInstanceOf(IllegalStateException.class);

        Files.writeString(oldBlob, "corrupted");
        assertThatThrownBy(() -> cache.verifyOrInvalidate(oldSha)).isInstanceOf(IllegalStateException.class);
        assertThat(oldBlob).as("no invalidation after close").exists();
        Files.delete(tmp);

        try (WorkerFileCache owner = WorkerFileCache.open(dir, false, 0L, PLENTY, FIXED_CLOCK);
             WorkerFileCache.PinLease pins = owner.pinAll(List.of(oldSha))) {
            cache.evictAfterPartition();
            assertThat(oldBlob).as("the new owner's directory is left alone").exists();
            assertThat(pins).isNotNull();
        }
    }

    @Test
    void shouldStageForkedOutputAndPinItIntoTheActiveLease() throws Exception {
        Path workspace = Files.createDirectories(tempDir.resolve("workspace"));
        Path outputDir = Files.createDirectories(workspace.resolve("target/out"));
        Files.writeString(outputDir.resolve("unchanged.txt"), "same");
        Map<String, String> baseline = new HashMap<>(
                org.citrusframework.distribution.cache.WorkspacePackager.snapshotBaselineHashes(workspace, List.of("target/out")));
        Files.writeString(outputDir.resolve("report.txt"), "new output");

        try (WorkerFileCache cache = WorkerFileCache.open(tempDir.resolve("cache"), true, GIB, p -> 0L, FIXED_CLOCK);
             WorkerFileCache.PinLease lease = cache.pinAll(List.of())) {
            Set<String> registered = new HashSet<>();
            Optional<WorkspaceFileSet> staged = cache.stageAndPinOutput(workspace, "target/out", baseline, sha -> {
                registered.add(sha);
                lease.pin(sha);
            });
            assertThat(staged).isPresent();
            String sha = staged.get().artifact().sha256();
            assertThat(staged.get().relativePath()).isEqualTo("target/out");
            assertThat(registered).containsExactly(sha);
            assertThat(cache.isPinned(sha)).isTrue();
            assertThat(cache.statsSnapshot().blobCount()).isEqualTo(1);

            cache.evictAfterPartition();
            assertThat(store(cache).resolveBlobPath(sha)).as("pinned output survives eviction").exists();
            assertThat(cache.stageAndPinOutput(workspace, "missing", baseline, lease::pin)).isEmpty();

            lease.close();
            assertThat(cache.isPinned(sha)).isFalse();
            assertThatThrownBy(() -> lease.pin(sha)).isInstanceOf(IllegalStateException.class);
            cache.evictAfterPartition();
            assertThat(store(cache).resolveBlobPath(sha)).doesNotExist();
        }
    }

    @Test
    void shouldNotTrustCorruptExistingBlobWhenPromotingOrStagingTheSameDigest() throws Exception {
        byte[] seededContent = "seeded content".getBytes(StandardCharsets.UTF_8);
        String seededSha = ClasspathPackager.sha256Hex(seededContent);
        byte[] stampedContent = "stamped content".getBytes(StandardCharsets.UTF_8);
        String stampedSha = ClasspathPackager.sha256Hex(stampedContent);
        byte[] outputContent = "forked output".getBytes(StandardCharsets.UTF_8);
        String outputSha = ClasspathPackager.sha256Hex(outputContent);

        // Blobs present before open have no stamp; corrupt them in place keeping size and mtime.
        Path cacheDir = Files.createDirectories(tempDir.resolve("cache")).toRealPath();
        BlobCacheDirectory seeding = new BlobCacheDirectory(cacheDir);
        Path seededBlob = corruptInPlace(seeding.put(seededSha, seededContent));
        Path outputBlob = corruptInPlace(seeding.put(outputSha, outputContent));

        Path workspace = Files.createDirectories(tempDir.resolve("workspace"));
        Files.createDirectories(workspace.resolve("target"));
        Map<String, String> baseline = Map.of();
        Files.write(workspace.resolve("target/report.txt"), outputContent);

        try (WorkerFileCache cache = WorkerFileCache.open(cacheDir, true, 0L, PLENTY, FIXED_CLOCK);
             WorkerFileCache.PinLease lease = cache.pinAll(List.of())) {
            assertThat(promote(cache, seededContent)).isEqualTo(seededBlob);
            assertThat(seededBlob).as("promotion replaced the corrupt cached file").hasBinaryContent(seededContent);
            assertThat(cache.verifyOrInvalidate(seededSha)).isEqualTo(seededBlob);

            Optional<WorkspaceFileSet> staged = cache.stageAndPinOutput(workspace, "target/report.txt", baseline, lease::pin);
            assertThat(staged).isPresent();
            assertThat(staged.get().artifact().sha256()).isEqualTo(outputSha);
            assertThat(cache.verifyOrInvalidate(outputSha)).as("staging must not stamp a corrupt existing blob").isNull();
            assertThat(outputBlob).doesNotExist();

            // Promotion also replaces a previously stamped blob whose content changed on disk.
            Path stampedBlob = promote(cache, stampedContent);
            Files.write(stampedBlob, corrupt(stampedContent));
            Files.setLastModifiedTime(stampedBlob, FileTime.from(FIXED_CLOCK.instant().plusSeconds(5)));
            assertThat(promote(cache, stampedContent)).isEqualTo(stampedBlob);
            assertThat(stampedBlob).hasBinaryContent(stampedContent);
            assertThat(cache.verifyOrInvalidate(stampedSha)).isEqualTo(stampedBlob);

            // A still-matching blob stays valid.
            assertThat(promote(cache, stampedContent)).exists();
            assertThat(cache.verifyOrInvalidate(stampedSha)).isEqualTo(stampedBlob);
            assertThat(cache.statsSnapshot().blobCount()).isEqualTo(2);
        }
    }

    @Test
    void shouldForgetAVanishedBlobQuietlyAndRemoveACorruptedOneOnlyUnderTheEvictionLock() throws Exception {
        byte[] vanishing = "blob that vanishes while it is hashed".getBytes(StandardCharsets.UTF_8);
        byte[] vanishingLate = "blob that vanishes after it was hashed".getBytes(StandardCharsets.UTF_8);
        byte[] corrupted = "blob that is corrupted on disk".getBytes(StandardCharsets.UTF_8);
        BlobCacheDirectory seeding = new BlobCacheDirectory(tempDir);
        for (byte[] content : List.of(vanishing, vanishingLate, corrupted)) {
            seeding.put(ClasspathPackager.sha256Hex(content), content);
        }
        String vanishingSha = ClasspathPackager.sha256Hex(vanishing);
        String vanishingLateSha = ClasspathPackager.sha256Hex(vanishingLate);
        String corruptedSha = ClasspathPackager.sha256Hex(corrupted);
        Path corruptedBlob = corruptInPlace(seeding.resolveBlobPath(corruptedSha));

        AtomicReference<WorkerFileCache> owner = new AtomicReference<>();
        List<Boolean> deletesUnderLock = new ArrayList<>();
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, true, 0L, PLENTY, FIXED_CLOCK, FileChannel::tryLock,
                root -> new BlobCacheDirectory(root) {
                    @Override
                    public boolean isIntact(String sha256) throws IOException {
                        if (sha256.equals(vanishingSha)) {
                            Files.delete(resolveBlobPath(sha256));
                        }
                        boolean intact = super.isIntact(sha256);
                        if (sha256.equals(vanishingLateSha)) {
                            Files.delete(resolveBlobPath(sha256));
                            return false;
                        }
                        return intact;
                    }

                    @Override
                    public Path getVerifiedPath(String sha256) {
                        throw new AssertionError("getVerifiedPath deletes blobs outside the eviction lock");
                    }

                    @Override
                    public boolean delete(String sha256) throws IOException {
                        deletesUnderLock.add(owner.get().evictionLockHeldByCurrentThread());
                        return super.delete(sha256);
                    }
                })) {
            owner.set(cache);
            assertThat(cache.statsSnapshot().blobCount()).isEqualTo(3);

            assertThat(cache.verifyOrInvalidate(vanishingSha)).isNull();
            assertThat(cache.verifyOrInvalidate(vanishingLateSha)).isNull();
            assertThat(cache.corruptedBlobRemovals()).as("a vanished blob is not reported as corrupted").isZero();
            assertThat(cache.statsSnapshot().blobCount()).isEqualTo(1);

            assertThat(cache.verifyOrInvalidate(corruptedSha)).isNull();
            assertThat(corruptedBlob).doesNotExist();
            assertThat(cache.corruptedBlobRemovals()).isEqualTo(1);
            assertThat(cache.statsSnapshot().blobCount()).isZero();
            assertThat(cache.statsSnapshot().cachedBytes()).isZero();
            assertThat(deletesUnderLock).isNotEmpty().containsOnly(true);
        }
    }

    @Test
    void shouldWaitForAnInFlightEvictionBeforePinning() throws Exception {
        String sha = ClasspathPackager.sha256Hex("pinned after eviction".getBytes(StandardCharsets.UTF_8));
        String other = ClasspathPackager.sha256Hex("added to a lease after eviction".getBytes(StandardCharsets.UTF_8));
        CountDownLatch listing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, true, GIB, p -> 0L, FIXED_CLOCK);
             WorkerFileCache.PinLease lease = cache.pinAll(List.of())) {
            cache.blobIndex(new WorkerFileCache.BlobIndex() {
                @Override
                public List<BlobEntry> listBlobs() {
                    listing.countDown();
                    awaitQuietly(release);
                    return List.of();
                }

                @Override
                public boolean delete(String sha256) {
                    return false;
                }
            });
            CompletableFuture<Void> eviction = CompletableFuture.runAsync(cache::evictAfterPartition);
            assertThat(listing.await(10, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<WorkerFileCache.PinLease> pinAll = CompletableFuture.supplyAsync(() -> cache.pinAll(List.of(sha)));
            CompletableFuture<Void> pin = CompletableFuture.runAsync(() -> lease.pin(other));
            assertThatThrownBy(() -> pinAll.get(300, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            assertThat(pin).as("pinning waits for the eviction").isNotDone();
            assertThat(cache.isPinned(sha)).isFalse();

            release.countDown();
            eviction.get(10, TimeUnit.SECONDS);
            pin.get(10, TimeUnit.SECONDS);
            try (WorkerFileCache.PinLease pinned = pinAll.get(10, TimeUnit.SECONDS)) {
                assertThat(cache.isPinned(sha)).isTrue();
                assertThat(cache.isPinned(other)).isTrue();
                assertThat(pinned).isNotNull();
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void shouldNotTouchAWarmBlobAfterCloseCompleted() throws Exception {
        byte[] content = "warm blob touched on lookup".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(content);
        AtomicReference<Runnable> onTouch = new AtomicReference<>();
        WorkerFileCache cache = WorkerFileCache.open(tempDir, false, 0L, PLENTY, clockWithTouchHook(onTouch));
        AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>();
        AtomicBoolean closedBeforeTouch = new AtomicBoolean();
        try {
            Path blob = promote(cache, content);
            assertThat(cache.verifyOrInvalidate(sha)).as("stamped").isEqualTo(blob);

            onTouch.set(() -> {
                closing.set(CompletableFuture.runAsync(cache::close));
                try {
                    closing.get().get(300, TimeUnit.MILLISECONDS);
                    closedBeforeTouch.set(true);
                } catch (TimeoutException expected) {
                    // close waits for the warm lookup that is touching the blob
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            assertThat(cache.verifyOrInvalidate(sha)).isEqualTo(blob);
            assertThat(closedBeforeTouch).as("a warm lookup touched the blob after close").isFalse();
            closing.get().get(10, TimeUnit.SECONDS);
            assertThatThrownBy(() -> cache.verifyOrInvalidate(sha)).isInstanceOf(IllegalStateException.class);
        } finally {
            cache.close();
        }
    }

    private static Clock clockWithTouchHook(AtomicReference<Runnable> onTouch) {
        return new Clock() {
            @Override
            public ZoneOffset getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                Runnable hook = onTouch.getAndSet(null);
                if (hook != null) {
                    hook.run();
                }
                return FIXED_CLOCK.instant();
            }
        };
    }

    @Test
    void shouldRetainCacheOwnershipWhilePublishingANamedJar() throws Exception {
        byte[] content = "classpath jar contents".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(content);
        ArtifactRef artifact = new ArtifactRef("library.jar", sha, ArtifactKind.JAR, content.length);
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch finishPublishing = new CountDownLatch(1);
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, false, 0L, PLENTY, FIXED_CLOCK, FileChannel::tryLock,
                root -> new BlobCacheDirectory(root) {
                    @Override
                    public Path materializeForClasspath(ArtifactRef ref, Path verifiedBlob, Path workspaceDir)
                            throws IOException {
                        publishing.countDown();
                        awaitQuietly(finishPublishing);
                        return super.materializeForClasspath(ref, verifiedBlob, workspaceDir);
                    }
                })) {
            Path blob = promote(cache, content);
            CompletableFuture<Path> result = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> {
                try {
                    result.complete(cache.materializeForClasspath(artifact, blob, tempDir));
                } catch (IOException | RuntimeException e) {
                    result.completeExceptionally(e);
                }
            });
            try {
                assertThat(publishing.await(10, TimeUnit.SECONDS)).isTrue();
                CountDownLatch closeStarted = new CountDownLatch(1);
                CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> {
                    closeStarted.countDown();
                    cache.close();
                });
                assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> closing.get(300, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                assertThatThrownBy(() -> WorkerFileCache.open(tempDir, false, 0L, PLENTY, FIXED_CLOCK))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("--cache-dir");
                finishPublishing.countDown();
                assertThat(result.get(10, TimeUnit.SECONDS)).hasBinaryContent(content);
                closing.get(10, TimeUnit.SECONDS);
            } finally {
                finishPublishing.countDown();
            }
        }
    }

    @Test
    void shouldRefuseCacheDirectoriesOfOtherComponentsAndMarkOwnAndLegacyDirectories() throws Exception {
        for (String foreignOwner : List.of(BlobCacheDirectory.OWNER_COORDINATOR, BlobCacheDirectory.OWNER_LAUNCHER)) {
            Path foreign = Files.createDirectories(tempDir.resolve(foreignOwner + "-cache"));
            BlobCacheDirectory.recordOwnerIfAbsent(foreign, foreignOwner);
            Path canonical = foreign.toRealPath();

            assertThatThrownBy(() -> WorkerFileCache.open(foreign, true, 0L, PLENTY, FIXED_CLOCK))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(canonical.toString())
                    .hasMessageContaining(foreignOwner)
                    .hasMessageContaining("--cache-dir");
            assertThat(WorkerFileCache.ACTIVE_JVM_LOCKS).doesNotContain(canonical);
            assertThat(canonical.resolve(".lock")).as("nothing written to a foreign directory").doesNotExist();
            assertThat(canonical.resolve("tmp")).doesNotExist();
            assertThat(BlobCacheDirectory.readOwner(canonical)).contains(foreignOwner);
        }

        // A worker cache from before owner markers: adopted and marked, its blobs kept.
        Path legacy = tempDir.resolve("legacy-cache");
        byte[] content = "legacy blob".getBytes(StandardCharsets.UTF_8);
        String sha = ClasspathPackager.sha256Hex(content);
        Path blob = new BlobCacheDirectory(legacy).put(sha, content).toRealPath();
        try (WorkerFileCache cache = WorkerFileCache.open(legacy, true, 0L, PLENTY, FIXED_CLOCK)) {
            assertThat(BlobCacheDirectory.readOwner(cache.rootDir())).contains(BlobCacheDirectory.OWNER_WORKER);
            assertThat(cache.verifyOrInvalidate(sha)).isEqualTo(blob);
        }
        try (WorkerFileCache cache = WorkerFileCache.open(legacy, true, 0L, PLENTY, FIXED_CLOCK)) {
            assertThat(cache.verifyOrInvalidate(sha)).as("its own marker is accepted").isEqualTo(blob);
        }
    }

    @Test
    void shouldLogRepeatedProbeFailuresAtDebugLevel() {
        String probeFailure = "simulated failure for log level test";
        AtomicBoolean failing = new AtomicBoolean(true);
        List<Level> levels = new ArrayList<>();
        String loggerName = WorkerFileCache.class.getName();
        LoggerContext context = (LoggerContext) LogManager.getContext(WorkerFileCache.class.getClassLoader(), false);
        Configuration configuration = context.getConfiguration();
        LoggerConfig previous = configuration.getLoggers().get(loggerName);
        LoggerConfig logger = new LoggerConfig(loggerName, Level.DEBUG, false);
        AbstractAppender capture = new AbstractAppender("worker-cache-probes", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                if (event.getMessage().getFormattedMessage().contains(probeFailure)) {
                    levels.add(event.getLevel());
                }
            }
        };
        capture.start();
        logger.addAppender(capture, Level.DEBUG, null);
        configuration.removeLogger(loggerName);
        configuration.addLogger(loggerName, logger);
        context.updateLoggers();
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, true, GIB, path -> {
            if (failing.get()) {
                throw new IOException(probeFailure);
            }
            return PLENTY.usableBytes(path);
        }, FIXED_CLOCK)) {
            assertThat(cache.statsSnapshot().usableDiskBytes()).isEqualTo(-1);
            failing.set(false);
            assertThat(cache.statsSnapshot().usableDiskBytes()).isPositive();
            failing.set(true);
            cache.evictAfterPartition();
        } finally {
            configuration.removeLogger(loggerName);
            if (previous != null) {
                configuration.addLogger(loggerName, previous);
            }
            context.updateLoggers();
            capture.stop();
        }
        assertThat(levels).containsExactly(Level.WARN, Level.DEBUG, Level.WARN);
    }

    @Test
    void shouldWarnOncePerProbeFailureEpisodeAndTellWhyLowDiskSpaceRemains() throws Exception {
        AtomicReference<WorkerFileCache.DiskSpaceProbe> probe = new AtomicReference<>(PLENTY);
        WorkerFileCache.DiskSpaceProbe failing = p -> {
            throw new IOException("simulated probe failure");
        };
        try (WorkerFileCache cache = WorkerFileCache.open(tempDir, true, GIB, p -> probe.get().usableBytes(p), FIXED_CLOCK)) {
            probe.set(failing);
            cache.evictAfterPartition();
            cache.ensureFreeSpaceForDownload(1024);
            cache.evictAfterPartition();
            assertThat(cache.probeFailureWarningCount()).as("one warning per failure episode").isEqualTo(1);

            probe.set(PLENTY);
            cache.evictAfterPartition();
            probe.set(failing);
            cache.evictAfterPartition();
            assertThat(cache.probeFailureWarningCount()).as("a successful probe ends the episode").isEqualTo(2);

            // Only pinned blobs left.
            byte[] pinnedContent = "pinned under pressure".getBytes(StandardCharsets.UTF_8);
            promote(cache, pinnedContent);
            try (WorkerFileCache.PinLease lease = cache.pinAll(List.of(ClasspathPackager.sha256Hex(pinnedContent)))) {
                probe.set(p -> 0L);
                cache.evictAfterPartition();
                assertThat(cache.lowDiskWarningReason()).isEqualTo(WorkerFileCache.LowDiskReason.NO_UNPINNED_BLOBS);
                assertThat(lease).isNotNull();
            }

            // The same low-disk episode now has unpinned blobs whose deletes fail.
            String undeletable = "f".repeat(64);
            cache.blobIndex(new WorkerFileCache.BlobIndex() {
                @Override
                public List<BlobEntry> listBlobs() {
                    return List.of(new BlobEntry(undeletable, tempDir.resolve("undeletable"), 1L, 1L));
                }

                @Override
                public boolean delete(String sha256) throws IOException {
                    throw new IOException("simulated delete failure");
                }
            });
            probe.set(p -> 0L);
            cache.evictAfterPartition();
            assertThat(cache.lowDiskWarningReason()).isEqualTo(WorkerFileCache.LowDiskReason.DELETES_FAILED);
            assertThat(cache.lowDiskWarningCount()).isEqualTo(2);
            assertThat(cache.evictionDeleteFailureCount()).isEqualTo(1);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static BlobCacheDirectory store(WorkerFileCache cache) {
        return new BlobCacheDirectory(cache.rootDir());
    }

    private static Path corruptInPlace(Path blob) throws IOException {
        FileTime mtime = Files.getLastModifiedTime(blob);
        Files.write(blob, corrupt(Files.readAllBytes(blob)));
        Files.setLastModifiedTime(blob, mtime);
        return blob;
    }

    private static Path promote(WorkerFileCache cache, byte[] bytes) throws IOException {
        Path tmp = cache.newDownloadTempFile();
        try {
            Files.write(tmp, bytes);
            return cache.promoteDownload(ClasspathPackager.sha256Hex(bytes), tmp);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /** Usable space equals the 1 GiB buffer plus {@code unit} per already deleted path, so the deficit is the download size minus freed blobs. */
    private static WorkerFileCache.DiskSpaceProbe missingCountProbe(List<Path> paths, long unit) {
        return p -> GIB + unit * paths.stream().filter(path -> !Files.exists(path)).count();
    }

    private static List<Boolean> existing(List<Path> paths) {
        return paths.stream().map(Files::exists).toList();
    }

    private static byte[] corrupt(byte[] content) {
        byte[] copy = content.clone();
        copy[0] = (byte) (copy[0] ^ 0x5A);
        return copy;
    }

    private static byte[] sized(String prefix, int size) {
        byte[] bytes = new byte[size];
        byte[] head = prefix.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(head, 0, bytes, 0, head.length);
        return bytes;
    }
}
