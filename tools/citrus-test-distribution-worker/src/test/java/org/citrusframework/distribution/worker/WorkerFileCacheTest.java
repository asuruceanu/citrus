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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
            assertThat(locked.get(60, TimeUnit.SECONDS)).as("child JVM acquired the cache lock").isTrue();

            Path canonical = dir.toRealPath();
            assertThatThrownBy(() -> WorkerFileCache.open(dir, true, 0L, PLENTY, FIXED_CLOCK))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(canonical.toString())
                    .hasMessageContaining("--cache-dir");
            assertThat(WorkerFileCache.ACTIVE_JVM_LOCKS).doesNotContain(canonical);
            assertThat(canonical.resolve("tmp").resolve("dl-orphan.tmp")).exists();
        } finally {
            assertThat(child.destroyForcibly().waitFor(30, TimeUnit.SECONDS)).isTrue();
        }

        try (WorkerFileCache cache = WorkerFileCache.open(dir, true, 0L, PLENTY, FIXED_CLOCK)) {
            assertThat(cache.rootDir().resolve("tmp").resolve("dl-orphan.tmp")).doesNotExist();
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
        Path stale = Files.createDirectories(tempDir.resolve("tmp")).resolve("dl-stale.tmp");
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
            namedJar = cache.blobCache().materializeForClasspath(
                    new ArtifactRef("lib-1.0.jar", jarSha, ArtifactKind.JAR, jarBytes.length), tempDir.resolve("ws"));
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
            Path pinnedJar = cache.blobCache().materializeForClasspath(
                    new ArtifactRef("lib-1.0.jar", jarSha, ArtifactKind.JAR, jarBytes.length), tempDir.resolve("ws"));
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
                probe.set(p -> Files.exists(secondBlob) ? GIB : 3 * GIB);
                cache.evictAfterPartition();
                assertThat(firstBlob).as("delete failed, blob survives").exists();
                assertThat(secondBlob).as("eviction continued with the next candidate").doesNotExist();
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
            assertThat(cache.blobCache().resolveBlobPath(sha)).as("pinned output survives eviction").exists();
            assertThat(cache.stageAndPinOutput(workspace, "missing", baseline, lease::pin)).isEmpty();

            lease.close();
            assertThat(cache.isPinned(sha)).isFalse();
            assertThatThrownBy(() -> lease.pin(sha)).isInstanceOf(IllegalStateException.class);
            cache.evictAfterPartition();
            assertThat(cache.blobCache().resolveBlobPath(sha)).doesNotExist();
        }
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
