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
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.BlobCacheDirectory.BlobEntry;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.model.WorkerCacheStats;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The worker's exclusive local file cache: a {@link BlobCacheDirectory} owned by exactly one worker through a
 * {@code .lock} file, with active-partition pinning and least recently used eviction that keeps a free space buffer
 * on the cache volume.
 * <ul>
 *     <li>{@link #open} locks the directory (in this JVM and across processes), sweeps leftovers of interrupted writes
 *     and, when cleanup is enabled, evicts down to the buffer. The lock file is never deleted, so concurrent openers
 *     always contend on the same inode; the operating system releases the lock if the worker dies.</li>
 *     <li>Blobs verified once are stamped with their size, modification time and file key, so later lookups re-hash
 *     only when the file changed on disk.</li>
 *     <li>Promotion of downloads, staging of forked partition outputs and eviction run under one lock, so a newly
 *     stored blob is stamped and pinned before eviction can see it.</li>
 *     <li>Removing a digest always removes its named jar directory as well, since a hard-linked named jar keeps the
 *     blob's disk space in use.</li>
 * </ul>
 */
final class WorkerFileCache implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerFileCache.class);

    /** Canonical cache directories locked by a {@code WorkerFileCache} in this JVM. */
    static final Set<Path> ACTIVE_JVM_LOCKS = ConcurrentHashMap.newKeySet();

    /** Re-probe free space at the latest after this many deletions. */
    static final int DELETIONS_PER_PROBE = 64;

    private static final String CACHE_DIR_GUIDANCE = "configure a distinct directory per worker via --cache-dir, "
            + "-Dcitrus.test.distribution.worker.cacheDir, or CITRUS_DIST_WORKER_CACHE_DIR "
            + "(alias TEST_DISTRIBUTION_AGENT_CACHE_DIR)";

    /** Reports the usable bytes of the file system holding a directory. */
    @FunctionalInterface
    interface DiskSpaceProbe {
        long usableBytes(Path dir) throws IOException;
    }

    /** Pins digests against eviction until closed. Closing is idempotent. */
    interface PinLease extends AutoCloseable {

        /**
         * Adds {@code sha256} to this lease; pinning a digest the lease already holds is a no-op.
         *
         * @throws IllegalStateException if the lease is closed
         */
        void pin(String sha256);

        @Override
        void close();
    }

    /** Acquires the file lock; a test seam for lock failures. */
    @FunctionalInterface
    interface LockAcquirer {
        FileLock tryLock(FileChannel channel) throws IOException;
    }

    /** The blob store operations eviction uses; a test seam for large synthetic stores. */
    interface BlobIndex {
        List<BlobEntry> listBlobs() throws IOException;

        boolean delete(String sha256) throws IOException;
    }

    private record VerifiedBlobStamp(long sizeBytes, FileTime lastModifiedTime, Object fileKey) {

        static VerifiedBlobStamp of(BasicFileAttributes attrs) {
            return new VerifiedBlobStamp(attrs.size(), attrs.lastModifiedTime(), attrs.fileKey());
        }

        boolean matches(BasicFileAttributes attrs) {
            return attrs.isRegularFile() && sizeBytes == attrs.size()
                    && lastModifiedTime.equals(attrs.lastModifiedTime()) && Objects.equals(fileKey, attrs.fileKey());
        }
    }

    private final Path canonicalDir;
    private final boolean cacheCleanup;
    private final long freeSpaceBufferBytes;
    private final DiskSpaceProbe spaceProbe;
    private final Clock clock;
    private final BlobCacheDirectory blobCache;
    private final FileChannel lockChannel;
    private final FileLock fileLock;
    private final AtomicBoolean closed = new AtomicBoolean();

    private final ReentrantLock evictionLock = new ReentrantLock();
    private final ConcurrentHashMap<String, AtomicInteger> pinCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, VerifiedBlobStamp> verifiedStamps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> accessSequence = new ConcurrentHashMap<>();
    private final AtomicLong sequenceGenerator = new AtomicLong();

    // Blob accounting, mutated under evictionLock only.
    private final Map<String, Long> knownBlobSizes = new ConcurrentHashMap<>();
    private final AtomicInteger blobCount = new AtomicInteger();
    private final AtomicLong cachedBytes = new AtomicLong();
    private final AtomicLong evictedBlobsTotal = new AtomicLong();

    // Low disk warning state, mutated under evictionLock only.
    private volatile boolean lowDiskWarningActive;
    private volatile int lowDiskWarningCount;

    private volatile BlobIndex blobIndex;

    private WorkerFileCache(Path canonicalDir, boolean cacheCleanup, long freeSpaceBufferBytes, DiskSpaceProbe spaceProbe,
                            Clock clock, FileChannel lockChannel, FileLock fileLock) {
        this.canonicalDir = canonicalDir;
        this.cacheCleanup = cacheCleanup;
        this.freeSpaceBufferBytes = freeSpaceBufferBytes;
        this.spaceProbe = spaceProbe;
        this.clock = clock;
        this.lockChannel = lockChannel;
        this.fileLock = fileLock;
        this.blobCache = new BlobCacheDirectory(canonicalDir);
        BlobCacheDirectory store = blobCache;
        this.blobIndex = new BlobIndex() {
            @Override
            public List<BlobEntry> listBlobs() throws IOException {
                return store.listBlobs();
            }

            @Override
            public boolean delete(String sha256) throws IOException {
                return store.delete(sha256);
            }
        };
    }

    static WorkerFileCache open(Path cacheDir, boolean cacheCleanup, long freeSpaceBufferBytes) {
        return open(cacheDir, cacheCleanup, freeSpaceBufferBytes, null, Clock.systemUTC());
    }

    /**
     * @param spaceProbe free space probe, or {@code null} for the cache volume's {@link FileStore}
     */
    static WorkerFileCache open(Path cacheDir, boolean cacheCleanup, long freeSpaceBufferBytes,
                                DiskSpaceProbe spaceProbe, Clock clock) {
        return open(cacheDir, cacheCleanup, freeSpaceBufferBytes, spaceProbe, clock, FileChannel::tryLock);
    }

    static WorkerFileCache open(Path cacheDir, boolean cacheCleanup, long freeSpaceBufferBytes,
                                DiskSpaceProbe spaceProbe, Clock clock, LockAcquirer lockAcquirer) {
        if (freeSpaceBufferBytes < 0) {
            throw new IllegalArgumentException("Worker cache free space buffer must not be negative: " + freeSpaceBufferBytes);
        }
        Path canonicalDir;
        try {
            canonicalDir = Files.createDirectories(cacheDir).toRealPath();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create worker local file cache directory '" + cacheDir + "': " + e, e);
        }
        if (!ACTIVE_JVM_LOCKS.add(canonicalDir)) {
            throw alreadyLocked(canonicalDir);
        }

        Path lockFile = canonicalDir.resolve(".lock");
        FileChannel channel = null;
        FileLock lock = null;
        try {
            channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try {
                lock = lockAcquirer.tryLock(channel);
            } catch (OverlappingFileLockException e) {
                throw alreadyLocked(canonicalDir);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot lock worker local file cache '" + lockFile
                        + "'. The cache directory must be on a local file system that supports file locking; "
                        + CACHE_DIR_GUIDANCE + ": " + e, e);
            }
            if (lock == null) {
                throw alreadyLocked(canonicalDir);
            }

            DiskSpaceProbe probe = spaceProbe != null ? spaceProbe : fileStoreProbe(canonicalDir);
            WorkerFileCache cache = new WorkerFileCache(canonicalDir, cacheCleanup, freeSpaceBufferBytes, probe,
                    clock, channel, lock);
            cache.initialize();
            return cache;
        } catch (Throwable t) {
            try {
                if (lock != null) {
                    lock.release();
                }
            } catch (IOException | RuntimeException e) {
                t.addSuppressed(e);
            } finally {
                try {
                    if (channel != null) {
                        channel.close();
                    }
                } catch (IOException | RuntimeException e) {
                    t.addSuppressed(e);
                } finally {
                    ACTIVE_JVM_LOCKS.remove(canonicalDir);
                }
            }
            if (t instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (t instanceof Error error) {
                throw error;
            }
            throw new UncheckedIOException("Cannot open worker local file cache '" + canonicalDir + "': " + t.getMessage(),
                    t instanceof IOException io ? io : new IOException(t));
        }
    }

    private static IllegalStateException alreadyLocked(Path canonicalDir) {
        return new IllegalStateException("Worker local file cache directory '" + canonicalDir
                + "' is already locked by another worker. Sharing a local file cache across multiple workers on the "
                + "same host is not supported; " + CACHE_DIR_GUIDANCE + ", or run each worker in its own container.");
    }

    private static DiskSpaceProbe fileStoreProbe(Path canonicalDir) throws IOException {
        FileStore store = Files.getFileStore(canonicalDir);
        return dir -> store.getUsableSpace();
    }

    private void initialize() throws IOException {
        Files.createDirectories(canonicalDir.resolve("blobs"));
        Files.createDirectories(canonicalDir.resolve("tmp"));
        int sweptTemp = blobCache.sweepTempFiles();
        int sweptStaging = blobCache.sweepClasspathStaging();
        if (sweptTemp > 0 || sweptStaging > 0) {
            LOG.info("Removed {} temporary and {} staging leftovers from worker cache {}", sweptTemp, sweptStaging,
                    canonicalDir);
        }
        evictionLock.lock();
        try {
            for (BlobEntry entry : blobCache.listBlobs()) {
                registerBlob(entry.sha256(), entry.sizeBytes());
            }
            if (cacheCleanup) {
                evictUnderLock(0L);
            }
        } finally {
            evictionLock.unlock();
        }
    }

    /** The canonical cache directory. */
    Path rootDir() {
        return canonicalDir;
    }

    BlobCacheDirectory blobCache() {
        return blobCache;
    }

    /** Pins {@code sha256Digests} (duplicates count once) until the returned lease is closed. */
    PinLease pinAll(Collection<String> sha256Digests) {
        Lease lease = new Lease();
        for (String sha256 : sha256Digests) {
            lease.pin(sha256);
        }
        return lease;
    }

    boolean isPinned(String sha256) {
        return pinCounts.containsKey(sha256);
    }

    /**
     * Returns the path of a valid blob for {@code sha256} and records the access, or {@code null} when the blob is
     * absent or corrupted. A blob whose size, modification time or file key no longer match its verification stamp
     * is re-hashed; a corrupted one is removed together with its named jars.
     */
    Path verifyOrInvalidate(String sha256) throws IOException {
        Path blob = blobCache.resolveBlobPath(sha256);
        BasicFileAttributes attrs = readAttributes(blob);
        if (attrs == null) {
            forgetMissing(sha256);
            return null;
        }
        VerifiedBlobStamp stamp = verifiedStamps.get(sha256);
        if (stamp != null && stamp.matches(attrs)) {
            recordAccess(sha256, blob);
            return blob;
        }

        verifiedStamps.remove(sha256);
        Path verified;
        try {
            verified = blobCache.getVerifiedPath(sha256);
        } catch (NoSuchFileException e) {
            verified = null;
        }
        evictionLock.lock();
        try {
            if (verified == null) {
                LOG.warn("Removing corrupted blob {} from worker cache {}", sha256, canonicalDir);
                blobIndex.delete(sha256);
                forgetBlob(sha256);
                return null;
            }
            BasicFileAttributes current = readAttributes(verified);
            if (current == null) {
                forgetBlob(sha256);
                return null;
            }
            registerBlob(sha256, current.size());
            recordAccess(sha256, verified);
            return verified;
        } finally {
            evictionLock.unlock();
        }
    }

    /** A new empty file under {@code tmp/} for a download; the caller deletes it when done. */
    Path newDownloadTempFile() throws IOException {
        return blobCache.newTempFile("dl-");
    }

    /**
     * Verifies {@code tempFile} against {@code expectedSha256} and moves it into the blob store, stamping and touching
     * the stored blob before eviction can see it.
     *
     * @throws IllegalArgumentException on a digest mismatch
     */
    Path promoteDownload(String expectedSha256, Path tempFile) throws IOException {
        evictionLock.lock();
        try {
            boolean existed = Files.exists(blobCache.resolveBlobPath(expectedSha256), LinkOption.NOFOLLOW_LINKS);
            Path stored = blobCache.putFromFile(expectedSha256, tempFile);
            registerStored(expectedSha256, stored, !existed);
            return stored;
        } finally {
            evictionLock.unlock();
        }
    }

    /**
     * Packages the new or changed files under the declared output {@code rawOutputPath} into the blob store (see
     * {@link WorkspacePackager#packageModifiedOutputToCache}) and hands the stored digest to {@code pinRegistrar}
     * (typically {@link PinLease#pin}) before eviction can see the blob.
     *
     * @return the packaged output, or empty when nothing new or changed exists
     */
    Optional<WorkspaceFileSet> stageAndPinOutput(Path workspaceDir, String rawOutputPath, Map<String, String> baselineHashes,
                                                 Consumer<String> pinRegistrar) throws IOException {
        evictionLock.lock();
        try {
            Optional<WorkspaceFileSet> output = WorkspacePackager.packageModifiedOutputToCache(blobCache, workspaceDir,
                    rawOutputPath, baselineHashes);
            if (output.isPresent()) {
                String sha256 = output.get().artifact().sha256();
                pinRegistrar.accept(sha256);
                // The packager cannot tell whether it stored the blob or found it already present, so an existing
                // blob is never trusted here without a matching stamp.
                registerStored(sha256, blobCache.resolveBlobPath(sha256), false);
            }
            return output;
        } finally {
            evictionLock.unlock();
        }
    }

    /**
     * Evicts least recently used unpinned blobs so that {@code expectedBytes} fit above the free space buffer. Never
     * throws: a remaining shortage is logged once per episode, and a real lack of space fails the write itself.
     * Does nothing when cache cleanup is disabled.
     */
    void ensureFreeSpaceForDownload(long expectedBytes) {
        evictIfNeeded(Math.max(0L, expectedBytes));
    }

    /** Evicts down to the free space buffer once a partition released its pins. Does nothing without cache cleanup. */
    void evictAfterPartition() {
        evictIfNeeded(0L);
    }

    private void evictIfNeeded(long additionalBytes) {
        if (!cacheCleanup) {
            return;
        }
        evictionLock.lock();
        try {
            evictUnderLock(additionalBytes);
        } catch (IOException | RuntimeException e) {
            LOG.warn("Worker cache eviction in {} failed: {}", canonicalDir, e.toString());
        } finally {
            evictionLock.unlock();
        }
    }

    /**
     * Deletes unpinned blobs ordered by (modification time, session access order, digest) until the deficit is
     * covered. Free space is re-probed only once the bytes deleted since the last probe cover the last known deficit,
     * or after {@link #DELETIONS_PER_PROBE} deletions, plus one final confirmation.
     */
    private void evictUnderLock(long additionalBytes) throws IOException {
        Long usable = probeUsable();
        if (usable == null) {
            return;
        }
        long deficit = deficit(freeSpaceBufferBytes, usable, additionalBytes);
        if (deficit <= 0) {
            lowDiskWarningActive = false;
            return;
        }

        List<BlobEntry> candidates = new ArrayList<>();
        for (BlobEntry entry : blobIndex.listBlobs()) {
            if (!isPinned(entry.sha256())) {
                candidates.add(entry);
            }
        }
        candidates.sort(Comparator.comparingLong(BlobEntry::lastModifiedMillis)
                .thenComparingLong(e -> accessSequence.getOrDefault(e.sha256(), Long.MIN_VALUE))
                .thenComparing(BlobEntry::sha256));

        long deletedSinceProbe = 0;
        int deletionsSinceProbe = 0;
        boolean confirmed = false;
        for (BlobEntry entry : candidates) {
            String sha256 = entry.sha256();
            if (isPinned(sha256)) {
                continue;
            }
            verifiedStamps.remove(sha256);
            boolean existed;
            try {
                existed = blobIndex.delete(sha256);
            } catch (IOException | RuntimeException e) {
                LOG.warn("Could not evict blob {} from worker cache {}: {}", sha256, canonicalDir, e.toString());
                continue;
            }
            forgetBlob(sha256);
            if (existed) {
                evictedBlobsTotal.incrementAndGet();
            }
            deletedSinceProbe = saturatedAdd(deletedSinceProbe, entry.sizeBytes());
            deletionsSinceProbe++;
            if (deletedSinceProbe >= deficit || deletionsSinceProbe >= DELETIONS_PER_PROBE) {
                usable = probeUsable();
                if (usable == null) {
                    return;
                }
                deficit = deficit(freeSpaceBufferBytes, usable, additionalBytes);
                deletedSinceProbe = 0;
                deletionsSinceProbe = 0;
                confirmed = true;
                if (deficit <= 0) {
                    break;
                }
            } else {
                confirmed = false;
            }
        }
        if (!confirmed) {
            usable = probeUsable();
            if (usable == null) {
                return;
            }
            deficit = deficit(freeSpaceBufferBytes, usable, additionalBytes);
        }

        if (deficit <= 0) {
            lowDiskWarningActive = false;
        } else if (!lowDiskWarningActive) {
            lowDiskWarningActive = true;
            lowDiskWarningCount++;
            LOG.warn("Worker cache {} has {} usable bytes, {} short of the {} byte free space buffer plus {} required bytes, "
                            + "and no unpinned blobs are left to evict",
                    canonicalDir, usable, deficit, freeSpaceBufferBytes, additionalBytes);
        }
    }

    private Long probeUsable() {
        try {
            return spaceProbe.usableBytes(canonicalDir);
        } catch (IOException e) {
            LOG.warn("Cannot determine usable disk space of worker cache {}: {}", canonicalDir, e.toString());
            return null;
        }
    }

    /** {@code freeSpaceBufferBytes + additionalBytes - usableBytes} with saturating arithmetic. */
    static long deficit(long freeSpaceBufferBytes, long usableBytes, long additionalBytes) {
        return saturatedSubtract(saturatedAdd(freeSpaceBufferBytes, additionalBytes), usableBytes);
    }

    private static long saturatedAdd(long a, long b) {
        long r = a + b;
        if (((a ^ r) & (b ^ r)) < 0) {
            return a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        return r;
    }

    private static long saturatedSubtract(long a, long b) {
        long r = a - b;
        if (((a ^ b) & (a ^ r)) < 0) {
            return a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        return r;
    }

    /**
     * Cache statistics for heartbeats: blob accounting is maintained incrementally, only usable disk space is probed
     * ({@code -1} when the probe fails).
     */
    WorkerCacheStats statsSnapshot() {
        long usable;
        try {
            usable = spaceProbe.usableBytes(canonicalDir);
        } catch (IOException e) {
            usable = -1L;
        }
        boolean lowDiskSpace = usable >= 0 && freeSpaceBufferBytes > 0 && usable < freeSpaceBufferBytes;
        return new WorkerCacheStats(cacheCleanup, freeSpaceBufferBytes, blobCount.get(), cachedBytes.get(), usable,
                evictedBlobsTotal.get(), lowDiskSpace);
    }

    boolean lowDiskWarningActive() {
        return lowDiskWarningActive;
    }

    int lowDiskWarningCount() {
        return lowDiskWarningCount;
    }

    void blobIndex(BlobIndex blobIndex) {
        this.blobIndex = Objects.requireNonNull(blobIndex);
    }

    /** Releases the lock; the {@code .lock} file stays. Idempotent. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            fileLock.release();
        } catch (IOException e) {
            LOG.warn("Could not release worker cache lock {}: {}", canonicalDir, e.toString());
        } finally {
            try {
                lockChannel.close();
            } catch (IOException e) {
                LOG.warn("Could not close worker cache lock file {}: {}", canonicalDir, e.toString());
            } finally {
                ACTIVE_JVM_LOCKS.remove(canonicalDir);
            }
        }
    }

    /**
     * Accounts for and touches a blob after a store operation. Its content is trusted only when {@code freshlyStored}
     * (verified while moved into place) or when its existing stamp still matches; the store operations return an
     * existing target without re-hashing it, so any other blob loses its stamp and is re-hashed on next lookup.
     */
    private void registerStored(String sha256, Path stored, boolean freshlyStored) throws IOException {
        BasicFileAttributes attrs = readAttributes(stored);
        if (attrs == null) {
            throw new NoSuchFileException(stored.toString(), null, "blob vanished right after it was stored");
        }
        registerBlob(sha256, attrs.size());
        VerifiedBlobStamp stamp = verifiedStamps.get(sha256);
        boolean trusted = freshlyStored || (stamp != null && stamp.matches(attrs));
        recordAccess(sha256, stored, trusted);
    }

    private void registerBlob(String sha256, long sizeBytes) {
        if (knownBlobSizes.putIfAbsent(sha256, sizeBytes) == null) {
            blobCount.incrementAndGet();
            cachedBytes.addAndGet(sizeBytes);
        }
    }

    private void forgetBlob(String sha256) {
        verifiedStamps.remove(sha256);
        accessSequence.remove(sha256);
        Long size = knownBlobSizes.remove(sha256);
        if (size != null) {
            blobCount.decrementAndGet();
            cachedBytes.addAndGet(-size);
        }
    }

    private void forgetMissing(String sha256) {
        if (!knownBlobSizes.containsKey(sha256) && !verifiedStamps.containsKey(sha256)) {
            return;
        }
        evictionLock.lock();
        try {
            if (readAttributes(blobCache.resolveBlobPath(sha256)) == null) {
                forgetBlob(sha256);
            }
        } finally {
            evictionLock.unlock();
        }
    }

    /** Records an access: session order, modification time (best effort) and a fresh verification stamp. */
    private void recordAccess(String sha256, Path blob) {
        recordAccess(sha256, blob, true);
    }

    /**
     * Records an access; refreshes the verification stamp only for {@code verified} content and drops it otherwise.
     */
    private void recordAccess(String sha256, Path blob, boolean verified) {
        if (!verified) {
            verifiedStamps.remove(sha256);
        }
        accessSequence.put(sha256, sequenceGenerator.incrementAndGet());
        try {
            Files.setLastModifiedTime(blob, FileTime.from(clock.instant()));
        } catch (IOException e) {
            LOG.debug("Could not touch cached blob {}: {}", blob, e.toString());
        }
        BasicFileAttributes attrs = verified ? readAttributes(blob) : null;
        if (attrs != null && attrs.isRegularFile()) {
            verifiedStamps.put(sha256, VerifiedBlobStamp.of(attrs));
        } else {
            verifiedStamps.remove(sha256);
        }
    }

    private static BasicFileAttributes readAttributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException e) {
            return null;
        }
    }

    private final class Lease implements PinLease {

        private final ConcurrentHashMap.KeySetView<String, Boolean> digests = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean leaseClosed = new AtomicBoolean();

        @Override
        public synchronized void pin(String sha256) {
            BlobCacheDirectory.validateDigest(sha256);
            if (leaseClosed.get()) {
                throw new IllegalStateException("Pin lease is closed");
            }
            if (digests.add(sha256)) {
                pinCounts.compute(sha256, (key, count) -> {
                    AtomicInteger next = count != null ? count : new AtomicInteger();
                    next.incrementAndGet();
                    return next;
                });
            }
        }

        @Override
        public synchronized void close() {
            if (!leaseClosed.compareAndSet(false, true)) {
                return;
            }
            for (String sha256 : digests) {
                pinCounts.computeIfPresent(sha256, (key, count) -> count.decrementAndGet() <= 0 ? null : count);
            }
        }
    }
}
