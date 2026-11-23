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
import java.io.OutputStream;
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
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
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
import java.util.function.Function;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.BlobCacheDirectory.BlobEntry;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.cache.WorkspacePackager.MaterializedEntry;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.WorkerCacheStats;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The worker's exclusive local file cache: a {@link BlobCacheDirectory} owned by exactly one worker through a
 * {@code .lock} file, with active-partition pinning and least recently used eviction that keeps a free space buffer
 * on the cache volume.
 * <ul>
 *     <li>{@link #open} refuses a directory whose {@link BlobCacheDirectory#OWNER_MARKER_FILE} names another component
 *     (the coordinator's or the launcher's cache), locks the directory (in this JVM and across processes), records the
 *     worker as owner when no marker exists yet, sweeps leftovers of interrupted writes and, when cleanup is enabled,
 *     evicts down to the buffer. The lock file is never deleted, so concurrent openers always contend on the same
 *     inode; the operating system releases the lock if the worker dies.</li>
 *     <li>Blobs verified once are stamped with their size, modification time and file key, so later lookups re-hash
 *     only when the file changed on disk. Callers materialize the path {@link #verifyOrInvalidate} returned without
 *     hashing it again.</li>
 *     <li>Promotion of downloads, publication of named JARs, staging of forked partition outputs, pinning, touching
 *     blobs on lookup, deleting corrupted blobs and eviction run under one lock, so a newly stored blob is stamped
 *     and pinned before eviction can see it, and no blob is pinned while an eviction is deciding what to delete.</li>
 *     <li>Removing a digest always removes its named jar directory as well, since a hard-linked named jar keeps the
 *     blob's disk space in use.</li>
 *     <li>Once closed, the cache never mutates its directory again (another worker may own it by then): eviction
 *     is a no-op, and storing, invalidating or creating temporary files throws {@link IllegalStateException}.
 *     {@link #close()} waits for an in-flight eviction or store operation to finish.</li>
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

    private record VerifiedBlobStamp(long sizeBytes, FileTime lastModifiedTime, Object fileKey, FileTime creationTime) {

        static VerifiedBlobStamp of(BasicFileAttributes attrs) {
            return new VerifiedBlobStamp(attrs.size(), attrs.lastModifiedTime(), attrs.fileKey(), attrs.creationTime());
        }

        boolean matches(BasicFileAttributes attrs) {
            return attrs.isRegularFile() && sameSnapshot(attrs);
        }

        boolean sameSnapshot(BasicFileAttributes attrs) {
            return sameFile(attrs) && lastModifiedTime.equals(attrs.lastModifiedTime());
        }

        boolean sameFile(BasicFileAttributes attrs) {
            return sizeBytes == attrs.size() && Objects.equals(fileKey, attrs.fileKey())
                    && (fileKey != null || creationTime.equals(attrs.creationTime()));
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
    private WorkerWorkspaceRegistry workspaceRegistry;

    private final ReentrantLock evictionLock = new ReentrantLock();
    private final Set<DownloadStaging> activeDownloads = new HashSet<>(); // guarded by evictionLock
    private final ConcurrentHashMap<String, AtomicInteger> pinCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, VerifiedBlobStamp> verifiedStamps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> accessSequence = new ConcurrentHashMap<>();
    private final AtomicLong sequenceGenerator = new AtomicLong();

    // Blob accounting, mutated under evictionLock only.
    private final Map<String, Long> knownBlobSizes = new ConcurrentHashMap<>();
    private final AtomicInteger blobCount = new AtomicInteger();
    private final AtomicLong cachedBytes = new AtomicLong();
    private final AtomicLong evictedBlobsTotal = new AtomicLong();

    /** Why the last low disk warning could not restore the free space buffer. */
    enum LowDiskReason {
        /** Every remaining blob is pinned by a running partition. */
        NO_UNPINNED_BLOBS,
        /** Unpinned blobs were left whose deletion failed. */
        DELETES_FAILED
    }

    // Warning state and counters, mutated under evictionLock only.
    private volatile boolean lowDiskWarningActive;
    private volatile int lowDiskWarningCount;
    private volatile LowDiskReason lowDiskWarningReason;
    private volatile int evictionDeleteFailureCount;
    private volatile int corruptedBlobRemovals;

    // Telemetry and eviction share the probe warning state without locking the heartbeat path.
    private final AtomicBoolean probeFailureActive = new AtomicBoolean();
    private final AtomicInteger probeFailureWarningCount = new AtomicInteger();

    private volatile BlobIndex blobIndex;

    private WorkerFileCache(Path canonicalDir, boolean cacheCleanup, long freeSpaceBufferBytes, DiskSpaceProbe spaceProbe,
                            Clock clock, FileChannel lockChannel, FileLock fileLock, BlobCacheDirectory store) {
        this.canonicalDir = canonicalDir;
        this.cacheCleanup = cacheCleanup;
        this.freeSpaceBufferBytes = freeSpaceBufferBytes;
        this.spaceProbe = spaceProbe;
        this.clock = clock;
        this.lockChannel = lockChannel;
        this.fileLock = fileLock;
        this.blobCache = store;
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
        return open(cacheDir, cacheCleanup, freeSpaceBufferBytes, spaceProbe, clock, lockAcquirer, BlobCacheDirectory::new);
    }

    /**
     * @param storeFactory creates the blob store for the canonical cache directory; a test seam
     */
    static WorkerFileCache open(Path cacheDir, boolean cacheCleanup, long freeSpaceBufferBytes,
                                DiskSpaceProbe spaceProbe, Clock clock, LockAcquirer lockAcquirer,
                                Function<Path, BlobCacheDirectory> storeFactory) {
        if (freeSpaceBufferBytes < 0) {
            throw new IllegalArgumentException("Worker cache free space buffer must not be negative: " + freeSpaceBufferBytes);
        }
        Path canonicalDir;
        try {
            canonicalDir = Files.createDirectories(cacheDir).toRealPath();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create worker local file cache directory '" + cacheDir + "': " + e, e);
        }
        // Before anything is written: the directory may be another component's cache.
        refuseForeignOwner(canonicalDir, readOwner(canonicalDir));
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
                    clock, channel, lock, storeFactory.apply(canonicalDir));
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

    private static Optional<String> readOwner(Path canonicalDir) {
        try {
            return BlobCacheDirectory.readOwner(canonicalDir);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read the owner marker " + BlobCacheDirectory.OWNER_MARKER_FILE
                    + " of worker local file cache directory '" + canonicalDir + "': " + e, e);
        }
    }

    /** Refuses a directory recorded as owned by another component; one without an owner is a legacy worker cache. */
    private static void refuseForeignOwner(Path canonicalDir, Optional<String> owner) {
        if (owner.isPresent() && !BlobCacheDirectory.OWNER_WORKER.equals(owner.get())) {
            throw new IllegalStateException("Worker local file cache directory '" + canonicalDir + "' belongs to the "
                    + "Citrus Test Distribution " + owner.get() + " (recorded in " + BlobCacheDirectory.OWNER_MARKER_FILE
                    + ") and must not be used as a worker cache; " + CACHE_DIR_GUIDANCE + ".");
        }
    }

    private static DiskSpaceProbe fileStoreProbe(Path canonicalDir) throws IOException {
        FileStore store = Files.getFileStore(canonicalDir);
        return dir -> store.getUsableSpace();
    }

    private void initialize() throws IOException {
        // Under the file lock; a cache without a marker (created before markers existed) becomes the worker's.
        refuseForeignOwner(canonicalDir,
                Optional.of(BlobCacheDirectory.recordOwnerIfAbsent(canonicalDir, BlobCacheDirectory.OWNER_WORKER)));
        Files.createDirectories(canonicalDir.resolve("blobs"));
        Files.createDirectories(canonicalDir.resolve("tmp"));
        workspaceRegistry = new WorkerWorkspaceRegistry(canonicalDir);
        workspaceRegistry.recover();
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

    WorkerWorkspaceRegistry.Workspace createWorkspace(boolean forked) throws IOException {
        evictionLock.lock();
        try {
            ensureOpen();
            return workspaceRegistry.create(forked);
        } finally {
            evictionLock.unlock();
        }
    }

    /** The canonical cache directory. */
    Path rootDir() {
        return canonicalDir;
    }

    /**
     * Pins {@code sha256Digests} (duplicates count once) until the returned lease is closed. Waits for an in-flight
     * eviction, so a digest is either pinned before eviction lists its candidates or after it finished deleting.
     *
     * @throws IllegalArgumentException on an invalid digest; nothing stays pinned then
     */
    PinLease pinAll(Collection<String> sha256Digests) {
        Lease lease = new Lease();
        evictionLock.lock();
        try {
            for (String sha256 : sha256Digests) {
                lease.pin(sha256);
            }
        } catch (RuntimeException e) {
            lease.close();
            throw e;
        } finally {
            evictionLock.unlock();
        }
        return lease;
    }

    /**
     * Places a classpath artifact from the blob {@link #verifyOrInvalidate} (or a store operation) returned, without
     * hashing it again (see {@link BlobCacheDirectory#materializeForClasspath(ArtifactRef, Path, Path)}). The digest
     * must be pinned, so eviction cannot remove its named jar while it is in use.
     *
     * @throws IllegalStateException if the cache is closed
     */
    Path materializeForClasspath(ArtifactRef artifact, Path verifiedBlob, Path workspaceDir) throws IOException {
        if (artifact.kind() != ArtifactKind.JAR) {
            ensureOpen();
            return blobCache.materializeForClasspath(artifact, verifiedBlob, workspaceDir);
        }
        evictionLock.lock();
        try {
            ensureOpen();
            return blobCache.materializeForClasspath(artifact, verifiedBlob, workspaceDir);
        } finally {
            evictionLock.unlock();
        }
    }

    /**
     * Places a workspace input from the blob {@link #verifyOrInvalidate} (or a store operation) returned, without
     * hashing it again (see {@link WorkspacePackager#materializeIntoDirectory(BlobCacheDirectory, WorkspaceFileSet, Path, Path)}).
     *
     * @throws IllegalStateException if the cache is closed
     */
    List<MaterializedEntry> materializeIntoDirectory(WorkspaceFileSet fileSet, Path verifiedBlob, Path targetRoot)
            throws IOException {
        ensureOpen();
        return WorkspacePackager.materializeIntoDirectory(blobCache, fileSet, verifiedBlob, targetRoot);
    }

    boolean isPinned(String sha256) {
        return pinCounts.containsKey(sha256);
    }

    /**
     * Returns the path of a valid blob for {@code sha256} and records the access, or {@code null} when the blob is
     * absent or corrupted. A blob whose size, modification time or file key no longer match its verification stamp
     * is re-hashed (outside the eviction lock); a corrupted one is then removed together with its named jars under the
     * lock, while one that vanished in the meantime is only forgotten.
     */
    Path verifyOrInvalidate(String sha256) throws IOException {
        return verifyOrInvalidate(sha256, true);
    }

    private Path verifyOrInvalidate(String sha256, boolean retryWhenChanged) throws IOException {
        ensureOpen();
        Path blob = blobCache.resolveBlobPathForAccess(sha256);
        BasicFileAttributes attrs = readAttributes(blob);
        if (attrs == null) {
            forgetMissing(sha256);
            return null;
        }
        VerifiedBlobStamp stamp = verifiedStamps.get(sha256);
        if (stamp != null && stamp.matches(attrs)) {
            evictionLock.lock();
            try {
                ensureOpen();
                VerifiedBlobStamp currentStamp = verifiedStamps.get(sha256);
                if (currentStamp != null && recordAccess(sha256, blob, currentStamp, true)) {
                    return blob;
                }
            } finally {
                evictionLock.unlock();
            }
            return retryWhenChanged ? verifyOrInvalidate(sha256, false) : null;
        }

        boolean intact = false;
        try {
            if (attrs.isRegularFile()) {
                intact = blobCache.isIntact(sha256);
            }
        } catch (NoSuchFileException e) {
            // Check below whether it vanished or was replaced.
        } catch (IOException e) {
            BasicFileAttributes current = readAttributes(blob);
            if (current != null && VerifiedBlobStamp.of(attrs).matches(current)) {
                throw e;
            }
        }
        evictionLock.lock();
        try {
            ensureOpen();
            BasicFileAttributes current = readAttributes(blob);
            if (current == null) {
                LOG.debug("Blob {} vanished from worker cache {} while it was verified", sha256, canonicalDir);
                forgetBlob(sha256);
                return null;
            }
            if (attrs.isRegularFile() == current.isRegularFile() && VerifiedBlobStamp.of(attrs).sameSnapshot(current)) {
                if (!intact) {
                    corruptedBlobRemovals++;
                    LOG.warn("Removing corrupted blob {} from worker cache {}", sha256, canonicalDir);
                    blobIndex.delete(sha256);
                    forgetBlob(sha256);
                    return null;
                }
                if (stamp != null && !stamp.sameFile(current)) {
                    blobCache.deleteNamedJars(sha256);
                }
                if (recordAccess(sha256, blob, VerifiedBlobStamp.of(current), true)) {
                    registerBlob(sha256, current.size());
                    return blob;
                }
            }
            if (!retryWhenChanged) {
                LOG.debug("Blob {} in worker cache {} keeps changing while it is verified", sha256, canonicalDir);
                return null;
            }
        } finally {
            evictionLock.unlock();
        }
        // replaced while it was hashed: neither trust nor delete what was hashed, verify the current file once more
        return verifyOrInvalidate(sha256, false);
    }

    /** Opens a download's staging output. */
    @FunctionalInterface
    interface DownloadFileOpener {
        OutputStream open(Path file) throws IOException;
    }

    /**
     * Local staging writes and cleanup run under the eviction lock, so none can continue after cache ownership is
     * released. The caller does network reads between writes, outside this lock.
     */
    DownloadStaging openDownloadStaging(DownloadFileOpener opener) throws IOException {
        evictionLock.lock();
        try {
            Path file = newDownloadTempFile();
            try {
                DownloadStaging staging = new DownloadStaging(file, opener.open(file));
                activeDownloads.add(staging);
                return staging;
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException | RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        } finally {
            evictionLock.unlock();
        }
    }

    final class DownloadStaging implements AutoCloseable {
        private final Path file;
        private OutputStream output;

        private DownloadStaging(Path file, OutputStream output) {
            this.file = file;
            this.output = output;
        }

        void write(byte[] bytes, int length) throws IOException {
            evictionLock.lock();
            try {
                ensureOpen();
                if (output == null) {
                    throw new IOException("Download staging file is no longer writable");
                }
                output.write(bytes, 0, length);
            } finally {
                evictionLock.unlock();
            }
        }

        Path promote(String sha256) throws IOException {
            evictionLock.lock();
            try {
                ensureOpen();
                if (output == null) {
                    throw new IOException("Download staging file is no longer writable");
                }
                closeOutput();
                return promoteDownload(sha256, file);
            } finally {
                evictionLock.unlock();
            }
        }

        @Override
        public void close() throws IOException {
            evictionLock.lock();
            try {
                if (activeDownloads.remove(this)) {
                    dispose();
                }
            } finally {
                evictionLock.unlock();
            }
        }

        private void dispose() throws IOException {
            try {
                closeOutput();
            } catch (IOException | RuntimeException e) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException | RuntimeException cleanup) {
                    e.addSuppressed(cleanup);
                }
                throw e;
            }
            Files.deleteIfExists(file);
        }

        private void closeOutput() throws IOException {
            OutputStream stream = output;
            output = null;
            if (stream != null) {
                stream.close();
            }
        }
    }

    /** A new empty file under {@code tmp/} for a download; the caller deletes it when done. */
    Path newDownloadTempFile() throws IOException {
        evictionLock.lock();
        try {
            ensureOpen();
            return blobCache.newTempFile("dl-");
        } finally {
            evictionLock.unlock();
        }
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
            ensureOpen();
            Path target = blobCache.resolveBlobPathForAccess(expectedSha256);
            BasicFileAttributes staged = readAttributes(tempFile);
            BasicFileAttributes previous = readAttributes(target);
            if (previous == null || !previous.isRegularFile()) {
                blobCache.deleteNamedJars(expectedSha256);
            }
            if (!blobCache.storeTempBlob(expectedSha256, tempFile)) {
                // A file that appeared during the download is independent of the bytes we just verified.
                Path existing = verifyOrInvalidate(expectedSha256);
                if (existing != null) {
                    return existing;
                }
                if (!blobCache.storeTempBlob(expectedSha256, tempFile)) {
                    throw new IOException("Blob " + expectedSha256 + " in worker cache " + canonicalDir
                            + " changed during download and could not be verified");
                }
            }
            VerifiedBlobStamp downloaded = staged != null ? VerifiedBlobStamp.of(staged) : null;
            if (registerStored(expectedSha256, target, downloaded)) {
                return target;
            }
            Path verified = verifyOrInvalidate(expectedSha256);
            if (verified == null) {
                throw new IOException("Blob " + expectedSha256 + " in worker cache " + canonicalDir
                        + " changed during promotion and could not be verified");
            }
            return verified;
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
            ensureOpen();
            Optional<WorkspaceFileSet> output = WorkspacePackager.packageModifiedOutputToCache(blobCache, workspaceDir,
                    rawOutputPath, baselineHashes);
            if (output.isPresent()) {
                String sha256 = output.get().artifact().sha256();
                pinRegistrar.accept(sha256);
                // The packager cannot tell whether it stored the blob or found it already present, so an existing
                // blob is never trusted here without a matching stamp.
                registerStored(sha256, blobCache.resolveBlobPathForAccess(sha256), verifiedStamps.get(sha256));
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
            if (closed.get()) {
                LOG.debug("Skipping eviction in closed worker cache {}", canonicalDir);
                return;
            }
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
        int failedDeletes = 0;
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
                failedDeletes++;
                evictionDeleteFailureCount++;
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
        } else {
            LowDiskReason reason = failedDeletes > 0 ? LowDiskReason.DELETES_FAILED : LowDiskReason.NO_UNPINNED_BLOBS;
            if (lowDiskWarningActive && lowDiskWarningReason == reason) {
                return;
            }
            lowDiskWarningActive = true;
            lowDiskWarningCount++;
            lowDiskWarningReason = reason;
            if (reason == LowDiskReason.DELETES_FAILED) {
                LOG.warn("Worker cache {} has {} usable bytes, {} short of the {} byte free space buffer plus {} required "
                                + "bytes, because {} unpinned blobs could not be deleted (see the previous warnings)",
                        canonicalDir, usable, deficit, freeSpaceBufferBytes, additionalBytes, failedDeletes);
            } else {
                LOG.warn("Worker cache {} has {} usable bytes, {} short of the {} byte free space buffer plus {} required "
                                + "bytes, and no unpinned blobs are left to evict",
                        canonicalDir, usable, deficit, freeSpaceBufferBytes, additionalBytes);
            }
        }
    }

    /** Probes under {@code evictionLock}; a failure is warned about once per episode of consecutive failures. */
    private Long probeUsable() {
        try {
            long usable = spaceProbe.usableBytes(canonicalDir);
            probeFailureActive.set(false);
            return usable;
        } catch (IOException e) {
            if (probeFailureActive.compareAndSet(false, true)) {
                probeFailureWarningCount.incrementAndGet();
                LOG.warn("Cannot determine usable disk space of worker cache {}: {}", canonicalDir, e.toString());
            } else {
                LOG.debug("Cannot determine usable disk space of worker cache {}: {}", canonicalDir, e.toString());
            }
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
        Long probed = probeUsable();
        long usable = probed != null ? probed : -1L;
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

    /** Cause named by the last low disk warning, or {@code null} before the first one. */
    LowDiskReason lowDiskWarningReason() {
        return lowDiskWarningReason;
    }

    /** Free space probe failures warned about: one per episode of consecutive failures. */
    int probeFailureWarningCount() {
        return probeFailureWarningCount.get();
    }

    /** Evictions whose delete failed and was warned about. */
    int evictionDeleteFailureCount() {
        return evictionDeleteFailureCount;
    }

    /** Corrupted blobs removed (and warned about) by {@link #verifyOrInvalidate}. */
    int corruptedBlobRemovals() {
        return corruptedBlobRemovals;
    }

    boolean evictionLockHeldByCurrentThread() {
        return evictionLock.isHeldByCurrentThread();
    }

    boolean hasThreadsWaitingForEvictionLock() {
        return evictionLock.hasQueuedThreads();
    }

    void blobIndex(BlobIndex blobIndex) {
        this.blobIndex = Objects.requireNonNull(blobIndex);
    }

    /**
     * Releases the lock; the {@code .lock} file stays. Waits for an in-flight eviction or store operation, after which
     * no operation mutates the directory any more. Idempotent.
     */
    @Override
    public void close() {
        boolean release = false;
        evictionLock.lock();
        try {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            release = true;
            List<DownloadStaging> pending = List.copyOf(activeDownloads);
            activeDownloads.clear();
            for (DownloadStaging staging : pending) {
                try {
                    staging.dispose();
                } catch (IOException | RuntimeException e) {
                    LOG.warn("Could not clean up download staging file {}: {}", staging.file, e.toString());
                }
            }
        } finally {
            evictionLock.unlock();
            if (release) {
                releaseOwnership();
            }
        }
    }

    private void releaseOwnership() {
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

    /** Mutating operations call this, under {@code evictionLock} where they hold it, so they serialize with close. */
    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Worker local file cache " + canonicalDir + " is closed");
        }
    }

    /**
     * Accounts for and touches a stored blob. Returns whether it still matches a verified snapshot of the moved
     * download or an existing blob. An unstamped blob is hashed the next time it is needed.
     */
    private boolean registerStored(String sha256, Path stored, VerifiedBlobStamp verified) throws IOException {
        BasicFileAttributes attrs = readAttributes(stored);
        if (attrs == null) {
            throw new NoSuchFileException(stored.toString(), null, "blob vanished right after it was stored");
        }
        registerBlob(sha256, attrs.size());
        boolean trusted = verified != null && verified.matches(attrs);
        return recordAccess(sha256, stored, VerifiedBlobStamp.of(attrs), trusted);
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

    private void forgetMissing(String sha256) throws IOException {
        if (!knownBlobSizes.containsKey(sha256) && !verifiedStamps.containsKey(sha256)) {
            return;
        }
        evictionLock.lock();
        try {
            if (!closed.get() && readAttributes(blobCache.resolveBlobPathForAccess(sha256)) == null) {
                forgetBlob(sha256);
            }
        } finally {
            evictionLock.unlock();
        }
    }

    /**
     * Records an access without treating a replacement file as verified. Checks the snapshot immediately before
     * touching the blob and confirms the touched file's identity before recording the updated timestamp.
     */
    private boolean recordAccess(String sha256, Path blob, VerifiedBlobStamp before, boolean verified) throws IOException {
        FileTime accessed = FileTime.from(clock.instant());
        blobCache.resolveBlobPathForAccess(sha256);
        BasicFileAttributes current = readAttributes(blob);
        if (current == null || !before.matches(current)) {
            return false;
        }
        accessSequence.put(sha256, sequenceGenerator.incrementAndGet());
        try {
            Files.getFileAttributeView(blob, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS)
                    .setTimes(accessed, null, null);
        } catch (IOException e) {
            LOG.debug("Could not touch cached blob {}: {}", blob, e.toString());
        }
        BasicFileAttributes attrs = verified ? readAttributes(blob) : null;
        if (attrs != null && attrs.isRegularFile() && before.sameFile(attrs)) {
            verifiedStamps.put(sha256, VerifiedBlobStamp.of(attrs));
            return true;
        }
        verifiedStamps.remove(sha256);
        return false;
    }

    private static BasicFileAttributes readAttributes(Path path) throws IOException {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    private final class Lease implements PinLease {

        private final ConcurrentHashMap.KeySetView<String, Boolean> digests = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean leaseClosed = new AtomicBoolean();

        /** Takes {@code evictionLock} before this lease's monitor (never the other way round), waiting for an eviction. */
        @Override
        public void pin(String sha256) {
            BlobCacheDirectory.validateDigest(sha256);
            evictionLock.lock();
            try {
                synchronized (this) {
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
            } finally {
                evictionLock.unlock();
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
