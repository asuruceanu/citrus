# Worker Local File Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an exclusive, self-healing, disk-space-bounded local file cache to Citrus Test Distribution workers with CLI, system property, and environment variable configuration across `DistributionWorkerClient`, `citrus jb dist worker start`, and `citrus-agent --mode worker`.

**Architecture:** Keep `BlobCacheDirectory` in `citrus-test-distribution-protocol` as a minimal content-addressed store by adding only non-recursive blob enumeration (`listBlobs`), deletion (`delete`), and clean non-atomic move fallback cleanup, while extending `DistributionSettings` with worker cache config resolution and strict byte-size/boolean parsers. Introduce package-private `WorkerFileCache` in `citrus-test-distribution-worker` owned once per `DistributionWorkerClient` to enforce exclusive `.lock` ownership, startup `tmp/` sweeping, `(size, mtime, fileKey)` fast-path verification, partition-scoped `PinLease` protection, streaming bounded downloads in `BrokerBlobClient`, and deterministic `(mtime, accessSequence, sha256)` LRU eviction down to `--cache-free-space-buffer-size`. Expose matching CLI flags in `DistWorkerStart` and `CitrusAgentOptions`/`CitrusAgentConfiguration`.

**Tech Stack:** Java 21 (`java.nio.file.Files`, `FileChannel`, `FileLock`, `FileStore`, `java.net.http.HttpClient`), Aesh CLI (`tools/jbang`), Citrus Agent (`core/citrus-api`, `tools/agent`, `tools/schema-generator`), JUnit Jupiter 5, AssertJ.

**Spec:** `docs/superpowers/specs/2026-11-22-test-distribution-local-file-cache-design.md`

## Global Constraints

- Java 21+, Maven 3.9+, zero new third-party dependencies.
- All new and modified Java files must carry the standard Apache 2.0 license header used in the module.
- Zero vendor product/company mentions in any code, comment, test, POM, manual, spec, or commit message (`git grep -i` check must return `0` matches).
- Precedence for worker cache settings (highest to lowest):
  1. Explicit `DistributionWorkerConfig.Builder` / CLI flag (`--cache-dir`, `--[no-]cache-cleanup[=true/false]`, `--cache-free-space-buffer-size`)
  2. JVM system properties: `citrus.test.distribution.worker.cacheDir`, `citrus.test.distribution.worker.cacheCleanup`, `citrus.test.distribution.worker.cacheFreeSpaceBufferSize`
  3. Citrus environment variables: `CITRUS_DIST_WORKER_CACHE_DIR`, `CITRUS_DIST_WORKER_CACHE_CLEANUP`, `CITRUS_DIST_WORKER_CACHE_FREE_SPACE_BUFFER_SIZE`
  4. Compatibility environment aliases: `TEST_DISTRIBUTION_AGENT_CACHE_DIR`, `TEST_DISTRIBUTION_AGENT_CACHE_CLEANUP`, `TEST_DISTRIBUTION_AGENT_CACHE_FREE_SPACE_BUFFER_SIZE`
  5. Defaults: `${user.home}/.citrus-test-distribution-agent/file-cache`, `true`, `2 GiB` (`2_147_483_648L` bytes).
- Legacy global `citrus.test.distribution.cacheDir` / `CITRUS_DIST_CACHE_DIR` continues to govern coordinator/launcher defaults only and must not affect the worker cache default.
- Blank (`null` or whitespace-only) system properties and environment variables are treated as unset and fall through; non-blank invalid values fail immediately with `IllegalArgumentException` (never silently falling back); blank CLI `=` values fail immediately with `IllegalArgumentException`.
- Maximum single blob size remains `ClasspathPackager.MAX_BLOB_BYTES` (`256 MiB`); downloads stream in `64 KiB` chunks with explicit `try-with-resources` file handle closure prior to atomic promotion.

## Review Focus

1. **Byte-size multiplication and required-space addition overflow:** `--cache-free-space-buffer-size=9999999999GiB` must throw `IllegalArgumentException` via `Math.multiplyExact` rather than wrapping negative, and `bufferBytes + expectedBytes` in `WorkerFileCache` must saturate at `Long.MAX_VALUE` rather than overflowing negative and skipping eviction (pinned in Tasks 1 and 2).
2. **Duplicate SHA-256 digests within a partition's artifacts or overlapping output digests:** `WorkerFileCache.pinAll` and `stageAndPinOutput` must track per-lease digest sets so duplicate digests in `classpathArtifacts` + `inputArtifacts` increment pin count once per lease and `PinLease.close()` decrements cleanly to `0` without leaking permanent pins or unpinning early (pinned in Task 2).
3. **Symlinked or relative `cacheDir` inside worker `user.dir` (including macOS `/var` -> `/private/var`):** `IsolatedPartitionRunner` must pass both `config.cacheDir()` and `cache.rootDir()` (or exclude canonicalized paths) when taking `WorkingDirectorySnapshot` so worker cache writes inside `user.dir` never trigger false-positive CWD mutation errors (pinned in Task 3).
4. **Suffix case-sensitivity, whitespace, and non-binary/decimal units in `--cache-free-space-buffer-size`:** Outer/inner whitespace around valid suffixes (`" 512 MiB "`) is accepted, while lowercase (`"2gib"`), SI (`"2GB"`), fractional (`"1.5GiB"`), and negative (`"-1MiB"`) values throw `IllegalArgumentException` naming `KiB`, `MiB`, `GiB`, or integer bytes (pinned in Task 1).
5. **Stray non-digest files inside `<cacheDir>/blobs/` (e.g., `.DS_Store` or manual notes):** `BlobCacheDirectory.listBlobs()` must ignore non-regular files, symlinks, and filenames not matching `^[a-f0-9]{64}$` instead of failing eviction with `IllegalArgumentException` (pinned in Task 1).

---

### Task 1: Protocol Worker Cache Settings and `BlobCacheDirectory` Primitives

**Files:**
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/config/DistributionSettings.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/cache/BlobCacheDirectory.java`
- Modify: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/config/DistributionSettingsAndPathRewriterTest.java`
- Create: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/cache/BlobCacheDirectoryTest.java`

**Interfaces:**
- Consumes: `ClasspathPackager.MAX_BLOB_BYTES`, `ClasspathPackager.sha256Hex(Path)`
- Produces:
  - `DistributionSettings` constants:
    - `WORKER_CACHE_DIR_PROPERTY = "citrus.test.distribution.worker.cacheDir"`
    - `WORKER_CACHE_DIR_ENV = "CITRUS_DIST_WORKER_CACHE_DIR"`
    - `AGENT_CACHE_DIR_ENV = "TEST_DISTRIBUTION_AGENT_CACHE_DIR"`
    - `WORKER_CACHE_CLEANUP_PROPERTY = "citrus.test.distribution.worker.cacheCleanup"`
    - `WORKER_CACHE_CLEANUP_ENV = "CITRUS_DIST_WORKER_CACHE_CLEANUP"`
    - `AGENT_CACHE_CLEANUP_ENV = "TEST_DISTRIBUTION_AGENT_CACHE_CLEANUP"`
    - `WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY = "citrus.test.distribution.worker.cacheFreeSpaceBufferSize"`
    - `WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_ENV = "CITRUS_DIST_WORKER_CACHE_FREE_SPACE_BUFFER_SIZE"`
    - `AGENT_CACHE_FREE_SPACE_BUFFER_SIZE_ENV = "TEST_DISTRIBUTION_AGENT_CACHE_FREE_SPACE_BUFFER_SIZE"`
    - `DEFAULT_WORKER_CACHE_CLEANUP = true`
    - `DEFAULT_WORKER_CACHE_FREE_SPACE_BUFFER_BYTES = 2L * 1024 * 1024 * 1024`
  - `DistributionSettings` methods:
    - `public static Path defaultWorkerCacheDir()` -> `Paths.get(System.getProperty("user.home", ".")).resolve(".citrus-test-distribution-agent").resolve("file-cache")`
    - `public static Path getWorkerCacheDir()`
    - `public static boolean isWorkerCacheCleanupEnabled()`
    - `public static long getWorkerCacheFreeSpaceBufferBytes()`
    - `public static long parseByteSize(String raw)`
    - `public static boolean parseStrictBoolean(String raw, String optionName)`
    - Package-private testable helpers accepting `(UnaryOperator<String> prop, UnaryOperator<String> env)` for `resolveWorkerCacheDir`, `resolveWorkerCacheCleanup`, `resolveWorkerCacheFreeSpaceBufferBytes`
  - `BlobCacheDirectory` types and methods:
    - `public record BlobEntry(String sha256, Path path, long sizeBytes, long lastModifiedMillis)`
    - `public List<BlobEntry> listBlobs() throws IOException`
    - `public boolean delete(String sha256) throws IOException`

- [ ] **Step 1: Write failing unit tests for `DistributionSettings` worker cache precedence and parsers**

Add tests to `DistributionSettingsAndPathRewriterTest.java` covering:
- `shouldParseByteSizeUnitsAndRejectInvalidInputs`:
  - `"0"` -> `0L`, `"4096"` -> `4096L`, `"512KiB"` -> `524_288L`, `" 256 MiB "` -> `268_435_456L`, `"2GiB"` -> `2_147_483_648L`
  - Rejects `null`, `""`, `"   "`, `"-1"`, `"2gib"`, `"2GB"`, `"1.5GiB"`, `"abc"`, and overflow `"9999999999GiB"` with `IllegalArgumentException`
- `shouldParseStrictBooleanAndRejectInvalidTokens`:
  - `"true"`, `"TRUE"`, `" false "` -> `true`/`false`
  - Rejects `null`, `""`, `"flase"`, `"1"`, `"yes"` with `IllegalArgumentException` containing `optionName`
- `shouldResolveWorkerCacheSettingsWithPrecedenceBlankFallthroughAndGlobalIsolation`:
  - Verifies `sysprop > CITRUS_DIST_WORKER_* > TEST_DISTRIBUTION_AGENT_* > default` across `resolveWorkerCacheDir`, `resolveWorkerCacheCleanup`, and `resolveWorkerCacheFreeSpaceBufferBytes`
  - Verifies blank `""` / `"  "` values fall through to the next tier
  - Verifies invalid non-blank `CITRUS_DIST_WORKER_CACHE_FREE_SPACE_BUFFER_SIZE="bad"` throws `IllegalArgumentException` even when `TEST_DISTRIBUTION_AGENT_CACHE_FREE_SPACE_BUFFER_SIZE="1GiB"` is valid
  - Verifies setting `citrus.test.distribution.cacheDir="/tmp/global-cache"` does NOT change `DistributionSettings.getWorkerCacheDir()`

- [ ] **Step 2: Write failing unit tests for `BlobCacheDirectory.listBlobs`, `delete`, and `moveIntoPlace` cleanup**

Create `BlobCacheDirectoryTest.java` testing:
- `shouldListStoredBlobsIgnoringStrayFilesAndSymlinks`: stores two valid SHA-256 blobs, writes `<root>/blobs/.DS_Store`, `<root>/blobs/aa/not-a-digest.txt`, and (on POSIX) a symlink inside `<root>/blobs/aa/`; asserts `listBlobs()` returns only the two valid `BlobEntry` records with exact `sha256`, `path`, `sizeBytes`, and `lastModifiedMillis`.
- `shouldDeleteExistingBlobAndReturnFalseWhenMissing`: asserts `delete(sha)` returns `true` then `false`, removing `<root>/blobs/<prefix>/<sha>` while rejecting malformed digest strings with `IllegalArgumentException`.

- [ ] **Step 3: Run protocol tests to verify failure**

Run: `./mvnw test -Dtest=DistributionSettingsAndPathRewriterTest,BlobCacheDirectoryTest -pl tools/citrus-test-distribution-protocol`
Expected: FAIL (compilation failure on missing symbols).

- [ ] **Step 4: Implement worker cache settings in `DistributionSettings.java` and `listBlobs`/`delete` in `BlobCacheDirectory.java`**

In `DistributionSettings.java`:
- Add the constants and methods from **Interfaces**.
- Implement `parseByteSize(String raw)` matching `^\s*(\d+)\s*(KiB|MiB|GiB)?\s*$` via `Long.parseLong` and `Math.multiplyExact` (`1024L`, `1024L * 1024`, `1024L * 1024 * 1024`), converting `NumberFormatException` / `ArithmeticException` into `IllegalArgumentException` with message naming valid suffixes `KiB`, `MiB`, `GiB`, or integer bytes.
- Implement `resolveFirstNonBlank(String propKey, String primaryEnvKey, String aliasEnvKey, UnaryOperator<String> prop, UnaryOperator<String> env)` returning the first trimmed non-empty string or `null`.

In `BlobCacheDirectory.java`:
- Add `BlobEntry` record, `delete(String sha256)` (`validateDigest(sha256)` + `Files.deleteIfExists(resolveBlobPath(sha256))`), and `listBlobs()` using `Files.walkFileTree(blobsDir, Set.of(), 2, visitor)` (no `FOLLOW_LINKS`) collecting regular files at depth 2 whose filename matches `SHA256_HEX` and matches parent 2-char prefix.
- In `moveIntoPlace(Path tmp, Path target)`, record `boolean targetExisted = Files.exists(target, LinkOption.NOFOLLOW_LINKS)` before the `AtomicMoveNotSupportedException` fallback; if fallback `Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)` throws `IOException` and `!targetExisted`, call `Files.deleteIfExists(target)` in catch before rethrowing.

- [ ] **Step 5: Run protocol tests to verify pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-protocol`
Expected: PASS (`0` failures).

- [ ] **Step 6: Commit**

```bash
git add tools/citrus-test-distribution-protocol
git commit -m "feat(distribution): add worker cache settings parsers and blob enumeration"
```

---

### Task 2: `DistributionWorkerConfig` Cache Fields and `WorkerFileCache` Core Engine

**Files:**
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/DistributionWorkerConfig.java`
- Create: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/WorkerFileCache.java`
- Create: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/WorkerFileCacheTest.java`

**Interfaces:**
- Consumes: `DistributionSettings.getWorkerCacheDir()`, `DistributionSettings.isWorkerCacheCleanupEnabled()`, `DistributionSettings.getWorkerCacheFreeSpaceBufferBytes()`, `BlobCacheDirectory`, `BlobCacheDirectory.BlobEntry`, `WorkspacePackager`, `ArtifactRef`
- Produces:
  - `DistributionWorkerConfig`:
    - `public boolean cacheCleanup()`
    - `public long cacheFreeSpaceBufferBytes()`
    - `Builder.cacheCleanup(boolean cacheCleanup)`
    - `Builder.cacheFreeSpaceBufferBytes(long cacheFreeSpaceBufferBytes)` (throws `IllegalArgumentException` if `< 0`)
    - Default `Builder.cacheDir` changed to `DistributionSettings.getWorkerCacheDir()`
  - Package-private `final class WorkerFileCache implements AutoCloseable`:
    - `@FunctionalInterface interface DiskSpaceProbe { long usableBytes(Path dir) throws IOException; }`
    - `interface PinLease extends AutoCloseable { void pin(String sha256); @Override void close(); }`
    - `static WorkerFileCache open(Path cacheDir, boolean cacheCleanup, long freeSpaceBufferBytes)`
    - `static WorkerFileCache open(Path cacheDir, boolean cacheCleanup, long freeSpaceBufferBytes, DiskSpaceProbe spaceProbe, Clock clock)`
    - `Path rootDir()` (canonicalized cache directory)
    - `BlobCacheDirectory blobCache()`
    - `PinLease pinAll(Collection<String> sha256Digests)`
    - `boolean isPinned(String sha256)`
    - `Path verifyOrInvalidate(String sha256) throws IOException`
    - `Path newDownloadTempFile() throws IOException`
    - `Path promoteDownload(String expectedSha256, Path tempFile) throws IOException`
    - `Optional<ArtifactRef> stageAndPinOutput(Path workspaceDir, String rawOutputPath, Map<String, String> baselineHashes, Consumer<String> pinRegistrar) throws IOException` (or `PinLease.pin(String sha256)` so newly packaged forked output digests join the active partition lease under `evictionLock`)
    - `void ensureFreeSpaceForDownload(long expectedBytes)`
    - `void evictAfterPartition()`
    - `@Override public void close()`

- [ ] **Step 1: Write failing tests in `WorkerFileCacheTest.java`**

Cover every P0/P1 invariant in `WorkerFileCacheTest.java`:
- `shouldDefaultWorkerConfigCacheSettingsFromDistributionSettingsAndRejectNegativeBuffer`: verifies `DistributionWorkerConfig.builder().build()` uses `DistributionSettings.getWorkerCacheDir()`, `cacheCleanup() == true`, `cacheFreeSpaceBufferBytes() == 2 GiB`, and `cacheFreeSpaceBufferBytes(-1)` throws `IllegalArgumentException`.
- `shouldRejectSecondWorkerOnSameCacheDirInSameJvmAndReleaseOnClose`: opens cache 1 on `tempDir`, asserts second `WorkerFileCache.open(tempDir, ...)` throws `IllegalStateException` mentioning canonical path and `--cache-dir`, closes cache 1, asserts `.lock` still exists on disk and third `open(tempDir, ...)` succeeds.
- `shouldRejectSecondJvmLockAndRecoverCleanlyAfterForcibleChildCrash`: spawns a helper `main` in a child JVM holding `WorkerFileCache.open(dir, ...)` and leaving `<dir>/tmp/dl-orphan.tmp`; verifies parent `open(dir, ...)` fails fast while child lives; kills child via `process.destroyForcibly().waitFor()`; verifies parent `open(dir, ...)` immediately acquires `.lock` and sweeps `dl-orphan.tmp`.
- `shouldRollBackJvmLockSetWhenFileLockThrowsIoException`: tests package-private lock factory / simulated `IOException` during `open()` and verifies `ACTIVE_JVM_LOCKS` does not retain `canonicalDir`.
- `shouldAlwaysSweepOrphanTmpFilesEvenWhenCacheCleanupDisabled`: creates `<dir>/tmp/dl-stale.tmp` and valid blob in `<dir>/blobs/..`, opens with `cacheCleanup = false` and `spaceProbe = p -> 0L`, asserts `dl-stale.tmp` is deleted while `<dir>/tmp` directory and valid blob survive.
- `shouldDetectStampMismatchOnExternalSizeMtimeOrInodeMutation`: promotes a blob, verifies warm `verifyOrInvalidate` hit, then (a) overwrites bytes changing size, (b) overwrites same-length corrupt bytes with bumped `mtime`, and (c) replaces file via move-over (new inode / `fileKey` with restored `mtime`); asserts `verifyOrInvalidate` returns `null` and deletes the corrupt file in all three cases.
- `shouldEvictInExactSessionAccessOrderAndDeterministicRestartOrderWhilePreservingPinnedBlobs`:
  - Stores 4 blobs at fixed `Clock` instant, accesses blob 1 then blob 2 via `verifyOrInvalidate`, pins blob 3 (including duplicate digest in `pinAll(List.of(sha3, sha3))`), triggers `ensureFreeSpaceForDownload` -> blob 4 (untouched) evicts first, then blob 1, while pinned blob 3 is never evicted; closing `PinLease` unpins blob 3 cleanly on first `close()` (idempotent second `close()`).
  - Opens a fresh `WorkerFileCache` (simulating restart with tied `mtime`s) and verifies deterministic `sha256` ascending eviction order.
- `shouldContinueEvictionWhenSingleBlobDeleteFailsAndRateLimitTightDiskWarning`:
  - Makes candidate #1's parent directory read-only (POSIX, `@DisabledOnOs(OS.WINDOWS)`) or injects delete `IOException`, verifies eviction logs `WARN`, leaves candidate #1 re-verifiable once permissions restore, and evicts candidate #2.
  - With `spaceProbe` returning `500 MiB` (`< 2 GiB` buffer) and no unpinned blobs, asserts `ensureFreeSpaceForDownload` does not throw, emits warning state once across repeated calls while shortage persists, and resets once `spaceProbe` returns `>= 2 GiB`.
- `shouldSaturateAdditionOnNearMaxBufferAndEvict100kSyntheticEntriesUnderFiveSeconds`:
  - Verifies `freeSpaceBufferBytes = Long.MAX_VALUE - 10` with `expectedBytes = 1024` does not overflow negative.
  - Verifies eviction batching over `100,000` synthetic `BlobEntry` items evicting `10,000` small entries completes in `< 5 s` and calls `DiskSpaceProbe` at most `(10_000 / 64) + 3` times.

- [ ] **Step 2: Run worker test to verify failure**

Run: `./mvnw test -Dtest=WorkerFileCacheTest -pl tools/citrus-test-distribution-worker`
Expected: FAIL (compilation failure on `WorkerFileCache` and new `DistributionWorkerConfig` methods).

- [ ] **Step 3: Implement `DistributionWorkerConfig` updates and `WorkerFileCache.java`**

- Update `DistributionWorkerConfig.java` with `cacheCleanup` and `cacheFreeSpaceBufferBytes` fields, getters, and `Builder` methods, changing `Builder`'s default `cacheDir` to `DistributionSettings.getWorkerCacheDir()`.
- Implement `WorkerFileCache.java`:
  - Canonicalize `cacheDir` after `Files.createDirectories(cacheDir)` via `cacheDir.toRealPath()`.
  - Track intra-JVM ownership in `static final Set<Path> ACTIVE_JVM_LOCKS = ConcurrentHashMap.newKeySet()` plus `FileChannel.open(canonicalDir.resolve(".lock"), CREATE, WRITE)` + `channel.tryLock()`. On `OverlappingFileLockException` or `null` lock or duplicate `ACTIVE_JVM_LOCKS`, throw `IllegalStateException` naming `canonicalDir` and `--cache-dir` / `-Dcitrus.test.distribution.worker.cacheDir` / `CITRUS_DIST_WORKER_CACHE_DIR` / `TEST_DISTRIBUTION_AGENT_CACHE_DIR`. Roll back channel/lock/`ACTIVE_JVM_LOCKS` in `catch (Throwable)` if any step in `open()` fails.
  - Track pins via `ConcurrentHashMap<String, AtomicInteger> pinCounts`; `PinLease` holds a deduplicated `ConcurrentHashMap.KeySetView<String, Boolean>` of digests pinned by that lease (supporting `lease.pin(String sha256)` for forked output staging) and decrements/removes counts once on `AtomicBoolean closed` transition.
  - Track `VerifiedBlobStamp(long sizeBytes, FileTime lastModifiedTime, Object fileKey)` in `ConcurrentHashMap<String, VerifiedBlobStamp>` and `ConcurrentHashMap<String, Long> accessSequence` fed by `AtomicLong sequenceGenerator`. `recordAccess(sha256, path)` updates `accessSequence`, calls `Files.setLastModifiedTime(path, FileTime.from(clock.instant()))` best-effort, and refreshes `VerifiedBlobStamp` from `Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS)`.
  - Guard promotion, output staging, and eviction with `ReentrantLock evictionLock` so `promoteDownload` and `stageAndPinOutput` stamp, touch, and pin newly stored blobs before any concurrent/subsequent eviction can inspect `blobs/`.
  - Implement byte-batched probing in `evictUnderLock(long requiredFreeBytes)` (re-probing `spaceProbe` once `deletedBytes >= deficit` or `deletionsSinceProbe >= 64`, plus final confirmation probe) and package-private hook for blob listing so the 100k-entry synthetic benchmark runs without creating 100k real inode files on CI filesystems.

- [ ] **Step 4: Run worker tests to verify pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker`
Expected: PASS (`0` failures).

- [ ] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution-worker
git commit -m "feat(distribution): implement exclusive WorkerFileCache with LRU eviction and pinning"
```

---

### Task 3: Streaming `BrokerBlobClient` and Shared Cache Wiring in Worker Client & Runners

**Files:**
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/BrokerBlobClient.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/IsolatedPartitionRunner.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/ForkedPartitionRunner.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/DistributionWorkerClient.java`
- Create: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/BrokerBlobClientCacheTest.java`
- Modify: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/DistributionWorkerClientTest.java`

**Interfaces:**
- Consumes: `WorkerFileCache`, `WorkerFileCache.PinLease`, `ClasspathPackager.MAX_BLOB_BYTES`, `ArtifactRef`
- Produces:
  - Package-private `BrokerBlobClient implements AutoCloseable`:
    - `BrokerBlobClient(DistributionWorkerConfig config, WorkerFileCache cache)`
    - `Path ensureCached(ArtifactRef artifact) throws IOException, InterruptedException`
    - `Path ensureCached(String sha256) throws IOException, InterruptedException`
    - `void uploadMissing(List<String> digests) throws IOException, InterruptedException`
    - `@Override public void close()`
  - Package-private constructors on `IsolatedPartitionRunner` and `ForkedPartitionRunner`:
    - `IsolatedPartitionRunner(DistributionWorkerConfig config, WorkerFileCache cache, BrokerBlobClient blobs)`
    - `IsolatedPartitionRunner(DistributionWorkerConfig config)` (package-private test convenience constructor; acquires and closes a locked `WorkerFileCache` per `execute()`)
    - `ForkedPartitionRunner(DistributionWorkerConfig config, WorkerFileCache cache, BrokerBlobClient blobs)`
    - `ForkedPartitionRunner(DistributionWorkerConfig config)` (package-private test convenience constructor; acquires and closes a locked `WorkerFileCache` per `execute()`)
  - `DistributionWorkerClient`:
    - Opens `WorkerFileCache` eagerly in `new DistributionWorkerClient(config)` (fail-fast `.lock`), shares single `WorkerFileCache` and `BrokerBlobClient` across `inProcessRunner` and `forkedRunner`, and closes `BrokerBlobClient` + `httpClient` + `WorkerFileCache` in `close()` after draining and awaiting `partitionExecutor` termination.

- [ ] **Step 1: Write failing tests in `BrokerBlobClientCacheTest.java` and `DistributionWorkerClientTest.java`**

In `BrokerBlobClientCacheTest.java` (using embedded `com.sun.net.httpserver.HttpServer` on port `0`):
- `shouldServeWarmBlobFromStampWithoutHttpRequestAndReDownloadAfterExternalCorruption`: first `ensureCached(ref)` hits HTTP `GET` once; second `ensureCached(ref)` makes `0` HTTP requests; corrupting the cached file on disk causes third `ensureCached(ref)` to delete corrupt file, re-fetch from HTTP server, and succeed.
- `shouldTriggerPreDownloadEvictionFromContentLengthWhenArtifactSizeIsZero`: `ArtifactRef` has `sizeBytes = 0`, HTTP response sends `Content-Length` requiring eviction of an unpinned cached blob; asserts unpinned blob is evicted before stream completes and `tmp/` is empty afterward.
- `shouldAbortOversizedDownloadAndHttpErrorsLeavingZeroTempFiles`:
  - `ArtifactRef` with `sizeBytes = MAX_BLOB_BYTES + 1` fails before issuing HTTP `GET`.
  - Chunked HTTP `200` streaming `MAX_BLOB_BYTES + 1` bytes (without `Content-Length`) aborts mid-stream with `IOException`, promotes nothing to `blobs/`, and leaves `0` files in `tmp/`.
  - HTTP `404` and `500` throw `IOException` and create `0` files in `tmp/`.
  - Corrupt transit bytes retry once and succeed when second response matches digest; two corrupt responses fail and leave `0` files in `tmp/` and `blobs/`.
  - Simulated physical write failure (`IOException("No space left on device")` when opening/writing `tmp` or severed mid-stream connection) deletes incomplete `dl-*.tmp`, marks only that partition failed, and allows the next partition on the same `DistributionWorkerClient` to succeed once space/stream recovers.

In `DistributionWorkerClientTest.java`:
- `shouldFailFastWhenSecondWorkerClientSharesCacheDirAndAllowReuseAfterClose`: starts `worker1` on `sharedCache`, asserts `new DistributionWorkerClient(worker2WithSameCache)` throws `IllegalStateException` while `worker1` still executes partitions, closes `worker1`, and verifies `worker2` can now construct, start, and execute partitions using warm cached blobs.
- `shouldKeepClasspathInputsAndOutputsPinnedUnderZeroFreeSpaceAndAllowCacheInsideUserDir`:
  - Configures worker with extreme buffer (`Long.MAX_VALUE / 2`) over pre-seeded unpinned blobs + multi-artifact partition (both in-process and forked with outputs), asserting pre-seeded unpinned blobs are evicted while active classpath/input/output blobs succeed without `NoSuchFileException`.
  - Verifies `WorkingDirectorySnapshot` does not report CWD violation when `cacheDir` is a subdirectory (or symlinked path) under `user.dir`.

- [ ] **Step 2: Run worker tests to verify failure**

Run: `./mvnw test -Dtest=BrokerBlobClientCacheTest,DistributionWorkerClientTest -pl tools/citrus-test-distribution-worker`
Expected: FAIL (compilation / missing behavior).

- [ ] **Step 3: Implement `BrokerBlobClient` streaming, shared runner constructors, and `DistributionWorkerClient` lifecycle**

- In `BrokerBlobClient.java`:
  - Replace `BodyHandlers.ofByteArray()` with `BodyHandlers.ofInputStream()`.
  - Check `artifact.sizeBytes() > ClasspathPackager.MAX_BLOB_BYTES` before `ensureFreeSpaceForDownload`.
  - On HTTP `200`, inspect `response.headers().firstValueAsLong("Content-Length")`: if `> MAX_BLOB_BYTES`, close body and throw `IOException`; if `> expectedBytes`, call `cache.ensureFreeSpaceForDownload(contentLength)`.
  - Stream `64 KiB` chunks into `try (OutputStream out = Files.newOutputStream(tmp))` checking `totalBytes <= MAX_BLOB_BYTES` on every read; after `out` closes, call `cache.promoteDownload(sha256, tmp)`; delete `tmp` in `finally` (`Files.deleteIfExists(tmp)`).
- In `IsolatedPartitionRunner.java` and `ForkedPartitionRunner.java`:
  - Change `IsolatedPartitionRunner` constructors to package-private; add `(config, cache, blobs)` constructors to both runners; have 1-arg test constructors open a locked `WorkerFileCache` and `BrokerBlobClient` inside `execute()` (`try (WorkerFileCache c = ...; BrokerBlobClient b = ...)`).
  - Collect all `classpathArtifacts` + `inputArtifacts` digests and acquire `WorkerFileCache.PinLease` *before* calling `blobs.ensureCached(artifact)`.
  - In `IsolatedPartitionRunner`, pass `excludeDirs` covering both `config.cacheDir()` and `cache.rootDir()` to `WorkingDirectorySnapshot` (extend `WorkingDirectorySnapshot.take(Path root, Path... excludedDirs)` if needed) so canonicalized symlinks inside `user.dir` are also excluded.
  - In `ForkedPartitionRunner`, package modified outputs via `cache.stageAndPinOutput(fork.workspaceDir, rawOutput, inputBaseline, lease::pin)` so output blobs are pinned before `blobs.uploadMissing(toUpload)`, release `PinLease` after upload, and run `cache.evictAfterPartition()` after `fork.close()`.
- In `DistributionWorkerClient.java`:
  - Open `WorkerFileCache` in constructor, create shared `BrokerBlobClient`, pass both to `inProcessRunner` and `forkedRunner` (closing `cache`/`blobClient` if constructor throws).
  - In `close()`, drain, cancel assignments, close WebSocket, shut down `scheduler` and `partitionExecutor` (`partitionExecutor.awaitTermination(5, TimeUnit.SECONDS)`), then close `blobClient`, `httpClient`, and `workerCache` in nested `try/finally`.

- [ ] **Step 4: Run all worker tests to verify pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker`
Expected: PASS (`0` failures).

- [ ] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution-worker
git commit -m "feat(distribution): stream worker blob downloads and wire shared WorkerFileCache"
```

---

### Task 4: JBang CLI & Citrus Agent Worker Cache Options and Schema

**Files:**
- Modify: `core/citrus-api/src/main/java/org/citrusframework/api/agent/CitrusAgentConfiguration.java`
- Modify: `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json`
- Modify: `tools/agent/src/main/java/org/citrusframework/agent/CitrusAgentOptions.java`
- Modify: `tools/agent/src/main/java/org/citrusframework/agent/DistributionWorkerBridge.java`
- Create: `tools/agent/src/test/java/org/citrusframework/agent/CitrusAgentWorkerOptionsTest.java`
- Modify: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/CitrusJBangMain.java`
- Modify: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistWorkerStart.java`
- Modify: `tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/DistCommandTest.java`

**Interfaces:**
- Consumes: `DistributionSettings.parseByteSize`, `DistributionSettings.parseStrictBoolean`, `DistributionWorkerConfig.Builder`
- Produces:
  - `CitrusAgentConfiguration`:
    - `Boolean getCacheCleanup()` / `void setCacheCleanup(Boolean cacheCleanup)` (`@SchemaProperty(description = "Enable or disable cleanup of the worker local file cache (worker mode)")`)
    - `String getCacheFreeSpaceBufferSize()` / `void setCacheFreeSpaceBufferSize(String cacheFreeSpaceBufferSize)` (`@SchemaProperty(description = "Minimum free space to keep on the worker cache drive, e.g. 2GiB or 512MiB (worker mode)")`)
  - `DistWorkerStart`:
    - `public static String[] normalizeWorkerStartArgs(String[] args)` (validates and normalizes `dist worker start` cache flags before Aesh execution; throws `IllegalArgumentException` on malformed/ambiguous flags)
    - Package-private `DistributionWorkerConfig buildWorkerConfig()`
    - CLI options in `--help`: `--cache-dir`, `--cache-cleanup`, `--no-cache-cleanup`, `--cache-free-space-buffer-size`
  - `CitrusAgentOptions` & `DistributionWorkerBridge`:
    - Exact `--<name>` / `--<name>=<value>` matching for `--cache-dir`, `--cache-cleanup`, `--no-cache-cleanup`, `--cache-free-space-buffer-size`, plus rejection of unknown `--cache*` / `--no-cache*` flags with `IllegalArgumentException`
    - Package-private `static DistributionWorkerConfig buildWorkerConfig(CitrusAgentConfiguration config)` in `DistributionWorkerBridge`

- [ ] **Step 1: Write failing CLI and schema tests in `CitrusAgentWorkerOptionsTest.java` and `DistCommandTest.java`**

Create `CitrusAgentWorkerOptionsTest.java` testing `CitrusAgentOptions` + `DistributionWorkerBridge.buildWorkerConfig`:
- Parameterized valid flag matrix:
  - `--cache-dir /tmp/c1` and `--cache-dir=/tmp/c1`
  - `--cache-cleanup` (`true`), `--no-cache-cleanup` (`false`), `--cache-cleanup=false` (`false`), `--cache-cleanup false` (`false`), `--no-cache-cleanup=false` (`true`), `--no-cache-cleanup false` (`true`)
  - Last-flag-wins: `--no-cache-cleanup --cache-cleanup` -> `true`; `--cache-cleanup --no-cache-cleanup` -> `false`
  - Following flag preserved: `--no-cache-cleanup --broker-url http://broker:9000` -> `cacheCleanup == false` and `brokerUrl == "http://broker:9000"`
  - `--cache-free-space-buffer-size 512MiB` and `--cache-free-space-buffer-size=512MiB` -> `536_870_912L`
  - Unset CLI options leave `DistributionWorkerConfig` inheriting system property / default values
- Parameterized invalid flag matrix throwing `IllegalArgumentException`:
  - `--cache`, `--cache-foo`, `--cache-cleanup flase`, `--cache-cleanup=flase`, `--no-cache-cleanup=maybe`, `--cache-dir`, `--cache-dir=""`, `--cache-dir --broker-url http://x`, `--cache-free-space-buffer-size`, `--cache-free-space-buffer-size=""`, `--cache-free-space-buffer-size --broker-url http://x`, `--cache-free-space-buffer-size=2GB`

Add matching parameterized CLI tests in `DistCommandTest.java`:
- Help output of `dist worker start --help` contains `--cache-dir`, `--cache-cleanup`, `--no-cache-cleanup`, and `--cache-free-space-buffer-size`.
- Valid flag matrix via `DistWorkerStart.normalizeWorkerStartArgs` + bounded `main.execute("dist", "worker", "start", ...)` run with `--no-cache-cleanup` and `--cache-free-space-buffer-size=64MiB`.
- Invalid flag matrix (`--cache`, `--cache-cleanup flase`, `--cache-dir --broker-url http://x`, `--cache-free-space-buffer-size=""`) returns non-zero exit code from `main.execute(...)` and prints error output.

- [ ] **Step 2: Run schema, agent, and JBang tests to verify failure**

Run: `./mvnw test -Dtest=CitrusSchemaGeneratorTest,CitrusAgentWorkerOptionsTest,DistCommandTest -pl core/citrus-api,tools/schema-generator,tools/agent,tools/jbang -am -DfailIfNoTests=false`
Expected: FAIL (compilation failure on new configuration/CLI symbols).

- [ ] **Step 3: Implement `CitrusAgentConfiguration`, control JSON schema, `CitrusAgentOptions`, `DistributionWorkerBridge`, and `DistWorkerStart`**

- Add `cacheCleanup` (`Boolean`, default `null` when unset on CLI/JSON) and `cacheFreeSpaceBufferSize` (`String`, default `null`) with `@SchemaProperty` annotations on `CitrusAgentConfiguration.java`.
- Update `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json` to match `CitrusSchemaGenerator` output exactly.
- In `CitrusAgentOptions.java`:
  - Override `apply(String[] arguments)` to pass `new CitrusAgentConfiguration()`.
  - Replace the `--cache-dir` `CliOption` and add exact-match `CliOption` implementations (overriding `processOption`) for `--cache-dir`, `--cache-cleanup`, `--no-cache-cleanup`, and `--cache-free-space-buffer-size`:
    - Exact match on `arg.equals("--<name>")` or `arg.startsWith("--<name>=")`.
    - For `--cache-dir` and `--cache-free-space-buffer-size`: reject missing/blank value or next token starting with `-` with `IllegalArgumentException` (without consuming a following `-...` flag); validate `--cache-free-space-buffer-size` immediately via `DistributionSettings.parseByteSize`.
    - For `--[no-]cache-cleanup`: if `=val` is present, parse via `DistributionSettings.parseStrictBoolean` (inverting for `--no-cache-cleanup`); if bare, peek `remainingArgs.peekFirst()` and consume only if non-null and not starting with `-`, parsing strictly via `parseStrictBoolean`.
  - In `CitrusAgentOptions.apply`, if no `CliOption` matched `arg` and `(arg.startsWith("--cache") || arg.startsWith("--no-cache"))`, throw `IllegalArgumentException("Unknown cache option: " + arg)`.
- In `DistributionWorkerBridge.java`, extract package-private `static DistributionWorkerConfig buildWorkerConfig(CitrusAgentConfiguration config)` wiring `cacheDir`, `cacheCleanup` (when non-null), and `cacheFreeSpaceBufferBytes` (when `StringUtils.hasText`, via `DistributionSettings.parseByteSize`).
- In `DistWorkerStart.java` and `CitrusJBangMain.java`:
  - Add `@Option(name = "cache-cleanup", description = "Enable or disable cleanup of the local file cache (true/false)") String cacheCleanup;`, `@Option(name = "no-cache-cleanup", hasValue = false, description = "Disable cleanup of the local file cache") boolean noCacheCleanup;`, and `@Option(name = "cache-free-space-buffer-size", description = "Minimum free space to keep on the cache drive (e.g. 2GiB, 512MiB, or bytes)") String cacheFreeSpaceBufferSize;`.
  - Implement `DistWorkerStart.normalizeWorkerStartArgs(String[] args)` handling `args.length >= 3 && "dist".equals(args[0]) && "worker".equals(args[1]) && "start".equals(args[2])` (skipping validation only if `--help` or `-h` is present), normalizing all `--[no-]cache-cleanup` forms to a single last-wins `--cache-cleanup=true|false`, validating `--cache-dir` and `--cache-free-space-buffer-size`, and rejecting unknown `--cache*`/`--no-cache*` flags with `IllegalArgumentException`.
  - Call `DistWorkerStart.normalizeWorkerStartArgs(args)` in `CitrusJBangMain.execute(String... args)`, catching `IllegalArgumentException` to print the message to `printer()` and return `CommandResult.FAILURE.getExitCode()`.

- [ ] **Step 4: Run schema, agent, and JBang tests to verify pass**

Run: `./mvnw test -pl core/citrus-api,tools/schema-generator,tools/agent,tools/jbang -am`
Expected: PASS (`0` failures, including `CitrusSchemaGeneratorTest`).

- [ ] **Step 5: Commit**

```bash
git add core/citrus-api tools/schema-generator tools/agent tools/jbang
git commit -m "feat(distribution): add worker cache CLI options to JBang and Citrus Agent"
```

---

### Task 5: End-to-End Warm Cache Verification, Documentation, and Spec Wording Hygiene

**Files:**
- Modify: `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/DistributedLauncherTest.java`
- Modify: `src/manual/tools-test-distribution.adoc`
- Modify: `docs/superpowers/specs/2026-11-21-test-distribution-external-resources-design.md`

**Interfaces:**
- Consumes: `DistributionWorkerConfig`, `DistributionWorkerClient`, `DistributedLauncher`, `DistributionSettings`
- Produces: End-to-end warm-cache reuse + extension-off regression tests, updated AsciiDoc user manual, and zero vendor mentions across the repository.

- [ ] **Step 1: Write E2E warm-cache reuse and extension-off tests in `DistributedLauncherTest.java`**

Add tests in `DistributedLauncherTest.java`:
- `shouldReuseWorkerLocalFileCacheAcrossSessionsAndPersistAfterWorkerRestart`:
  - Starts broker + worker 1 configured with explicit `workerCacheDir` (`cacheCleanup = true`, `cacheFreeSpaceBufferBytes = 0L`).
  - Runs session 1 with `src/test/resources/data.txt` input and verifies test passes and `<workerCacheDir>/blobs/` + `<workerCacheDir>/.lock` exist with zero files in `<workerCacheDir>/tmp/`.
  - Stops worker 1, deletes the blob from the *broker's* blob cache directory (`brokerCacheDir.resolve("blobs")`) to prove session 2 cannot re-download inputs from the broker, starts worker 2 on the same `workerCacheDir`, and runs session 2 with the identical input artifact — asserting session 2 passes purely from the warm worker local file cache!
- `shouldNotCreateDefaultWorkerCacheWhenDistributionDisabled`:
  - Overrides `user.home` to an isolated subdirectory under `tempDir` in `try/finally`, sets `citrus.test.distribution.enabled=false`, executes a local fixture via JUnit `Launcher`, and asserts `<isolatedUserHome>/.citrus-test-distribution-agent` does not exist.

- [ ] **Step 2: Run E2E tests in `citrus-test-distribution`**

Run: `./mvnw test -Dtest=DistributedLauncherTest -pl tools/citrus-test-distribution -am`
Expected: PASS (`0` failures).

- [ ] **Step 3: Update `src/manual/tools-test-distribution.adoc` and commit feature docs + E2E tests**

Update `src/manual/tools-test-distribution.adoc`:
- In `== Worker Pool and Citrus Agent Mode` (around lines 306–329), show `--cache-dir`, `--[no-]cache-cleanup`, and `--cache-free-space-buffer-size` on both `citrus dist worker start` and `citrus-agent.jar --mode worker`, and add an admonition warning that sharing a local file cache directory across multiple workers on the same host is not supported (workers enforce an exclusive `.lock` and require distinct `--cache-dir` paths or container isolation when running >1 worker per host).
- In `== Configuration Reference` (around lines 468–499):
  - Replace the stale sentence `Workers keep their own cache in <cacheDir>/worker...` with the dedicated worker cache default `${user.home}/.citrus-test-distribution-agent/file-cache` and note that `citrus.test.distribution.cacheDir` governs only coordinator/launcher caches.
  - Add rows to `=== Worker settings` for `citrus.test.distribution.worker.cacheDir` (`CITRUS_DIST_WORKER_CACHE_DIR` / `TEST_DISTRIBUTION_AGENT_CACHE_DIR`, default `${user.home}/.citrus-test-distribution-agent/file-cache`), `citrus.test.distribution.worker.cacheCleanup` (`CITRUS_DIST_WORKER_CACHE_CLEANUP` / `TEST_DISTRIBUTION_AGENT_CACHE_CLEANUP`, default `true`), and `citrus.test.distribution.worker.cacheFreeSpaceBufferSize` (`CITRUS_DIST_WORKER_CACHE_FREE_SPACE_BUFFER_SIZE` / `TEST_DISTRIBUTION_AGENT_CACHE_FREE_SPACE_BUFFER_SIZE`, default `2GiB`, supporting `KiB`, `MiB`, `GiB`, or bytes, with shared-drive sizing guidance).
- In `== Known Limitations` (around line 515), remove `* Worker caches are not evicted automatically.` and note cross-restart tie-breaking is deterministic approximate LRU when filesystem `mtime`s tie.

Commit E2E tests and manual updates:
```bash
git add tools/citrus-test-distribution src/manual/tools-test-distribution.adoc
git commit -m "test(distribution): verify warm worker file cache reuse and update manual"
```

- [ ] **Step 4: Remove historical vendor wording in a standalone `docs:` commit and run full verification**

Edit lines 6 and 69 of `docs/superpowers/specs/2026-11-21-test-distribution-external-resources-design.md` to replace the 2 third-party product references with neutral phrasing (`distributed test execution setups` / `other distributed test runners`).

Verify zero third-party vendor mentions across the entire repository (`0` matches in `git grep -i`) and run full multi-module verification:
- Run: `./mvnw verify -pl core/citrus-api,tools/schema-generator,tools/citrus-test-distribution-protocol,tools/citrus-test-distribution-coordinator,tools/citrus-test-distribution-worker,tools/citrus-test-distribution,tools/maven/citrus-test-distribution-maven-extension,tools/agent,tools/jbang` -> Expected: `BUILD SUCCESS` (`0` failures).

Commit standalone wording cleanup:
```bash
git add docs/superpowers/specs/2026-11-21-test-distribution-external-resources-design.md
git commit -m "docs(distribution): use vendor-neutral terminology in external resources spec"
```
