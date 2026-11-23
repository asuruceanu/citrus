# Test Distribution Worker Local File Cache — Design Specification

**Date:** 2026-11-22
**Target Branch Base:** `origin/incubator/test-distribution` (`1b4cb519ef`, after PR #4 "run the full repo distributed with parity to local")

> **Rebase note (2026-11-23):** PR #4 changed three things this design depends on. (a) `BlobCacheDirectory.materializeForClasspath` now publishes each JAR under its original file name as `<cacheDir>/classpath/<sha256>/<name>.jar`: a hard link to the blob, or a copy where links are unsupported. It stages through `.staging-*.jar` temp files. (b) The default worker fork mode is now `ALWAYS`, so `ForkedPartitionRunner` is the main execution path. (c) `JdbcDistributionStore` migrates columns through one generic `ensureColumns` `additions` map. Sections 5.3, 5.4, 5.6, and 5.7 below are updated for these changes.
**Status:** Approved

---

## 1. Intent & Why

Citrus Test Distribution workers download classpath JARs, packaged `target/test-classes` archives, and external resource input archives from the coordinator into a local content-addressed SHA-256 directory (`BlobCacheDirectory`). Today:

1. Worker caches default to a working-directory-relative path (`.citrus-dist/cache/worker`), scattering duplicate caches across directories whenever workers start from different locations and risking accidental interference with working-directory checks.
2. Worker caches never evict entries, eventually exhausting disk space on long-lived CI/VM worker hosts.
3. Multiple workers accidentally pointed at the same `--cache-dir` have no lock guard against concurrent eviction or corruption.
4. `IsolatedPartitionRunner` and `ForkedPartitionRunner` construct separate `BlobCacheDirectory` and `BrokerBlobClient` instances, and `BrokerBlobClient` buffers full blob downloads in heap (`BodyHandlers.ofByteArray()`) rather than streaming bounded chunks to a closed file before atomic cache insertion.

This feature implements a **Worker Local File Cache**: a deterministic user-home default cache directory, fail-fast exclusive per-worker directory ownership, bounded streaming downloads with closed handles prior to atomic promotion, attribute-stamped blob verification, active-partition blob pinning, and automatic LRU eviction governed by a configurable disk free-space buffer (`--cache-free-space-buffer-size`, default `2 GiB`) and cleanup toggle (`--[no-]cache-cleanup`, default `true`).

---

## 2. Goals, Non-Goals & Anti-Goals

### Goals
- Support the three worker cache settings across `DistributionWorkerConfig`, `DistributionSettings`, `citrus jb dist worker start`, and `citrus-agent --mode worker`:
  - `--cache-dir DIR`
  - `--[no-]cache-cleanup[=true/false]`
  - `--cache-free-space-buffer-size SIZE` (`KiB`, `MiB`, `GiB`, or raw bytes)
- Default the worker cache directory to `${user.home}/.citrus-test-distribution-agent/file-cache`, `cacheCleanup` to `true`, and `cacheFreeSpaceBufferSize` to `2 GiB` (`2,147,483,648` bytes).
- Support Citrus system properties (`citrus.test.distribution.worker.cache*`) and Citrus environment variables (`CITRUS_DIST_WORKER_CACHE_*`) as primary configuration keys, with `TEST_DISTRIBUTION_AGENT_CACHE_*` environment variables accepted as fallback aliases.
- Enforce exclusive single-worker ownership of `<cacheDir>` across OS processes and within the same JVM with actionable operator error guidance.
- Share a single `WorkerFileCache` and `BrokerBlobClient` owned and closed by `DistributionWorkerClient` across both `IsolatedPartitionRunner` and `ForkedPartitionRunner`, with package-private runner constructors preventing external unlocked instantiation.
- Stream blob downloads via bounded `64 KiB` chunks capped at `ClasspathPackager.MAX_BLOB_BYTES` (`256 MiB`) into `<cacheDir>/tmp/`, closing file handles deterministically before atomic promotion into `blobs/`, and self-heal corrupt or externally mutated entries.
- Evict unpinned cached blobs in Least-Recently-Used (LRU) order whenever usable disk space on the cache's `FileStore` drops below `cacheFreeSpaceBufferSize`, while strictly pinning blobs across download, classloading, child JVM execution, and output upload.
- Report sanitized `WorkerCacheStats` telemetry (`cleanupEnabled`, `freeSpaceBufferBytes`, `blobCount`, `cachedBytes`, `usableDiskBytes`, `evictedBlobsTotal`, `lowDiskSpace` — omitting host filesystem paths) on worker registration and heartbeats, persist it in `JdbcDistributionStore`, and render local file cache health and low-disk alerts in the Coordinator Web UI (`Workers Pool` and `Overview`).
- Enforce zero external vendor product mentions across all new code, tests, specs, and user manual updates; isolate the 2-line cleanup of historical `docs/superpowers/specs/2026-11-21-test-distribution-external-resources-design.md` into a separate `docs:` commit (or standalone doc PR).

### Non-Goals
- Time-based retention flags (`--cache-cleanup-interval`, `--cache-retention-period`): intentionally omitted in favor of free-space buffer eviction.
- Automatic eviction for the Maven host launcher cache (`target/.citrus-dist/cache`, cleaned by `mvn clean`) or the coordinator cache (`~/.citrus/dist-broker-cache`).
- Cross-worker shared local caches or cross-process host disk admission daemons: each worker on a multi-worker host must configure its own `--cache-dir`, and operators size `--cache-free-space-buffer-size` to cover concurrent workers sharing a mount.

### Anti-Goals
- Never derive random per-`workerId` default cache subdirectories under `${user.home}` (which would defeat cache reuse across restarts when `--worker-id` is omitted and leak unbounded directories).
- Never evict a JAR, input archive, or staged output archive while an active in-process `PartitionClassLoader` or forked child JVM is downloading, reading, or uploading it.
- Never hard-fail a partition upfront solely because usable disk space remains below the `2 GiB` soft buffer after all unpinned blobs are evicted; fail only on actual `IOException`/`ENOSPC` while cleaning up incomplete files.
- Never rely on a persistent sidecar metadata index that can drift out of sync with actual files on disk after `SIGKILL` (`kill -9`) or power loss.

---

## 3. Constraints & Accepted Limitations

- **Java 21+ & Existing Dependencies Only:** Use `java.nio.file` (`FileChannel`, `FileLock`, `FileStore`, `Files.walkFileTree`, `BasicFileAttributes`, `FileTime`), `java.net.http.HttpClient`, Aesh (in `tools/jbang`), and `CitrusAppOptions` (in `tools/agent`).
- **Local Filesystem Requirement:** `<cacheDir>` must reside on a local filesystem (or container volume/bind mount) supporting kernel `FileLock` (`fcntl`/`LockFileEx`) and same-filesystem rename between `<cacheDir>/tmp` and `<cacheDir>/blobs`. Mounts that reject `FileChannel.tryLock()` (such as lockless NFSv3) fail fast on `open()`.
- **Deterministic Approximate LRU Across Restarts:** Within a worker session, `(lastModifiedMillis, accessSequence)` provides exact LRU ordering. Across worker restarts, `accessSequence` resets; pre-existing blobs whose pre-restart `mtime` values fall in distinct filesystem timestamp quanta retain exact order, while sub-quantum ties break deterministically by `sha256` ascending.
- **Shared-Volume Multi-Worker Disk Accounting:** `FileStore.getUsableSpace()` reflects mount-wide unprivileged available space. Because $W$ independent workers on the same volume check usable space independently, operators running $W > 1$ workers on one drive must configure distinct `--cache-dir` paths and size drive free space / `--cache-free-space-buffer-size` to absorb concurrent downloads and workspaces.
- **`CitrusAgentConfiguration` JSON Schema Control:** Optional properties added to `CitrusAgentConfiguration` (`core/citrus-api`) must update `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json`.

---

## 4. Approaches Considered

1. **Filesystem `mtime` LRU + Attribute Stamps in `WorkerFileCache` (`citrus-test-distribution-worker`) — Selected (User Decision)**
   - Keep `BlobCacheDirectory` (`citrus-test-distribution-protocol`) focused on content-addressed storage primitives (`BlobEntry`, `listBlobs`, `touch`, `delete`, `sweepTempFiles`), and implement `.lock` ownership, `VerifiedBlobStamp` tracking, reference-counted `PinLease`, injectable `DiskSpaceProbe`, and byte-batched LRU eviction in `WorkerFileCache`.
   - *Why chosen:* Crash-proof (`kill -9` leaves zero stale metadata index), keeps `protocol` lightweight, avoids cross-object eviction callback races, and walks `blobs/` only when usable space drops below the buffer.

2. **Persistent Sidecar Metadata Index (`<cacheDir>/cache-index.json`) — Rejected**
   - Avoids directory walks at 100k+ blobs during pressure, but adds write amplification on read hits and still requires startup disk reconciliation after crashes.

3. **All-in-One Managed `BlobCacheDirectory` in `citrus-test-distribution-protocol` — Rejected**
   - Leaks worker-specific `.lock`, partition pinning, and verification-stamp concerns into the shared protocol module used by Maven host launchers and the coordinator.

---

## 5. Detailed Design

### 5.1 Configuration Keys, CLI Flags & Precedence

| Setting | CLI Flag (`jbang` & `citrus-agent`) | System Property | Primary Env Var | Fallback Env Alias | Default |
|---|---|---|---|---|---|
| Cache directory | `--cache-dir DIR` | `citrus.test.distribution.worker.cacheDir` | `CITRUS_DIST_WORKER_CACHE_DIR` | `TEST_DISTRIBUTION_AGENT_CACHE_DIR` | `${user.home}/.citrus-test-distribution-agent/file-cache` |
| Cache cleanup enabled | `--[no-]cache-cleanup[=true/false]` | `citrus.test.distribution.worker.cacheCleanup` | `CITRUS_DIST_WORKER_CACHE_CLEANUP` | `TEST_DISTRIBUTION_AGENT_CACHE_CLEANUP` | `true` |
| Free-space buffer size | `--cache-free-space-buffer-size SIZE` | `citrus.test.distribution.worker.cacheFreeSpaceBufferSize` | `CITRUS_DIST_WORKER_CACHE_FREE_SPACE_BUFFER_SIZE` | `TEST_DISTRIBUTION_AGENT_CACHE_FREE_SPACE_BUFFER_SIZE` | `2 GiB` (`2147483648`) |

**Resolution Precedence:**
1. Explicit CLI flag / `DistributionWorkerConfig.Builder` setter
2. Worker Java system property (`citrus.test.distribution.worker.*`)
3. Primary Citrus worker environment variable (`CITRUS_DIST_WORKER_*`)
4. Fallback agent environment variable (`TEST_DISTRIBUTION_AGENT_*`)
5. Built-in default

**Blank vs Invalid Semantics:**
- **System properties & environment variables:** `null` or `isBlank()` values are treated as **unset** and fall through to the next precedence tier. Any **non-blank invalid** value throws `IllegalArgumentException` immediately (naming the source key and raw value) and **never** falls through silently.
- **CLI flags:** Passing `--cache-dir`, `--cache-free-space-buffer-size`, or `--[no-]cache-cleanup=` with a missing or blank value fails CLI parsing immediately.
- **`DistributionWorkerConfig.Builder`:** `cacheDir(Path)` requires non-null; `cacheFreeSpaceBufferBytes(long)` rejects `< 0`; `cacheFreeSpaceBufferSize(String)` parses via `parseByteSize` (rejecting null/blank/invalid); `cacheCleanup(boolean)` sets explicit override.
- **Legacy Global `cacheDir` Isolation:** `citrus.test.distribution.cacheDir` / `CITRUS_DIST_CACHE_DIR` (`DistributionSettings.getCacheDir()`) remain exclusively for host/coordinator and never affect `DistributionSettings.getWorkerCacheDir()`.

### 5.2 Parsing & Exact CLI Option Matching

- **Buffer Size (`DistributionSettings.parseByteSize(String raw, String source)`):**
  - Trims surrounding whitespace; allows optional whitespace between integer digits and suffix (`"2GiB"`, `"2 GiB"`, `"512MiB"`, `"1024KiB"`, `"2147483648"`, `"0"`).
  - Case-insensitive binary suffixes: none ($\times 1$), `KiB` ($\times 1{,}024$), `MiB` ($\times 1{,}048{,}576$), `GiB` ($\times 1{,}073{,}741{,}824$).
  - Rejects blank strings, negatives, decimals (`1.5GiB`), unknown/SI suffixes (`GB`, `MB`, `KB`, `TiB`), and `long` overflow (`Math.multiplyExact`) with `IllegalArgumentException` naming `source` and `raw`.
- **Cleanup Boolean (`DistributionSettings.parseBooleanStrict(String raw, String source)`):**
  - Case-insensitive `"true"` or `"false"` only; any other value throws `IllegalArgumentException` naming `source` and `raw`.
- **Exact CLI Parsing in `DistWorkerStart` (`tools/jbang`) & `CitrusAgentOptions` (`tools/agent`):**
  - All three worker cache flags match **exact** option names (`--<name>` or `--<name>=<value>`), overriding `CitrusAppOptions.CliOption.processOption` so prefix `--cache` never ambiguously matches `--cache-dir`, `--cache-cleanup`, or `--cache-free-space-buffer-size`.
  - `--cache-dir` and `--cache-free-space-buffer-size` accept `--flag value` or `--flag=value`; if missing, blank, or immediately followed by another `-`/`--` option token, fail fast without consuming the following flag.
  - `--[no-]cache-cleanup[=true/false]` supports:
    - `--cache-cleanup`, `--cache-cleanup=true`, `--cache-cleanup true` -> `true`
    - `--cache-cleanup=false`, `--cache-cleanup false` -> `false`
    - `--no-cache-cleanup`, `--no-cache-cleanup=true`, `--no-cache-cleanup true` -> `false`
    - `--no-cache-cleanup=false`, `--no-cache-cleanup false` -> `true`
  - When `--[no-]cache-cleanup` is passed without `=`:
    - If at end-of-args or the next token starts with `-` -> bare flag (`--cache-cleanup` -> `true`, `--no-cache-cleanup` -> `false`), leaving the next `-`/`--` flag unconsumed.
    - If the next token does **not** start with `-` (since neither `dist worker start` nor `citrus-agent` accepts positional arguments) -> consumed as the boolean value and validated with `parseBooleanStrict`, so typos like `--cache-cleanup flase` fail fast with a clear `--[no-]cache-cleanup` error.
  - When multiple `--[no-]cache-cleanup` flags appear, **last flag wins**.

### 5.3 Directory Layout, Exclusive Ownership & Lifecycle (`WorkerFileCache`)

```text
<cacheDir>/
  .lock                       # Persistent 0-byte lock file (never unlinked on close)
  tmp/                        # Ephemeral staging files (dl-*.tmp, blob-*.tmp, ws-*.tmp)
  blobs/
    <2-hex-prefix>/
      <64-hex-sha256>         # Immutable content-addressed artifact files
  classpath/
    <64-hex-sha256>/
      <name>.jar              # Named JAR (PR #4): hard link to blobs/../<sha256>, or a copy if links are unsupported
      .staging-*.jar          # Transient publish staging; orphans swept on open()
  extracted/ ...              # Per-partition workspace extraction (unchanged, outside cache accounting)
```

- **Named JARs are part of a digest's footprint:** Deleting only `blobs/<2-hex>/<sha256>` frees **no** disk space while the `classpath/<sha256>/<name>.jar` hard link still exists, and a copy fallback doubles the footprint. Every digest removal (eviction or corrupt-blob invalidation) therefore deletes `blobs/<2-hex>/<sha256>` **and** the whole `classpath/<sha256>/` directory, and pinning a digest also protects its named JAR.

- **Lock Acquisition & Partial-Open Rollback (`WorkerFileCache.open()`):**
  - Resolves `canonicalDir = Files.createDirectories(cacheDir).toRealPath()`.
  - Registers `canonicalDir` in static `Set<Path> ACTIVE_JVM_LOCKS` (`ConcurrentHashMap.newKeySet()`). If `add(canonicalDir)` returns `false`, throws `IllegalStateException` immediately with operator guidance before opening a `FileChannel`.
  - Inside a rollback guard (`try / catch (Throwable t)`): opens `<canonicalDir>/.lock` (`CREATE`, `WRITE`), calls `channel.tryLock()`, creates `blobs/` and `tmp/`, sweeps `<canonicalDir>/tmp/`, and runs initial `evictIfNeeded(0L)` when `cacheCleanup == true`.
  - If `tryLock()` returns `null` or throws `OverlappingFileLockException`, rolls back `channel` + `ACTIVE_JVM_LOCKS` and throws `IllegalStateException`:
    `"Worker local file cache directory '<canonicalDir>' is already locked by another worker. Sharing a local file cache across multiple workers on the same host is not supported; configure a distinct directory per worker via --cache-dir, -Dcitrus.test.distribution.worker.cacheDir, or CITRUS_DIST_WORKER_CACHE_DIR (alias TEST_DISTRIBUTION_AGENT_CACHE_DIR), or run each worker in its own container."`
  - If `tryLock()` throws `IOException` (e.g., lockless network mount), rolls back `channel` + `ACTIVE_JVM_LOCKS` and throws `IllegalStateException` naming `<canonicalDir>/.lock` and requiring a local lock-capable filesystem.
  - If any other error occurs during `open()`, releases `FileLock`, closes `FileChannel`, and removes `canonicalDir` from `ACTIVE_JVM_LOCKS` before rethrowing.
- **Crash Recovery:** OS kernel automatically releases `FileLock` on `SIGKILL` / `OOMKilled` / power loss; `.lock` is never unlinked on `close()` so concurrent POSIX openers always contend on the same inode.
- **Ownership & Ordered Shutdown (`DistributionWorkerClient`):**
  - `DistributionWorkerClient` is the sole `public` worker entry point; `IsolatedPartitionRunner` and `ForkedPartitionRunner` constructors are **package-private**.
  - `DistributionWorkerClient` owns one shared `WorkerFileCache` and one shared `BrokerBlobClient`. `start()` opens `WorkerFileCache` before connecting WebSocket or starting heartbeats (resetting `running` if `open()` throws).
  - `close()` cancels/drains active partitions, shuts down `scheduler` and `partitionExecutor` (awaiting bounded executor termination so any active runner `finally` completes), closes `HttpClient`s, and finally calls `WorkerFileCache.close()`.
  - `WorkerFileCache.close()` uses nested `try / finally` blocks so `lock.release()`, `channel.close()`, and `ACTIVE_JVM_LOCKS.remove(canonicalDir)` each execute even if an earlier step throws, and is idempotent on repeated calls.
  - Package-private 1-arg test convenience constructors (`IsolatedPartitionRunner(config)`, `ForkedPartitionRunner(config)`) open and close a locked `WorkerFileCache` around each `execute(...)` call so tests also respect `.lock` exclusivity without leaking handles.

### 5.4 `BlobCacheDirectory` Primitives (`citrus-test-distribution-protocol`)

- `record BlobEntry(String sha256, Path path, long sizeBytes, long lastModifiedMillis)`
- `List<BlobEntry> listBlobs()` — walks `blobs/` (`Files.walkFileTree`, `maxDepth = 2`, `Set.of()` no symlink following), reusing `visitFile(Path, BasicFileAttributes)` directly (1 stat per file), ignoring non-digest filenames and benign `NoSuchFileException`.
- `Optional<BasicFileAttributes> touchAndStat(String sha256, Instant timestamp)` / `touch(String sha256, Instant timestamp)` — updates `Files.setLastModifiedTime` on `blobs/<2-hex>/<sha256>` (`NOFOLLOW_LINKS`) if regular file exists.
- `boolean delete(String sha256)` — deletes `blobs/<2-hex>/<sha256>` if present, then recursively deletes `classpath/<sha256>/` (named JAR links/copies and staging files, `NOFOLLOW_LINKS`, confined under `rootDir`), and best-effort prunes empty `<2-hex>` directory (`DirectoryNotEmptyException` ignored). Returns `true` if the blob file existed.
- `int sweepClasspathStaging()` — deletes orphan `classpath/*/.staging-*.jar` files left by a crash during named-JAR publishing; runs on `WorkerFileCache.open()` alongside the `tmp/` sweep.
- The existing comment in `namedJar` ("Several worker JVMs may share the cache directory") is reworded: worker JVMs no longer share a cache directory once `.lock` ownership is enforced. Only the owning worker and its forked children (which read named-JAR paths but never write the cache) touch it.
- `int sweepTempFiles()` — strictly confined to `rootDir.resolve("tmp")` with `LinkOption.NOFOLLOW_LINKS` (never touching `blobs/` or `.lock`); deletes leftover children inside `tmp/` while keeping `tmp/` itself, logging `WARN` and continuing on any per-entry `IOException`. Runs unconditionally on `WorkerFileCache.open()`, even when `--no-cache-cleanup` is set.
- **Non-Atomic Move Fallback Hardening:** In `BlobCacheDirectory.moveIntoPlace(tmp, target)`, if `ATOMIC_MOVE` throws `AtomicMoveNotSupportedException` and the fallback `Files.move(tmp, target, REPLACE_EXISTING)` fails with `IOException`, delete any partial `target` (`Files.deleteIfExists(target)`) before rethrowing (unless `target` already existed with valid content).

### 5.5 Bounded Streaming Downloads & Attribute-Stamped Verification (`WorkerFileCache` + `BrokerBlobClient`)

- **Unified Verification Stamps in `WorkerFileCache`:**
  - `WorkerFileCache` maintains `ConcurrentHashMap<String, VerifiedBlobStamp> verifiedStamps` where `record VerifiedBlobStamp(long sizeBytes, FileTime lastModifiedTime, Object fileKey)`.
  - **Fast Path (`workerCache.getVerifiedOrTouch(sha256)`):** Reads `Files.readAttributes(blobPath, BasicFileAttributes.class, NOFOLLOW_LINKS)`. Hits fast path **only** when `attrs.isRegularFile()` and `(size, lastModifiedTime, fileKey)` match the recorded `VerifiedBlobStamp` — immediately catching deletion, symlink replacement, truncation/append, in-place modification, or inode replacement without re-reading file bytes. On hit, updates `touch` + `VerifiedBlobStamp` + monotonic `accessSequence`.
  - **Disk Verification Fallback:** On missing or mismatched stamp, delegates to `blobCache.getVerifiedPath(sha256)` (full streaming SHA-256 check; deletes corrupt file on mismatch). If valid, records `touch` + fresh `VerifiedBlobStamp` + `accessSequence`.
  - **Coherent Eviction Invalidation:** Inside `synchronized (evictionLock)`, `verifiedStamps.remove(sha256)` runs **before** `blobCache.delete(sha256)` with zero cross-object callbacks. If `blobCache.delete(sha256)` throws `IOException`, `WARN` is logged, the next candidate is tried, and any surviving file simply re-verifies on disk if accessed later.
- **Bounded Streaming Download (`BrokerBlobClient.ensureCached(ArtifactRef)` / `ensureCached(String)`):**
  1. If `ref.sizeBytes() > ClasspathPackager.MAX_BLOB_BYTES` (`256 MiB`), fail immediately with `IOException` before issuing HTTP `GET`.
  2. Check `workerCache.getVerifiedOrTouch(sha256)`; if non-null, return.
  3. Compute `expectedBytes = Math.clamp(sizeBytes, 0L, ClasspathPackager.MAX_BLOB_BYTES)` and run `workerCache.ensureFreeSpaceForDownload(expectedBytes)`.
  4. Send `GET /api/v1/cache/blobs/<sha256>` with `HttpResponse.BodyHandlers.ofInputStream()`.
     - Check HTTP `200` before creating/writing `tmp`.
     - Check `Content-Length`: if `> MAX_BLOB_BYTES`, abort with `IOException`; if `Content-Length > expectedBytes` (unknown/zero/underestimated `ArtifactRef.sizeBytes`), call `workerCache.ensureFreeSpaceForDownload(contentLength)` before streaming.
     - Stream body in `64 KiB` chunks into `try (OutputStream out = Files.newOutputStream(tmp))`, enforcing `totalBytes <= ClasspathPackager.MAX_BLOB_BYTES` on every chunk. `out` closes deterministically at exit of `try-with-resources` on all platforms before promotion.
     - Promote via `workerCache.putFromTempFile(sha256, tmp)` (which delegates to `blobCache.putFromFile`, verifying SHA-256, moving into `blobs/`, and recording `VerifiedBlobStamp` + `accessSequence`), retrying download once on transient hash mismatch.
     - `finally { Files.deleteIfExists(tmp); }` guarantees zero incomplete `.tmp` files survive `ENOSPC`, cap violations, or network drops.

### 5.6 Active-Partition Pinning & Byte-Batched LRU Eviction

- **Pin-Before-Download Through Completion (`WorkerFileCache.PinLease`):**
  - `IsolatedPartitionRunner.execute(...)`: pins all `classpathArtifacts` digests **before** the first `blobs.ensureCached(...)` call and holds the `PinLease` until **after** `PartitionClassLoader.close()` finishes in `finally`.
  - `ForkedPartitionRunner.execute(...)`: pins the union of `classpathArtifacts` and `inputArtifacts` digests **before** the first `blobs.ensureCached(...)` call and holds the `PinLease` across materialization, child JVM execution, `fork.killAll()`, output packaging, and `blobs.uploadMissing(...)`.
  - **Zero-Gap Output Pinning:** `workerCache.packageModifiedOutput(workspace, outputPath, baseline, pinLease)` stages the output archive and registers its resulting `sha256` into `pinLease` + `verifiedStamps` atomically with respect to `evictionLock`, so newly packaged output blobs can never be evicted prior to `uploadMissing`.
- **Eviction Triggers (when `cacheCleanup == true`):**
  1. **Startup (`WorkerFileCache.open()`):** `evictIfNeeded(0L)` after `.lock` acquisition and `tmp/` sweep.
  2. **Pre-Download (`ensureFreeSpaceForDownload`):** `evictIfNeeded(expectedBytes)` (and again if `Content-Length > expectedBytes`).
  3. **Post-Partition (`DistributionWorkerClient` `finally`):** `evictIfNeeded(0L)` after the runner's `PinLease` has closed.
- **Byte-Batched LRU Eviction (`evictIfNeeded(long additionalBytesRequired)`):**
  - Default `DiskSpaceProbe` caches the resolved `FileStore` for `canonicalDir` (falling back to `Files.getFileStore(canonicalDir)` if needed) so probing calls `FileStore.getUsableSpace()` (`statvfs`) directly without re-parsing `/proc/mounts` on Linux.
  - Deficit check (overflow-safe): `deficit = freeSpaceBufferBytes - usableSpace + additionalBytesRequired` (using saturating `Math` arithmetic). If `deficit <= 0`, returns immediately without walking `blobs/`.
  - Eviction removes a digest via `blobCache.delete(sha256)`, which also drops `classpath/<sha256>/`. Without that, hard-linked named JARs would keep the inode alive and the loop would delete every candidate while freeing nothing.
  - Walks `blobCache.listBlobs()`, excludes digests in `pinnedCounts`, and sorts candidates by `(lastModifiedMillis ASC, accessSequence ASC, sha256 ASC)`.
  - Deletes candidates while accumulating `freedBytes += entry.sizeBytes()`, re-probing `diskSpaceProbe.usableSpace(canonicalDir)` only once `freedBytes >= deficit` or every `64` deletions (plus a final verification probe before exiting), keeping syscalls bounded even at `100k+` small blobs.
  - **Soft Buffer Shortfall vs Hard `ENOSPC`:** If unpinned candidates are exhausted while `usableSpace` remains below `freeSpaceBufferBytes + additionalBytesRequired`, logs a single rate-limited `WARN` per shortage episode (resetting once `usableSpace >= freeSpaceBufferBytes`) and returns so the partition may still succeed if actual free space suffices; actual `ENOSPC` fails at write time with `IOException`, cleans up `tmp/` and `partitionRoot` in `finally`, and fails the partition cleanly without crashing the worker daemon.

### 5.7 Worker Cache Telemetry & Web UI (`WorkerCacheStats`)

- **Protocol Wire Model (`citrus-test-distribution-protocol`):**
  - `public record WorkerCacheStats(boolean cleanupEnabled, long freeSpaceBufferBytes, int blobCount, long cachedBytes, long usableDiskBytes, long evictedBlobsTotal, boolean lowDiskSpace)` — intentionally omits host `cacheDir` paths so filesystem layout is never exposed via `/api/v1/workers`.
  - `WorkerRegisterPayload`, `WorkerHeartbeatPayload`, and `WorkerRecord` gain nullable trailing `WorkerCacheStats cacheStats` fields while preserving backward-compatible convenience constructors (`null` `cacheStats`) for older workers and existing callers.
- **`O(1)` Worker Snapshot (`WorkerFileCache.statsSnapshot()`):**
  - `WorkerFileCache` seeds `blobCount` and `cachedBytes` once during `open()` and maintains `blobCount`, `cachedBytes`, and `evictedBlobsTotal` incrementally under `evictionLock` across download promotion, forked output staging, corrupt-blob removal, and eviction — performing **zero** `listBlobs()` directory walks on heartbeats. `cachedBytes` counts blob bytes only. Hard-linked named JARs share the blob inode. Copy-fallback named JARs (filesystems without hard links) are not counted, which is an accepted under-count.
  - `statsSnapshot()` queries `spaceProbe.usableBytes(canonicalDir)` (returning `-1L` if `IOException` occurs so heartbeats never fail) and sets `lowDiskSpace = usableDiskBytes >= 0 && freeSpaceBufferBytes > 0 && usableDiskBytes < freeSpaceBufferBytes`.
  - `DistributionWorkerClient` attaches `workerCache.statsSnapshot()` to `WorkerRegisterPayload` and `WorkerHeartbeatPayload`.
- **Coordinator Persistence & Demo Seeding (`citrus-test-distribution-coordinator`):**
  - `JdbcDistributionStore` adds `cache_stats_json VARCHAR(2048)` to the `dist_workers` DDL, plus an additive migration registered as `additions.put("dist_workers", Map.of("cache_stats_json", "VARCHAR(2048)"))` in the existing generic `ensureColumns`, and round-trips `WorkerRecord.cacheStats()`.
  - `BrokerSessionCoordinator` records `cacheStats` on registration, updates it on heartbeats when non-null, and preserves it across state transitions.
  - `DemoDataSeeder` populates realistic `WorkerCacheStats` across demo workers (healthy warm caches, `--no-cache-cleanup`, and low-disk pressure with evictions).
- **Web UI (`citrus-test-distribution-ui`):**
  - `Workers Pool` cards display a `Local File Cache` section when `w.cacheStats` is present (`AUTO LRU` / `NO CLEANUP` policy badge, `LOW DISK` warning badge, cached bytes + blob count, usable disk space vs configured buffer bar with safe `0`-buffer / `-1`-unknown guards, and `evictedBlobsTotal`), plus a `Low Disk` filter pill and compact `LOW DISK` indicator in `Overview` (`Worker Pool Health`).

---

## 6. Entry Points & Documentation Touchpoints

1. **`tools/citrus-test-distribution-protocol`:** `DistributionSettings` (worker cache keys, precedence, `parseByteSize`, `parseBooleanStrict`), `BlobCacheDirectory` (`BlobEntry`, `listBlobs`, `touch`, `delete`, `sweepTempFiles`, `moveIntoPlace` fallback cleanup), `WorkerCacheStats`, and backward-compatible `cacheStats` field on `WorkerRegisterPayload`, `WorkerHeartbeatPayload`, and `WorkerRecord`.
2. **`tools/citrus-test-distribution-worker`:** `DistributionWorkerConfig`, `WorkerFileCache` (including `O(1)` `statsSnapshot()`), `BrokerBlobClient`, `DistributionWorkerClient`, package-private `IsolatedPartitionRunner` & `ForkedPartitionRunner` constructors.
3. **`tools/citrus-test-distribution-coordinator` & `tools/citrus-test-distribution-ui`:** `JdbcDistributionStore` (`cache_stats_json` column + migration), `BrokerSessionCoordinator`, `DemoDataSeeder`, and Web UI `Workers Pool` / `Overview` cache telemetry rendering (`app.js`, `styles.css`).
4. **`tools/jbang`:** `DistWorkerStart` + worker CLI boolean/option normalizer + `DistCommandTest`.
5. **`core/citrus-api`, `tools/agent`, `tools/schema-generator`:** `CitrusAgentConfiguration`, `citrus-agent-configuration.json`, `CitrusAgentOptions` (exact option matching + `--[no-]cache-cleanup` + validation), `DistributionWorkerBridge`.
6. **`src/manual/tools-test-distribution.adoc`:** Worker Local File Cache flags, properties, env vars + aliases, single-worker exclusivity requirement, and multi-worker shared-drive buffer sizing guidance.
7. **Separate Hygiene Commit / PR:** Remove the 2 vendor references in `docs/superpowers/specs/2026-11-21-test-distribution-external-resources-design.md` as an isolated `docs:` commit (or standalone doc PR) separate from cache implementation commits.

---

## 7. Minimum Verification Matrix & Performance Thresholds

### Verification Matrix
1. **Intra-JVM Lock Contention:** Two `DistributionWorkerClient`s in one JVM with identical `cacheDir` -> 2nd `start()` fails fast with actionable `--cache-dir` message; succeeds after 1st `close()`.
2. **Cross-JVM Lock Contention & Forced Crash (`destroyForcibly`):** Child JVM holds `.lock` and leaves orphan `<cacheDir>/tmp/dl-partial.tmp` -> parent `open()` fails while child alive; after `destroyForcibly()`, parent `open()` acquires `.lock`, sweeps `tmp/dl-partial.tmp`, and preserves valid `blobs/`.
3. **Corruption & External Mutation:** Pre-existing corrupt blob, externally truncated/modified blob after warm `VerifiedBlobStamp` hit, HTTP corrupt payload retry, and `> 256 MiB` (`MAX_BLOB_BYTES`) stream cap abort with zero leftover `tmp` files.
4. **Pinning Across Full Lifecycle:** Concurrent/simulated `usableSpace = 0` during active in-process `PartitionClassLoader` execution and during forked execution + output packaging/upload -> zero pinned inputs/JARs/outputs evicted; unpinned older blobs evicted; pinned blobs become evictable post-partition.
5. **Low Disk & `ENOSPC`:** Soft buffer shortfall (`usableSpace < buffer`, unpinned empty) logs once per episode and completes partition; hard `IOException("No space left on device")` during download cleans `tmp` and fails partition without terminating worker.
6. **CLI & Config Precedence (`jbang` & `citrus-agent`):** Exact flag matching (`--cache` rejected/not matched), `--cache-cleanup` / `--no-cache-cleanup` / `=true|false` / space `true|false` / typo `flase` / last-flag-wins / following `--broker-url` unconsumed; blank env/sysprop fallthrough vs invalid non-blank `IllegalArgumentException`; global `citrus.test.distribution.cacheDir` isolation.
7. **OS & E2E Coverage:** Path canonicalization, closed download stream before promotion (Windows-safe), warm-cache 0-redownload distributed E2E run, and extension-disabled local test execution unaffected.

### Performance & Memory Acceptance Thresholds
- **Download heap:** Fixed `64 KiB` stream buffer (`O(1)` heap per download up to `256 MiB`).
- **Warm-cache hit path:** `0` SHA-256 re-hashes and `0` directory walks when `usableSpace >= freeSpaceBufferBytes + expectedBytes`.
- **100k-blob eviction test:** Scanning/sorting `100,000` synthetic `BlobEntry` items and evicting `10,000` entries completes in `< 5 s` with `< 64 MiB` transient heap and bounded `DiskSpaceProbe` invocations (`<= ceil(deleted / 64) + 2` before byte deficit is met).
