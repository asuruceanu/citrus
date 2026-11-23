# Test Distribution Partition Overhead Reduction — Design Specification

**Date:** 2026-11-23
**Target Branch Base:** `origin/incubator/test-distribution` (`ad4833a390`, after PR #5)
**Status:** Draft (Pending Approval)

---

## 1. Intent & Why

Benchmark measurements on Citrus Test Distribution revealed substantial per-partition and Wave-1 overhead when `forkMode=ALWAYS` (default since PR #4) is active:

1. **Core benchmark (`citrus-api`, `citrus-base`, `citrus-spring` — 11,889 tests, 6 local workers):** Across 33 partitions, total partition wall time was `260.31s` while actual JUnit test execution time summed to `98.86s` — leaving **`161.45s` (`62%`, `~4.89s` per partition)** in non-test startup, classpath inspection, JUnit/context bootstrap, and workspace teardown overhead. Raising partition size from `10` to `100` on 2 workers cut `citrus-base` wall time from `81.0s` to `23.3s` (`-57.7s`).
2. **Full-repository unit suite (`47` modules, 6 local workers):** Distributed execution took `316s` vs `239s` for local Surefire (`+77s`). Because Maven executes reactor modules sequentially by default and `~40` of the `47` modules produce `P <= W` partitions on 6 workers, each worker runs at most one partition of a given module. Reusing a forked child JVM across partitions of the same module would therefore save **zero** JVM startups on `~40/47` modules (`~68s` of the `+77s` gap), cannot span Surefire (`workingDirectory="target"`, unpacked `target/classes`) and Failsafe (`workingDirectory=""`, packaged JAR), and complicates Testcontainers/Ryuk process-exit cleanup.

Instead of same-module JVM reuse, this specification addresses both Wave-1 and multi-wave overhead directly through four targeted optimizations:

1. **Bullet 1 (`ForkedPartitionRunner`):** Cache watched JAR resource presence (`RUNTIME_ANCHORS` + JUnit `CancellationToken`) by JAR file attributes and cache worker `codeSource` resolution so workers stop re-opening every classpath JAR (`new JarFile(...)`) before every partition.
2. **Bullet 2 (`WorkerWorkspaceRegistry` + `WorkspaceReferences`):** Replace unconditional full-host `/usr/sbin/lsof +D` scans (`~150–350ms` per partition on macOS) during normal unleased forked workspace cleanup with a post-launch `ProcessHandle` survivor check and targeted `lsof -p <pids>`, while preserving full `lsof +D` on startup recovery, leased workspaces, and in-process runs.
3. **Bullet 3a (`LptPartitionScheduler` + `BrokerSessionCoordinator`):** Apply adaptive partition sizing (minimum partition estimated duration floor `5,000ms` and per-worker wave cap `2 * W`) when the caller has not pinned an explicit `maxPartitionSize`, preventing fast suites from splitting into dozens of tiny forks while preserving LPT load-balancing headroom for slow suites.
4. **Bullet 3b (`DistributedLauncher` + Broker History API + Store Aggregation Fix):** Fix `JdbcDistributionStore.findHistoricalDurationsMs` so multi-method class work units aggregate total per-session work-unit duration instead of averaging single `@Test` method rows, expose `POST /api/v1/history/durations`, and let `DistributedLauncher` keep eligible `< 2,000ms` modules local in the Maven Surefire/Failsafe JVM (`executeTestPlanLocally` on the already-discovered `TestPlan`) while still reporting executions to the shared reactor session.

---

## 2. Goals, Non-Goals & Anti-Goals

### Goals
- Eliminate repeated `JarFile` opens on unchanged classpath JARs and repeated `Class.forName` / `CodeSource` URI resolution across forked partitions in `ForkedPartitionRunner`.
- Reduce normal clean forked workspace teardown on macOS from `~150–350ms` (`lsof +D`) to `< 1ms` (`0` subprocesses spawned) when no post-launch processes survive `ActiveFork.killAll()`, while still preserving workspaces when a surviving background process holds open descriptors/CWD inside `root` or `childTmpDir` (`~25ms` via `lsof -p`) or when a Unix domain socket remains in either directory.
- Consolidate fast/multi-wave LPT submissions automatically via broker-configurable `minPartitionDurationMs` (default `5,000ms`) and `maxWavesPerWorker` (default `2`) whenever the session uses broker-managed partition sizing (`CreateSessionRequest.maxPartitionSize <= 0` and `outputPaths` empty), while strictly honoring explicit `-Dcitrus.test.distribution.maxPartitionSize=<N>` overrides.
- Let `DistributedLauncher` and `TestDistributionLifecycleParticipant` omit hardcoded `5` (`maxPartitionSize = 0` in `CreateSessionRequest`) when the user has not explicitly configured `citrus.test.distribution.maxPartitionSize` / `CITRUS_DIST_MAX_PARTITION_SIZE`, so broker `defaultMaxPartitionSize` and adaptive sizing take effect out of the box.
- Expose `POST /api/v1/history/durations` on the coordinator and allow `DistributedLauncher` to run eligible `< 2,000ms` modules (`citrus.test.distribution.minRemoteDurationMs`, default `2,000ms`, plus opt-in `citrus.test.distribution.minRemoteColdUnits`, default `0`) locally prior to `ClasspathPackager` scanning, reporting results via `reportLocalTests` so session telemetry and rolling duration history remain accurate.
- Document all new configuration settings and the updated macOS workspace cleanup behavior in `src/manual/tools-test-distribution.adoc`.

### Non-Goals
- Reusing a persistent forked child JVM across multiple partitions of a module (deferred unless post-change benchmarks still show material JVM-boot overhead on multi-wave modules).
- Changing Linux `/proc` workspace reference detection (`LinuxWorkspaceReferences`).
- Running `@RemoteOnly` units, `@AgentRequirement`-constrained units, or modules declaring distribution `inputs` / `outputs` through the small-module local bypass.

### Anti-Goals
- Never cache `IOException` / corrupt-JAR failures in `ForkedPartitionRunner`; unreadable JARs must still fail immediately on use.
- Never delete a forked workspace on macOS while an unleased background process spawned during the partition still references `root` or `childTmpDir` (`WorkerWorkspaceRecoveryTest#shouldPreserveBothForkedDirectoriesWhenABackgroundProcessStillUsesOne`) or while an unattributed Unix socket file exists (`shouldPreserveWorkspaceWithAnUnattributedUnixSocketPath`).
- Never silently ignore an explicit user-supplied `citrus.test.distribution.maxPartitionSize` / `CITRUS_DIST_MAX_PARTITION_SIZE` setting.
- Never bypass cold-history modules on Run 1 by default (`minRemoteColdUnits` defaults to `0`), ensuring cold runs exercise remote distribution and seed broker duration history.
- Never introduce external vendor product references in code, tests, specs, or documentation.

---

## 3. Constraints & Accepted Limitations

- **Java 21 (`--release 21`) & Existing Dependencies Only:** No preview FFM (`java.lang.foreign`) or new Maven dependencies.
- **Backward Wire & API Compatibility:**
  - Existing `LptPartitionScheduler.schedule(...)` 5-arg and 6-arg overloads remain unchanged.
  - `DistributionBrokerConfig.Builder` retains all existing methods with backward-compatible defaults.
  - If `DistributedLauncher` talks to an older broker without `POST /api/v1/history/durations` (HTTP `404` or transport error), `BrokerHttpClient.findHistoricalDurations` logs at `DEBUG` and returns `Optional.empty()`, falling back cleanly without failing the build.
- **Self-Correcting Warm History Loop:** Because bypassed `< 2,000ms` modules report per-test durations to the broker via `reportLocalTests` (stored in `test_executions`), if a module later grows to `>= 2,000ms`, the broker's rolling average reflects the new duration and the next session automatically distributes the module remotely. For standalone runs without a reactor `sessionId`, local bypass skips opening an empty broker session and therefore does not update broker history until the next reactor session.

---

## 4. Approaches Considered

### 4.1 Bullet 1 — JAR Resource Inspection Caching
1. **Bounded JAR File-Identity Cache in `ForkedPartitionRunner` + `codeSource` Cache (1A — Selected)**
   - Key non-directory classpath entries by `(realPath, size, lastModifiedMillis, fileKey)` (`BasicFileAttributes`, bounded LRU of `4,096` entries) storing which of the 9 watched class resources exist in that JAR, plus a `ConcurrentHashMap<String, Path>` for `codeSource(className)`.
   - *Why chosen:* Eliminates repeated `JarFile` opens across both `verifyJUnitPlatform(List<Path>)` and `childClasspath(List<Path>)` without coupling JUnit/Jackson anchor constants into `WorkerFileCache`, and auto-invalidates if a non-cached JAR on disk changes.
2. **Digest-Keyed Cache Inside `WorkerFileCache` (1B — Rejected)**
   - Avoids `Files.readAttributes` on hit, but leaks JUnit/Jackson class names into `WorkerFileCache` and breaks package-private `Path`-based callers in `ForkedPartitionRunner`.

### 4.2 Bullet 2 — macOS Workspace Reference Probing
1. **Post-Launch `ProcessHandle` Survivor Diff + Targeted `lsof -p <pids>` on Unleased Forked `close()` (2A — Selected)**
   - Snapshot alive `ProcessHandle` `(pid -> startInstant)` entries (`< 1ms`) when creating an unleased forked workspace on macOS. On `close()`, check `containsUnixSocket(roots)` first; if zero post-snapshot processes owned by the worker's user remain alive, skip `lsof` completely (`0` subprocesses). If `1..256` candidate PIDs survive, run `/usr/sbin/lsof -n -P -Fn +w -p <csv>` (`~25ms`) and check `'n'` path records against `roots`. Keep full `lsof +D` for startup `recover()`, leased workspaces, and in-process runs.
   - *Why chosen:* Drops normal macOS cleanup overhead from `~150–350ms` to `< 1ms` while keeping PR #5's background-process preservation contract intact. (Benchmarking showed `lsof -a -u <uid> +D` still takes `~177ms` on macOS because `+D` walks kernel vnode tables regardless of `-u`.)
2. **Skip `lsof` on `close()` Unless `workspace.lease.enabled=true` (2B — Rejected)**
   - Simplest implementation, but breaks `WorkerWorkspaceRecoveryTest#shouldPreserveBothForkedDirectoriesWhenABackgroundProcessStillUsesOne` and weakens unleased background-helper safety documented in `src/manual/tools-test-distribution.adoc`.

### 4.3 Bullet 3a — Adaptive Partition Sizing
1. **Minimum Partition Duration Floor (`5,000ms`) + `2 * W` Wave Cap on Broker-Managed Sessions (3a-1 — Selected)**
   - When `CreateSessionRequest.maxPartitionSize <= 0` and `outputPaths` is empty, compute `binCount = min(ceil(n / defaultMaxPartitionSize), max(1, ceil(totalEstimatedMs / minPartitionDurationMs)), maxWavesPerWorker * W)` (skipping disabled caps and `W == 0`), then set `effectiveCapSize = ceil(n / binCount)`.
   - *Why chosen:* Packs fast modules into 1–3 partitions, bounds cold-DB 185-class modules to at most `2 * W` partitions (`12` on 6 workers instead of `37`), yet keeps 2 waves of headroom so outlier integration test classes still rebalance dynamically across workers.
2. **Hard Single-Wave Cap (`binCount <= W` Always) (3a-2 — Rejected)**
   - Minimizes forks to at most 1 per worker, but prevents LPT work-stealing across waves when one integration test class runs 10x longer than historical/default estimates.

### 4.4 Bullet 3b — Keeping `< 2s` Modules Local
1. **Broker Historical Duration Lookup (`POST /api/v1/history/durations`) + Opt-In Cold Threshold (Option A — Selected)**
   - Query rolling historical durations from the broker before packaging classpaths; bypass remote execution when all remote units have known history totaling `< minRemoteDurationMs` (`2,000ms` default), or when history is incomplete and `remoteUnits.size() <= minRemoteColdUnits` (`0` default).
   - *Why chosen:* Distinguishes genuinely fast `< 2s` unit modules (even with 25 fast classes) from 1-class `45s` Testcontainers integration modules, while preserving cold-run remote verification by default.
2. **Unit-Count-Only Heuristic Without Broker History (Rejected)**
   - Misclassifies 1-class slow integration modules as tiny and misses 20-class `< 1s` unit modules.

---

## 5. Detailed Design

### 5.1 Bullet 1 — `ForkedPartitionRunner` JAR Resource & `codeSource` Caching

**Module:** `tools/citrus-test-distribution-worker`
**Class:** `org.citrusframework.distribution.worker.ForkedPartitionRunner`

1. **Watched Resource Constants:**
   - Extract `JUNIT_TEST_ENGINE_RESOURCE = "org/junit/platform/engine/TestEngine.class"` and `JUNIT_CANCELLATION_TOKEN_RESOURCE = "org/junit/platform/engine/CancellationToken.class"`.
   - Build an immutable `Set<String> WATCHED_JAR_RESOURCES` containing all 8 `RUNTIME_ANCHORS` (`ClassName -> resourcePath`) plus `JUNIT_CANCELLATION_TOKEN_RESOURCE` (9 resources total; `TestEngine.class` is already in `RUNTIME_ANCHORS`).
2. **JAR Attribute Cache Key & Bounded LRU Store (Builder-Check Call — Content-Addressed `mtime` Immunity):**
   - Note that `WorkerFileCache.recordAccess(...)` updates `lastModifiedTime` on cached blob inodes on every partition, and `BlobCacheDirectory.namedJar(...)` hard-links `<cacheDir>/classpath/<64-hex-sha256>/<name>.jar` to that blob inode. Including `lastModifiedMillis` for content-addressed `classpath/<sha256>/<name>.jar` entries would therefore miss the cache on every partition.
   - Define `record JarResourceCacheKey(Path realPath, long size, long lastModifiedMillis, Object fileKey, long creationMillis)`:
     - When `entry` (before `toRealPath()`, or `real`) sits inside a content-addressed `.../classpath/<64-lowercase-hex>/<jarName>` path (`isContentAddressedClasspathJar`), set `lastModifiedMillis = 0L` and key by `(realPath, attrs.size(), 0L, attrs.fileKey(), attrs.fileKey() == null ? attrs.creationTime().toMillis() : 0L)` (matching `WorkerFileCache.VerifiedBlobStamp.sameFile`).
     - For all external/non-content-addressed JAR paths, include `attrs.lastModifiedTime().toMillis()` (`creationMillis = 0L`) so in-place modifications or timestamp changes invalidate immediately.
   - Maintain a bounded LRU cache (`MAX_JAR_CACHE_ENTRIES = 4_096`) backed by `Collections.synchronizedMap(new LinkedHashMap<>(256, 0.75f, true) { @Override protected boolean removeEldestEntry(...) { return size() > MAX_JAR_CACHE_ENTRIES; } })` mapping `JarResourceCacheKey -> Set<String>` (the subset of `WATCHED_JAR_RESOURCES` present in that JAR).
   - Perform `JarFile` I/O **outside** the synchronized map lock:
     1. Resolve `Path real = entry.toRealPath()` and `BasicFileAttributes attrs = Files.readAttributes(real, BasicFileAttributes.class)`.
     2. Construct `JarResourceCacheKey key` using the content-addressed vs external rule above.
     3. On cache hit (`Set<String> cached = jarResourceCache.get(key)`), if all requested `resources` are within `WATCHED_JAR_RESOURCES`, intersect `cached` with requested `resources` with zero `JarFile` opens.
     4. On cache miss (or when a caller asks for a resource outside `WATCHED_JAR_RESOURCES`), open `new JarFile(real.toFile())` once:
        - Check all entries in `WATCHED_JAR_RESOURCES`, collecting present ones into an immutable `Set<String> presentWatched`, and put `(key, presentWatched)` into `jarResourceCache`.
        - Also check any requested `resource` not in `WATCHED_JAR_RESOURCES` directly against the open `JarFile` so arbitrary `presentResources` calls remain exact.
        - If opening/reading `JarFile` throws `IOException`, rethrow `UncheckedIOException` **without** inserting into `jarResourceCache`.
     5. Directory entries (`Files.isDirectory(entry)`) continue checking `Files.isRegularFile(entry.resolve(resource))` directly without caching.
     6. Stop iterating `classpath` as soon as `found.size() == resources.size()`.
3. **Worker `codeSource` Cache:**
   - Maintain `ConcurrentHashMap<String, Path> codeSourceCache` in `ForkedPartitionRunner`.
   - `codeSource(String className)` resolves via `codeSourceCache.computeIfAbsent(className, ForkedPartitionRunner::resolveCodeSource)`; `IllegalStateException` / `ClassNotFoundException` propagates uncached.
4. **Test Observability:**
   - Expose package-private `LongAdder jarOpenCount` (or package-private `jarOpens()` / `clearCachesForTesting()` helpers) on `ForkedPartitionRunner` so unit tests deterministically assert that repeated `verifyJUnitPlatform(classpath)` + `childClasspath(classpath)` calls open each JAR exactly once, and re-open only if the JAR's size or mtime changes.

---

### 5.2 Bullet 2 — Targeted macOS Workspace Reference Probing

**Module:** `tools/citrus-test-distribution-worker`
**Classes:** `WorkerWorkspaceRegistry`, `WorkspaceReferences`

1. **Process Baseline Snapshot (`WorkspaceReferences.ProcessSnapshot`):**
   - Add package-private `record ProcessSnapshot(Map<Long, Instant> startInstants)` in `WorkspaceReferences` with factory `ProcessSnapshot captureProcessSnapshot()`:
     - When `!isMacOs()`, returns `null`.
     - On macOS, iterates `ProcessHandle.allProcesses()`, recording `(handle.pid(), startInstant)` for every alive process where `handle.info().startInstant()` is present (`Map.copyOf(...)`).
2. **`WorkerWorkspaceRegistry` Integration:**
   - In `WorkerWorkspaceRegistry.create(boolean forked, boolean leased)`:
     - Capture `WorkspaceReferences.ProcessSnapshot baseline = (forked && !leased) ? WorkspaceReferences.captureProcessSnapshot() : null;` **before** returning the `Workspace` (before the child JVM or any test helper process can start).
     - Pass `baseline` into `Workspace` (`private final WorkspaceReferences.ProcessSnapshot macOsBaseline`).
   - Extend `removeOwnedDirectories(String id, Record record, FileIdentity expectedRoot, FileIdentity expectedShortTemp, boolean recovering, WorkspaceReferences.ProcessSnapshot macOsBaseline)`:
     - `recover()` passes `macOsBaseline = null` (`recovering = true`).
     - `Workspace.close()` passes `record.leased() ? null : macOsBaseline` (`recovering = false`).
3. **`WorkspaceReferences` Fast Path on macOS:**
   - Keep existing 3-arg `inUse(roots, recovering, linuxWorkerStart)` delegating to 4-arg `inUse(roots, recovering, linuxWorkerStart, null)`.
   - On macOS, when `!recovering && macOsBaseline != null`:
     1. If `containsUnixSocket(roots)` is `true`, return `true` immediately.
     2. Collect candidate post-baseline survivor PIDs (`findPostBaselineCandidatePids(macOsBaseline)`):
        - Let `long selfPid = ProcessHandle.current().pid()` and `Optional<String> currentUser = ProcessHandle.current().info().user()`.
        - Iterate `ProcessHandle.allProcesses()`:
          - Skip `pid == selfPid` or `!handle.isAlive()`.
          - Check `Optional<String> procUser = handle.info().user()`: if `currentUser.isPresent() && procUser.isPresent() && !currentUser.get().equals(procUser.get())`, skip (different OS user). If either user is empty, include conservatively.
          - Check `Optional<Instant> started = handle.info().startInstant()`: include `pid` if `started.isEmpty()` or `!Objects.equals(macOsBaseline.startInstants().get(pid), started.get())`.
     3. If candidate PIDs is **empty**, return `false` (`0` `lsof` invocations).
     4. If candidate PIDs size `> 256` (`MAX_TARGETED_LSOF_PIDS = 256`), fall back to `macOsReferences(roots)` (`+D`).
     5. Otherwise invoke `macOsPidReferences(roots, candidatePids)` (`/usr/sbin/lsof -n -P -Fn +w -p <commaSeparatedPids>`, 5-second timeout, `redirectErrorStream(true)`):
        - Precompute normalized root prefixes for every existing root in `roots`: both `root.toAbsolutePath().normalize()` and `root.toRealPath()` (handling macOS `/tmp` -> `/private/tmp`).
        - If process does not finish within 5s (`destroyForcibly()`), or throws `IOException`/`InterruptedException`, return `true` conservatively.
        - Read output lines (`UTF-8`):
          - Valid `-Fn` field lines begin with `'p'`, `'f'`, or `'n'` (or empty trailing newline). If any non-empty line starts with any other character, or `process.exitValue()` is neither `0` nor `1`, fall back to `macOsReferences(roots)`.
          - For each `'n'` line (`String pathText = line.substring(1)`), return `true` if `matchesAnyRoot(pathText, normalizedRootStrings)` (`pathText.equals(rootStr) || pathText.startsWith(rootStr + File.separator)`).
        - Return `false` if no `'n'` line matched any workspace root.
   - When `recovering == true` or `macOsBaseline == null` (startup recovery, `leased == true`, or `forked == false`), execute existing `macOsReferences(roots) || containsUnixSocket(roots)` unchanged.

---

### 5.3 Bullet 3a — Adaptive Partition Sizing

**Modules:** `tools/citrus-test-distribution-protocol`, `tools/citrus-test-distribution-coordinator`, `tools/citrus-test-distribution`, `tools/maven/citrus-test-distribution-maven-extension`

1. **Configuration (`DistributionSettings` & `DistributionBrokerConfig`):**
   - In `DistributionSettings`:
     - Add `BROKER_MIN_PARTITION_DURATION_MS_PROPERTY = "citrus.test.distribution.broker.minPartitionDurationMs"`, `BROKER_MIN_PARTITION_DURATION_MS_ENV = "CITRUS_DIST_BROKER_MIN_PARTITION_DURATION_MS"`, `DEFAULT_BROKER_MIN_PARTITION_DURATION_MS = 5_000L`, and `getBrokerMinPartitionDurationMs()`.
     - Add `BROKER_MAX_WAVES_PER_WORKER_PROPERTY = "citrus.test.distribution.broker.maxWavesPerWorker"`, `BROKER_MAX_WAVES_PER_WORKER_ENV = "CITRUS_DIST_BROKER_MAX_WAVES_PER_WORKER"`, `DEFAULT_BROKER_MAX_WAVES_PER_WORKER = 2`, and `getBrokerMaxWavesPerWorker()`.
     - Add `OptionalInt findConfiguredMaxPartitionSize()` (plus `findConfiguredMaxPartitionSize(Properties, Function<String, String>)` helper) returning `OptionalInt.empty()` when neither the system/user property nor `CITRUS_DIST_MAX_PARTITION_SIZE` is set, and the parsed positive integer when explicitly configured. Existing `getMaxPartitionSize()` continues returning `DEFAULT_MAX_PARTITION_SIZE` (`5`) when unset.
   - In `DistributionBrokerConfig` & `DistributionBrokerConfig.Builder`:
     - Add `long minPartitionDurationMs` (Builder default `5_000L`; `fromSettings()` reads `DistributionSettings.getBrokerMinPartitionDurationMs()`) and `int maxWavesPerWorker` (Builder default `2`; `fromSettings()` reads `DistributionSettings.getBrokerMaxWavesPerWorker()`).
2. **Propagating Unset vs Explicit `maxPartitionSize`:**
   - In `DistributedLauncher`: when creating a standalone session, pass `DistributionSettings.findConfiguredMaxPartitionSize().orElse(0)` in `CreateSessionRequest.maxPartitionSize()`.
   - In `TestDistributionLifecycleParticipant`: resolve whether `MAX_PARTITION_SIZE_PROPERTY` / `MAX_PARTITION_SIZE_ENV` was explicitly configured across CLI user properties, top-level project properties, system properties, or environment variables. When unset, pass `0` in `CreateSessionRequest.maxPartitionSize()` and omit `MAX_PARTITION_SIZE_PROPERTY` from `sharedProps` (so per-module POM properties can still set it if desired; if set at reactor level, keep passing that explicit positive integer).
3. **Coordinator Session Tracking (`BrokerSessionCoordinator`):**
   - On `SessionRuntime`, record `boolean adaptivePartitionSizing = request.maxPartitionSize() <= 0` alongside `int maxPartitionSize = request.maxPartitionSize() > 0 ? request.maxPartitionSize() : config.defaultMaxPartitionSize()`.
   - Add package-private `int countEligibleOnlineWorkers(Map<String, String> requiredCapabilities)`:
     - Counts distinct `conn.workerId` across `workers.values()` where `(conn.state == WorkerState.IDLE || conn.state == WorkerState.BUSY) && conn.cancelledPartitionId == null && (now - conn.lastHeartbeatAt) <= config.leaseTimeoutMs() && CapabilityMatcher.matches(requiredCapabilities, conn.capabilities)`.
   - In `submitWork(...)`:
     - Enable adaptive sizing when `session.adaptivePartitionSizing && request.outputPaths().isEmpty()` (passing `config.minPartitionDurationMs()`, `config.maxWavesPerWorker()`, `this::countEligibleOnlineWorkers`); otherwise pass `(0L, 0, caps -> 0)` so explicit `maxPartitionSize` overrides and output-producing submissions keep exact `maxPartitionSize` chunking.
4. **Adaptive LPT Bin Formula (`LptPartitionScheduler`):**
   - Keep existing 5-arg and 6-arg `schedule(...)` methods delegating with `(0L, 0, caps -> 0)`.
   - Add 9-arg `schedule(..., long minPartitionDurationMs, int maxWavesPerWorker, ToIntFunction<Map<String, String>> eligibleWorkerCounter)`.
   - Under `PartitionPolicy.LPT`, for each capability group (`groupUnits` of size `n`, `capSize = Math.max(1, maxPartitionSize)`):
     1. `int binCount = Math.ceilDiv(n, capSize);`
     2. If `minPartitionDurationMs > 0`:
        - `long totalEstimatedMs = groupUnits.stream().mapToLong(WorkUnitSpec::estimatedDurationMs).sum();`
        - `long durationBins = totalEstimatedMs <= 0 ? 1L : Math.ceilDiv(totalEstimatedMs, minPartitionDurationMs);`
        - `binCount = (int) Math.min(binCount, Math.max(1L, durationBins));`
     3. If `maxWavesPerWorker > 0 && eligibleWorkerCounter != null`:
        - `int eligibleWorkers = eligibleWorkerCounter.applyAsInt(capabilities);`
        - If `eligibleWorkers > 0`:
          - `long waveCapBins = (long) eligibleWorkers * maxWavesPerWorker;`
          - `binCount = (int) Math.min(binCount, Math.max(1L, waveCapBins));`
     4. `int effectiveCapSize = Math.ceilDiv(n, binCount);`
     5. Run the existing LPT greedy placement across `binCount` bins using `bin.units.size() < effectiveCapSize`.

---

### 5.4 Bullet 3b — Small-Module Local Execution Bypass

**Modules:** `tools/citrus-test-distribution-protocol`, `tools/citrus-test-distribution-coordinator`, `tools/citrus-test-distribution`, `tools/maven/citrus-test-distribution-maven-extension`

1. **Store Historical Work-Unit Duration Aggregation (Builder-Check Call — Session-Level Sum vs Single-Method Average):**
   - `DiscoveredWorkExtractor` emits class-level `WorkUnitSpec`s (`key = className`), while `dist_test_executions` records one row per `@Test` method under that shared `work_unit_key`. Previously, `JdbcDistributionStore.findHistoricalDurationsMs` averaged individual `@Test` rows (`sum / count` of the last `recentLimit` rows), under-reporting a 50-test class of `20ms` methods as `20ms` instead of `1,000ms` (which would cause both `LptPartitionScheduler` and Bullet 3b's `< 2,000ms` check to severely underestimate multi-test classes).
   - Update `JdbcDistributionStore.findHistoricalDurationsMs(Collection<String> workUnitKeys, int recentLimit)` to aggregate total duration **per session attempt**:
     ```sql
     SELECT COALESCE(session_id, '') AS sess_id,
            COALESCE(attempt, 1) AS run_attempt,
            SUM(CASE WHEN duration_ms > 0 THEN duration_ms ELSE 0 END) AS total_ms,
            MAX(recorded_at) AS last_recorded_at
     FROM dist_test_executions
     WHERE work_unit_key = ? AND status IN ('PASSED', 'FAILED')
     GROUP BY COALESCE(session_id, ''), COALESCE(attempt, 1)
     ORDER BY last_recorded_at DESC, run_attempt DESC
     ```
     For each `workUnitKey`, iterate the grouped rows keeping only the latest attempt per non-empty `sess_id` (so a partial failed `attempt=1` in the same session does not dilute or double-count `attempt=2`), up to `Math.max(1, recentLimit)` sessions, and compute `sumOfSessionTotals / sessionCount` (flooring each session total at `1L`). Single-method work units (`OrderIT#createOrder`) produce identical values while multi-method class work units reflect true class execution time.
2. **Protocol Models (`org.citrusframework.distribution.model`):**
   - `public record HistoricalDurationsRequest(List<String> workUnitKeys, int recentLimit)` (compact constructor defaults null `workUnitKeys` to `List.of()`).
   - `public record HistoricalDurationsResponse(Map<String, Long> durationsMs)` (compact constructor defaults null `durationsMs` to `Map.of()`).
3. **Coordinator Endpoint (`DistributionBrokerVerticle` + `BrokerSessionCoordinator`):**
   - Add `BrokerSessionCoordinator.findHistoricalDurationsMs(Collection<String> workUnitKeys, int recentLimit)` delegating to `store.findHistoricalDurationsMs(workUnitKeys, recentLimit)`.
   - Mount `router.post("/api/v1/history/durations").handler(this::handleHistoricalDurations)` in `DistributionBrokerVerticle` (covered by existing `/api/*` auth + `BodyHandler`):
     - Reject null body or `request.workUnitKeys().size() > 10_000` with `400 Bad Request` (`Map.of("error", "workUnitKeys exceeds maximum batch size of 10000")`).
     - Normalize `int limit = request.recentLimit() <= 0 ? 10 : Math.min(request.recentLimit(), 100)`.
     - Return `200 OK` with `new HistoricalDurationsResponse(coordinator.findHistoricalDurationsMs(request.workUnitKeys(), limit))`.
4. **Configuration (`DistributionSettings` + `TestDistributionLifecycleParticipant`):**
   - In `DistributionSettings`:
     - `MIN_REMOTE_DURATION_MS_PROPERTY = "citrus.test.distribution.minRemoteDurationMs"`, `MIN_REMOTE_DURATION_MS_ENV = "CITRUS_DIST_MIN_REMOTE_DURATION_MS"`, `DEFAULT_MIN_REMOTE_DURATION_MS = 2_000L`, `getMinRemoteDurationMs()`.
     - `MIN_REMOTE_COLD_UNITS_PROPERTY = "citrus.test.distribution.minRemoteColdUnits"`, `MIN_REMOTE_COLD_UNITS_ENV = "CITRUS_DIST_MIN_REMOTE_COLD_UNITS"`, `DEFAULT_MIN_REMOTE_COLD_UNITS = 0`, `getMinRemoteColdUnits()`.
   - In `TestDistributionLifecycleParticipant`:
     - Resolve `minRemoteDurationMs` and `minRemoteColdUnits` via `resolveProperty(...)` and include both in `sharedProps` so Surefire/Failsafe forked JVMs inherit reactor CLI `-D` / root POM values.
5. **Broker HTTP Client (`BrokerHttpClient`):**
   - Add `public Optional<Map<String, Long>> findHistoricalDurations(List<String> workUnitKeys, int recentLimit)`:
     - If `workUnitKeys == null || workUnitKeys.isEmpty()`, return `Optional.of(Map.of())`.
     - `POST` JSON `new HistoricalDurationsRequest(workUnitKeys, recentLimit)` to `/api/v1/history/durations`.
     - On `200 OK`, parse `HistoricalDurationsResponse` and return `Optional.of(resp.durationsMs())`.
     - On non-200 status (e.g. `404` on older coordinator) or `IOException`, log at `DEBUG` and return `Optional.empty()`. On `InterruptedException`, restore `Thread.currentThread().interrupt()`, log at `DEBUG`, and return `Optional.empty()`.
6. **Launcher Bypass (`DistributedLauncher` + `LocalFallbackExecutor` — Builder-Check Call: Direct `TestPlan` Execution):**
   - After `ExtractedWork extracted = DiscoveredWorkExtractor.extract(testPlan)` (when `!extracted.remoteUnits().isEmpty()`) and `WorkspaceSettings workspace = WorkspaceSettings.resolve()`, evaluate `shouldRunSmallModuleLocally(extracted, workspace, client)`:
     1. Read `long minRemoteDurationMs = DistributionSettings.getMinRemoteDurationMs()` and `int minRemoteColdUnits = DistributionSettings.getMinRemoteColdUnits()`. If `minRemoteDurationMs <= 0 && minRemoteColdUnits <= 0`, return `false`.
     2. Require `!extracted.hasRemoteOnlyUnits()`.
     3. Require every unit in `extracted.remoteUnits()` to have null or empty `requiredCapabilities()`.
     4. Require `workspace.inputArtifacts().isEmpty() && workspace.outputPaths().isEmpty()`.
     5. Query `Optional<Map<String, Long>> historyOpt = client.findHistoricalDurations(remoteKeys, 10)`:
        - **Warm bypass:** If `minRemoteDurationMs > 0 && historyOpt.isPresent()`, check whether `historyOpt.get().keySet().containsAll(remoteKeys)`. If all keys have recorded history, compute `long totalMs = remoteKeys.stream().mapToLong(k -> historyOpt.get().get(k)).sum()`, and return `totalMs < minRemoteDurationMs`.
        - **Cold bypass:** If history was unavailable (`historyOpt.isEmpty()`) or incomplete (`!containsAll(remoteKeys)`), return `minRemoteColdUnits > 0 && extracted.remoteUnits().size() <= minRemoteColdUnits`.
   - Add `LocalFallbackExecutor.executeTestPlanLocally(Launcher delegate, TestPlan testPlan, List<WorkUnitSpec> allUnits, TestExecutionListener[] additionalListeners)`:
     - Executes `delegate.execute(testPlan, mergedListeners)` directly in **one** pass (avoiding a redundant JUnit discovery pass, preserving `TestPlan` configuration parameters/filters, and preventing `@CitrusTestFactory` dynamic sources from starting/finishing the same class container multiple times and overwriting Surefire `TEST-<Class>.xml`), while collecting `WorkerTestEventPayload` telemetry mapped to `allUnits` keys (matching dynamic source names from `TestIdentifier` displayName/uniqueId when present).
   - When `shouldRunSmallModuleLocally(...)` returns `true` (**before** `createSession` and **before** `packageCurrentClasspath()`):
     - Merge `extracted.localUnits()` and `extracted.remoteUnits()` into `allUnits`, log at `INFO` (`"Running {} work unit(s) locally in module {} (below remote distribution threshold)"`), and invoke `LocalFallbackExecutor.executeTestPlanLocally(delegate, testPlan, allUnits, listeners)`.
     - If `citrus.test.distribution.reportDir` is configured (`writeLocalReportsIfConfigured`), write Surefire XML reports from `localRun.executions()`.
     - If `sharedSessionId != null && !sharedSessionId.isBlank()`, report `localRun.executions()` via `client.reportLocalTests(sharedSessionId, new ReportLocalTestsRequest("local-host", localRun.executions()))` (catching and logging unexpected transport errors when `fallbackLocal=true`, matching the `@LocalOnly` path). Standalone runs without `sharedSessionId` return immediately without opening an empty broker session.

---

### 5.5 User Manual Updates (`src/manual/tools-test-distribution.adoc`)

- Add configuration rows for:
  - `citrus.test.distribution.minRemoteDurationMs` / `CITRUS_DIST_MIN_REMOTE_DURATION_MS` (default `2000`)
  - `citrus.test.distribution.minRemoteColdUnits` / `CITRUS_DIST_MIN_REMOTE_COLD_UNITS` (default `0`)
  - `citrus.test.distribution.broker.minPartitionDurationMs` / `CITRUS_DIST_BROKER_MIN_PARTITION_DURATION_MS` (default `5000`)
  - `citrus.test.distribution.broker.maxWavesPerWorker` / `CITRUS_DIST_BROKER_MAX_WAVES_PER_WORKER` (default `2`)
- Update the macOS workspace reference paragraph in the Workspace Cleanup section to note that normal unleased forked workspace cleanup checks post-launch survivor processes via `lsof -p` (skipping `lsof` when none remain), while startup recovery, leased workspaces, and in-process runs use full `lsof +D`.

---

## 6. Testing & Verification

1. **Bullet 1 (`ForkedPartitionRunnerTest`):**
   - Verify repeated `verifyJUnitPlatform` + `childClasspath` calls on the same JAR classpath open each JAR only once (`jarOpenCount`), remain cache hits on content-addressed `classpath/<sha256>/<name>.jar` paths even when `WorkerFileCache.recordAccess` updates `lastModifiedTime`, invalidate and re-open when an external JAR's `mtime` or size changes on disk, do not cache corrupt JAR `IOException`s, and resolve `codeSource` via cache.
2. **Bullet 2 (`WorkerWorkspaceRecoveryTest` + `WorkspaceReferencesTest`):**
   - Verify on macOS that closing a clean unleased forked workspace spawns zero `lsof` processes (or completes without `macOsReferences` `+D`), still preserves both directories when a post-baseline background process holds an open file inside `childTmpDir` (`shouldPreserveBothForkedDirectoriesWhenABackgroundProcessStillUsesOne`), still preserves workspaces with an unattributed Unix domain socket (`shouldPreserveWorkspaceWithAnUnattributedUnixSocketPath`), and still uses full `+D` during startup `recover()` and for `leased` workspaces.
3. **Bullet 3a (`LptPartitionSchedulerTest` + `BrokerSessionCoordinatorTest`):**
   - Verify duration-floor consolidation (e.g. 19 fast `150ms` units consolidate into `1` partition at `minPartitionDurationMs = 5_000ms`), wave-cap bounding (e.g. 185 `1,000ms` cold units on `6` online workers cap at `12` partitions = `2` waves), capability-group isolation when counting eligible workers, strict preservation of explicit `CreateSessionRequest.maxPartitionSize > 0` and non-empty `outputPaths`, and `0` disable semantics.
4. **Bullet 3b (`JdbcDistributionStoreTest`, `DistributionBrokerServerTest`, `DistributedLauncherTest`, `TestDistributionLifecycleParticipantTest`):**
   - Verify `JdbcDistributionStore.findHistoricalDurationsMs` sums all `@Test` method durations within a session per `work_unit_key` (taking the latest attempt per session) rather than averaging individual method rows.
   - Verify `POST /api/v1/history/durations` (`200 OK` warm map, `limit` clamping, `> 10,000` `400 Bad Request`, auth enforcement).
   - Verify `DistributedLauncher` distributes on Run 1 (cold DB, `minRemoteColdUnits = 0`) and bypasses classpath packaging + remote submission on Run 2 (warm DB `< 2,000ms`), running locally and reporting executions to `sharedSessionId` via `reportLocalTests`.
   - Verify bypass does **not** trigger when total historical duration `>= minRemoteDurationMs`, when `@RemoteOnly` or `@AgentRequirement` capabilities are present, when `inputs`/`outputs` are non-empty, or when `minRemoteDurationMs = 0` and `minRemoteColdUnits = 0`; verify `minRemoteColdUnits > 0` cold bypass and graceful `404` fallback on older brokers.
   - Verify `TestDistributionLifecycleParticipant` forwards `minRemoteDurationMs` and `minRemoteColdUnits` in `sharedProps` and sends `maxPartitionSize = 0` when unset.
5. **Full Distribution Suite Verification:**
   - Run `./mvnw test -pl tools/citrus-test-distribution-protocol,tools/citrus-test-distribution-coordinator,tools/citrus-test-distribution-worker,tools/citrus-test-distribution,tools/maven/citrus-test-distribution-maven-extension -Dcitrus.test.distribution.enabled=false` and confirm 100% pass with zero regressions.
