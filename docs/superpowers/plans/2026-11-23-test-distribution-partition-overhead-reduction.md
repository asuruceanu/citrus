# Test Distribution Partition Overhead Reduction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate repeated per-partition JAR scanning, full-host macOS `lsof +D` teardown stalls, excessive LPT partition fan-out on fast/multi-wave modules, and cold remote round-trip overhead on `<2s` modules.

**Architecture:** Five self-contained changes aligned with Maven module boundaries: (1) protocol DTOs, settings, and adaptive `LptPartitionScheduler` bin-sizing in `citrus-test-distribution-protocol`; (2) bounded JAR resource and `codeSource` caching in `ForkedPartitionRunner`; (3) post-launch `ProcessHandle` survivor diffing with targeted `lsof -Fn -p` on macOS in `WorkerWorkspaceRegistry`/`WorkspaceReferences`; (4) session-summed historical durations, adaptive coordinator scheduling, and `POST /api/v1/history/durations` in `citrus-test-distribution-coordinator`; and (5) pre-packaging `<2s` local bypass in `DistributedLauncher` plus reactor property propagation in `citrus-test-distribution-maven-extension`.

**Tech Stack:** Java 21, Maven 3.9+, JUnit Platform 1.x / Jupiter 5, Vert.x HTTP router, JDBC (H2 / PostgreSQL SQL-compatible), Jackson (`DistributionJson`).

**Spec:** `docs/superpowers/specs/2026-11-23-test-distribution-partition-overhead-reduction-design.md`

## Global Constraints

- Target **Java 21** (`<maven.compiler.release>21</maven.compiler.release>`); no preview APIs or new third-party Maven dependencies.
- All new `.java` files must start with the standard Citrus Apache 2.0 license header (`Copyright the original author or authors.` / `SPDX-License-Identifier: Apache-2.0`).
- Always pass `-Dcitrus.test.distribution.enabled=false` when running `./mvnw test` on distribution modules so unit tests never self-distribute.
- Preserve all existing public/package-private overloads (`LptPartitionScheduler.schedule` 5-arg and 6-arg methods, `WorkspaceReferences.inUse` 3-arg method) so existing callers and tests compile unchanged.
- Never weaken workspace safety: startup `recover()`, leased workspaces (`record.leased() == true`), in-process workspaces (`forked == false`), and `containsUnixSocket(roots)` must continue using full directory reference checks.
- Explicit `-Dcitrus.test.distribution.maxPartitionSize=<N>` (`> 0`) and non-empty `outputPaths` must bypass adaptive duration/wave packing and honor `maxPartitionSize` strictly.
- Default `DEFAULT_MIN_REMOTE_COLD_UNITS = 0` so cold runs distribute normally, seed broker history, and preserve existing cold-broker integration tests; warm `< 2_000ms` runs bypass automatically via `DEFAULT_MIN_REMOTE_DURATION_MS = 2_000L`.

## Review Focus

- Corrupt or truncated external JAR on classpath: throws `UncheckedIOException` without caching failure, recovering immediately once replaced with a valid JAR (Task 2).
- Sibling workspace path prefix collision in `lsof -Fn` output (`/private/tmp/cdtmp-10/file` vs `/private/tmp/cdtmp-1`): exact path/directory-separator boundary prevents false-positive retention (Task 3).
- Retried partition attempts within one session (`attempt=1` failed fast, `attempt=2` ran full suite): `JdbcDistributionStore.findHistoricalDurationsMs` keeps only the latest attempt per non-empty `session_id` instead of summing or averaging retries (Task 4).
- Explicit `maxPartitionSize` in `CreateSessionRequest` (`> 0`) on a fast module: `BrokerSessionCoordinator` disables adaptive duration floor and wave cap so explicit partition count expectations hold (Task 4).
- Older or failing broker on `POST /api/v1/history/durations` (`404` or `500`): `BrokerHttpClient.findHistoricalDurations` returns `Optional.empty()` at `DEBUG` without failing the build, falling back to normal remote distribution when `minRemoteColdUnits == 0` (Task 5).

---

### Task 1: Protocol DTOs, Settings & Adaptive `LptPartitionScheduler`

**Files:**
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/HistoricalDurationsRequest.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/HistoricalDurationsResponse.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/config/DistributionSettings.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/scheduler/LptPartitionScheduler.java`
- Test: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/config/DistributionSettingsTest.java`
- Test: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/scheduler/LptPartitionSchedulerTest.java`

**Interfaces:**
- Consumes: `DistributionJson`, `WorkUnitSpec`, `PartitionPolicy`, `PartitionRecord` in `citrus-test-distribution-protocol`.
- Produces:
  - `public record HistoricalDurationsRequest(List<String> testKeys, int limit)` in `org.citrusframework.distribution.model` (null `testKeys` normalized to `List.of()`).
  - `public record HistoricalDurationsResponse(Map<String, Long> durationsMs)` in `org.citrusframework.distribution.model` (null `durationsMs` normalized to `Map.of()` via `Collections.unmodifiableMap(new LinkedHashMap<>(...))`).
  - `DistributionSettings` constants & getters:
    - `MIN_REMOTE_DURATION_MS_PROPERTY = "citrus.test.distribution.minRemoteDurationMs"`
    - `MIN_REMOTE_DURATION_MS_ENV = "CITRUS_DIST_MIN_REMOTE_DURATION_MS"`
    - `DEFAULT_MIN_REMOTE_DURATION_MS = 2_000L`
    - `MIN_REMOTE_COLD_UNITS_PROPERTY = "citrus.test.distribution.minRemoteColdUnits"`
    - `MIN_REMOTE_COLD_UNITS_ENV = "CITRUS_DIST_MIN_REMOTE_COLD_UNITS"`
    - `DEFAULT_MIN_REMOTE_COLD_UNITS = 0`
    - `public static OptionalInt findConfiguredMaxPartitionSize()` and `static OptionalInt findConfiguredMaxPartitionSize(UnaryOperator<String> prop, UnaryOperator<String> env)` (returns `OptionalInt.of(parsed)` when `> 0`, else `OptionalInt.empty()`)
    - `public static long getMinRemoteDurationMs()` and `static long getMinRemoteDurationMs(UnaryOperator<String> prop, UnaryOperator<String> env)` (returns `Math.max(0L, ...)` defaulting to `2_000L`)
    - `public static int getMinRemoteColdUnits()` and `static int getMinRemoteColdUnits(UnaryOperator<String> prop, UnaryOperator<String> env)` (returns `Math.max(0, ...)` defaulting to `0`)
  - `LptPartitionScheduler.schedule(String sessionId, List<WorkUnitSpec> units, Map<String, Long> historicalDurationsMs, long defaultDurationMs, int maxPartitionSize, PartitionPolicy policy, long minPartitionDurationMs, int maxWavesPerWorker, ToIntFunction<Map<String, String>> eligibleWorkerCounter)`

- [ ] **Step 1: Write failing tests for DTO JSON round-trip, new `DistributionSettings` lookups, and adaptive `LptPartitionScheduler` sizing**

Add tests in `DistributionSettingsTest.java`:
- `shouldReturnEmptyConfiguredMaxPartitionSizeWhenUnsetAndPositiveValueWhenConfigured`: verifies `findConfiguredMaxPartitionSize` returns `OptionalInt.empty()` when unset/invalid/`<=0` and `OptionalInt.of(12)` when property or env is `"12"`, while `getMaxPartitionSize()` still returns `5`.
- `shouldReadMinRemoteDurationMsAndColdUnitsWithClampedNonNegativeValues`: verifies defaults (`2_000L`, `0`), property/env overrides, and negative strings clamping to `0`.
- `shouldRoundTripHistoricalDurationsDtos`: serializes/deserializes `HistoricalDurationsRequest(List.of("pkg.A"), 10)` and `HistoricalDurationsResponse(Map.of("pkg.A", 450L))` via `DistributionJson`, plus null normalization to empty collections.

Add tests in `LptPartitionSchedulerTest.java`:
- `shouldPackFastModuleIntoFewerPartitionsWhenMinPartitionDurationFloorApplies`: 19 units of `150ms` each (`2_850ms` total), `maxPartitionSize = 5`, `minPartitionDurationMs = 5_000L`, `maxWavesPerWorker = 2`, `eligibleWorkers = 6` -> produces `1` partition containing all 19 units.
- `shouldCapColdMultiWaveExplosionUsingMaxWavesPerWorker`: 185 units with cold default `1_000ms` (`185_000ms` total), `maxPartitionSize = 5`, `minPartitionDurationMs = 5_000L`, `maxWavesPerWorker = 2`, `eligibleWorkers = 6` -> produces `12` partitions (`6 * 2`) with at most `16` units per partition (`ceil(185 / 12)`).
- `shouldEvaluateEligibleWorkersPerCapabilityGroupIndependently`: 20 default-capability units (`W = 6`) + 20 `docker=true` units (`W = 1`) at `1_000ms` each, `maxPartitionSize = 5`, `minPartitionDurationMs = 5_000L`, `maxWavesPerWorker = 2` -> default group gets `4` bins (`min(4, 4, 12)`), `docker=true` group gets `2` bins (`min(4, 4, 2)`).
- `shouldPreserveLegacyBehaviorWhenAdaptiveParametersAreDisabled`: `minPartitionDurationMs = 0L`, `maxWavesPerWorker = 0`, `eligibleWorkers = 0` -> 19 units at `maxPartitionSize = 5` produces `4` partitions.

- [ ] **Step 2: Run protocol tests to verify RED**

Run: `./mvnw test -pl tools/citrus-test-distribution-protocol -Dtest=DistributionSettingsTest,LptPartitionSchedulerTest -Dcitrus.test.distribution.enabled=false`
Expected: FAIL to compile (`HistoricalDurationsRequest`, `findConfiguredMaxPartitionSize`, and 9-arg `schedule` not defined).

- [ ] **Step 3: Implement DTOs, `DistributionSettings` methods, and adaptive `LptPartitionScheduler`**

1. Create `HistoricalDurationsRequest` and `HistoricalDurationsResponse` in `org.citrusframework.distribution.model` with `@JsonCreator`/`@JsonProperty` or canonical record annotations matching `MissingDigestsRequest`/`MissingDigestsResponse`.
2. Add the new constants and methods to `DistributionSettings` (note: guard `aliasEnvKey != null` in `resolveFirstNonBlank` if passing `null` when no secondary alias env exists).
3. In `LptPartitionScheduler`, keep existing 5-arg and 6-arg `schedule` methods delegating to the 9-arg `schedule(..., 0L, 0, caps -> 0)`. In `lptPartitions`:
   - `int maxBinsFromSize = Math.ceilDiv(n, capSize);`
   - `long totalEstimatedMs = groupUnits.stream().mapToLong(EnrichedUnit::estimatedDurationMs).sum();`
   - `int targetBinsByDuration = minPartitionDurationMs > 0 ? (int) Math.clamp(Math.ceilDiv(Math.max(1L, totalEstimatedMs), minPartitionDurationMs), 1L, n) : maxBinsFromSize;`
   - `int eligibleWorkers = eligibleWorkerCounter == null ? 0 : Math.max(0, eligibleWorkerCounter.apply(groupCapabilities));`
   - `int maxBinsByWorkers = (maxWavesPerWorker > 0 && eligibleWorkers > 0) ? (int) Math.min((long) n, (long) eligibleWorkers * maxWavesPerWorker) : maxBinsFromSize;`
   - `int binCount = Math.max(1, Math.min(maxBinsFromSize, Math.min(targetBinsByDuration, maxBinsByWorkers)));`
   - `int effectiveCapSize = Math.ceilDiv(n, binCount);`
   - Place each unit (descending `estimatedDurationMs`, tie-broken by `key`) into the least-loaded bin with `bin.units.size() < effectiveCapSize`.

- [ ] **Step 4: Run all protocol tests and install reactor artifact**

Run: `./mvnw test-compile test -pl tools/citrus-test-distribution-protocol -Dcitrus.test.distribution.enabled=false && ./mvnw install -DskipTests -pl tools/citrus-test-distribution-protocol`
Expected: PASS (all protocol tests green; `citrus-test-distribution-protocol` installed to local Maven repo).

- [ ] **Step 5: Commit Task 1**

```bash
git add tools/citrus-test-distribution-protocol
git commit -m "feat(distribution): add adaptive LPT bin sizing and history duration DTOs"
```

---

### Task 2: Worker Classpath Resource & `codeSource` Cache (Bullet 1)

**Files:**
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/ForkedPartitionRunner.java`
- Create: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/ForkedPartitionRunnerClasspathCacheTest.java`

**Interfaces:**
- Consumes: `ForkedPartitionRunner.verifyJUnitPlatform(List<Path>)`, `ForkedPartitionRunner.childClasspath(List<Path>)`.
- Produces:
  - Bounded `jarResourceCache` (`MAX_JAR_RESOURCE_CACHE_ENTRIES = 4_096`) and `codeSourceCache` on `ForkedPartitionRunner`, plus package-private `int jarOpenCountForTesting()` (or `LongAdder` counter incremented inside `scanJarWatchedResources` right before `new JarFile(...)`) so unit tests deterministically assert exact `JarFile` open counts without filesystem brittle hacks.

- [ ] **Step 1: Write failing tests in `ForkedPartitionRunnerClasspathCacheTest`**

Create `ForkedPartitionRunnerClasspathCacheTest.java` testing real temporary JAR files:
- `shouldOpenContentAddressedCachedJarOnlyOnceAcrossPartitionsEvenWhenMtimeChanges`: creates `<tmp>/classpath/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/sample.jar` (without Jackson 3 `tools/jackson/...` resources) + JUnit JARs, runs `verifyJUnitPlatform` + `childClasspath` twice while mutating `Files.setLastModifiedTime` between runs (simulating `WorkerFileCache.recordAccess`), and asserts `runner.jarOpenCountForTesting()` increments on pass 1 and increments by `0` on pass 2.
- `shouldInvalidateExternalJarCacheWhenExternalJarIsModifiedInPlace`: creates `<tmp>/external/lib.jar` without `tools/jackson/databind/ObjectMapper.class`, calls `childClasspath`, overwrites `<tmp>/external/lib.jar` with a JAR containing `tools/jackson/databind/ObjectMapper.class` (updating size/mtime), calls `childClasspath` again, and asserts `jarOpenCountForTesting()` incremented by `1` and the resource change is reflected.
- `shouldNotCacheCorruptJarFailureAndRecoverWhenReplaced`: writes corrupt non-ZIP bytes to `<tmp>/external/broken.jar`, asserts `verifyJUnitPlatform` throws `UncheckedIOException`, replaces `broken.jar` with a valid JAR, and asserts the next call succeeds.
- `shouldBoundJarResourceCacheAtMaxEntries`: populates `> 4_096` tiny distinct JAR entries (or exercises package-private cache capacity helper) and asserts cache size never exceeds `4_096` without throwing.

- [ ] **Step 2: Run `ForkedPartitionRunnerClasspathCacheTest` to verify RED**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker -Dtest=ForkedPartitionRunnerClasspathCacheTest -Dcitrus.test.distribution.enabled=false`
Expected: FAIL (`jarOpenCountForTesting` missing / JAR re-opened on every call).

- [ ] **Step 3: Implement bounded JAR resource cache and `codeSource` cache in `ForkedPartitionRunner`**

In `ForkedPartitionRunner.java`:
- Define `CANCELLATION_TOKEN_RESOURCE = "org/junit/platform/engine/CancellationToken.class"` and `WATCHED_RESOURCES` (`Set<String>` containing the 8 `RUNTIME_ANCHORS` class resource paths plus `CANCELLATION_TOKEN_RESOURCE`).
- Define `record JarResourceCacheKey(Path realPath, long size, long lastModifiedMillis, Object fileKey, long creationMillis)` and `isContentAddressedCacheJar(Path realPath)` matching `<parentName == 64 lowercase hex chars>` under parent-of-parent named `"classpath"`.
- For content-addressed cache JARs, set `lastModifiedMillis = 0L` and `creationMillis = attrs.fileKey() == null ? attrs.creationTime().toMillis() : 0L`; for all other JARs, set `lastModifiedMillis = attrs.lastModifiedTime().toMillis()` and `creationMillis = 0L`.
- Cache `Set<String>` of present `WATCHED_RESOURCES` in a `ConcurrentHashMap<JarResourceCacheKey, Set<String>>` bounded at `4_096` entries (evicting via iterator when inserting at capacity) and increment `jarOpenCount` inside `scanJarWatchedResources` when opening `new JarFile(entry.toFile())`. Do not cache if `JarFile` throws `IOException` (rethrow `UncheckedIOException` as today).
- Cache `codeSource(String className)` in `ConcurrentHashMap<String, Optional<Path>>`.

- [ ] **Step 4: Run worker tests to verify GREEN**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker -Dcitrus.test.distribution.enabled=false`
Expected: PASS (all existing worker tests + 4 new cache tests pass).

- [ ] **Step 5: Commit Task 2**

```bash
git add tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/ForkedPartitionRunner.java \
        tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/ForkedPartitionRunnerClasspathCacheTest.java
git commit -m "perf(distribution): cache classpath JAR anchor lookups and worker code sources"
```

---

### Task 3: Worker macOS Survivor-Process Workspace Cleanup (Bullet 2)

**Files:**
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/WorkspaceReferences.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/WorkerWorkspaceRegistry.java`
- Create: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/WorkspaceReferencesTest.java`

**Interfaces:**
- Consumes: `ProcessHandle`, `/usr/sbin/lsof`.
- Produces:
  - `WorkspaceReferences.captureMacOsBaselineProcesses()` returning `Map<Long, Instant>` (`Map.of()` on non-macOS; `pid -> startInstant` for alive processes with present `startInstant()` on macOS).
  - `WorkspaceReferences.inUse(List<Path> roots, boolean recovering, LinuxWorkspaceReferences.StartTicks linuxWorkerStart, Map<Long, Instant> macOsBaselineProcesses)` (keeping existing 3-arg `inUse(...)` delegating with `null` so startup `recover()`, `record.leased()`, and `forked == false` keep full `lsof +D`).
  - Package-private helpers in `WorkspaceReferences` for deterministic unit testing on all OSes:
    - `static boolean matchesAnyRoot(String lsofNameField, List<Path> normalizedRoots)`
    - `static Optional<Boolean> parseMacOsProcessLsofOutput(String stdout, int exitCode, List<Path> normalizedRoots)` (`Optional.of(true)` if any `'n'` line matches `roots`, `Optional.of(false)` if exit code `0` or `1` and all non-empty lines start with `'p'`/`'f'`/`'n'` with no match, `Optional.empty()` on unexpected exit code or warning/unrecognized lines to trigger fallback).

- [ ] **Step 1: Write failing tests in `WorkspaceReferencesTest`**

Create `WorkspaceReferencesTest.java` covering:
- `shouldAvoidSiblingPrefixFalsePositiveWhenMatchingLsofPaths`: `/private/tmp/cdtmp-10/open.txt` and `/private/tmp/cdtmp-10` do **not** match root `/private/tmp/cdtmp-1`, whereas `/private/tmp/cdtmp-1` and `/private/tmp/cdtmp-1/sub/file.txt` **do** match.
- `shouldTreatExitedTargetPidExitCodeOneWithEmptyOutputAsUnused`: `parseMacOsProcessLsofOutput("", 1, roots)` returns `Optional.of(false)`.
- `shouldReturnEmptyForFallbackWhenLsofEmitsWarningOrUnexpectedExitCode`: non-`p`/`f`/`n` warning output or exit code `2` returns `Optional.empty()` so caller falls back to full `macOsReferences`.
- `shouldSkipFullLsofOnMacOsWhenNoPostLaunchProcessesSurvived` (`@EnabledOnOs(OS.MAC)`): captures `baseline = WorkspaceReferences.captureMacOsBaselineProcesses()`, creates two empty temp dirs, calls 4-arg `WorkspaceReferences.inUse(roots, false, null, baseline)` using a package-private `CommandRunner` seam (or counter on `lsof` invocations), and asserts **zero** `/usr/sbin/lsof` processes are spawned!
- `shouldFallbackConservativelyWhenSurvivorBatchExceedsMaxPids`: passes a synthetic survivor PID list of `257` PIDs into package-private helper and verifies it delegates to the fallback path rather than truncating PIDs.

- [ ] **Step 2: Run `WorkspaceReferencesTest` to verify RED**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker -Dtest=WorkspaceReferencesTest -Dcitrus.test.distribution.enabled=false`
Expected: FAIL to compile (`captureMacOsBaselineProcesses`, 4-arg `inUse`, and parser helpers not defined).

- [ ] **Step 3: Implement macOS post-launch survivor diff and targeted `lsof -Fn -p` in `WorkspaceReferences` & `WorkerWorkspaceRegistry`**

1. In `WorkspaceReferences.java`:
   - Keep 3-arg `inUse(roots, recovering, linuxWorkerStart)` delegating to 4-arg `inUse(roots, recovering, linuxWorkerStart, null)`.
   - On macOS, when `!recovering && macOsBaselineProcesses != null`:
     - Check `containsUnixSocket(roots)` first; if `true`, return `true`.
     - Compute `postLaunchPids`: alive `ProcessHandle.allProcesses()` excluding `currentPid`, keeping handles where `info().user()` is empty or equals `currentUser`, and where `info().startInstant()` is empty or `!Objects.equals(macOsBaselineProcesses.get(pid), startInstant)`.
     - If `postLaunchPids.isEmpty()`, return `false` (`0` `lsof` invocations).
     - If `postLaunchPids.size() <= 256`, run `/usr/sbin/lsof -n -P -Fn +w -p <comma-joined-pids>` with `LSOF_TIMEOUT_SECONDS` (`5s`), normalize `roots` via `toRealPath()` (falling back to `toAbsolutePath().normalize()`), and evaluate `parseMacOsProcessLsofOutput`: if present return its boolean; if empty (timeout, `IOException`, warning line, unexpected exit code) fall back to `macOsReferences(roots)`. If `postLaunchPids.size() > 256`, fall back directly to `macOsReferences(roots)`.
   - Note on `lsof -Fn` output format: `lsof -Fn` emits `p<pid>`, optional `f<fd>` (on some macOS builds `-Fn` alone emits `p` and `n`; accept `'p'`, `'f'`, `'n'` prefixes cleanly), and `n<path>`.
2. In `WorkerWorkspaceRegistry.java`:
   - In `create(boolean forked, boolean leased)`, capture `Map<Long, Instant> baselineProcesses = forked ? WorkspaceReferences.captureMacOsBaselineProcesses() : null` **before** returning the `Workspace`, store it on `Workspace`, and pass `(!recovering && forked && !current.leased()) ? baselineProcesses : null` into `removeOwnedDirectories` -> `WorkspaceReferences.inUse(...)`.

- [ ] **Step 4: Run full worker test suite including `WorkerWorkspaceRecoveryTest` to verify GREEN**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker -Dcitrus.test.distribution.enabled=false`
Expected: PASS (all `WorkerWorkspaceRecoveryTest` background-process & Unix-socket tests and `WorkspaceReferencesTest` pass).

- [ ] **Step 5: Commit Task 3**

```bash
git add tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/WorkspaceReferences.java \
        tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/WorkerWorkspaceRegistry.java \
        tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/WorkspaceReferencesTest.java
git commit -m "perf(distribution): skip full-host macOS lsof when no post-launch processes survive"
```

---

### Task 4: Coordinator Session-Sum History Store, Adaptive Scheduling & `/api/v1/history/durations` (Bullet 3a + 3b Broker)

**Files:**
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/store/JdbcDistributionStore.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/broker/DistributionBrokerConfig.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/broker/BrokerSessionCoordinator.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/broker/DistributionBrokerVerticle.java`
- Test: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/store/JdbcDistributionStoreTest.java`
- Test: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/broker/BrokerSessionCoordinatorTest.java`
- Test: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/broker/DistributionBrokerServerTest.java`

**Interfaces:**
- Consumes: `HistoricalDurationsRequest`, `HistoricalDurationsResponse`, 9-arg `LptPartitionScheduler.schedule(...)` from Task 1.
- Produces:
  - `DistributionBrokerConfig`:
    - `MIN_PARTITION_DURATION_MS_PROPERTY = "citrus.test.distribution.broker.minPartitionDurationMs"` / `CITRUS_DIST_BROKER_MIN_PARTITION_DURATION_MS` (default `5_000L`)
    - `MAX_WAVES_PER_WORKER_PROPERTY = "citrus.test.distribution.broker.maxWavesPerWorker"` / `CITRUS_DIST_BROKER_MAX_WAVES_PER_WORKER` (default `2`)
    - `long minPartitionDurationMs()`, `int maxWavesPerWorker()`, and builder setters `.minPartitionDurationMs(long)` / `.maxWavesPerWorker(int)`.
  - `BrokerSessionCoordinator.findHistoricalDurations(List<String> testKeys, int limit)` returning `Map<String, Long>` (clamping `limit <= 0 ? 10 : Math.min(limit, 100)`).
  - HTTP endpoint `POST /api/v1/history/durations` on `DistributionBrokerVerticle`:
    - Accepts `HistoricalDurationsRequest`, rejects `testKeys.size() > 10_000` or malformed JSON with `400`, returns `200` `HistoricalDurationsResponse` (`{"durationsMs":{}}` when `testKeys` is empty).

- [ ] **Step 1: Write failing tests in `JdbcDistributionStoreTest`, `BrokerSessionCoordinatorTest`, and `DistributionBrokerServerTest`**

Add to `JdbcDistributionStoreTest.java`:
- `shouldSumMethodDurationsPerSessionAndKeepOnlyLatestAttemptWhenComputingHistoricalDurations`: records in `sess-1` `attempt=1` (`FAILED`, `100ms`) + `attempt=2` (`PASSED` methods `400ms` + `600ms` = `1_000ms`), and in `sess-2` `attempt=1` (`PASSED` methods `700ms` + `800ms` + `SKIPPED` `0ms` = `1_500ms`) for `workUnitKey = "pkg.MultiMethodTest"` -> asserts `findHistoricalDurationsMs(List.of("pkg.MultiMethodTest"), 10)` returns `1_250L` (`(1_000 + 1_500) / 2`), **not** `520L` (per-row average) and **not** `1_300L` (summing retry attempt 1).

Add to `BrokerSessionCoordinatorTest.java`:
- `shouldPackFastUnitsAdaptivelyWhenSessionMaxPartitionSizeIsDefaultZero`: creates session with `maxPartitionSize = 0` (`CreateSessionRequest`), registers 2 online workers, submits 10 units with `200ms` historical duration each (`2_000ms` total) and empty `outputPaths` -> asserts `1` partition is scheduled.
- `shouldHonorExplicitPositiveSessionMaxPartitionSizeWithoutAdaptivePacking`: creates session with explicit `maxPartitionSize = 2`, registers 2 online workers, submits 6 units with `200ms` historical duration each -> asserts `3` partitions are scheduled (`ceil(6 / 2)`).

Add to `DistributionBrokerServerTest.java`:
- `shouldServeHistoricalDurationsEndpointAndValidateBounds`: records test executions, posts `HistoricalDurationsRequest(List.of("pkg.SampleTest", "pkg.Unknown"), 10)` to `/api/v1/history/durations` -> asserts `200` with `durationsMs` containing `"pkg.SampleTest"` only; posts empty `testKeys` -> `200` empty map; posts `10_001` keys -> `400`.

- [ ] **Step 2: Run coordinator tests to verify RED**

Run: `./mvnw test -pl tools/citrus-test-distribution-coordinator -Dtest=JdbcDistributionStoreTest,BrokerSessionCoordinatorTest,DistributionBrokerServerTest -Dcitrus.test.distribution.enabled=false`
Expected: FAIL (`minPartitionDurationMs` / `/api/v1/history/durations` missing; `JdbcDistributionStoreTest` multi-method assertion fails).

- [ ] **Step 3: Implement `JdbcDistributionStore` session-sum query, `DistributionBrokerConfig`, adaptive `BrokerSessionCoordinator`, and HTTP route**

1. In `JdbcDistributionStore.findHistoricalDurationsMs` (`lines 815-843`):
   - Query:
     ```sql
     SELECT COALESCE(session_id, '') AS sess_key,
            COALESCE(attempt, 1) AS run_attempt,
            SUM(CASE WHEN duration_ms > 0 THEN duration_ms ELSE 0 END) AS total_duration_ms,
            MAX(recorded_at) AS last_recorded_at
     FROM dist_test_executions
     WHERE work_unit_key=? AND status IN ('PASSED','FAILED')
     GROUP BY COALESCE(session_id, ''), COALESCE(attempt, 1)
     ORDER BY last_recorded_at DESC, run_attempt DESC
     ```
   - Iterate rows in order: for non-empty `sess_key`, skip if already seen in `seenSessions` (keeping only the latest attempt); include rows with `total_duration_ms > 0` (or floor positive runs at `1L` when row matched `PASSED`/`FAILED`) up to `Math.max(1, recentLimit)` session runs, and put `Math.max(1L, Math.round((double) sum / count))` into `result` when `count > 0`.
2. In `DistributionBrokerConfig`: add `minPartitionDurationMs` (default `5_000L`, `Math.max(0L, ...)`) and `maxWavesPerWorker` (default `2`, `Math.max(0, ...)`), wired in `Builder` and `fromSettings()`.
3. In `BrokerSessionCoordinator`:
   - Track `final int requestedMaxPartitionSize` on `SessionRuntime` (`request.maxPartitionSize()`).
   - Add `countEligibleOnlineWorkers(Map<String, String> requiredCapabilities)` counting distinct `conn.workerId` in `workers.values()` where `(conn.state == WorkerState.IDLE || conn.state == WorkerState.BUSY) && conn.cancelledPartitionId == null && conn.lastHeartbeatAt >= (System.currentTimeMillis() - config.leaseTimeoutMs()) && CapabilityMatcher.matches(requiredCapabilities, conn.capabilities)`.
   - In `submitWork`, set `boolean adaptive = session.requestedMaxPartitionSize <= 0 && (request.outputPaths() == null || request.outputPaths().isEmpty());` and call 9-arg `scheduler.schedule(sessionId, enrichedUnits, historicalDurations, config.defaultDurationMs(), session.maxPartitionSize, policy, adaptive ? config.minPartitionDurationMs() : 0L, adaptive ? config.maxWavesPerWorker() : 0, adaptive ? this::countEligibleOnlineWorkers : caps -> 0)`.
   - Add `public Map<String, Long> findHistoricalDurations(List<String> testKeys, int limit)` returning `Map.of()` when `testKeys == null || testKeys.isEmpty()`, otherwise delegating to `store.findHistoricalDurationsMs(dedupedNonBlankKeys, limit <= 0 ? 10 : Math.min(limit, 100))`.
4. In `DistributionBrokerVerticle`: mount `router.post("/api/v1/history/durations").handler(this::handleHistoricalDurations)` validating body & `testKeys.size() <= 10_000` (`400` on violation) and returning `200` JSON `HistoricalDurationsResponse`.

- [ ] **Step 4: Run all coordinator tests and install reactor artifact**

Run: `./mvnw test -pl tools/citrus-test-distribution-coordinator -Dcitrus.test.distribution.enabled=false && ./mvnw install -DskipTests -pl tools/citrus-test-distribution-coordinator`
Expected: PASS (all coordinator unit and server tests green; `citrus-test-distribution-coordinator` installed to local Maven repo).

- [ ] **Step 5: Commit Task 4**

```bash
git add tools/citrus-test-distribution-coordinator
git commit -m "feat(distribution): add session-summed history durations and adaptive broker scheduling"
```

---

### Task 5: Client `<2s` Local Bypass, Maven Extension Propagation & Manual Docs (Bullet 3b Client)

**Files:**
- Modify: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/BrokerHttpClient.java`
- Modify: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/LocalFallbackExecutor.java`
- Modify: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/DistributedLauncher.java`
- Modify: `tools/maven/citrus-test-distribution-maven-extension/src/main/java/org/citrusframework/mvn/distribution/TestDistributionLifecycleParticipant.java`
- Modify: `src/manual/tools-test-distribution.adoc`
- Test: `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/DistributedLauncherTest.java`
- Test: `tools/maven/citrus-test-distribution-maven-extension/src/test/java/org/citrusframework/mvn/distribution/TestDistributionMavenExtensionTest.java`

**Interfaces:**
- Consumes: `HistoricalDurationsRequest`, `HistoricalDurationsResponse`, `DistributionSettings.findConfiguredMaxPartitionSize()`, `getMinRemoteDurationMs()`, `getMinRemoteColdUnits()`, `POST /api/v1/history/durations`.
- Produces:
  - `BrokerHttpClient.findHistoricalDurations(List<String> workUnitKeys, int recentLimit)` returning `Optional<Map<String, Long>>` (`Optional.of(Map.of())` when `workUnitKeys.isEmpty()`, `Optional.of(response.durationsMs())` on HTTP `200`, `Optional.empty()` at `LOG.debug` on non-`200` or `IOException`; restores interrupt flag and returns `Optional.empty()` on `InterruptedException`).
  - `LocalFallbackExecutor.executeTestPlanLocally(Launcher delegate, TestPlan testPlan, List<WorkUnitSpec> units, TestExecutionListener[] additionalListeners)` executing `delegate.execute(testPlan, merged)` once, capturing `WorkerTestEventPayload` executions mapped to each `WorkUnitSpec.key()`, and writing `SurefireReportWriter` XML when `citrus.test.distribution.reportDir` is set.

- [ ] **Step 1: Write failing tests in `DistributedLauncherTest` and `TestDistributionMavenExtensionTest`**

Add to `DistributedLauncherTest.java`:
- `shouldRunSmallModuleLocallyOnWarmHistorySkipRemoteDispatchAndReportLocalTests`:
  - Run 1 (cold DB): executes `RemoteSampleFixture` remotely against embedded broker + worker, seeding `< 2_000ms` history in broker store (`EXECUTED_IN_COORDINATOR == false` on Run 1).
  - Run 2 (warm DB, shared reactor `sessionId` set via `SESSION_ID_PROPERTY`): resets `RemoteSampleFixture.EXECUTED_IN_COORDINATOR`, executes via `DistributedLauncher` -> asserts `RemoteSampleFixture.EXECUTED_IN_COORDINATOR == true` (ran locally!), broker session recorded `0` remote partitions for Run 2, and `reportLocalTests` recorded the local execution in the shared session.
- `shouldKeepDistributingWhenRemoteOnlyPresentOrHistoryEndpointUnavailable`:
  - Verifies `@RemoteOnly` fixture still runs remotely even when warm history `< 2_000ms`.
  - Verifies a stub HTTP server returning `404` on `/api/v1/history/durations` causes `BrokerHttpClient.findHistoricalDurations` to return `Optional.empty()` without throwing, falling back to normal remote path when `minRemoteColdUnits == 0`.
- `shouldBypassOnColdHistoryWhenMinRemoteColdUnitsThresholdMatches`: sets `MIN_REMOTE_COLD_UNITS_PROPERTY = "2"` on a cold broker with 1 remote unit -> asserts local execution (`EXECUTED_IN_COORDINATOR == true`) and zero sessions created when `SESSION_ID_PROPERTY` is unset.

Add to `TestDistributionMavenExtensionTest.java`:
- `shouldOmitMaxPartitionSizeWhenUnsetAndPropagateMinRemoteThresholds`: verifies that when `maxPartitionSize` is not configured, `CreateSessionRequest.maxPartitionSize()` sent by the extension is `0` and `MAX_PARTITION_SIZE_PROPERTY` is not injected into plugin system properties, whereas configured `minRemoteDurationMs` and `minRemoteColdUnits` are propagated into Surefire/Failsafe properties.

- [ ] **Step 2: Run launcher and Maven extension tests to verify RED**

Run: `./mvnw test -pl tools/citrus-test-distribution,tools/maven/citrus-test-distribution-maven-extension -Dtest=DistributedLauncherTest,TestDistributionMavenExtensionTest -Dcitrus.test.distribution.enabled=false`
Expected: FAIL (`findHistoricalDurations` / warm local bypass / `maxPartitionSize == 0` not implemented).

- [ ] **Step 3: Implement `BrokerHttpClient`, `LocalFallbackExecutor.executeTestPlanLocally`, `DistributedLauncher` bypass, `TestDistributionLifecycleParticipant`, and manual docs**

1. In `BrokerHttpClient.java`: implement `findHistoricalDurations(List<String> workUnitKeys, int recentLimit)` posting `HistoricalDurationsRequest` to `/api/v1/history/durations` via `sendJsonPost`, returning `Optional.of(resp.durationsMs())` on `200` and `Optional.empty()` (logging at `DEBUG`) on non-`200` or `IOException`/`InterruptedException`.
2. In `LocalFallbackExecutor.java`: extract shared listener/collector logic from `executeLocally` and add `executeTestPlanLocally(Launcher delegate, TestPlan testPlan, List<WorkUnitSpec> units, TestExecutionListener[] additionalListeners)`:
   - Index `units` by `className` (separating standard class units from `dynamicSourceName` `@CitrusTestFactory` units).
   - Run `delegate.execute(testPlan, merged)` once, resolving each leaf test's `workUnitKey` against the matching `@CitrusTestFactory` unit (matching `dynamicSourceName` in ancestor/method `TestIdentifier` uniqueId or displayName) or class unit, and writing `SurefireReportWriter` XML once per class when `reportDir` is configured.
3. In `DistributedLauncher.java`:
   - Before `ensureSessionId()`, evaluate `shouldBypassRemoteDistribution(extracted, workspace, client)`:
     - Must satisfy `!extracted.hasRemoteOnlyUnits()`, `workspace.inputArtifacts().isEmpty()`, `workspace.outputPaths().isEmpty()`, and `extracted.remoteUnits().stream().allMatch(u -> u.requiredCapabilities() == null || u.requiredCapabilities().isEmpty())`.
     - Read `long minRemoteDurationMs = DistributionSettings.getMinRemoteDurationMs();` and `int minRemoteColdUnits = DistributionSettings.getMinRemoteColdUnits();`. If both are `0`, return `false`.
     - Call `Optional<Map<String, Long>> historyOpt = minRemoteDurationMs > 0 ? client.findHistoricalDurations(remoteKeys, 10) : Optional.empty();`.
     - Warm rule: if `minRemoteDurationMs > 0 && historyOpt.isPresent()` and every key in `remoteKeys` is present in `historyOpt.get()`, return `remoteKeys.stream().mapToLong(k -> historyOpt.get().get(k)).sum() < minRemoteDurationMs`.
     - Cold/partial rule: return `minRemoteColdUnits > 0 && remoteKeys.size() <= minRemoteColdUnits`.
   - When bypassing: combine `allUnits` (`localUnits + remoteUnits`), call `LocalFallbackExecutor.executeTestPlanLocally(delegate, testPlan, allUnits, additionalListeners)`, and if `sharedSessionId` is non-blank, report `stats.executions()` via `client.reportLocalTests(sharedSessionId, new ReportLocalTestsRequest("local-host", stats.executions()))` (catching/logging transient report errors at `WARN` without failing tests), then `return` without calling `ensureSessionId()` or `packageCurrentClasspath()`.
   - In `ensureSessionId(...)`, pass `DistributionSettings.findConfiguredMaxPartitionSize().orElse(0)` instead of `DistributionSettings.getMaxPartitionSize()`.
4. In `TestDistributionLifecycleParticipant.java`:
   - Pass `DistributionSettings.findConfiguredMaxPartitionSize(mergedProps::getProperty, System::getenv).orElse(0)` in `CreateSessionRequest`.
   - Only put `MAX_PARTITION_SIZE_PROPERTY` into `sharedProps` when `findConfiguredMaxPartitionSize` is present.
   - Propagate `MIN_REMOTE_DURATION_MS_PROPERTY` and `MIN_REMOTE_COLD_UNITS_PROPERTY` into `sharedProps` when configured in `mergedProps` or environment.
5. In `src/manual/tools-test-distribution.adoc`:
   - Update the macOS workspace reference paragraph (~line 579) to note that clean forked workspace closes skip full-host `lsof +D` when no post-launch processes survive (using targeted `lsof -p` on surviving post-launch PIDs), while startup recovery, leased workspaces, and in-process runs retain full `lsof +D`.
   - Document `citrus.test.distribution.minRemoteDurationMs`, `citrus.test.distribution.minRemoteColdUnits`, `citrus.test.distribution.broker.minPartitionDurationMs`, and `citrus.test.distribution.broker.maxWavesPerWorker` in the configuration reference section.

- [ ] **Step 4: Run full 5-module test suite to verify GREEN**

Run: `./mvnw test -pl tools/citrus-test-distribution-protocol,tools/citrus-test-distribution-coordinator,tools/citrus-test-distribution-worker,tools/citrus-test-distribution,tools/maven/citrus-test-distribution-maven-extension -Dcitrus.test.distribution.enabled=false`
Expected: PASS (0 failures, 0 errors across all 5 modules).

- [ ] **Step 5: Commit Task 5**

```bash
git add tools/citrus-test-distribution \
        tools/maven/citrus-test-distribution-maven-extension \
        src/manual/tools-test-distribution.adoc
git commit -m "feat(distribution): bypass remote dispatch for <2s warm modules and document settings"
```
