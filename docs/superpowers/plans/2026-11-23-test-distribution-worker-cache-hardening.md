# Worker Local File Cache Hardening Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve every remaining minor, deferred, and recommended item from the whole-branch review of PR #5 (worker local file cache), except items the spec mandates or explicitly accepts.

**Spec:** `docs/superpowers/specs/2026-11-22-test-distribution-local-file-cache-design.md` (binding). Base: `19f9c965d8`.

## Global Constraints

- Java 21+, Maven 3.9+, zero new third-party dependencies; Apache 2.0 header on new files; zero third-party vendor/company product names in anything added.
- Keep all existing behavior for valid inputs; precedence stays CLI/builder > `citrus.test.distribution.worker.*` sysprop > `CITRUS_DIST_WORKER_CACHE_*` env > `TEST_DISTRIBUTION_AGENT_CACHE_*` env > default.
- Tests verify behavior, have bounded waits, never write to the real `${user.home}`, never bind port 8765.
- Known environment-only failures (not to be fixed): jbang `ListTest` (other CitrusJBang processes), `JBangTestEngineTest` (worktree path contains `/.claude/`), testcontainers tests (no Docker).

## Already done (no work)

Offline workers excluded from low-disk; `Builder.cacheDir(null)` rejected; empty `blobs/<2-hex>/` pruned; named-JAR dir deleted before blob (stats stay accurate); read-only-dir test skipped as root; jbang Aesh binding of normalized cache flags asserted; restricted tmp sweep with foreign-entry and symlink tests.

## Out of scope (spec-mandated or spec-accepted)

Package-private `IsolatedPartitionRunner` constructor (spec 5.3); soft free-space shortfall policy (spec 2 anti-goals / 5.6); copy-fallback named JARs not stamped/counted (spec 5.7 accepted under-count); `user.home` set to `?` (JDK falls back to `$HOME`; environmental).

---

### Task 1: Shared Worker Cache CLI Parser, JBang & Agent Fixes

**Files:**
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/config/WorkerCacheCliOptions.java`
- Create: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/config/WorkerCacheCliOptionsTest.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/config/DistributionSettings.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/DistributionWorkerConfig.java`
- Modify: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistWorkerStart.java`, `tools/jbang/src/main/java/org/citrusframework/jbang/cli/CitrusJBangMain.java`, `tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/DistCommandTest.java`
- Modify: `tools/agent/src/main/java/org/citrusframework/agent/CitrusAgentOptions.java`, `tools/agent/src/main/java/org/citrusframework/agent/DistributionWorkerBridge.java`, `tools/agent/src/main/java/org/citrusframework/agent/CitrusAgentServer.java` (or the agent main entry), `tools/agent/src/test/java/org/citrusframework/agent/CitrusAgentWorkerOptionsTest.java`

**Requirements:**
1. One shared parser for the worker cache flags in the protocol module (both jbang and agent already depend on it), owning the full spec 5.2 matrix: exact-name matching (`--<name>` / `--<name>=<value>`), `--cache-dir` / `--cache-free-space-buffer-size` value rules (missing/blank/next token starts with `-` → `IllegalArgumentException("Missing parameter value for <flag> option")`, never consuming the following flag), `--[no-]cache-cleanup[=true|false]` incl. space-separated boolean consumption for non-`-` tokens and strict validation via `parseBooleanStrict`, last flag wins, unknown `--cache*`/`--no-cache*` → `IllegalArgumentException("Unknown cache option: <arg>")`. Expose a pure API, e.g. `static Result parse(List<String> args, Set<String> valueOptionsOfHost)` returning the extracted cache settings (nullable `cacheDir`, `Boolean cacheCleanup`, `Long cacheFreeSpaceBufferBytes`) plus the remaining non-cache args in original order. `valueOptionsOfHost` lists the host command's value-taking options so tokens that are values of other options (e.g. `--auth-token --cache-x`) are skipped identically in both hosts.
2. JBang `DistWorkerStart.normalizeWorkerStartArgs` and agent `CitrusAgentOptions` both delegate to the shared parser; remove their duplicated logic. Behavior must be identical in both hosts, including `--help`/`-h` handling: decide one rule (validate cache flags even when help is present, except print help) and apply to both; document it in the parser javadoc.
3. JBang: normalization must also apply when tokens precede `dist worker start` (locate the `dist`,`worker`,`start` subsequence after leading global options rather than requiring `args[0..2]`); a bare `--cache-cleanup` then works; `--no-cache-cleanup --cache-cleanup=true` resolves last-flag-wins (`true`).
4. JBang: comment the `noCacheCleanup` field as help-only (normalizer rewrites it). Replace the help test's global `System.out` swap with capturing the command printer/output used by other `DistCommandTest` tests. Add tests: unset cache flags inherit a sysprop value (`citrus.test.distribution.worker.cacheFreeSpaceBufferSize=1MiB` → `1048576`, restored in finally); blank `--cache-dir " "` and `--cache-dir=` rejected.
5. JBang shutdown ordering: `DistWorkerStart` must close the `DistributionWorkerClient` (releasing the cache lock) from its shutdown hook / before returning, not rely on JVM exit.
6. Agent: a cache option parse error surfaces as a one-line operator error (message, no stack trace) and non-zero exit, consistent with how the agent reports other invalid options at its entry point.
7. Invalid-matrix tests in both `DistCommandTest` and `CitrusAgentWorkerOptionsTest` assert the message (`hasMessageContaining` the flag name or `Unknown cache option`), not only the type; `WorkerCacheCliOptionsTest` covers the full matrix once.
8. `DistributionWorkerConfig.Builder.cacheFreeSpaceBufferSize(String size)` (spec 5.1): parses via `DistributionSettings.parseByteSize(size, "cacheFreeSpaceBufferSize")`, rejecting null/blank/invalid with `IllegalArgumentException`. Tests.
9. `DistributionSettings`: replace the `String[]` `{value, source}` pair from `resolveFirstNonBlank` with a small private record.

- [ ] Write failing tests (shared parser matrix, jbang/agent delegation incl. leading-token and last-wins cases, sysprop inheritance, blank `--cache-dir`, message assertions, agent one-line error, builder `cacheFreeSpaceBufferSize(String)`), run RED.
- [ ] Implement, run GREEN: `./mvnw -o test -pl tools/citrus-test-distribution-protocol,tools/citrus-test-distribution-worker,tools/agent,tools/jbang -am`.
- [ ] Commit: `refactor(distribution): share worker cache CLI parsing between JBang and Citrus Agent`.

---

### Task 2: Cache Engine, Warm-Path Materialization, and Lifecycle Hardening

**Files:**
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/cache/BlobCacheDirectory.java`, `.../cache/WorkspacePackager.java`, their tests (`BlobCacheDirectoryTest`, `WorkspacePackagerTest`)
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/WorkerFileCache.java`, `IsolatedPartitionRunner.java`, `ForkedPartitionRunner.java`, `DistributionWorkerClient.java`, and tests (`WorkerFileCacheTest`, `DistributionWorkerClientTest`, `ForkedPartitionRunnerTest`, `PartitionClassLoaderTest` as needed)
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/broker/...` (coordinator blob cache owner marker) and `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/...` (host launcher cache owner marker) — only the lines that create those caches.

**Requirements:**
1. Warm path (spec 7 "0 SHA-256 re-hashes"): add overloads that accept an already-verified blob path, e.g. `BlobCacheDirectory.materializeForClasspath(ArtifactRef ref, Path verifiedBlob, Path workspaceDir)` and `WorkspacePackager.materializeIntoDirectory(BlobCacheDirectory cache, WorkspaceFileSet set, Path verifiedBlob, Path target)`. Both runners pass the path returned by `BrokerBlobClient.ensureCached(...)` (which went through `WorkerFileCache.verifyOrInvalidate`). Existing overloads keep their verifying behavior for other callers. Test: warm second partition performs zero full SHA-256 hashes of directory/input archives (count via a package-private hashing hook or by asserting `getVerifiedPath` is not called).
2. Corrupt-blob deletion only under `evictionLock` with stats: no `getVerifiedPath` call from the worker deletes a blob outside `WorkerFileCache` (route through `verifyOrInvalidate`). `verifyOrInvalidate` must distinguish "vanished" (log DEBUG, `forgetBlob`) from "corrupt" (WARN "Removing corrupted blob", delete, `forgetBlob`) — no false "corrupted" WARN for a blob that disappeared.
3. Pin vs eviction: `PinLease.pin`/`pinAll` acquire `evictionLock`, so pinning and eviction are mutually exclusive regardless of thread. Keep the existing single-thread comment in `afterPartition` updated.
4. Fast-path touch after close: the `verifyOrInvalidate` fast path must not touch mtime/stamps once closed (check `closed` under `evictionLock`, or take the lock for the touch).
5. Close latency: `DistributionWorkerClient.closeResources()` must not hold the client monitor while `WorkerFileCache.close()` waits on `evictionLock` (capture references under the monitor, close outside it), keeping idempotency and the start/close race safety.
6. Forked children on close: when `DistributionWorkerClient.close()` gives up waiting for the partition thread, it kills any still-running forked child JVMs (via the runner's existing `killAll`) before closing the cache.
7. In-process jar handles: the in-process runner must not keep named JARs open after the partition (JDK `jar:` URL caching) — e.g. disable `URLConnection` caching for `jar` URLs in `PartitionClassLoader` (`setDefaultUseCaches("jar", false)` scoped appropriately, or opening resources with `useCaches=false`) and close the loader deterministically. Test on POSIX: after an in-process partition, evicting its JAR digest frees the inode (link count / `Files.notExists`) and on all OSes deletion succeeds.
8. Temp sweep allow-list: replace the generic `^[a-z0-9]{1,32}-[0-9]+\.tmp$` with an explicit allow-list of the prefixes the cache creates (enumerate all `newTempFile` callers: 8-hex staging prefixes, `ws-`, `output-`, `dl-`, and any others) so a user's `backup-2024.tmp` is never deleted. Test it.
9. Foreign-cache guard: the coordinator blob cache and the host launcher cache write a small owner marker file (e.g. `.citrus-cache-owner` containing `coordinator` / `launcher`) when they create/open their cache root; `WorkerFileCache.open()` writes `worker` if absent and refuses (IllegalStateException naming the dir and owner, with `--cache-dir` guidance) a directory whose marker names another owner. Tests for refuse + accept-own + legacy-no-marker.
10. Probe-failure WARN rate limit: a failing `DiskSpaceProbe` logs WARN once per failure episode (reset after a successful probe), DEBUG otherwise. Low-disk WARN wording distinguishes "no unpinned blobs left" from "deletes failed".
11. `blobCache()` accessor: remove it from `WorkerFileCache` if no production caller needs it after req 1; otherwise document that direct mutation bypasses accounting and make callers use only read methods.
12. Protocol cleanups: translate `DirectoryIteratorException` to its `IOException` cause in `sweepTempFiles`/`sweepClasspathStaging`; remove the dead `!classpathDir.startsWith(rootDir)` guard in `delete` (digest already validated); fix import order (`FileVisitResult`, `SimpleFileVisitor`) and wrap the over-long `namedJar` javadoc line; add a test for `moveIntoPlace` partial-target cleanup on non-atomic fallback failure (via an injectable move hook or a filesystem that rejects ATOMIC_MOVE).
13. Test hygiene: the read-only-dir tests check actual unwritability after chmod (`assumeTrue(!Files.isWritable(dir))`) instead of `user.name`; the cross-JVM lock test asserts after the `try` (kill in `finally`, assert afterwards); the delete-failure WARN is asserted by capturing the log (existing test logging setup) or a package-private warning counter consistent with the low-disk counter.

- [ ] Failing tests first (each numbered requirement), RED.
- [ ] Implement, GREEN: `./mvnw -o test -pl tools/citrus-test-distribution-protocol,tools/citrus-test-distribution-worker,tools/citrus-test-distribution-coordinator,tools/citrus-test-distribution -am`.
- [ ] Commit(s): `perf(distribution): reuse verified worker cache blobs during materialization`, `fix(distribution): harden worker cache pinning, close, and foreign cache directories`.

---

### Task 3: Blob Download Idle Timeout and Worker Client Telemetry

**Files:**
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/BrokerBlobClient.java`, `DistributionWorkerClient.java`, `DistributionWorkerConfig.java`
- Modify: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/BrokerBlobClientCacheTest.java`, `DistributionWorkerClientTest.java`

**Requirements:**
1. Per-read idle timeout for blob downloads: if no bytes arrive for `blobReadIdleTimeout` (default 60 s; package-private/config-settable for tests), abort the download with `IOException("Blob download stalled ...")`, close the body stream, delete the temp file. Implement without new dependencies (e.g. a watchdog on the shared scheduler that closes the `InputStream`, reset per chunk). Test with a server that sends headers + some bytes then stalls: aborts within the timeout, zero tmp files, partition fails cleanly, next download works.
2. `DistributionWorkerClient.close()` aborts an in-flight stalled download promptly (blob client shutdown happens before/independent of the bounded partition await, or the watchdog closes the stream on close). Test: close() returns within a bounded time while a download is stalled.
3. `cacheStats()` failure logging: WARN once per failure episode (reset after success), DEBUG thereafter.
4. Oversized-download tests: make the max blob size injectable (package-private constructor/config used only by tests) so cap tests stream a few KiB instead of ~256 MiB; keep one assertion that production uses `ClasspathPackager.MAX_BLOB_BYTES`.
5. ENOSPC when opening the temp file: test that a failure creating/opening the download temp file fails the download with `IOException`, leaves zero tmp files, and the next partition succeeds.
6. Fail-fast lock test: assert the second client's `start()` throws while worker1 is executing a partition (hold worker1's partition with a latch-controlled fixture), then succeeds after worker1.close().

- [ ] Failing tests first, RED.
- [ ] Implement, GREEN: `./mvnw -o test -pl tools/citrus-test-distribution-worker -am`.
- [ ] Commit: `fix(distribution): abort stalled worker blob downloads and tighten worker cache tests`.

---

### Task 4: Coordinator, UI, Docs, and E2E Test Cleanups

**Files:**
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/store/JdbcDistributionStore.java` and `JdbcDistributionStoreTest.java`
- Modify: `tools/citrus-test-distribution-ui/src/main/resources/META-INF/resources/dist-ui/app.js`, `styles.css`, `tools/citrus-test-distribution-ui/src/test/java/org/citrusframework/distribution/ui/DistributionUiResourcesTest.java`
- Modify: `src/manual/tools-test-distribution.adoc`
- Modify: `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/DistributedLauncherTest.java`

**Requirements:**
1. `listWorkers`: malformed `cache_stats_json` (and malformed capabilities JSON) logs WARN with the worker id and yields `null` cache stats / empty capabilities instead of failing the whole listing. Test with a corrupted row.
2. UI: the low-disk filter pill reads `LOW DISK` (display label) while the filter value stays `LOW_DISK`; the Overview low-disk badge sits in a styled flex wrapper aligned with the state badge (add a CSS class, no inline styles).
3. UI tests: strengthen `DistributionUiResourcesTest` beyond `contains` where feasible without new dependencies — e.g. if a JavaScript engine is unavailable, assert the concrete guard expressions (offline exclusion, `<= 0` buffer → 100, `< 0` usable → N/A) and the pill label mapping.
4. Manual: document the agent's space-separated `--cache-cleanup true|false` form alongside `=value`; document the foreign-cache refusal (Task 2 req 9) and the blob download idle timeout (Task 3 req 1) in one sentence each; document the shared help/validation rule from Task 1 req 2 if user-visible.
5. `DistributedLauncherTest`: replace the inline fully-qualified names added by the cache E2E tests with imports.

- [ ] Failing tests first where behavior changes, RED.
- [ ] Implement, GREEN: `./mvnw -o test -pl tools/citrus-test-distribution-ui,tools/citrus-test-distribution-coordinator,tools/citrus-test-distribution -am`.
- [ ] Commit: `fix(distribution): tolerate malformed worker telemetry rows and polish cache UI and docs`.
