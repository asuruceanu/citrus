# Agent Pools, Workers Pool Parity, and Historical Usage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Develocity-modeled Agent Pools (CRUD, priority ordering, registration keys, 1h/4h grace windows), Workers Pool parity (pool grouping, version, live executing test/idle status, warnings), Historical Usage (`1h`/`24h`/`7d` charts with capability/requirement filtering), and a generic Build Sessions Explorer subtitle to Citrus Test Distribution.

**Architecture:** Extend `citrus-test-distribution-protocol` with pool, registration-key, warning, renewal, and usage-sample records plus bare-capability support. Persist pools, SHA-256 hashed keys, worker pool/warning metadata, and 15-second usage snapshots in `JdbcDistributionStore` (HSQLDB & PostgreSQL), enforce pool priority + capability matching + grace windows in `SessionCoordinator`, expose `/api/v1/agent-pools*` and `/api/v1/usage` in `DistributionCoordinatorVerticle`, wire `--pool` and `--registration-key` + hourly renewal through `DistributionWorkerClient`, `citrus-jbang`, and `citrus-agent`, and render the zero-Node ESM UI in `dist-ui`.

**Tech Stack:** Java 21, Maven 3.9+, JUnit Jupiter, AssertJ, Vert.x 4 (HTTP/WebSocket/SSE), JDBC (HSQLDB & PostgreSQL), Aesh CLI (`citrus-jbang`), `CitrusAppOptions` (`citrus-agent`), Jackson JSON Schema (`schema-generator`), browser ESM + `htm-mini.js` + inline SVG (`citrus-test-distribution-ui`), AsciiDoc.

**Spec:** `docs/superpowers/specs/2026-11-24-agent-pools-and-historical-usage-design.md`

## Global Constraints

- Do **not** implement Connecting Builds / Jobs view, separate `Agents` tab, access control / RBAC, project groups, or autoscaling (`minSize` / `maxSize`).
- Keep `tools/citrus-test-distribution-protocol` free of `citrus-api` and Vert.x dependencies (`jackson-databind` and `slf4j-api` only).
- Keep `tools/citrus-test-distribution-ui` dependency-free browser ESM (`htm-mini.js`, vanilla JS, inline SVG, CSS custom properties) with no Node/npm build step.
- Never store plaintext registration keys in JDBC or return secret keys after initial generation (`GeneratedRegistrationKey.secretKey` is returned once on `POST`; `RegistrationKeyRecord` exposes only `keyPrefix` + metadata).
- Preserve backward-compatible hybrid mode (`citrus.test.distribution.coordinator.requireAgentPool=false` by default) so existing unpooled workers continue working unless strict pool mode is enabled.
- On `CitrusAgentOptions`, use long `--pool` only (short `-p` is already `--package` in `CitrusAppOptions`) and `-k` / `--registration-key`; on `DistWorkerStart`, support `-p` / `--pool` and `-k` / `--registration-key` and register all four tokens in `DistWorkerStart.VALUE_OPTIONS`.
- Replace the `Build Sessions Explorer` subtitle `"Inspect distributed Maven & JBang reactor runs, classpath cache hit rates, and test outcomes."` verbatim with `"Inspect distributed build sessions, artifact cache hit rates, and test outcomes."`

## Review Focus

1. **Cross-pool or revoked registration key reuse:** A valid registration key belonging to Pool A presented by a worker joining Pool B (or revoked 1ms earlier) must fail immediately with `INVALID_REGISTRATION_KEY` / `REVOKED_REGISTRATION_KEY`, leave `lastUsedAt` untouched, and stop `DistributionWorkerClient` from reconnect-looping (pinned in Tasks 3 and 5).
2. **Worker-only extra capabilities leaking past pool capabilities:** A worker in Pool A (`jdk=21, os=linux`) advertising extra `postgres=18` must neither be scheduled for a session requiring `postgres=18` nor match `GET /api/v1/usage?filter=postgres=18` (pinned in Task 3).
3. **Vert.x route shadowing on `/api/v1/agent-pools/priority`:** `PUT /api/v1/agent-pools/priority` must be registered before `PUT /api/v1/agent-pools/:poolId` so `"priority"` is never matched as a `:poolId` path parameter (pinned in Task 4).
4. **CLI short-flag and cache-parser collisions:** `DistWorkerStart.normalizeWorkerStartArgs` must preserve `-p my-pool -k ctrk_a_b` alongside `--no-cache-cleanup`, and `CitrusAgentOptions` must bind `-p com.acme.tests --pool my-pool -k ctrk_a_b` to `packages=["com.acme.tests"]`, `pool="my-pool"`, and `registrationKey="ctrk_a_b"` without collision (pinned in Task 5).
5. **Double-counted `desired` across multiple compatible pools in Historical Usage:** When high-priority Pool A (`connected=2`) and lower-priority Pool B (`connected=4`) both match a job with 3 active/pending partitions, global `desired` must be `3` (not `6`), Pool A `desired` must be `2`, and Pool B `desired` must be `1` (pinned in Task 3).

---

### Task 1: Protocol Records, Capability Validation, `@AgentRequirement` Default, and Settings

**Files:**
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/AgentPoolRecord.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/AgentPoolSummary.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/RegistrationKeyRecord.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/GeneratedRegistrationKey.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/WorkerWarningCode.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/WorkerWarning.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/WorkerRejectedPayload.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/WorkerRenewPayload.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/UsageSampleRecord.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/UsagePoint.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/PoolUsageSeries.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/HistoricalUsageResponse.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/WorkerRecord.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/WorkerRegisterPayload.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/WorkerWsMessage.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/annotations/AgentRequirement.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/scheduler/CapabilityMatcher.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/config/DistributionSettings.java`
- Test: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/model/ProtocolModelSerializationTest.java`
- Test: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/scheduler/CapabilityMatcherTest.java`
- Test: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/config/DistributionSettingsAndPathRewriterTest.java`

**Interfaces:**
- Consumes: `DistributionJson`, `WorkerState`, `WorkerCacheStats`
- Produces:
  - `AgentPoolRecord(String poolId, String displayName, Map<String, String> capabilities, int priorityOrder, long createdAt, long updatedAt)`
  - `AgentPoolSummary(String poolId, String displayName, Map<String, String> capabilities, int priorityOrder, int connectedWorkers, int busyWorkers, int idleWorkers, int activeKeyCount, long createdAt, long updatedAt)`
  - `RegistrationKeyRecord(String keyId, String poolId, String keyPrefix, String description, long createdAt, Long lastUsedAt, String lastUsedBy, Long revokedAt)`
  - `GeneratedRegistrationKey(RegistrationKeyRecord metadata, String secretKey)`
  - `WorkerWarningCode { POOL_CAPABILITY_MISMATCH, POOL_DELETED, KEY_REVOKED }`
  - `WorkerWarning(WorkerWarningCode code, String message, long detectedAt, long disconnectAt)`
  - `WorkerRejectedPayload(String code, String message)`
  - `WorkerRenewPayload(String workerId, String poolId, String registrationKey)`
  - `WorkerRegisterPayload(String workerId, String host, String version, String poolId, String registrationKey, Map<String, String> capabilities, WorkerCacheStats cacheStats)` (retaining 4-arg and 3-arg convenience constructors)
  - `WorkerRecord(String workerId, String host, String version, String poolId, String poolDisplayName, Map<String, String> capabilities, WorkerState state, String activePartitionId, String activeTestDisplay, long stateUpdatedAt, long lastHeartbeatAt, long registeredAt, WorkerCacheStats cacheStats, List<WorkerWarning> warnings)` (retaining 8-arg and 7-arg convenience constructors with `warnings = List.of()` when null)
  - `WorkerWsMessage.WORKER_REJECTED = "worker.rejected"`, `WORKER_RENEW = "worker.renew"`, `WORKER_RENEWED = "worker.renewed"`
  - `UsageSampleRecord(long sampledAt, List<JobDemandBucket> activeJobs, List<PoolSampleBucket> pools, List<UnpooledWorkerBucket> unpooledWorkers)` with nested records `JobDemandBucket(Map<String, String> requiredCapabilities, int activeSessions, int activePartitions, int pendingPartitions)`, `PoolSampleBucket(String poolId, String displayName, int priorityOrder, Map<String, String> capabilities, int connected, int utilized)`, `UnpooledWorkerBucket(Map<String, String> capabilities, int connected, int utilized)`
  - `UsagePoint(long timestamp, int activeJobs, int activePartitions, int utilized, int desired, int connected)`
  - `PoolUsageSeries(String poolId, String displayName, Map<String, String> capabilities, List<UsagePoint> points)`
  - `HistoricalUsageResponse(String window, long fromTimestamp, long toTimestamp, long bucketMs, Map<String, String> filter, List<UsagePoint> series, List<PoolUsageSeries> pools)`
  - `AgentRequirement#value()` default `"true"`
  - `CapabilityMatcher.validateCapabilities(Map<String, String> capabilities)` and `DistributionSettings.parseStrictCapabilitiesCsv(String csv)` validating `[A-Za-z0-9._-]+` tokens and at most one `=` per entry
  - `DistributionSettings`:
    - `WORKER_POOL_PROPERTY = "citrus.test.distribution.worker.pool"`, `WORKER_POOL_ENV = "CITRUS_DIST_WORKER_POOL"`, `DEVELOCITY_AGENT_POOL_ENV = "TEST_DISTRIBUTION_AGENT_POOL"`
    - `WORKER_REGISTRATION_KEY_PROPERTY = "citrus.test.distribution.worker.registrationKey"`, `WORKER_REGISTRATION_KEY_ENV = "CITRUS_DIST_WORKER_REGISTRATION_KEY"`, `DEVELOCITY_AGENT_REGISTRATION_KEY_ENV = "TEST_DISTRIBUTION_AGENT_REGISTRATION_KEY"`
    - `DEVELOCITY_AGENT_NAME_ENV = "TEST_DISTRIBUTION_AGENT_NAME"`, `DEVELOCITY_AGENT_CAPABILITIES_ENV = "TEST_DISTRIBUTION_AGENT_CAPABILITIES"`
    - `COORDINATOR_REQUIRE_AGENT_POOL_PROPERTY = "citrus.test.distribution.coordinator.requireAgentPool"`, `COORDINATOR_REQUIRE_AGENT_POOL_ENV = "CITRUS_DIST_COORDINATOR_REQUIRE_AGENT_POOL"`, `isCoordinatorRequireAgentPool()`
    - `resolveWorkerPool(Properties, Map<String, String>)`, `resolveWorkerRegistrationKey(Properties, Map<String, String>)`, `resolveWorkerId(Properties, Map<String, String>)`, `resolveWorkerCapabilitiesCsv(Properties, Map<String, String>)`

- [ ] **Step 1: Write failing protocol serialization, capability validation, annotation, and settings tests**

In `ProtocolModelSerializationTest.java`, add round-trip JSON assertions via `DistributionJson.MAPPER` for `AgentPoolRecord`, `AgentPoolSummary`, `RegistrationKeyRecord`, `GeneratedRegistrationKey`, `WorkerWarning`, `WorkerRejectedPayload`, `WorkerRenewPayload`, `UsageSampleRecord`, `HistoricalUsageResponse`, extended `WorkerRegisterPayload`, and extended `WorkerRecord` (verifying 8-arg legacy `WorkerRecord` constructor defaults `warnings` to `List.of()`).

In `CapabilityMatcherTest.java`, add tests verifying:
1. `@AgentRequirement(key = "docker")` compiles without `value` and defaults `value()` to `"true"`, matching `DistributionSettings.parseStrictCapabilitiesCsv("docker,postgres=18")` (`{"docker": "true", "postgres": "18"}`).
2. `DistributionSettings.parseStrictCapabilitiesCsv` rejects malformed entries (`"a=b=c"`, `"bad space=1"`, `"=val"`, `"key="`) with `IllegalArgumentException`.

In `DistributionSettingsAndPathRewriterTest.java`, add tests verifying precedence `CITRUS_DIST_WORKER_POOL` > `TEST_DISTRIBUTION_AGENT_POOL` > `citrus.test.distribution.worker.pool` (and matching precedence for `registrationKey`, `workerId` / `TEST_DISTRIBUTION_AGENT_NAME`, `capabilities` / `TEST_DISTRIBUTION_AGENT_CAPABILITIES`, and `isCoordinatorRequireAgentPool()`).

- [ ] **Step 2: Run protocol tests to verify they fail**

Run: `./mvnw test -pl tools/citrus-test-distribution-protocol`
Expected: FAIL with compilation errors for missing model records and `DistributionSettings` methods.

- [ ] **Step 3: Implement protocol records, `@AgentRequirement` default, strict capability validation, and `DistributionSettings` resolvers**

Create all records listed in **Produces** with defensive `Map.copyOf` / `List.copyOf` canonical constructors where nullable collections may arrive from Jackson. Add `default "true"` to `AgentRequirement#value()`, add `CapabilityMatcher.validateCapabilities` + `DistributionSettings.parseStrictCapabilitiesCsv`, and add the pool/key/alias constants and resolvers in `DistributionSettings`.

- [ ] **Step 4: Run protocol tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-protocol`
Expected: PASS (`BUILD SUCCESS`, 0 failures).

- [ ] **Step 5: Commit Task 1**

```bash
git add tools/citrus-test-distribution-protocol
git commit -m "feat(test-distribution-protocol): add agent pool, registration key, warning, and usage models"
```

---

### Task 2: JDBC Persistence for Agent Pools, Registration Keys, Worker Metadata, and Usage Samples

**Files:**
- Create: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/store/StoredRegistrationKey.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/store/DistributionStore.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/store/JdbcDistributionStore.java`
- Test: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/store/JdbcDistributionStoreTest.java`

**Interfaces:**
- Consumes: Task 1 protocol records (`AgentPoolRecord`, `RegistrationKeyRecord`, `WorkerRecord`, `WorkerWarning`, `UsageSampleRecord`)
- Produces:
  - `StoredRegistrationKey(RegistrationKeyRecord metadata, String keyHash)`
  - `DistributionStore`:
    - `void upsertAgentPool(AgentPoolRecord pool)`
    - `Optional<AgentPoolRecord> findAgentPool(String poolId)`
    - `List<AgentPoolRecord> listAgentPools()`
    - `boolean deleteAgentPool(String poolId)`
    - `void updateAgentPoolPriorities(List<String> orderedPoolIds, long updatedAt)`
    - `void insertRegistrationKey(StoredRegistrationKey key)`
    - `List<StoredRegistrationKey> listRegistrationKeys(String poolId)`
    - `Optional<StoredRegistrationKey> findRegistrationKey(String keyId)`
    - `boolean revokeRegistrationKey(String poolId, String keyId, long revokedAt)`
    - `void touchRegistrationKeyUsage(String keyId, long usedAt, String workerId)`
    - `void recordUsageSample(UsageSampleRecord sample, long pruneBeforeTimestamp)`
    - `List<UsageSampleRecord> listUsageSamples(long fromTimestamp, long toTimestamp)`

- [ ] **Step 1: Write failing JDBC store tests**

In `JdbcDistributionStoreTest.java`, add tests covering:
1. Agent pool CRUD + `listAgentPools()` ordered by `priority_order ASC, created_at ASC` + `updateAgentPoolPriorities(List.of("pool2", "pool1"), now)` + `deleteAgentPool`.
2. Registration key insert, `listRegistrationKeys(poolId)` returning `keyHash` + `RegistrationKeyRecord`, `touchRegistrationKeyUsage` updating `lastUsedAt` and `lastUsedBy`, and `revokeRegistrationKey` setting `revokedAt` (returning `false` when `poolId` does not match).
3. `upsertWorker` / `listWorkers` round-tripping `version`, `poolId`, `poolDisplayName`, `activeTestDisplay`, `stateUpdatedAt`, and `List<WorkerWarning>`, plus schema migration when opening an existing `dist_workers` table created before the new columns existed.
4. `recordUsageSample` persisting snapshots, pruning rows with `sampled_at < pruneBeforeTimestamp`, and `listUsageSamples(from, to)` returning ascending samples within `[from, to]`.

- [ ] **Step 2: Run `JdbcDistributionStoreTest` to verify it fails**

Run: `./mvnw test -Dtest=JdbcDistributionStoreTest -pl tools/citrus-test-distribution-coordinator -am`
Expected: FAIL with compilation errors on missing `DistributionStore` methods and `StoredRegistrationKey`.

- [ ] **Step 3: Implement schema tables, `ensureColumns` migration, and JDBC queries in `JdbcDistributionStore`**

Add `dist_agent_pools`, `dist_registration_keys` (plus `idx_dist_reg_keys_pool`), and `dist_usage_samples` in `JdbcDistributionStore.initialize()` using `longText` (`CLOB` on HSQLDB, `TEXT` on PostgreSQL). Extend `dist_workers` and `ensureColumns(...)` with `version`, `pool_id`, `pool_display_name`, `active_test_display`, `state_updated_at`, and `warnings_json`. Implement all methods in `DistributionStore` and `JdbcDistributionStore`.

- [ ] **Step 4: Run `JdbcDistributionStoreTest` to verify it passes**

Run: `./mvnw test -Dtest=JdbcDistributionStoreTest -pl tools/citrus-test-distribution-coordinator`
Expected: PASS (`BUILD SUCCESS`, 0 failures).

- [ ] **Step 5: Commit Task 2**

```bash
git add tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/store \
        tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/store
git commit -m "feat(test-distribution-coordinator): persist agent pools, registration keys, and usage samples"
```

---

### Task 3: Pool Auth, Grace Windows, Priority Scheduling, Live Worker Status, and Usage Aggregation

**Files:**
- Create: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/RegistrationKeyCrypto.java`
- Create: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/HistoricalUsageAggregator.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorConfig.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/SessionCoordinator.java`
- Test: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/coordinator/SessionCoordinatorTest.java`
- Test: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/coordinator/HistoricalUsageAggregatorTest.java`

**Interfaces:**
- Consumes: Task 1 models & Task 2 `DistributionStore` / `StoredRegistrationKey`
- Produces:
  - `DistributionCoordinatorConfig` additions:
    - `boolean requireAgentPool()` (default `DistributionSettings.isCoordinatorRequireAgentPool()`)
    - `long keyRevocationGraceMs()` (default `3_600_000L`)
    - `long poolDriftGraceMs()` (default `14_400_000L`)
    - `long usageSampleIntervalMs()` (default `15_000L`)
    - `long usageRetentionMs()` (default `604_800_000L`)
    - `LongSupplier clock()` (default `System::currentTimeMillis`)
  - `RegistrationKeyCrypto`:
    - `static String generatePoolId()` (8 lowercase `[a-z0-9]` chars via `SecureRandom`)
    - `static GeneratedSecret generateSecret()` returning `(String keyPrefix, String secretKey, String sha256Hex)` with format `ctrk_<8chars>_<32chars>`
    - `static boolean verifySecret(String candidateSecret, String expectedSha256Hex)` (constant-time `MessageDigest.isEqual`)
  - `HistoricalUsageAggregator`:
    - `static HistoricalUsageResponse aggregate(String window, long nowMs, Map<String, String> filter, List<UsageSampleRecord> samples, List<AgentPoolRecord> currentPools)`
  - `SessionCoordinator`:
    - `record CreatedAgentPool(AgentPoolSummary pool, GeneratedRegistrationKey generatedKey)`
    - `record AgentPoolDetail(AgentPoolSummary pool, List<RegistrationKeyRecord> registrationKeys)`
    - `List<AgentPoolSummary> listAgentPools()`
    - `Optional<AgentPoolDetail> getAgentPoolDetail(String poolId)`
    - `CreatedAgentPool createAgentPool(String displayName, Map<String, String> capabilities, boolean generateKey, String keyDescription)`
    - `Optional<AgentPoolSummary> updateAgentPool(String poolId, String displayName, Map<String, String> capabilities)`
    - `boolean deleteAgentPool(String poolId)`
    - `List<AgentPoolSummary> reorderAgentPools(List<String> orderedPoolIds)`
    - `Optional<GeneratedRegistrationKey> generateRegistrationKey(String poolId, String description)`
    - `Optional<List<RegistrationKeyRecord>> listRegistrationKeys(String poolId)`
    - `Optional<RegistrationKeyRecord> revokeRegistrationKey(String poolId, String keyId)`
    - `void registerWorker(String connectionId, WorkerRegisterPayload payload, Consumer<WorkerWsMessage> sender, Runnable closer)` (retaining 3-arg overload)
    - `UsageSampleRecord sampleUsageNow()`
    - `HistoricalUsageResponse queryHistoricalUsage(String window, String filterCsv)`

- [ ] **Step 1: Write failing unit tests for `HistoricalUsageAggregator` and `SessionCoordinator`**

In `HistoricalUsageAggregatorTest.java`, add tests verifying:
1. Window sizing: `"1h"` -> 60 x `60_000ms` buckets; `"24h"` -> 96 x `900_000ms` buckets; `"7d"` -> 84 x `7_200_000ms` buckets; unknown window throws `IllegalArgumentException`.
2. **Review Focus #5 (cascading `desired`):** When `high-prio` (`priorityOrder=0, connected=2`) and `low-prio` (`priorityOrder=1, connected=4`) both match a job bucket with `activePartitions=2, pendingPartitions=1` (total demand `3`), global `desired == 3`, `high-prio` `desired == 2`, and `low-prio` `desired == 1`. When demand is `8`, `high-prio` absorbs the unmet remainder (`2 + (8 - 6) = 4`) and `low-prio` gets `4`, while global `desired == 8`.
3. **Review Focus #2 (filter scoping):** Filtering by `postgres=18` excludes pools whose capabilities are `{jdk=21, os=linux}` and jobs whose requirements are `{jdk=21}`, and includes jobs/pools/unpooled buckets containing `postgres=18`.
4. Multiple distinct job buckets compatible with A (connected 2) and B (connected 4) and each demanding two workers must yield A=2, B=2, global=4. Cover deterministic processing of buckets with different compatible pool sets and input order.

In `SessionCoordinatorTest.java` (using controllable `AtomicLong` clock on `DistributionCoordinatorConfig.Builder`), add tests verifying:
1. **Review Focus #1 (cross-pool & revoked key rejection):** Registering with a non-existent pool (`POOL_NOT_FOUND`), Pool A's key against Pool B (`INVALID_REGISTRATION_KEY`), a revoked key (`REVOKED_REGISTRATION_KEY`), or missing mandatory pool capability (`MISSING_POOL_CAPABILITIES`) sends `WORKER_REJECTED`, invokes `closer`, does not register the worker, and leaves Pool A's key `lastUsedAt` null.
2. **Grace windows & renewal:**
   - Revoking an active key immediately adds `KEY_REVOKED` warning (`disconnectAt = now + 1h`) on connected workers using that key; sending `WORKER_RENEW` (or advancing clock `+ 1h` + `sweepExpiredLeases()`) disconnects the worker.
   - Adding mandatory capability `docker=true` to a pool marks connected workers lacking `docker` with `POOL_CAPABILITY_MISMATCH` (`disconnectAt = now + 4h`); those workers still match `{jdk=21}` jobs during grace, do NOT match `{jdk=21, docker=true}` jobs, clear the warning if `docker` is removed from the pool, and disconnect after `+ 4h`.
   - Deleting a pool marks its connected workers with `POOL_DELETED` (`disconnectAt = now + 4h`), stops assigning new pool-matched partitions to them, lets active partitions finish, and disconnects them after `+ 4h`. A matching renewal during either four-hour grace period preserves the original deadline, and a revoked key still ends grace at renewal. Test a renewal just before the deadline and at the deadline.
3. **Review Focus #2 & priority scheduling:** Between two matching pools (`prio 0` and `prio 1`) plus an unpooled worker, jobs dispatch to `prio 0` first, then `prio 1`, then unpooled; a pooled worker advertising extra `postgres=18` not configured on its pool never matches a job requiring `postgres=18`.
4. **Live worker status:** `TEST_EVENT` with `STARTED` (`className="org.acme.OrderIT"`, `testName="shouldCreate"`) updates `WorkerRecord.activeTestDisplay()` to `"OrderIT > shouldCreate"` (falling back to full name if unqualified), and `PARTITION_COMPLETED` clears `activeTestDisplay` and updates `stateUpdatedAt`. Exercise stale/replayed events, cancellation, and disconnect so only the current attempt changes live status. At coordinator restart, preserve persisted pool and version fields while marking workers offline and clearing live execution state.

- [ ] **Step 2: Run coordinator unit tests to verify they fail**

Run: `./mvnw test -Dtest=HistoricalUsageAggregatorTest,SessionCoordinatorTest -pl tools/citrus-test-distribution-coordinator`
Expected: FAIL with compilation errors on missing `RegistrationKeyCrypto`, `HistoricalUsageAggregator`, and `SessionCoordinator` methods.

- [ ] **Step 3: Implement `RegistrationKeyCrypto`, `HistoricalUsageAggregator`, `DistributionCoordinatorConfig` clock/grace fields, and `SessionCoordinator` pool/key/scheduling/usage logic**

Implement `RegistrationKeyCrypto` (8-char `[a-z0-9]` pool IDs; `ctrk_<8chars>_<32chars>` keys hashed with SHA-256). Implement `HistoricalUsageAggregator` with shared connected capacity across demand buckets, priority-ordered `desired` attribution and per-bucket `max` aggregation as specified. Extend `DistributionCoordinatorConfig` and `SessionCoordinator` to load pools from `store` on construction, enforce registration/renewal/grace rules, track `version`, `activeTestDisplay`, `stateUpdatedAt`, and `warnings` on `WorkerConnection`, schedule by pool priority using pool capabilities, and expose `sampleUsageNow()` + `queryHistoricalUsage(...)`. Publish `pool.updated`, `pool.deleted`, `worker.updated`, and `worker.rejected` via the existing durable event hub, with no secret or hash in event payloads. Preserve the upstream result journal and restart behavior.

- [ ] **Step 4: Run coordinator unit tests to verify they pass**

Run: `./mvnw test -Dtest=HistoricalUsageAggregatorTest,SessionCoordinatorTest -pl tools/citrus-test-distribution-coordinator`
Expected: PASS (`BUILD SUCCESS`, 0 failures).

- [ ] **Step 5: Commit Task 3**

```bash
git add tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator \
        tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/coordinator
git commit -m "feat(test-distribution-coordinator): implement agent pool auth, grace windows, priority scheduling, and usage aggregation"
```

---

### Task 4: Coordinator HTTP Endpoints, WebSocket Rejection Close, and Usage Timer

**Files:**
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorVerticle.java`
- Test: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorServerTest.java`

**Interfaces:**
- Consumes: Task 3 `SessionCoordinator` pool, key, worker closer, and usage methods
- Produces HTTP endpoints under `/api/v1`:
  - `GET /api/v1/agent-pools` -> `200` `List<AgentPoolSummary>`
  - `POST /api/v1/agent-pools` -> `201` `SessionCoordinator.CreatedAgentPool` (`400` on invalid input)
  - `PUT /api/v1/agent-pools/priority` -> `200` `List<AgentPoolSummary>` (`400` on invalid pool IDs)
  - `GET /api/v1/agent-pools/:poolId` -> `200` `SessionCoordinator.AgentPoolDetail` (`404` if missing)
  - `PUT /api/v1/agent-pools/:poolId` -> `200` `AgentPoolSummary` (`400` / `404`)
  - `DELETE /api/v1/agent-pools/:poolId` -> `200` `{"deleted": true, "poolId": "..."}` (`404` if missing)
  - `GET /api/v1/agent-pools/:poolId/keys` -> `200` `List<RegistrationKeyRecord>` (`404` if missing)
  - `POST /api/v1/agent-pools/:poolId/keys` -> `201` `GeneratedRegistrationKey` (`404` if missing)
  - `POST /api/v1/agent-pools/:poolId/keys/:keyId/revoke` -> `200` `RegistrationKeyRecord` (`404` if missing)
  - `GET /api/v1/usage?window=1h|24h|7d&filter=...` -> `200` `HistoricalUsageResponse` (`400` on invalid window or malformed filter)

- [ ] **Step 1: Write failing HTTP & WebSocket server tests in `DistributionCoordinatorServerTest`**

Add tests in `DistributionCoordinatorServerTest.java` verifying:
1. Full HTTP lifecycle: `POST /api/v1/agent-pools` (with `generateRegistrationKey=true` returning secret once), `GET /api/v1/agent-pools`, `GET /api/v1/agent-pools/:poolId` (verifying `secretKey` and `keyHash` are absent from listed keys), `PUT /api/v1/agent-pools/:poolId`, `POST /api/v1/agent-pools/:poolId/keys`, `POST /api/v1/agent-pools/:poolId/keys/:keyId/revoke`, and `DELETE /api/v1/agent-pools/:poolId`.
2. **Review Focus #3 (route order):** `PUT /api/v1/agent-pools/priority` with `{"poolIds": [poolB, poolA]}` returns `200` and reorders `GET /api/v1/agent-pools` without colliding with `PUT /api/v1/agent-pools/:poolId`.
3. `GET /api/v1/usage?window=1h&filter=jdk=21` returns `200` with 60 points and matching pool series; `GET /api/v1/usage?window=bad` and `GET /api/v1/usage?window=1h&filter=a=b=c` return `400`.
4. Verify SSE delivery of pool updates/deletions, worker warning changes and worker rejection. Verify that structured WebSocket rejection is written before close with code `4003` and neither channel leaks key material.

- [ ] **Step 2: Run `DistributionCoordinatorServerTest` to verify it fails**

Run: `./mvnw test -Dtest=DistributionCoordinatorServerTest -pl tools/citrus-test-distribution-coordinator`
Expected: FAIL with `404` on `/api/v1/agent-pools` and `/api/v1/usage`.

- [ ] **Step 3: Register routes, `DELETE` CORS + `PUT` body handler, WebSocket close callback, and periodic usage sampler in `DistributionCoordinatorVerticle`**

In `DistributionCoordinatorVerticle.java`:
1. Add `HttpMethod.DELETE` to `CorsHandler` and reuse the existing size-limited PUT body handler and bearer authentication on `/api/*`.
2. Register `PUT /api/v1/agent-pools/priority` **before** `/api/v1/agent-pools/:poolId`, plus all remaining `/api/v1/agent-pools*` and `/api/v1/usage` handlers on `executeBlocking`.
3. Wire the close callback into `coordinator.registerWorker(...)`. Write the structured rejection before initiating WebSocket close `4003` and close even when the write fails.
4. Start a periodic timer `usageTimerId = vertx.setPeriodic(config.usageSampleIntervalMs(), ...)` calling `coordinator.sampleUsageNow()` on `executeBlocking` (and trigger one initial sample on startup; cancel `usageTimerId` in `stop()`).

- [ ] **Step 4: Run `DistributionCoordinatorServerTest` to verify it passes**

Run: `./mvnw test -Dtest=DistributionCoordinatorServerTest -pl tools/citrus-test-distribution-coordinator`
Expected: PASS (`BUILD SUCCESS`, 0 failures).

- [ ] **Step 5: Commit Task 4**

```bash
git add tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorVerticle.java \
        tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorServerTest.java
git commit -m "feat(test-distribution-coordinator): expose agent pool and historical usage REST APIs"
```

---

### Task 5: Worker Client Registration/Renewal & CLI / Agent Options + Schema Sync

**Files:**
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/DistributionWorkerConfig.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/DistributionWorkerClient.java`
- Modify: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistWorkerStart.java`
- Modify: `core/citrus-api/src/main/java/org/citrusframework/api/agent/CitrusAgentConfiguration.java`
- Modify: `tools/agent/src/main/java/org/citrusframework/agent/CitrusAgentOptions.java`
- Modify: `tools/agent/src/main/java/org/citrusframework/agent/DistributionWorkerBridge.java`
- Modify: `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json`
- Test: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/DistributionWorkerConfigTest.java`
- Test: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/DistributionWorkerClientTest.java`
- Test: `tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/DistCommandTest.java`
- Test: `tools/agent/src/test/java/org/citrusframework/agent/CitrusAgentWorkerOptionsTest.java`
- Test: `tools/schema-generator/src/test/java/org/citrusframework/dsl/schema/generator/CitrusSchemaGeneratorTest.java`

**Interfaces:**
- Consumes: Task 1 `WorkerRegisterPayload`, `WorkerRenewPayload`, `WorkerRejectedPayload`, `WorkerWsMessage`, `DistributionSettings`
- Produces:
  - `DistributionWorkerConfig`:
    - `String poolId()`, `String registrationKey()`, `String version()`, `long registrationRenewalIntervalMs()` (default `3_600_000L`)
    - `Builder#poolId(String)`, `Builder#registrationKey(String)`, `Builder#version(String)`, `Builder#registrationRenewalIntervalMs(long)`
  - `DistributionWorkerClient`:
    - `Optional<WorkerRejectedPayload> rejectionReason()`
  - `DistWorkerStart`: `@Option(name = "pool", shortName = 'p')`, `@Option(name = "registration-key", shortName = 'k')`, plus `"--pool"`, `"-p"`, `"--registration-key"`, `"-k"` in `VALUE_OPTIONS`
  - `CitrusAgentConfiguration`: `getPool()` / `setPool(String)` (`@SchemaProperty`) and `getRegistrationKey()` / `setRegistrationKey(String)` (`@SchemaProperty`), positioned in `citrus-agent-configuration.json` alphabetically (`pool` between `packages` and `port`; `registrationKey` between `port` and `reset`)
  - `CitrusAgentOptions`: `--pool` (empty short option `""`) and `-k` / `--registration-key`

- [ ] **Step 1: Write failing worker config, worker client, JBang CLI, agent CLI, and schema tests**

1. In `DistributionWorkerConfigTest.java`: verify `poolId`, `registrationKey`, `TEST_DISTRIBUTION_AGENT_POOL`, `TEST_DISTRIBUTION_AGENT_REGISTRATION_KEY`, `TEST_DISTRIBUTION_AGENT_NAME`, `TEST_DISTRIBUTION_AGENT_CAPABILITIES`, and non-blank `version()` resolution.
2. In `DistributionWorkerClientTest.java`:
   - **Review Focus #1:** A structured `WORKER_REJECTED("INVALID_REGISTRATION_KEY", "Invalid key")` makes `awaitRegistered(2, SECONDS)` return `false` promptly and preserves `"INVALID_REGISTRATION_KEY"` in `rejectionReason()`. A bare `4003` close yields a generic terminal rejection. Neither case reconnects.
   - When configured with `poolId`, `registrationKey`, and `registrationRenewalIntervalMs(150)`, the client sends `WORKER_RENEW` payloads periodically and stops if a renewal returns `WORKER_REJECTED`.
3. In `DistCommandTest.java`: **Review Focus #4** — verify `DistWorkerStart.normalizeWorkerStartArgs(new String[]{"dist", "worker", "start", "-p", "pool1", "-k", "ctrk_1_2", "--no-cache-cleanup"})` preserves `-p pool1 -k ctrk_1_2` and normalizes `--cache-cleanup=false`.
4. In `CitrusAgentWorkerOptionsTest.java`: **Review Focus #4** — verify `new CitrusAgentOptions().apply(new String[]{"--mode", "worker", "-p", "com.acme.tests", "--pool", "pool1", "-k", "ctrk_1_2"})` binds `packages` to `["com.acme.tests"]`, `pool` to `"pool1"`, and `registrationKey` to `"ctrk_1_2"`, and `DistributionWorkerBridge.buildWorkerConfig(...)` forwards both to `DistributionWorkerConfig`.

- [ ] **Step 2: Run worker, JBang, agent, and schema tests to verify they fail**

Run: `./mvnw test -Dtest=DistributionWorkerConfigTest,DistributionWorkerClientTest,DistCommandTest,CitrusAgentWorkerOptionsTest,CitrusSchemaGeneratorTest -pl core/citrus-api,tools/citrus-test-distribution-worker,tools/jbang,tools/agent,tools/schema-generator -am`
Expected: FAIL with compilation errors on missing `poolId` / `registrationKey` / `rejectionReason` methods.

- [ ] **Step 3: Implement worker config/client, JBang options, agent options/config/bridge, and control schema JSON**

1. Extend `DistributionWorkerConfig` (resolving `version` from `Implementation-Version` or `/META-INF/maven/org.citrusframework/citrus-test-distribution-worker/pom.properties` with `"dev"` fallback) and `DistributionWorkerClient` (sending `version`, `poolId`, `registrationKey`; scheduling periodic `WORKER_RENEW` when `poolId` is non-blank; halting reconnects and releasing `registeredLatch` on `WORKER_REJECTED` or close status `4003`).
2. Add `-p` / `--pool` and `-k` / `--registration-key` to `DistWorkerStart` (and `VALUE_OPTIONS`) and fail fast with the coordinator's rejection message when `client.rejectionReason()` is present.
3. Add `pool` and `registrationKey` to `CitrusAgentConfiguration`, `CitrusAgentOptions` (`--pool` with `""` short name; `-k` / `--registration-key`), `DistributionWorkerBridge`, and `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json`.

- [ ] **Step 4: Run worker, JBang, agent, and schema tests to verify they pass**

Run: `./mvnw test -Dtest=DistributionWorkerConfigTest,DistributionWorkerClientTest,DistCommandTest,CitrusAgentWorkerOptionsTest,CitrusSchemaGeneratorTest -pl core/citrus-api,tools/citrus-test-distribution-worker,tools/jbang,tools/agent,tools/schema-generator -am`
Expected: PASS (`BUILD SUCCESS`, 0 failures, including `CitrusSchemaGeneratorTest#verifyAgentConfigurationJson`).

- [ ] **Step 5: Commit Task 5**

```bash
git add core/citrus-api/src/main/java/org/citrusframework/api/agent/CitrusAgentConfiguration.java \
        tools/citrus-test-distribution-worker \
        tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistWorkerStart.java \
        tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/DistCommandTest.java \
        tools/agent \
        tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json
git commit -m "feat(test-distribution-worker): support agent pool registration, hourly renewal, and CLI options"
```

---

### Task 6: Web UI — Generic Sessions Subtitle, Workers Pool Parity, Agent Pools View, and Historical Usage View

**Files:**
- Modify: `tools/citrus-test-distribution-ui/src/main/resources/META-INF/resources/dist-ui/index.html`
- Modify: `tools/citrus-test-distribution-ui/src/main/resources/META-INF/resources/dist-ui/styles.css`
- Modify: `tools/citrus-test-distribution-ui/src/main/resources/META-INF/resources/dist-ui/app.js`
- Test: `tools/citrus-test-distribution-ui/src/test/java/org/citrusframework/distribution/ui/DistributionUiResourcesTest.java`
- Test: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorUiTest.java`

**Interfaces:**
- Consumes: Task 4 REST endpoints (`/api/v1/workers`, `/api/v1/agent-pools*`, `/api/v1/usage`) and SSE events (`pool.updated`, `pool.deleted`, `worker.updated`, `worker.rejected`)
- Produces UI routes & components in `dist-ui`:
  - `#sessions`: updated subtitle `"Inspect distributed build sessions, artifact cache hit rates, and test outcomes."`
  - `#workers`: fleet summary (`<N> workers, <B> busy, <I> idle`), grouped by Agent Pool + `Unpooled Workers`, `Version` column, live `Last status update` (`Executing <activeTestDisplay>` / `Idle for <duration> · Waiting for test execution requests`), warning banners (`POOL_CAPABILITY_MISMATCH`, `POOL_DELETED`, `KEY_REVOKED`) + `WARNING` filter pill, and existing `renderWorkerCacheTelemetry`
  - `#pools` & `#pools/<poolId>`: priority-ordered Agent Pools table with drag-and-drop + Up/Down controls (`PUT /api/v1/agent-pools/priority`), Create/Edit Pool form with capability chips + `Save and generate registration key`, Delete confirmation (`DELETE /api/v1/agent-pools/:poolId`), and Registration Keys table with one-time copyable `secretKey` callout + `Revoke` action
  - `#usage`: capability/requirement chip filter + `1 hour` / `24 hours` / `7 days` toggle (`1h` / `24h` / `7d`), SVG `Active jobs` chart, SVG `Worker utilization` (`utilized`, `desired`, `connected`) chart, and per-pool SVG utilization charts with hover tooltips

- [ ] **Step 1: Write failing UI static resource and coordinator UI serving tests**

In `DistributionUiResourcesTest.java` and `DistributionCoordinatorUiTest.java`, add assertions verifying:
1. `app.js` contains `"Inspect distributed build sessions, artifact cache hit rates, and test outcomes."` and does **not** contain `"Inspect distributed Maven & JBang reactor runs, classpath cache hit rates, and test outcomes."`
2. `index.html` exposes navigation items for `data-view="usage"` (`Historical Usage`) and `data-view="pools"` (`Agent Pools`) and does **not** expose Connecting Builds, project groups, access control, or `minSize`/`maxSize` controls.
3. `app.js` defines `renderAgentPools`, `renderHistoricalUsage`, `apiPut`, `apiDelete`, `/api/v1/agent-pools`, `/api/v1/agent-pools/priority`, `/api/v1/usage`, `POOL_CAPABILITY_MISMATCH`, `POOL_DELETED`, `KEY_REVOKED`, `Waiting for test execution requests`, `activeTestDisplay`, and preserves all existing `renderWorkerCacheTelemetry` and pagination assertions.

- [ ] **Step 2: Run UI tests to verify they fail**

Run: `./mvnw test -Dtest=DistributionUiResourcesTest,DistributionCoordinatorUiTest -pl tools/citrus-test-distribution-ui,tools/citrus-test-distribution-coordinator -am`
Expected: FAIL with missing subtitle/views/functions in `app.js` and `index.html`.

- [ ] **Step 3: Implement UI navigation, subtitle update, `Workers Pool` upgrades, `Agent Pools` view, and `Historical Usage` SVG charts**

Update `index.html`, `styles.css`, and `app.js` with hash routes `#pools`, `#pools/<poolId>`, and `#usage`, `apiPut`/`apiDelete` helpers, live SSE event handling (`pool.updated`, `pool.deleted`, `worker.updated`, `worker.rejected`), pool-grouped `Workers Pool` with version/live status/warnings, `Agent Pools` CRUD + drag-reorder + registration keys UI, and `Historical Usage` interactive SVG time-series charts.

- [ ] **Step 4: Run UI tests to verify they pass**

Run: `./mvnw test -Dtest=DistributionUiResourcesTest,DistributionCoordinatorUiTest -pl tools/citrus-test-distribution-ui,tools/citrus-test-distribution-coordinator -am`
Expected: PASS (`BUILD SUCCESS`, 0 failures).

- [ ] **Step 5: Commit Task 6**

```bash
git add tools/citrus-test-distribution-ui \
        tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorUiTest.java
git commit -m "feat(test-distribution-ui): add agent pools, historical usage charts, and workers pool parity"
```

---

### Task 7: End-to-End Integration Verification and User Manual Documentation

**Files:**
- Create: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/AgentPoolLifecycleIT.java`
- Modify: `src/manual/tools-test-distribution.adoc`

**Interfaces:**
- Consumes: Tasks 1–6 end-to-end (`DistributionCoordinatorServer`, `DistributionWorkerClient`, `DistributionWorkerConfig`, REST endpoints)
- Produces: End-to-end coordinator + worker pool lifecycle verification and user manual sections for Agent Pools, Registration Keys, Grace Windows, Pool Priority, Workers Pool warnings, and Historical Usage

- [ ] **Step 1: Write end-to-end pool registration, priority routing, key revocation, and historical usage test**

In the worker module, add an end-to-end test using real `DistributionCoordinatorServer` + `DistributionWorkerClient` instances verifying:
1. Creating `high-prio` (`priorityOrder=0`, `capabilities={jdk=21, docker=true}`) and `low-prio` (`priorityOrder=1`, `capabilities={jdk=21, docker=true}`) pools with generated registration keys.
2. Connecting a `DistributionWorkerClient` with `low-prio`'s key against `high-prio` is rejected (`INVALID_REGISTRATION_KEY`), and connecting a worker missing `docker=true` is rejected (`MISSING_POOL_CAPABILITIES`).
3. Connecting valid workers to both pools routes a submitted partition requiring `{docker=true}` to `high-prio` first and populates `version` + `poolDisplayName` on `GET /api/v1/workers`. Use enough concurrent demand to dispatch work to both pools, hold that work active until a usage sample observes it, and synchronize sampling and revocation assertions on observable state.
4. Revoking `high-prio`'s key surfaces `KEY_REVOKED` warning on `GET /api/v1/workers`, disconnects the worker on its next renewal tick, and `GET /api/v1/usage?window=1h&filter=docker` returns non-zero `connected` / `utilized` / `desired` metrics for both pools.

- [ ] **Step 2: Run end-to-end test and full Test Distribution test suite**

Run: `./mvnw verify -pl core/citrus-api,tools/citrus-test-distribution-protocol,tools/citrus-test-distribution-ui,tools/citrus-test-distribution-coordinator,tools/citrus-test-distribution-worker,tools/citrus-test-distribution,tools/maven/citrus-test-distribution-maven-extension,tools/jbang,tools/agent,tools/schema-generator -am`
Expected: PASS (`BUILD SUCCESS`, 0 failures across all Test Distribution, JBang, Agent, and Schema Generator modules).

- [ ] **Step 3: Document Agent Pools, Registration Keys, Worker CLI/Env Options, Grace Windows, and Historical Usage in `src/manual/tools-test-distribution.adoc`**

Update `src/manual/tools-test-distribution.adoc` to document:
- Agent Pools configuration, unique pool IDs, mandatory capabilities (including bare capabilities like `docker`), priority ordering, and pool-capability matching rules.
- Registration keys (`ctrk_...`), multi-key rotation, revocation, and the 1-hour / 4-hour grace windows + `Workers Pool` warning indicators.
- Worker CLI and environment configuration (`--pool` / `-p`, `--registration-key` / `-k`, `CITRUS_DIST_WORKER_POOL` / `TEST_DISTRIBUTION_AGENT_POOL`, `CITRUS_DIST_WORKER_REGISTRATION_KEY` / `TEST_DISTRIBUTION_AGENT_REGISTRATION_KEY`, and `citrus.test.distribution.coordinator.requireAgentPool`).
- Historical Usage views (`1 hour`, `24 hours`, `7 days`) and requirement/capability filtering.

- [ ] **Step 4: Commit Task 7**

```bash
git add tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/AgentPoolLifecycleIT.java \
        src/manual/tools-test-distribution.adoc
git commit -m "docs(test-distribution): document agent pools, registration keys, and historical usage"
```
