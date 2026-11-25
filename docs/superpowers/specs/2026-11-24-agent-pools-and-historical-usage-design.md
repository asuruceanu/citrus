# Citrus Test Distribution: Agent Pools, Workers Pool Parity, and Historical Usage

**Date:** 2026-11-24

**Status:** Approved

## Intent and Why

Citrus Test Distribution currently lets any worker connect directly to the coordinator and matches build requirements directly against individual workers' advertised capabilities. As shared worker fleets grow, operators need controlled grouping, capability governance, credential rotation, pool prioritization, fleet health warnings, and historical capacity visibility modeled on Develocity Test Distribution Administration — tailored to Citrus without enterprise bloat (no access control/RBAC, no project groups, no autoscaler sizing, and no duplicate Connecting Builds view since `Build Sessions Explorer` already covers live sessions).

## Goals, Non-Goals, and Anti-Goals

- **Goals:**
  - **Agent Pools (full lifecycle & registration):** Create, edit, prioritize (drag-and-drop + move controls), and delete agent pools with generated short unique IDs, mandatory capability sets (`key=value` and bare `key` tokens), and multiple pool-scoped registration keys (`Generate`, prefix/metadata tracking, `Last used at`, `Last used by`, `Revoke`).
  - **Develocity-parity registration, grace windows, and scheduling:**
    - Reject connecting workers missing mandatory pool capabilities, referencing a non-existent pool, or presenting an invalid, wrong-pool, or revoked registration key.
    - Keep already-connected workers online with explicit warning indicators for up to **4 hours** after pool deletion (`POOL_DELETED`) or mandatory capability additions (`POOL_CAPABILITY_MISMATCH`), and up to **1 hour** (or next hourly registration renewal) after registration key revocation (`KEY_REVOKED`), then forcibly disconnect them.
    - Match build requirements against pool capabilities (ignoring extra capabilities declared only on pooled workers), schedule higher-priority pools before lower-priority pools, and support hybrid mode where unpooled workers remain backward-compatible fallback capacity unless `citrus.test.distribution.coordinator.requireAgentPool=true`.
  - **Workers Pool parity with Develocity Agents:** Enhance the existing `Workers Pool` view (without adding a separate `Agents` tab) with top fleet summary counts (`N workers, B busy, I idle`), grouping by Agent Pool, worker `Version`, live `Last status update` (`Executing <ClassName> > <testName>` when busy; `Idle for <duration> · Waiting for test execution requests` when idle), and grace-window warning badges, while preserving local file cache telemetry.
  - **Historical Usage (`1 hour` / `24 hours` / `7 days`):** Persist periodic coordinator usage snapshots and render `Active jobs`, `Worker utilization` (`utilized`, `desired`, `connected`), and per-pool utilization time-series charts with capability/requirement chip filtering.
  - **Generic `Build Sessions Explorer` subtitle:** Replace `"Inspect distributed Maven & JBang reactor runs, classpath cache hit rates, and test outcomes."` with `"Inspect distributed build sessions, artifact cache hit rates, and test outcomes."`
- **Non-Goals:**
  - Connecting Builds / Jobs tab (`Build Sessions Explorer` already covers active and historical sessions).
  - Separate `Agents` tab (`Workers Pool` is upgraded in place).
  - Autoscaling (`minSize` / `maxSize` / external autoscaler control loops).
  - Project groups.
- **Anti-Goals:**
  - Access control, RBAC, user/role permissions, or per-pool user ACLs of any kind.
  - Storing plaintext registration keys in JDBC or returning secret keys after initial generation.
  - Infinite reconnect loops when a worker is rejected for an invalid/revoked key, missing pool, or missing mandatory pool capability.

## Constraints

- **Zero-Node UI:** `tools/citrus-test-distribution-ui` must remain dependency-free browser ESM (`htm-mini.js`, vanilla JS, inline SVG charts, CSS custom properties) with no npm/Node build step.
- **Protocol module dependency boundary:** `tools/citrus-test-distribution-protocol` depends only on `jackson-databind` and `slf4j-api`; do not add `citrus-api` or Vert.x dependencies to it.
- **JDBC portability:** Schema and queries in `JdbcDistributionStore` must work identically on embedded HSQLDB (`CLOB`) and PostgreSQL (`TEXT`), including `ensureColumns` upgrades on existing databases.
- **CLI short-flag collision:** `CitrusAppOptions` reserves `-p` for `--package`. Therefore `citrus dist worker start` supports `-p` / `--pool` and `-k` / `--registration-key`, whereas `citrus-agent` supports long `--pool` (no `-p` short alias) and `-k` / `--registration-key`. Both must register new value-taking flags with `WorkerCacheCliOptions` host value-option sets.
- **JSON Schema control test:** Any `@SchemaProperty` added to `CitrusAgentConfiguration` (`core/citrus-api`) must update `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json` in exact property order.

## Approaches Considered

1. **Approach A — Hybrid Pool Model + Periodic 15s JDBC Usage Samples (Selected):**
   - Agent pools, SHA-256 hashed registration keys, and compact 15-second usage sample buckets are persisted in `JdbcDistributionStore`.
   - Workers with `--pool` + `--registration-key` join a pool, renew hourly, and follow pool priority + capability rules; workers without `--pool` continue working in an `Unpooled Workers` fallback group unless `requireAgentPool=true` is enabled on the coordinator.
   - Chosen because it delivers full Develocity Agent Pool & Historical Usage semantics while keeping existing zero-config local/CI setups and tests working out of the box.
2. **Approach B — Auto-Seeded `default` Pool + Query-Time Usage Reconstruction:**
   - Reconstructs utilization curves from partition start/end timestamps instead of sampling. Rejected because historical `connected` and `desired` worker counts cannot be reconstructed accurately from partition rows alone, especially under capability filters and offline transitions.
3. **Approach C — Strict Pool-Only Enforcement:**
   - Rejects all workers lacking `--pool` and `--registration-key`. Rejected because it breaks existing zero-config Citrus Test Distribution workflows and tests when no pools are configured.

## Detailed Design

### 1. Data Model & JDBC Schema

- **`dist_agent_pools`:**
  - `pool_id VARCHAR(64) PRIMARY KEY` — generated 8-char lowercase `[a-z0-9]` identifier (e.g., `756cdscc`).
  - `display_name VARCHAR(255) NOT NULL`
  - `capabilities_json CLOB/TEXT NOT NULL` — mandatory capability map (`Map<String, String>`; bare capabilities like `docker` normalize to `"docker": "true"`).
  - `priority_order INT NOT NULL` — `0`-based priority index (`0` = highest priority).
  - `created_at BIGINT NOT NULL`, `updated_at BIGINT NOT NULL`.
- **`dist_registration_keys`:**
  - `key_id VARCHAR(64) PRIMARY KEY` — UUID.
  - `pool_id VARCHAR(64) NOT NULL` — owning pool ID.
  - `key_prefix VARCHAR(32) NOT NULL` — visible prefix `ctrk_<8chars>` shown in UI.
  - `key_hash VARCHAR(128) NOT NULL` — hex SHA-256 digest of full secret `ctrk_<8chars>_<32chars>` (secret returned only once at creation).
  - `description VARCHAR(512)`
  - `created_at BIGINT NOT NULL`, `last_used_at BIGINT`, `last_used_by VARCHAR(255)`, `revoked_at BIGINT`.
- **`dist_workers` columns (via `CREATE TABLE` + `ensureColumns`):**
  - Add `version VARCHAR(64)`, `pool_id VARCHAR(64)`, `pool_display_name VARCHAR(255)`, `active_test_display VARCHAR(512)`, `state_updated_at BIGINT`, `warnings_json CLOB/TEXT`.
- **`dist_usage_samples`:**
  - `sampled_at BIGINT PRIMARY KEY`
  - `payload_json CLOB/TEXT NOT NULL` — serialized `UsageSampleRecord` containing:
    - `activeJobs`: aggregated `JobDemandBucket(requiredCapabilities, activeSessions, activePartitions, pendingPartitions)`
    - `pools`: `PoolSampleBucket(poolId, displayName, priorityOrder, capabilities, connected, utilized)`
    - `unpooledWorkers`: `UnpooledWorkerBucket(capabilities, connected, utilized)`
  - Coordinator samples every 15s (configurable via `usageSampleIntervalMs`) and prunes samples older than 7 days (`usageRetentionMs`).

### 2. Capability Rules, Grace Windows, and Priority Scheduling

- **Capability syntax:** Keys and values allow `[A-Za-z0-9._-]+` with at most one `=`. Bare tokens (`docker`) normalize to `key="docker", value="true"`. `@AgentRequirement.value()` defaults to `"true"` so `@AgentRequirement(key = "docker")` matches bare `docker`.
- **Registration validation:**
  - Pooled worker (`poolId` non-blank): pool must exist (`POOL_NOT_FOUND`), `registrationKey` must belong to `poolId` and not be revoked (`INVALID_REGISTRATION_KEY` / `REVOKED_REGISTRATION_KEY`), and worker capabilities must satisfy all mandatory pool capabilities (`MISSING_POOL_CAPABILITIES`).
  - Unpooled worker (`poolId` blank): allowed when `requireAgentPool == false`; rejected with `POOL_REQUIRED` when `requireAgentPool == true`.
  - Hourly renewal (`worker.renew`): the coordinator checks the credentials against the identity and key bound to the current WebSocket and rejects a revoked key immediately. A matching renewal during an existing capability-drift or pool-deletion grace period leaves the original deadline in place. Successful renewal updates the usage metadata for an active pool key. Renewal with a deleted pool acknowledges the already-connected worker through its grace deadline without updating deleted key metadata. Fresh registration always uses the current pool and capability requirements.
- **Grace windows & eligibility:**
  - **Pool capabilities added (`POOL_CAPABILITY_MISMATCH`):** Already-connected workers in that pool missing a newly added capability stay connected up to **4 hours** (`poolDriftGraceMs = 14_400_000L`) with a warning. During grace, they finish active work and remain eligible only for jobs whose requirements satisfy **both** the pool's current capabilities and the worker's declared capabilities.
  - **Pool deleted (`POOL_DELETED`):** Already-connected workers in that pool stay connected up to **4 hours** with a warning, finish any active partition, and receive no new pool-matched partitions because the pool no longer exists.
  - **Key revoked (`KEY_REVOKED`):** Future registrations with the key fail immediately. Connected workers authenticated with that key receive a warning and are forcibly disconnected after **1 hour** (`keyRevocationGraceMs = 3_600_000L`) or on their next renewal attempt, whichever comes first.
- **Scheduling order:**
  - A pooled worker's effective advertised capability set for job matching is **its pool's capabilities** (extra capabilities declared only on the worker do not satisfy build requirements; removing a capability from a pool immediately stops matching jobs requiring it).
  - Eligible `IDLE` workers are selected from matching pools in ascending `priorityOrder` (`0` first), tie-broken by least-recently-assigned (`lastAssignedAt`), falling back to matching unpooled `IDLE` workers when no pooled worker is available.

### 3. Historical Usage Aggregation & Filtering

- **Time windows & buckets (`GET /api/v1/usage?window=1h|24h|7d&filter=...`):**
  - `1h`: 1-minute buckets (60 points)
  - `24h`: 15-minute buckets (96 points)
  - `7d`: 2-hour buckets (84 points)
- **Filter semantics:**
  - Empty filter includes all active jobs, all agent pools, and all unpooled workers.
  - Non-empty filter (`filterMap`) narrows to:
    - Jobs with at least those requirements: `CapabilityMatcher.matches(filterMap, job.requiredCapabilities)`
    - Agent pools with at least those capabilities: `CapabilityMatcher.matches(filterMap, pool.capabilities)` (pooled workers are governed by their pool's capabilities, not worker-only extra tags)
    - Unpooled workers with at least those capabilities: `CapabilityMatcher.matches(filterMap, unpooled.capabilities)`
- **Cascading `desired` calculation:**
  - Global `desired` at a sample equals total `activePartitions + pendingPartitions` across matching job buckets.
  - Per-pool `desired` attributes matching demand across compatible pools in priority order using a single connected-capacity budget per pool per sample. Process demand buckets with fewer compatible pools first, breaking ties by canonical sorted requirement pairs. Consume remaining pool capacity in priority order. Attribute demand left after capacity is exhausted to the highest-priority compatible pool as unmet demand. Demand with no compatible pool remains visible in the global series.
  - Bucket aggregation takes the bucket maximum (`max`) across 15s samples in that bucket so short spikes remain visible on 24h and 7d views.

### 4. Coordinator REST & WebSocket API

- `GET /api/v1/agent-pools` — priority-ordered pools with live `connectedWorkers`, `busyWorkers`, `idleWorkers`, `activeKeyCount`.
- `POST /api/v1/agent-pools` — create pool (optionally generating initial registration key in one step).
- `PUT /api/v1/agent-pools/priority` — reorder pool IDs (`0..N-1`).
- `GET /api/v1/agent-pools/:poolId` — pool detail + live counts + `List<RegistrationKeyRecord>`.
- `PUT /api/v1/agent-pools/:poolId` — update display name and mandatory capabilities (triggers capability-mismatch warnings/clears on connected workers).
- `DELETE /api/v1/agent-pools/:poolId` — delete pool (triggers 4h `POOL_DELETED` warning on connected workers).
- `GET /api/v1/agent-pools/:poolId/keys` — list registration key metadata (prefix, description, timestamps, last used by, revoked timestamp; never secret/hash).
- `POST /api/v1/agent-pools/:poolId/keys` — generate new registration key, returning `GeneratedRegistrationKey(metadata, secretKey)` once.
- `POST /api/v1/agent-pools/:poolId/keys/:keyId/revoke` — revoke key (triggers 1h `KEY_REVOKED` warning on connected workers using it).
- `GET /api/v1/usage?window=1h|24h|7d&filter=...` — `HistoricalUsageResponse`.
- WebSocket `/ws/v1/worker` messages added: `worker.rejected`, `worker.renew`, `worker.renewed`.
- SSE events added: `pool.updated` (pool creation, edits, priority or key metadata changes), `pool.deleted`, `worker.updated` (live status or warning changes), and `worker.rejected`. Events carry identifiers and refresh information without registration secrets or hashes. The worker receives the structured rejection reason before the terminal close when delivery is possible. A bare close with status `4003` indicates terminal rejection with no specific reason.

### 5. Web UI (`tools/citrus-test-distribution-ui`)

- **`Build Sessions Explorer` (`#sessions`):** Subtitle updated to `"Inspect distributed build sessions, artifact cache hit rates, and test outcomes."`
- **`Workers Pool` (`#workers`):**
  - Fleet summary header: `<N> workers, <B> busy, <I> idle`.
  - Grouped sections per Agent Pool (in priority order) plus `Unpooled Workers` when present.
  - Columns/fields: `Worker`, `Hostname`, `Version`, `State`, `Capabilities`, `Last status update` (`Executing <ClassName> > <testName>` when busy; `Idle for <duration> · Waiting for test execution requests` when idle), warning banners (`POOL_CAPABILITY_MISMATCH`, `POOL_DELETED`, `KEY_REVOKED` with disconnect countdown), and existing `Local File Cache` telemetry.
  - Filter pills: `ALL`, `IDLE`, `BUSY`, `WARNING`, `DRAINING`, `OFFLINE`, `LOW_DISK`.
- **`Agent Pools` (`#pools` & `#pools/<poolId>`):**
  - Drag-and-drop + Up/Down priority table (`Display name`, copyable `ID`, `Workers`, `Capabilities` with `+ N more`, `Registration keys`, `Manage` / `Delete`).
  - Create / Edit form with capability chip input and `Save and generate registration key` / `Save` actions.
  - Registration keys management with one-time copyable secret callout and `Revoke` action.
- **`Historical Usage` (`#usage`):**
  - Capability/requirement chip filter + `1 hour` / `24 hours` / `7 days` selector.
  - Interactive SVG charts with hover tooltips for `Active jobs`, `Worker utilization` (`utilized`, `desired`, `connected`), and per-pool utilization (`<Pool Display Name>`).

### 6. CLI & Agent Worker Configuration

- **System properties & environment variables:**
  - Pool ID: `citrus.test.distribution.worker.pool`, `CITRUS_DIST_WORKER_POOL`, `TEST_DISTRIBUTION_AGENT_POOL`.
  - Registration key: `citrus.test.distribution.worker.registrationKey`, `CITRUS_DIST_WORKER_REGISTRATION_KEY`, `TEST_DISTRIBUTION_AGENT_REGISTRATION_KEY`.
  - Develocity env aliases also honored for worker name (`TEST_DISTRIBUTION_AGENT_NAME`) and capabilities (`TEST_DISTRIBUTION_AGENT_CAPABILITIES`).
  - Coordinator strict pool flag: `citrus.test.distribution.coordinator.requireAgentPool` / `CITRUS_DIST_COORDINATOR_REQUIRE_AGENT_POOL` (default `false`).
- **CLI flags:**
  - `citrus dist worker start`: `-p` / `--pool` and `-k` / `--registration-key` (added to `VALUE_OPTIONS`).
  - `citrus-agent`: `--pool` (long option only) and `-k` / `--registration-key`, backed by `CitrusAgentConfiguration` (`pool`, `registrationKey`) and synced with `citrus-agent-configuration.json`.

## Left to the Builder

- Exact CSS class names and SVG path styling in `dist-ui/styles.css` and `dist-ui/app.js`, matching existing Citrus dark/light theme tokens.
- Internal helper decomposition within `SessionCoordinator`, `HistoricalUsageAggregator`, and `RegistrationKeyCrypto`.
