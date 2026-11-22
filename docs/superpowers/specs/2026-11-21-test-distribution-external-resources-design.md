# Citrus Test Distribution: External Test Resources (Declared Inputs, Outputs & Path Rewriting)

- **Date:** 2026-11-21
- **Revision:** 6 (As built — see §0 for amendments made during implementation; Revision 5 approved 2026-11-21)
- **Status:** Approved
- **Reference:** [Develocity Test Distribution 3.8 — External Test Resources](https://docs.develocity.ai/test-distribution/3.8/#external-test-resources)

## 0. As-Built Amendments (Revision 6, 2026-11-22)

Implementation on branch `feature/test-distribution-external-resources` (commits `212de0c2b4` … `fce36c3fe2`) refined this design after review. **Where this section and the text below disagree, this section is authoritative.** Each item names where the code now lives.

### 0.1 Working directory parity (supersedes the CWD assumption throughout)
- The root `pom.xml` runs Surefire with `<workingDirectory>${project.build.directory}</workingDirectory>` (since 2014); Failsafe has no override. Tests therefore run with `<module>/target` (Surefire) or the module root (Failsafe) as working directory — not the module root as assumed in §5.4/§5.6.
- `SubmitWorkRequest` and `PartitionAssignPayload` carry `workingDirectory`: the host test JVM's `user.dir` relative to the module (`/`-separated, empty for the module root or when outside it). The coordinator forwards it; `ForkedPartitionRunner` starts the child JVM in `<workspace>/<workingDirectory>` (validated, no links). `basedir` stays the workspace root.

### 0.2 Settings and precedence (§5.1)
- Root `pom.xml` declares all four settings **empty** (not `fork=false` / `partitionPolicy=LPT`), so the `CITRUS_DIST_*` tier stays reachable; built-in defaults apply in the launcher.
- In the test JVM a **blank or unresolved `${...}` value counts as unset** (the root POM always passes the settings through); `none` (case-insensitive) clears a path list. `SurefirePluginMutator` resolves CLI > explicit `systemPropertyVariables` > module property > env > unset per Surefire/Failsafe configuration and writes the normalized winner (`none` for cleared lists, `false`/`LPT` for cleared fork/policy); unset settings are removed. CLI values are normalized **in the session user properties** too, because Surefire promotes user properties after `systemPropertyVariables`. An empty `-D` value therefore clears only with the extension; `-D…=none` clears everywhere.
- Invalid `fork`/`partitionPolicy` values are validated for every project **before** the reactor session is opened, with the module id and winning tier in the message.
- `?` marks a path optional when leading **or** trailing; `outputs` reject `?`. A required path below an optional ancestor is kept (the optional ancestor does not swallow it). The fork property constant is `FORK_PROPERTY` (`citrus.test.distribution.fork`).

### 0.3 Packaging, limits and validation (§5.2)
- `MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES = 2 GiB` (not 512 MiB); `MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES = 256 MiB`; `MAX_ZIP_ENTRIES = 50,000`; `MAX_BLOB_BYTES = 256 MiB` applies to every blob including JARs.
- Deterministic ZIP entries use the fixed DOS time 1980-02-01T00:00 (an epoch time would force an extended-timestamp extra field; 1980-01-01T00:00 is the JDK's "before 1980" marker) — no extra fields, no comments. Classpath directory archives use the same writer.
- Symbolic links and Windows junctions are rejected; packaging raises `IllegalArgumentException`, extraction `IOException`. Entry names are validated at packaging time with the same rules as extraction (`validateRelativePath`, including reserved device names on every OS).
- Extraction rejects directory entries carrying data, duplicate names and names colliding on case-insensitive file systems, and returns per-file digests (`extractZipSafely` → `ExtractedFile`, `materializeIntoDirectory` → `MaterializedEntry`).
- `PathRewriter` also matches after `@` and `file:/`, `file://`, `file:///`, and matches case-insensitively on Windows.

### 0.4 Coordinator fencing (§5.3)
- The fence accepts partitions in **`ASSIGNED` or `RUNNING`** (`PARTITION_STARTED` moves to `RUNNING`) on the active connection; `PARTITION_STARTED` (with its attempt, when sent) and heartbeat lease refreshes are fenced too.
- Attempt `0` (sender without attempts) is accepted only from a worker id that never held an earlier attempt of the partition (covers reconnects on a new connection); retries prefer workers that did not run an earlier attempt.
- On lease expiry the worker receives `PARTITION_CANCEL {partitionId, sessionId, attempt}` and **stays out of dispatch (`BUSY`, no partition) until it confirms** the cancelled attempt ended — its stale completion (still discarded) or a heartbeat no longer naming the partition. Workers silent for longer than the lease timeout are skipped by dispatch; dispatch runs after a message is fully processed.
- `SINGLE` compares capability values case-insensitively; different `>=` thresholds count as a conflict. Scheduling happens before any state change, so a rejected submission leaves no trace.

### 0.5 Worker (§5.4)
- `PartitionJUnitLauncher.executeWorkUnits(partitionId, sessionId, attempt, …)` uses JUnit's `CancellationToken`; failing or skipped containers produce events; work units matching no tests fail the partition; a bare method name selects the no-argument overload (declared or inherited).
- The worker tracks assignments by identity, accepts a new assignment only when idle or the active one is cancelled (it then queues), cancels on WebSocket loss, abort and close, and reports the **executing** partition in heartbeats.
- Forked child classpath: worker runtime (fork entry point, protocol) **first**, then the partition classpath, then Jackson/JUnit/OpenTest4J/SLF4J only if the partition lacks them. Forked partitions require JUnit Platform 6. The argfile uses `sun.jnu.encoding`. `/tmp/cdtmp-*` is used when `/tmp` (or its symlink target) is a writable directory.
- Process cleanup: the parent remembers every process seen under the child and kills them all at cleanup (orphans included); the child kills its descendants in a shutdown hook and halts if the worker disappears (not when it closes the socket itself).
- In-process partitions: a relative report/trace directory that would leave the workspace falls back to the default; blobs are re-verified once per JVM.

### 0.6 Host (§5.5)
- Outputs are staged per partition as they arrive (`tx-<uuid>/staged/<n>-<partition>/`); only files inside a declared output path are accepted, never under `target/.citrus-dist-outputs`, `target/.citrus-dist` or the host cache; names differing only in case are a conflict; backups are numbered; replaced files keep their POSIX permissions; destinations are re-checked for links just before each move; staging is discarded on every abort path.
- The host cache defaults to `<module>/target/.citrus-dist/cache`. Any partition state other than `COMPLETED` fails the run, as do lost partitions whose work units cannot be identified. Inputs are resolved and packaged before the coordinator is contacted.

### 0.7 Rollout (§5.6) — re-derived, see the rewritten §5.6.

### 0.8 Known limitations added during implementation
- Host cache and worker cache are not evicted. Crashed partitions' tests are absent from JUnit/Surefire reports (the build still fails). A pre-attempt worker cannot finish a retry of a partition it ran before (fails safely after lease expiry). Protocol classes in the child come from the worker, so client/worker protocol version skew moves to the module side. The rollout was verified locally (standard Maven); the fork-for-fixed-ports choices and leaving jbang `ListTest`/`CommandParityTest` unpinned are unverified on real workers.

## 1. Intent and Why

Citrus Test Distribution currently ships only the test runtime classpath (`java.class.path` JARs and `DIRECTORY_ZIP` directories) and filtered JVM system properties to remote workers, executing partitions in-process inside `DistributionWorkerClient` where the OS/JVM working directory (`user.dir` / `getcwd()`) is frozen to the worker daemon's startup directory.

Across the full 84-POM Citrus repository, modules fall into four external-resource patterns that fail or pollute worker state when executed remotely without workspace awareness:

1. **Relative CWD / `basedir` input file reads (7 modules):** Tests and embedded Jetty/SOAP servers read `src/test/resources/...`, `pom.xml`, `target/generated-test-sources/...`, or `target/test-classes/...` relative to `new File(...)`, `Path.of(...)`, `System.getProperty("user.dir")`, or `System.getProperty("basedir")`.
2. **Generated test outputs (2 of those 7 modules):** `tools/docs-generator` (`target/docs`) and `tools/restdocs` (`target/citrus-docs`) write generated documentation/snippet artifacts relative to the module working directory during test execution.
3. **Ephemeral CWD / `target/` / `java.io.tmpdir` scratch writes & global logger mutation (11 modules):** Modules such as `core/citrus-api`, `core/citrus-base`, `endpoints/citrus-{ftp,camel,kafka,ssh}`, `connectors/citrus-{playwright,selenium,jbang-connector}`, `tools/schema-generator`, and `tools/jbang` write transient directories/files under relative CWD paths (`target/ftp`, `target/sftp`, `target/data`, `target/schema`, `target/citrus-reports`, `.citrus-jbang`, screenshots/HARs/videos), write fixed filenames in `java.io.tmpdir` (`hostkey.ser`), or mutate process-wide Log4j2 levels (`Configurator.setRootLevel(...)`). Furthermore, Citrus's default `HtmlReporter`, `JUnitReporter`, and `MessageTracingTestListener` write to relative `target/citrus-reports` and `target/citrus-logs/trace/messages` across **all** Citrus modules unless redirected.
4. **True host-coupled tests (4 modules):** JBang CLI/engine tests (`tools/jbang` and `connectors/citrus-jbang-connector`) that inspect live host JVM processes (`ProcessHandle.allProcesses()`) or invoke non-`--offline` JBang/Maven resolution against `~/.m2/repository` / `~/.jbang`, plus Quarkus bootstrap tests (`citrus-quarkus-deployment`, `citrus-quarkus-it`) whose `io.quarkus.bootstrap` augmentation resolves local SNAPSHOT workspace artifacts out of `~/.m2/repository`.

Following the Develocity Test Distribution 3.8 model for External Test Resources, workers will materialize declared input file-sets into an isolated per-partition working directory, redirect default Citrus report/trace directories per partition, rewrite host workspace paths in propagated system properties, export declared new/modified output files back to the host workspace using all-or-nothing staging with conflict detection, and classify all 84 POMs across the repository.

## 2. Goals, Non-Goals, and Anti-Goals

### Goals
- **Declared input file-sets (`inputs`):** Allow modules to declare relative files or directories (e.g., `src/test/resources`, `pom.xml`, `target/generated-test-sources`) that are deterministically hashed, deduplicated via the SHA-256 blob cache (`/api/v1/cache/missing` + `/api/v1/cache/blobs/:sha256`), and materialized at matching relative paths in the worker's partition workspace. Required inputs fail fast on the host if missing; optional inputs may use a trailing `?` suffix.
- **Real CWD, `basedir` & `java.io.tmpdir` isolation on workers:** Ensure `new File("src/test/resources/...")`, `Path.of("target/...")`, `System.getProperty("user.dir")`, `System.getProperty("basedir")`, and `System.getProperty("java.io.tmpdir")` resolve to isolated per-partition directories via a forked child JVM when workspace isolation is requested, without exceeding POSIX `AF_UNIX` (`sun_path`) socket path limits or polluting the worker daemon directory.
- **Universal Citrus report/trace redirection & non-destructive CWD write diagnostic in `AUTO` mode:** Redirect default `citrus.report.directory` and `citrus.message.trace.directory` into `<partitionRoot>/workspace/target/...` for in-process partitions, and perform a read-only best-effort CWD snapshot before/after execution (never deleting files in the worker's startup directory) that fails the partition with an actionable configuration error when unexpected CWD writes are observed.
- **System property path rewriting:** Rewrite host module `basedir` path prefixes (including `toRealPath()` aliases, Windows slash variants, path lists, and `file://` URIs) inside propagated system properties to `${citrus.dist.workspace}` on submission and expand them to the worker's `workspaceDir` before execution, without rewriting similarly prefixed sibling directories.
- **Declared output syncing (`outputs`) with transactional backup-and-rollback publication:** Package only newly created or modified regular files under declared `outputs` (ignoring empty pre-created directories and unchanged baseline inputs), upload missing blobs to the coordinator blob store, stage all partition outputs on the host until all partitions of the submission complete, and publish under `<hostBaseDir>/target/.citrus-dist-outputs/commit.lock` using a backup-and-rollback undo journal—failing without modifying `hostBaseDir` on cross-partition content conflicts and rolling back partial file updates if an `IOException` occurs mid-commit.
- **Attempt + connection fencing on retries:** Fence `TEST_EVENT` and `PARTITION_COMPLETED` (including `outputArtifacts`) on `(partitionState == ASSIGNED, connectionId == activeConnectionId, attempt == activeAttempt)` so late events or outputs from expired/retried attempts are deterministically dropped.
- **Standard Maven compatibility:** Use native Maven `<properties>` and `<systemPropertyVariables>` exclusively so builds work identically whether `citrus-test-distribution-maven-extension` is active, disabled, or absent from `.mvn/extensions.xml`.
- **Full 84-POM Citrus repository coverage:** Explicitly classify and configure all 84 POMs across `connectors`, `core`, `endpoints`, `runtime`, `tools`, `utils`, `validation`, and `catalog` (19 modules receiving explicit configuration/annotations, 1 already distribution-disabled, 19 non-test/BOM/archetype/aggregator POMs, and 45 verified zero-config in-process test modules).

### Non-Goals
- Rewriting embedded paths inside compiled bytecode or binary bootstrap archives (`processedResources` such as Quarkus binary model files / `io.quarkus.bootstrap` local Maven workspace descriptors).
- Supporting paths outside the Maven module `basedir` (`../other-module/...`, `~/.m2/repository`, `/etc/...`), symbolic links inside workspace file-sets, or single blobs exceeding `256 MiB`.
- Propagating file deletions of pre-existing input files from worker `outputs` back to the host `basedir`.
- Distributed remote Maven Invoker builds expecting a populated local `~/.m2/repository` reactor snapshot tree on workers.

### Anti-Goals
- **Mutating `user.dir` in-process on the worker daemon:** Java 21 freezes `StaticProperty.USER_DIR` and OS `getcwd()` at JVM boot, so `System.setProperty("user.dir", ...)` cannot redirect `new File(rel)` / `Path.of(rel)`.
- **Custom `<testDistribution>` XML inside Surefire/Failsafe `<configuration>`:** Rejected because Maven's Plexus Mojo configurator fails ordinary builds whenever `citrus-test-distribution-maven-extension` is not loaded or `-Dcitrus.test.distribution.enabled=false`.
- **Leaving lock or staging dotfiles in `<hostBaseDir>` root:** Lock and staging files must stay strictly under `<hostBaseDir>/target/.citrus-dist-outputs/` so `git status`, Apache RAT, and license plugins never see untracked root dotfiles.
- **Paying JVM fork overhead for every partition unconditionally:** ~73.7% of active Citrus test modules (`56 / 76`, excluding distribution-disabled `mcp-server` and non-test aggregator/BOM/archetype POMs) need no workspace inputs/outputs/forking and retain zero-fork in-process execution under `forkMode=AUTO`.

## 3. Constraints & Accepted Limitations

- **Java 21+ & Jackson 3 (`tools.jackson.databind`):** Uses existing `DistributionJson`, Java 21 `HttpClient`, `ProcessBuilder`, `ProcessHandle`, and `java.nio.file` APIs with zero new external dependencies.
- **Single active partition per `DistributionWorkerClient`:** `DistributionWorkerClient` executes strictly `1` partition (and at most `1` child JVM) at a time on its single-threaded `partitionExecutor`, matching `BrokerSessionCoordinator`'s `IDLE -> BUSY` worker state machine. Multi-partition concurrency on a machine is achieved by running `W` worker daemons, with each child JVM bounded by `citrus.test.distribution.worker.forkJvmArgs` (default `-Xmx768m -XX:ActiveProcessorCount=2`).
- **POSIX `AF_UNIX` (`sun_path`) socket path bound (`104` bytes macOS / `108` bytes Linux):** Per-partition temporary directories used for `-Djava.io.tmpdir` must remain short enough (`<= 24` bytes on POSIX when `/tmp` is writable, e.g., `/tmp/cdtmp-<rand>`) that child JVMs, Netty, SSH agents, or Testcontainers binding Unix domain sockets inside `java.io.tmpdir` never hit `SocketException: Unix domain path too long`.
- **Accepted limitations:**
  1. **Arbitrary JVM-global state & best-effort CWD diagnostic in `AUTO` mode:** In-process partitions isolate `System.getProperties()` (`ThreadScopedProperties`), redirect default Citrus report/trace properties, and reset `CitrusExtension`. The before/after CWD snapshot (`worker.cwdCheck`) is a **read-only best-effort diagnostic**, not an OS sandbox: it cannot observe transient create-then-delete operations within a partition, restored mutations with identical size/mtime, reads of pre-existing daemon-CWD files, or writes outside watched paths, and never deletes detected files so shared daemon directories are safe. Tests mutating JVM-wide singletons (`TimeZone.setDefault`, `Locale.setDefault`, Log4j2 root levels), doing transient CWD scratch I/O, or reading `System.getProperty("basedir")` on pre-existing shared pools (`ForkJoinPool.commonPool()`) must set `citrus.test.distribution.fork=true` (module) or `citrus.test.distribution.worker.forkMode=ALWAYS` (worker).
  2. **Host output commit under `SIGKILL` / power loss:** While `IOException`s and runtime failures during host output commit trigger deterministic journaled rollback under `commit.lock`, POSIX/NTFS filesystems lack multi-file kernel transactions; an uncatchable `SIGKILL` or power loss mid-rename loop in Phase 3 cannot execute in-process rollback (mitigated by performing all blob verification and ZIP extraction in Phase 1 on the target volume so Phase 3 consists solely of fast same-volume renames).
  3. **Output file deletions:** Only created or content-modified regular files under `outputs` are exported; deleting a pre-existing input file inside a partition does not delete it on the host.
  4. **Artifact & archive caps:** Single file/archive blobs are capped at `MAX_ARTIFACT_BYTES = 256 MiB` compressed and `MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES = 2 GiB` (§0.3) / `MAX_ZIP_ENTRIES = 50,000` uncompressed; symbolic links are rejected on both packaging and extraction.
  5. **Path rewriting scope:** Base64-encoded, URL-percent-encoded, or binary-serialized paths, and paths outside `hostBaseDir`, are not rewritten.

## 4. Approaches Considered

### Approach A: Hybrid Fork-on-Workspace (`AUTO`) + Explicit Module Properties (Selected)
- Modules declare `citrus.test.distribution.inputs`, `citrus.test.distribution.outputs`, and/or `citrus.test.distribution.fork` via standard Maven `<properties>` (propagated through root `pom.xml` Surefire/Failsafe `<systemPropertyVariables>` and `SurefirePluginMutator`).
- Worker defaults to `citrus.test.distribution.worker.forkMode=AUTO`:
  - Partitions with `!forkWorkspace && inputArtifacts.isEmpty() && outputPaths.isEmpty()` run in-process via `IsolatedPartitionRunner` (with per-partition Citrus report/trace property redirection and undeclared-CWD-write guard).
  - Partitions with `forkWorkspace || !inputArtifacts.isEmpty() || !outputPaths.isEmpty()` (or `forkMode=ALWAYS`) run in a bounded child JVM via `ForkedPartitionRunner` with `ProcessBuilder.directory(workspaceDir)`, pre-created `workspaceDir/target`, short socket-safe `-Djava.io.tmpdir`, `-Dbasedir=<workspaceDir>`, and authenticated loopback socket event streaming.
- **Trade-offs:** Keeps zero-fork speed for ~73.7% of active test modules (`56 / 76`) while providing real OS/JVM CWD (`getcwd()` + `user.dir` + `basedir`), isolated `java.io.tmpdir`, and full JVM isolation for workspace-coupled modules (~300–600 ms typical child JVM startup, `< 1,500 ms` p95 gate).

### Approach B: Always-Fork Child JVM per Partition (`ALWAYS` only)
- Spawn a child JVM for every partition regardless of inputs/outputs.
- **Trade-offs:** Uniform execution path, but pays child JVM startup + classloading cost on every small unit-test partition across 56 pure-classpath modules. Kept available via `citrus.test.distribution.worker.forkMode=ALWAYS`, not default.

### Approach C: Auto-Ship `src/test/resources` for Every Module
- Implicitly zip and ship `src/test/resources` for all modules.
- **Trade-offs:** Unnecessarily hashes/extracts resources and triggers JVM forking across dozens of modules that already load resources via classpath (`target/test-classes`), while still requiring explicit config for `pom.xml`, `target/generated-test-sources`, `target/docs`, `target/citrus-docs`, and `fork=true` scratch-writing modules. Rejected.

## 5. Architecture & Detailed Design

### 5.1 Configuration, Precedence & Maven Integration

#### `DistributionSettings` (`tools/citrus-test-distribution-protocol`)
Add constants and helpers:
- `BASEDIR_PROPERTY = "citrus.test.distribution.basedir"`
- `INPUTS_PROPERTY = "citrus.test.distribution.inputs"` / `INPUTS_ENV = "CITRUS_DIST_INPUTS"`
- `OUTPUTS_PROPERTY = "citrus.test.distribution.outputs"` / `OUTPUTS_ENV = "CITRUS_DIST_OUTPUTS"`
- `FORK_WORKSPACE_PROPERTY = "citrus.test.distribution.fork"` / `FORK_WORKSPACE_ENV = "CITRUS_DIST_FORK"` (`false` default; strict `true`/`false` validation)
- `PARTITION_POLICY_PROPERTY = "citrus.test.distribution.partitionPolicy"` / `PARTITION_POLICY_ENV = "CITRUS_DIST_PARTITION_POLICY"` (`LPT` default, `SINGLE`; invalid values fail fast with `IllegalArgumentException`)
- `WORKER_FORK_MODE_PROPERTY = "citrus.test.distribution.worker.forkMode"` / `WORKER_FORK_MODE_ENV = "CITRUS_DIST_WORKER_FORK_MODE"` (`AUTO` default, `ALWAYS`; invalid values fail fast with `IllegalArgumentException`)
- `WORKER_CWD_CHECK_PROPERTY = "citrus.test.distribution.worker.cwdCheck"` / `WORKER_CWD_CHECK_ENV = "CITRUS_DIST_WORKER_CWD_CHECK"` (`FAIL` default, `WARN`, `OFF`; invalid values fail fast with `IllegalArgumentException`)
- `WORKER_FORK_JVM_ARGS_PROPERTY = "citrus.test.distribution.worker.forkJvmArgs"` / `WORKER_FORK_JVM_ARGS_ENV = "CITRUS_DIST_WORKER_FORK_JVM_ARGS"` (whitespace-split JVM args; default `List.of("-Xmx768m", "-XX:ActiveProcessorCount=2")`)
- `WORKER_HANDSHAKE_TIMEOUT_PROPERTY = "citrus.test.distribution.worker.handshakeTimeoutMs"` (default `15_000L`)
- `WORKSPACE_CAPABILITY_KEY = "citrus.workspace.v1"` (`"true"`)
- `WORKSPACE_TOKEN = "${citrus.dist.workspace}"`
- `NONE_SENTINEL = "none"` (case-insensitive explicit empty-list override for `inputs` and `outputs`)

#### Precedence, Replacement, Clearing & Fork Semantics (`AC-19` & `AC-2`)
1. **Single-Winner 5-Tier Replacement Precedence (highest to lowest — never merges across tiers):**
   For each setting (`inputs`, `outputs`, `fork`, `partitionPolicy`), the **first tier that explicitly defines the setting wins completely and replaces all lower tiers** (preventing inherited paths from leaking when overridden):
   1. **Tier 1 — Maven CLI / JVM User Property:** `-Dcitrus.test.distribution.<key>=<value>` (`session.getUserProperties().containsKey(key)` in Maven extension; explicit `-D` system property in standalone JUnit). Key presence wins even when `<value>` is empty `""` (`-Dcitrus.test.distribution.inputs=` clears lower tiers to `[]`).
   2. **Tier 2 — Explicit Plugin/Execution `<systemPropertyVariables>`:** Value configured directly in the module's `maven-surefire-plugin` or `maven-failsafe-plugin` block when non-blank and not equal to the root pass-through expression `${citrus.test.distribution.<key>}`.
   3. **Tier 3 — Module Maven `<properties>`:** `project.getProperties().getProperty(key)` when non-null and non-empty (root `pom.xml`'s `<citrus.test.distribution.inputs/>` evaluates to `""`, which counts as unset at Tier 3 so Tier 4 `CITRUS_DIST_*` env vars remain reachable; child modules clear an inherited parent POM list in XML via `<citrus.test.distribution.inputs>none</citrus.test.distribution.inputs>`).
   4. **Tier 4 — Environment Variable (`CITRUS_DIST_*`):** Applied when Tiers 1–3 did not define a value and the env var is non-null and non-blank.
   5. **Tier 5 — Built-in Default:** `inputs = []`, `outputs = []`, `fork = false`, `partitionPolicy = LPT`, `worker.forkMode = AUTO`, `worker.cwdCheck = FAIL`.
2. **Explicit Empty Sentinel (`none`) & Intra-Tier Normalization (`WorkspacePackager.normalizeDeclaredPaths`):**
   - If the winning tier's trimmed raw string is `""` or equals `"none"` (case-insensitive), normalization immediately returns `List.of()` (`[]`). To reference a literal file named `none` in `hostBaseDir`, write `./none` (a single leading `./` is stripped per token after the `"none"` sentinel check).
   - Otherwise, split the winning tier's string on `,`, trim whitespace, ignore empty tokens, extract optional trailing `?` on `inputs` (`DeclaredInputPath(String relativePath, boolean optional)`), normalize `\` to `/`, strip a single leading `./` and any trailing `/`, validate via `validateRelativePath`, deduplicate exact matches (if both `foo?` and `foo` appear in the same list, required `foo` wins), and collapse subsumed child paths under declared ancestors (`src/test,src/test/resources` -> `["src/test"]`).
3. **Fork Flag Precedence vs Effective Execution Mode (`shouldFork`):**
   - `citrus.test.distribution.fork` (`boolean forkWorkspace`, strict `true`/`false`) is resolved by the same 5-tier replacement rule (`-Dcitrus.test.distribution.fork=false` overrides `<citrus.test.distribution.fork>true</citrus.test.distribution.fork>` in a module POM).
   - On the worker, effective execution mode is monotonic over active workspace requirements:
     `shouldFork = (workerConfig.forkMode() == ALWAYS) || assignment.forkWorkspace() || !assignment.inputArtifacts().isEmpty() || !assignment.outputPaths().isEmpty()`.
     Because materializing `inputs` or `outputs` requires an isolated CWD, non-empty `inputs`/`outputs` always fork even when `fork=false`.
4. **Concrete Precedence Examples:**
   - Module POM sets `inputs=src/test/resources`; CLI passes `-Dcitrus.test.distribution.inputs=pom.xml` -> effective `inputs=["pom.xml"]` (Tier 1 replaces Tier 3, never `["src/test/resources", "pom.xml"]`).
   - Module POM sets `inputs=src/test/resources`; CLI passes `-Dcitrus.test.distribution.inputs=none` (or `-Dcitrus.test.distribution.inputs=`) -> effective `inputs=[]` (cleared; runs in-process if `outputs=[]` and `fork=false`).
   - Module POM sets `fork=true` (`inputs=[]`, `outputs=[]`); CLI passes `-Dcitrus.test.distribution.fork=false` -> `forkWorkspace=false`, runs in-process under `forkMode=AUTO`.
   - Module POM sets `inputs=src/test/resources`; CLI passes `-Dcitrus.test.distribution.fork=false` -> `forkWorkspace=false`, but `inputArtifacts` is non-empty so `shouldFork=true` unless CLI also passes `-Dcitrus.test.distribution.inputs=none`.
5. **Explicit Single-Partition Policy (`PartitionPolicy.SINGLE` — `AC-2`):**
   - Replaces magic numeric thresholds (`maxPartitionSize=10000`) with an explicit `PartitionPolicy { LPT, SINGLE }` on `SubmitWorkRequest` (configured via `citrus.test.distribution.partitionPolicy=LPT|SINGLE`).
   - Under `LPT` (default), `LptPartitionScheduler` groups by `@AgentRequirement` capabilities and chunks by coordinator `maxPartitionSize`.
   - Under `SINGLE`, `LptPartitionScheduler` co-schedules **100% of submitted remote `WorkUnitSpec`s** from that submission into **strictly `1` `PartitionRecord`** regardless of work-unit count (bypassing `maxPartitionSize` completely). Non-conflicting `@AgentRequirement` maps across units are merged (union) onto the single partition; if two units in a `SINGLE` submission declare conflicting values for the same capability key (e.g., `os=linux` vs `os=windows`), `submitWork` rejects the submission immediately (`400 Bad Request` / `IllegalArgumentException`) rather than silently splitting the module across partitions.

#### Root `pom.xml`, Module `pom.xml`s & `SurefirePluginMutator`
- **No custom XML inside Surefire/Failsafe `<configuration>`:** All module configurations use standard `<properties>`, ensuring `./mvnw test` works identically without `citrus-test-distribution-maven-extension`.
- **Root `pom.xml`:**
  - Add `<properties>`: `<citrus.test.distribution.inputs/>`, `<citrus.test.distribution.outputs/>`, `<citrus.test.distribution.fork/>` (empty as built — §0.2), `<citrus.test.distribution.partitionPolicy/>` (empty as built — §0.2).
  - Add to both `maven-surefire-plugin` and `maven-failsafe-plugin` `<pluginManagement><configuration><systemPropertyVariables>`:
    - `<citrus.test.distribution.basedir>${project.basedir}</citrus.test.distribution.basedir>`
    - `<citrus.test.distribution.inputs>${citrus.test.distribution.inputs}</citrus.test.distribution.inputs>`
    - `<citrus.test.distribution.outputs>${citrus.test.distribution.outputs}</citrus.test.distribution.outputs>`
    - `<citrus.test.distribution.fork>${citrus.test.distribution.fork}</citrus.test.distribution.fork>`
    - `<citrus.test.distribution.partitionPolicy>${citrus.test.distribution.partitionPolicy}</citrus.test.distribution.partitionPolicy>`
- **`SurefirePluginMutator` (`tools/maven/citrus-test-distribution-maven-extension`):**
  - Injects `citrus.test.distribution.basedir = project.getBasedir().toPath().toAbsolutePath().normalize().toString()`.
  - Resolves `citrus.test.distribution.{inputs,outputs,fork,partitionPolicy}` using exact key-presence Tier 1 (`session.getUserProperties().containsKey(key)`) > Tier 2 (explicit non-placeholder `<systemPropertyVariables>`) > Tier 3 (`project.getProperties()`) replacement semantics.

### 5.2 Protocol, Versioning, Security & Packaging (`tools/citrus-test-distribution-protocol`)

#### Wire Models & Version Compatibility
- `ArtifactKind`: `enum { JAR, DIRECTORY_ZIP, REGULAR_FILE }`
- `PartitionPolicy`: `enum { LPT, SINGLE }`
- `WorkspaceFileSet(String relativePath, ArtifactRef artifact)`
- Updated wire records (preserving legacy constructors and normalizing `null` collections/enums to `List.of()` / `PartitionPolicy.LPT` in compact constructors; `DistributionJson` already disables `FAIL_ON_UNKNOWN_PROPERTIES`):
  - `SubmitWorkRequest`: add `List<WorkspaceFileSet> inputArtifacts`, `List<String> outputPaths`, `boolean forkWorkspace`, `PartitionPolicy partitionPolicy`
  - `PartitionAssignPayload`: add `List<WorkspaceFileSet> inputArtifacts`, `List<String> outputPaths`, `boolean forkWorkspace`
  - `WorkerTestEventPayload`: add `int attempt` (defaults to `0` for legacy senders)
  - `PartitionCompletedPayload`: add `int attempt`, `List<WorkspaceFileSet> outputArtifacts`
- **Capability Gating (`citrus.workspace.v1`):**
  - Updated workers automatically include `"citrus.workspace.v1": "true"` in their `WorkerRegistration` capabilities.
  - When `SubmitWorkRequest` has `forkWorkspace || !inputArtifacts.isEmpty() || !outputPaths.isEmpty()`, `BrokerSessionCoordinator` merges `"citrus.workspace.v1": "true"` into each partition's `requiredCapabilities`, preventing workspace or `REGULAR_FILE` payloads from ever being dispatched to older pre-workspace workers.

#### Security, Limits, Integrity & `WorkspacePackager`
- **Hard Limits:**
  - `MAX_ARTIFACT_BYTES = 256L * 1024 * 1024` (256 MiB compressed per blob; enforced pre-flight before upload on both host and worker).
  - `MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES = 2L * 1024 * 1024 * 1024` (2 GiB total uncompressed bytes per ZIP archive, as built — §0.3; enforced via counting stream during both pack and unpack — never trusting `ZipEntry.getSize()`).
  - `MAX_ZIP_ENTRIES = 50_000` entries per ZIP archive.
- **Strict Path Validation (`validateRelativePath`):**
  - Rejects `null`, blank, `.`, leading `/` or `\`, NUL/control characters (`c < 0x20`), `:`, UNC paths (`\\...`), `.` or `..` segments, and Windows reserved device names (`CON`, `PRN`, `AUX`, `NUL`, `COM1`–`COM9`, `LPT1`–`LPT9`, case-insensitive with or without extension).
- **Symlink & TOCTOU Protection:**
  - Packaging walks directories without `FOLLOW_LINKS`; encountering `Files.isSymbolicLink(p)` throws `IllegalArgumentException` immediately (as built; extraction raises `IOException` — §0.3).
  - Extraction/materialization verifies every path prefix component from `targetRoot` down to `dest` with `!Files.isSymbolicLink(component)` (`LinkOption.NOFOLLOW_LINKS`) before creating directories or writing files, preventing symlink-pivot escapes.
- **Deterministic Streaming ZIPs & Cache Self-Healing:**
  - Input ZIP archives stream directly to temporary files (`try / finally { Files.deleteIfExists(tmp); }`) with sorted UTF-8 `/`-separated entry names (including `dir/` entries for empty input subdirectories), a fixed DOS time of 1980-02-01T00:00 (as built — §0.3; no extended-timestamp extra field), `extra = null`, `comment = null`, and `Deflater.DEFAULT_COMPRESSION`.
  - Differential output packaging (`packageModifiedOutputs(workspaceDir, outputPath, baselineHashes)`):
    - `baselineHashes` records SHA-256 digests of **regular files** present under declared `outputPaths` immediately after input materialization.
    - After test execution, `packageModifiedOutputs` collects only regular files under `outputPath` whose SHA-256 differs from `baselineHashes` (or were newly created). If `outputPath` does not exist or contains **zero** new/modified regular files (for example, when `<workspaceDir>/target` was pre-created as an empty directory and a test produced nothing under `target/docs`), `packageModifiedOutputs` returns `Optional.empty()` and never emits an empty `DIRECTORY_ZIP`.
  - `BlobCacheDirectory.put` wraps temporary file writes in `try / finally { Files.deleteIfExists(tmp); }` so disk-full or I/O errors leave zero orphan `.tmp-*` files.
  - `BlobCacheDirectory` verifies SHA-256 on reads/extractions; if a cached blob is corrupted on disk (`ZipException` or digest mismatch), the worker/launcher evicts the corrupted cache file and re-downloads once from the coordinator, failing hard if re-verification still fails.
- **Missing Declared Inputs:**
  - Required input paths (`!optional`) that do not exist under `hostBaseDir` fail fast in `DistributedLauncher` with `CitrusRuntimeException` before contacting the coordinator. Optional input paths (`?` suffix) are skipped with `DEBUG` logging if absent.

#### `PathRewriter` (`org.citrusframework.distribution.config.PathRewriter`)
- Collects candidate host prefixes for `hostBaseDir`: `hostBaseDir.toRealPath()` (if resolvable, handling macOS `/var` -> `/private/var`) and `hostBaseDir.toAbsolutePath().normalize()`, plus `/` forward-slash and `\\` escaped-backslash variants on Windows, ordered longest-first.
- Matches candidate prefix ONLY when:
  - Preceded by start-of-string, whitespace, `=`, `:`, `;`, `,`, `"`, `'`, `(`, `[`, or `file://` (`(?<=file://)`).
  - Followed by end-of-string, `/`, `\`, whitespace, `:`, `;`, `,`, `"`, `'`, `)`, or `]`.
  - Sibling directories sharing a prefix (`/work/citrus/tools/restdocs` or `/work/citrus/tools/rest-api` when `hostBaseDir = /work/citrus/tools/rest`) and super-paths (`/other/work/citrus/tools/rest`) are never rewritten.
- Note that Maven properties such as `project.build.directory` (`<hostBaseDir>/target`) are not excluded by `HOST_SYSTEM_PROPERTY_PREFIXES`, so `PathRewriter` automatically rewrites `<hostBaseDir>/target` -> `${citrus.dist.workspace}/target` -> `<workspaceDir>/target`!
- Expands `${citrus.dist.workspace}` on the worker to `workspaceDir.toAbsolutePath().normalize().toString()`, normalizing path separators immediately following `${citrus.dist.workspace}` to `File.separatorChar`.

### 5.3 Coordinator Forwarding & Attempt Fencing (`tools/citrus-test-distribution-coordinator`)

In `BrokerSessionCoordinator`:
- Extend `PartitionRuntime` with:
  - `List<WorkspaceFileSet> inputArtifacts`
  - `List<String> outputPaths`
  - `boolean forkWorkspace`
  - `String activeConnectionId` (set to `conn.connectionId` on dispatch in `dispatchPendingPartitions()`, cleared to `null` on lease expiry or worker disconnect)
- Enforce `SubmitWorkRequest.partitionPolicy()` (`LPT` vs `SINGLE`) when invoking `LptPartitionScheduler` during `submitWork(...)` (`SINGLE` co-schedules 100% of submitted units into strictly `1` partition regardless of count and rejects conflicting `@AgentRequirement` values for the same key with `400 Bad Request`).
- In `sweepExpiredLeases()`:
  - When a partition's lease expires, send best-effort `PARTITION_CANCEL` to the stalled `WorkerConnection`, clear `stalledConn.activePartitionId`, transition stalled worker out of `BUSY`, and clear `pr.activeConnectionId = null` before requeueing (`attempt + 1`) or marking `LOST`.
- **Strict Attempt + Connection Fencing:**
  - In **both** `recordTestEvent(WorkerConnection conn, WorkerTestEventPayload event)` and `completePartition(WorkerConnection conn, PartitionCompletedPayload done)`:
    - Verify `pr != null && pr.record.state() in (ASSIGNED, RUNNING) (as built — §0.4) && conn.connectionId.equals(pr.activeConnectionId) && (payloadAttempt == pr.record.attempt())` (allowing `payloadAttempt == 0` only when `pr.record.attempt() == 1` for backward compatibility with legacy workers).
    - Any message failing this fence (including late `outputArtifacts` from a slow/GC-paused prior attempt, even if the same `workerId` reconnected on a new `connectionId`) is logged at `WARN` and **discarded**.
- In `completePartition(...)`, publish `"attempt", pr.record.attempt()` and `"outputArtifacts", done.outputArtifacts()` (when non-empty) inside `"partition.completed"` SSE `data`.

### 5.4 Worker Execution: `IsolatedPartitionRunner`, `ForkedPartitionRunner` & Shared `PartitionJUnitLauncher` (`tools/citrus-test-distribution-worker`)

#### Concurrency, Short Socket-Safe Temp Paths & Workspace Layout
- `DistributionWorkerClient` executes at most `1` partition at a time (`partitionExecutor` single thread). Rejects duplicate `PARTITION_ASSIGN` if `activePartitionId != null`.
- `DistributionWorkerClient.abortConnection()` and `DistributionWorkerClient.close()` both invoke `activeCancellation.cancel()` so any running child JVM is terminated immediately on WebSocket loss or client shutdown.
- **Short directory prefixes (`AF_UNIX` `sun_path` safety):**
  - `partitionRoot = Files.createTempDirectory("cdist-")` (replacing the verbose `citrus-dist-partition-<uuid>-<rand>` prefix):
    ```text
    <temp>/cdist-<rand>/
      extracted/    # unpacked DIRECTORY_ZIP classpath entries
      ipc/          # request.json + classpath.argfile (owner-only 0700/0600 perms)
      workspace/    # clean CWD: pre-created target/, materialized inputs & test outputs
    ```
  - For child JVM `-Djava.io.tmpdir`, `ForkedPartitionRunner` allocates `partitionTmpDir`:
    - On POSIX (`!isWindows`) when `/tmp` is an existing writable non-symlink directory, creates `Files.createTempDirectory(Path.of("/tmp"), "cdtmp-")` (~18–24 bytes total, leaving `80+` bytes of headroom under macOS's `104`-byte and Linux's `108`-byte `AF_UNIX` `sun_path` limit even when macOS `TMPDIR` is a 57-byte `/private/var/folders/...` path!).
    - Otherwise (Windows or restricted container without `/tmp`), falls back to `Files.createDirectories(partitionRoot.resolve("t"))`.
    - Both `partitionRoot` and `partitionTmpDir` are registered for cleanup in `finally` and in the worker JVM shutdown hook.

#### Shared `PartitionJUnitLauncher` (Parity Guarantee)
Both `IsolatedPartitionRunner` and `ForkedPartitionMain` delegate JUnit discovery and execution to a single shared class `PartitionJUnitLauncher`:
1. `resetCitrusExtensionIfPresent(ClassLoader)`
2. `setDynamicSourceFilterIfPresent(ClassLoader, Set<String>)` (cleared in `finally`)
3. Method selector parsing supporting both bare `methodName` and parameterized/overloaded `methodName(paramType1,paramType2)` (generated by `DiscoveredWorkExtractor`) via `DiscoverySelectors.selectMethod(testClass, rawMethodName, parameterTypeNames)`
4. `LauncherDiscoveryRequestBuilder` with `junit.platform.launcher.interceptors.enabled = false`
5. Unified `TestExecutionListener` emitting identical `WorkerTestEventPayload` (including `attempt`) and failure/duration/skip semantics.

#### `IsolatedPartitionRunner` (`AUTO` In-Process Fast Path)
- Used only when `forkMode == AUTO && !assignment.forkWorkspace() && assignment.inputArtifacts().isEmpty() && assignment.outputPaths().isEmpty()`.
- **Verified Citrus Report/Trace Dynamic Property Redirection:**
  - `TestReporterSettings.getReportDirectory()` (`core/citrus-api`) and `CitrusSettings.getMessageTraceDirectory()` (`core/citrus-base`) both read `System.getProperty(...)` **dynamically on every call** (neither is cached in a `static final` field), and `ThreadScopedProperties` intercepts `System.getProperty(...)` regardless of whether a class is loaded by child-first `PartitionClassLoader` or parent `ClassLoader`.
  - `IsolatedPartitionRunner` places into `partitionOverrides` (passed to `ThreadScopedProperties.enterScope(partitionOverrides)`, never mutating `SCOPED_PROPERTIES.base()`):
    - `basedir = workspaceDir.toString()`
    - `citrus.report.directory`: if unset in `assignment.systemProperties()`, set to `workspaceDir.resolve("target/citrus-reports").toString()`; if set to a relative path, resolve against `workspaceDir`.
    - `citrus.message.trace.directory`: if unset in `assignment.systemProperties()`, set to `workspaceDir.resolve("target/citrus-logs/trace/messages").toString()`; if set to a relative path, resolve against `workspaceDir`.
  - Because `Paths.get(reportDirectory)` receives an absolute path inside `<partitionRoot>/workspace/target/...`, default Citrus HTML/JUnit/MessageTrace reporters across all 56 in-process modules write strictly inside the partition's temporary workspace and never touch the worker daemon's CWD.
- **Read-Only Best-Effort Undeclared CWD Diagnostic (`worker.cwdCheck` — `AC-3`):**
  - Explicitly a **best-effort post-execution diagnostic**, not an OS sandbox: it cannot observe transient create-then-delete operations within a test, restored modifications with identical size/mtime, reads of pre-existing daemon-CWD files, or writes outside watched paths.
  - **Non-destructive shared-directory safety boundary:** `IsolatedPartitionRunner` **never deletes or modifies** any file observed in the worker daemon's CWD (`System.getProperty("user.dir")`). Deleting unexpected files in `user.dir` could destroy files owned by another worker daemon process, host tool, or operator sharing that directory.
  - Controlled by `citrus.test.distribution.worker.cwdCheck` (`FAIL` default, `WARN`, `OFF`): under `WORKER_JVM_LOCK`, snapshots `(sizeBytes, lastModifiedTime)` of top-level entries in worker `user.dir` plus up to 2 directory levels under `target/` and `src/` (excluding `workerConfig.cacheDir()` if nested under `user.dir`) before and after `PartitionJUnitLauncher.execute(...)`. When new or modified entries are observed, leaves the filesystem untouched and either fails the partition (`FAIL`) or logs a warning (`WARN`) with the observed paths and remediation (`citrus.test.distribution.fork=true`, `inputs`/`outputs`, or running each worker daemon in a dedicated empty working directory).

#### `ForkedPartitionRunner` & `ForkedPartitionMain`
1. **Materialization, `target/` Pre-Creation & Baseline Snapshot:**
   - Prefetch missing `classpathArtifacts` and `inputArtifacts` blobs (with corruption self-healing).
   - Materialize `classpathArtifacts` into `partitionRoot/extracted`, pre-create `Files.createDirectories(workspaceDir.resolve("target"))` (so tests assuming Maven's `target/` directory already exists—such as `citrus-ftp`, `citrus-kafka`, `citrus-playwright`, `citrus-selenium`, and `jbang`—succeed out of the box), and materialize `inputArtifacts` into `workspaceDir`.
   - For each declared `outputPath`, walk any pre-existing materialized **regular files** under `workspaceDir.resolve(outputPath)` and record `Map<String, String> baselineHashes` (`workspaceRelPath -> sha256`). Empty pre-created directories (such as `workspaceDir/target`) produce zero entries in `baselineHashes` and zero output archives if unmodified.
2. **Portable Child Classpath & `@argfile` Escaping:**
   - Build ordered classpath list:
     1. All materialized `assignment.classpathArtifacts()` in exact submission order.
     2. Worker runtime `CodeSource` paths (`ForkedPartitionMain` / worker JAR, protocol JAR) if not already on the list, plus fallback `CodeSource` paths for Jackson 3, JUnit Platform Launcher/Engine/Commons, OpenTest4J, and SLF4J **only if** the corresponding anchor class resource is absent from the materialized test classpath (preserving the module under test's library versions).
   - Join entries with `File.pathSeparator` (`:` on POSIX, `;` on Windows) and write `partitionRoot/ipc/classpath.argfile` using `Charset.defaultCharset()` with strict JDK `@argfile` double-quote escaping (`\` -> `\\`, `"` -> `\"`; reject `\r`/`\n` in paths).
3. **Authenticated Loopback Socket & Bounded Timeouts:**
   - Bind `ServerSocket(0, 50, InetAddress.getLoopbackAddress())` and generate a 256-bit `SecureRandom` hex `authToken` written into `partitionRoot/ipc/request.json`.
   - Accept connections until `handshakeTimeoutMs` (`15_000 ms` default); each accepted socket has a `5_000 ms` read timeout and `1 MiB` line cap to read `"HELLO " + authToken`. Unauthenticated sockets are closed immediately without aborting acceptance for the real child JVM.
4. **Child JVM Process Launch:**
   ```text
   <java.home/bin/java[.exe]> @<ipc/classpath.argfile>
     <forkJvmArgs: default -Xmx768m -XX:ActiveProcessorCount=2>
     -Djava.io.tmpdir=<partitionTmpDir>
     -Dcitrus.test.distribution.role=worker
     -Djunit.platform.launcher.interceptors.enabled=false
     -Dbasedir=<workspaceDir>
     org.citrusframework.distribution.worker.ForkedPartitionMain
     <ipc/request.json> <loopbackPort>
   ```
   - `ProcessBuilder.directory(workspaceDir.toFile())` sets real OS `getcwd()` and `StaticProperty.USER_DIR` to `workspaceDir`.
   - Virtual/daemon threads drain child `stdout`/`stderr` to SLF4J and retain a 50-line bounded ring buffer for crash diagnostics.
   - `ForkedPartitionMain` sets expanded `request.systemProperties()`, sends `"HELLO " + token`, streams `EVENT <json>\n` via `PartitionJUnitLauncher`, sends `RESULT <json>\n` (`ForkedExecutionSummary(boolean success, String errorMessage, int eventCount)`), flushes, and exits `0`.
5. **Cancellation, Crash & Descendant Cleanup:**
   - If `CancellationToken.isCancellationRequested()` fires, or handshake times out, or socket closes before `RESULT`, or child exits non-zero:
     - `destroyProcessTree(process)`: snapshots `process.toHandle().descendants()` + `process.toHandle()`, sends `destroy()` bottom-up (waiting up to `2s`), then `destroyForcibly()` (waiting up to `3s` so Windows file handles release before directory deletion).
     - Registers active child handles, `partitionRoot`, and `partitionTmpDir` in a worker JVM shutdown hook as last-resort cleanup on worker `SIGTERM`.
     - If cancelled, returns immediately **without** packaging or uploading outputs.
     - If crashed/exited without `RESULT`, returns `PartitionExecutionResult(false, diagnosticWithExitCodeAndStderrTail, List.of())`.
6. **Differential Output Packaging & Upload:**
   - On normal `RESULT` completion (both passing and assertion-failing test runs, so diagnostic/RestDocs outputs from completed runs are preserved, never on cancel/crash):
     - Walk each declared `outputPath` under `workspaceDir` via `WorkspacePackager.packageModifiedOutputs(...)`:
       - Include only regular files where `!sha256.equals(baselineHashes.get(workspaceRelPath))` (newly created or modified files; unchanged input files and empty pre-created directories are excluded, and deleted input files are not propagated).
       - Enforce `sizeBytes <= 256 MiB`, `MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES <= 2 GiB`, `MAX_ZIP_ENTRIES <= 50_000`, and symlink rejection.
       - Store blobs in worker `BlobCacheDirectory`, query `POST /api/v1/cache/missing`, and `PUT /api/v1/cache/blobs/:sha256` for missing digests.
   - Delete `partitionRoot` and `partitionTmpDir` recursively in `finally` (with short retry on Windows transient file-handle release).

### 5.5 Host Event Isolation, All-or-Nothing Output Staging & Lock Hygiene (`tools/citrus-test-distribution`)

In `DistributedLauncher`:
1. **Strict Reactor `sessionId` Partition Filtering:**
   - Filter **every** SSE event (`test.event`, `partition.completed`, `partition.lost`) by `submittedPartitionIds.contains(event.partitionId())` so parallel Maven modules sharing a reactor `sessionId` never cross-contaminate test events, failure state, or outputs.
2. **Runner/Crash Failure Propagation:**
   - When `"partition.completed"` arrives for a submitted partition with `state == FAILED`, record partition-level execution failure (`errorMessage`) so child JVM crashes or runner failures with zero JUnit `test.event`s deterministically fail the build.
3. **All-or-Nothing Output Staging & `target/.citrus-dist-outputs/` Lock Hygiene:**
   - Maintain `Map<String, StagedOutputEntry> stagedOutputs` (`relFile -> StagedOutputEntry(String partitionId, String sha256, StagedSource source)`) during `streamEventsUntilDone`:
     - Only process `"partition.completed"` when `pendingPartitions.remove(event.partitionId())` returns `true` (idempotent across SSE reconnects/replays).
     - Download missing output blobs via `BrokerHttpClient.downloadBlob(sha256)` into host `BlobCacheDirectory` (verifying SHA-256).
     - Inspect every output file (`REGULAR_FILE` or each file entry in `DIRECTORY_ZIP`), validating relative paths and ZIP limits without writing to `hostBaseDir` yet:
       - New `relFile`: record in `stagedOutputs`.
       - Existing `relFile` with identical `sha256`: idempotent no-op.
       - Existing `relFile` with differing `sha256`: record conflict error (`"Conflicting output file '" + relFile + "' produced with different content by partitions " + existing.partitionId() + " and " + partitionId`).
   - **Transactional Journaled Commit Phase (after all submitted partitions terminate, only when `!stagedOutputs.isEmpty()` or conflicts exist — `AC-8` & `AC-9`):**
     - **Conflict check before any disk mutation:** If any cross-partition SHA-256 conflict occurred, **write nothing to `hostBaseDir`** and throw `CitrusRuntimeException` listing every conflicting path and partition pair.
     - **Dual-Lock Serialization & 4-Phase Backup-and-Rollback Protocol:**
       1. Create `<hostBaseDir>/target/.citrus-dist-outputs/` (`Files.createDirectories(...)` — keeping lock, staging, and backup files strictly inside `target/`, never touching `<hostBaseDir>` root) and acquire **both** an in-JVM `ReentrantLock` keyed by canonical `hostBaseDir` (preventing `OverlappingFileLockException` across concurrent launcher threads in one JVM) **and** `FileChannel.lock()` on `<hostBaseDir>/target/.citrus-dist-outputs/commit.lock` (serializing separate launcher JVMs sharing `hostBaseDir`).
       2. **Phase 1 — Full Target-Volume Staging & Validation:** Extract/copy all `stagedOutputs` into `<hostBaseDir>/target/.citrus-dist-outputs/tx-<uuid>/staged/<relFile>` and verify `!Files.isSymbolicLink` (`LinkOption.NOFOLLOW_LINKS`) along every prefix component from `hostBaseDir` to `<hostBaseDir>/<relFile>`. Because `tx-<uuid>/staged/` resides on the same filesystem as `hostBaseDir`, corrupt archives, symlink violations, and disk-space exhaustion fail in Phase 1 before any destination path outside `target/.citrus-dist-outputs/` is touched.
       3. **Phase 2 & 3 — Backup Pre-Existing Files & Apply Moves with Undo Journal:** Maintain an ordered `Deque<UndoAction>` (`DELETE_CREATED_FILE(dest)`, `RESTORE_BACKUP(dest, backupPath)`, `DELETE_CREATED_DIR(dir)`). Create missing destination parent directories (recording each newly created directory), move/copy any pre-existing regular file at `<hostBaseDir>/<relFile>` into `tx-<uuid>/backup/<relFile>` (recording `RESTORE_BACKUP`), and move `tx-<uuid>/staged/<relFile>` into `<hostBaseDir>/<relFile>` (`ATOMIC_MOVE` with `REPLACE_EXISTING` fallback; recording `DELETE_CREATED_FILE` when no prior file existed).
       4. **Rollback on Exception & Cleanup:** If any `IOException` or `RuntimeException` occurs during Phase 2 or Phase 3, replay `UndoAction` in LIFO order while still holding both locks—deleting newly created files, restoring backed-up pre-existing files (`REPLACE_EXISTING`), and removing newly created empty directories—attaching any rollback `IOException` via `addSuppressed(...)` before throwing `CitrusRuntimeException`. Always delete `tx-<uuid>/` in `finally` before releasing `commit.lock` and the in-JVM lock. (Uncatchable OS `SIGKILL` / power loss mid-Phase-3 rename loop is an explicit accepted filesystem limitation.)

### 5.6 Citrus Repository Rollout (as built)

Re-derived from each module's tests under the working-directory parity of §0.1 (Surefire tests run in `target/`, Failsafe tests in the module directory, locally and on workers). The test classpath is always shipped, so files read through the classpath need no declaration.

| Module | Setting | Evidence |
|--------|---------|----------|
| `endpoints/citrus-http`, `endpoints/citrus-ws` | `fork=true` | No input needed (`resource-base="src/test/resources"` resolves to a missing path both locally and remotely; only Citrus servlets are registered). Fork because ITs start fixed-port servers from cached Spring contexts that the shared worker JVM never closes. |
| `endpoints/citrus-kafka` | `fork=true` | Fixed port 9092 in a cached context, non-daemon threads (no working-directory writes found). |
| `tools/restdocs` | `fork=true` | Unit tests and ITs write snippets relative to the working directory; nothing on the host consumes them (no outputs). |
| `tools/docs-generator` | `fork=true` | Writes `src/test/{resources,java}` and `target/docs` relative to the working directory; never reads the module's real `src/test/resources`; each class creates its own sample (no `SINGLE`, no outputs). |
| `tools/test-generator` | `fork=true` | Writes the default test source directory relative to the working directory; specs load via classpath fallback; compiles against `java.class.path`. |
| `core/citrus-api`, `core/citrus-base` | `fork=true` | `MessageTracingTestListenerTest` / `TestFlowReporterTest` write relative to the working directory. |
| `endpoints/citrus-ftp`, `endpoints/citrus-ssh`, `endpoints/citrus-camel` | `fork=true` | FTP/SFTP home directories, SSH default user home and the JBang work directory are relative to the working directory; camel also spawns processes. |
| `connectors/citrus-playwright`, `connectors/citrus-selenium`, `tools/schema-generator` | `fork=true` | Screenshots/evidence and generated schemas written relative to the working directory. |
| `tools/jbang`, `connectors/citrus-jbang-connector` | `fork=true` | Relative writes (`target`, `.citrus-jbang`) and global Log4j configuration. |
| `runtime/citrus-cucumber` | `fork=true` | `TerminationLogReporterIT` writes `target/termination.log` relative to the working directory (missed by the original audit). |
| `tools/test-api-generator/citrus-openapi-codegen` | `inputs=target/generated-test-sources` | Generated sources are a test source root, not on the classpath. `ExpectedCodeGenIT` now locates files via `basedir` and package-relative paths. |
| `tools/test-api-generator/citrus-openapi-codegen-maven-plugin` | `inputs=pom.xml,src/test/resources,target/test-classes/SpringMetaFileGeneratorTest` | Mojo IT reads `basedir/src/test/resources/...` and `pom.xml` (stub now reads it from `PlexusTestCase.getBasedir()`); `SpringMetaFileGeneratorTest` rewrites fixtures under `<user.dir>/test-classes/...`. |

`@LocalOnly`: `tools/jbang` `RunTest` (resolves Citrus modules through Maven unless they appear by name on `java.class.path`); `connectors/citrus-jbang-connector` `JBangTestEngineTest#testRunFromWorkDirectory`, `#testRunDirectory`, `#testRunSource`, `#testRunNoMatch` (real JBang downloads in the host working directory); `runtime/citrus-quarkus/citrus-quarkus-it` `DemoApplicationTest` (`@QuarkusTest` bootstraps from the local project layout and Maven repository). The last two modules received the test-scoped `citrus-test-distribution-protocol` dependency. `ListTest` and `CommandParityTest#shouldListWithSortOption` are not pinned (they only assert the always-printed table header).

No module declares `outputs` or `partitionPolicy=SINGLE`. `tools/mcp-server` remains distribution-disabled; all other test modules run in-process unchanged.

### 5.7 Documentation Update
Update `src/manual/tools-test-distribution.adoc` with a dedicated **External Test Resources (Inputs, Outputs, Forking & Path Rewriting)** section documenting properties, 5-tier single-winner replacement precedence (including `none` sentinel clearing), `partitionPolicy=LPT|SINGLE`, optional `?` inputs, `forkMode` (`AUTO`/`ALWAYS`), `cwdCheck` (`FAIL`/`WARN`/`OFF`), `forkJvmArgs`, socket-safe temp directory isolation, `basedir` rewriting boundaries and limitations, transactional backup-and-rollback output commit under `target/.citrus-dist-outputs/`, and `@LocalOnly` usage.

## 6. Review Disposition & Acceptance Criteria (`AC-1` – `AC-25`)

> As built (Revision 6): AC-7 (attempt 0 rule, cancellation confirmation), AC-12 (2 GiB total uncompressed, symlink exception types), AC-19 (empty values count as unset; `none` clears) and AC-22 (re-derived rollout, §5.6) were refined during implementation — see §0.

| # | Tier | Disposition | Summary & Testable Acceptance Criteria |
|---|------|-------------|----------------------------------------|
| **AC-1** | P0 | **Design change** | **Worker concurrency & JVM limits:** `DistributionWorkerClient` runs strictly 1 partition / <=1 child JVM at a time (rejecting concurrent `PARTITION_ASSIGN`); `ForkedPartitionRunner` applies `forkJvmArgs` (default `-Xmx768m -XX:ActiveProcessorCount=2`), verified via child `RuntimeMXBean.getInputArguments()`. |
| **AC-2** | P0 | **Design change** | **Partition isolation & explicit `PartitionPolicy.SINGLE`:** Partitions execute in disjoint `cdist-<rand>/workspace` directories; `citrus.test.distribution.partitionPolicy=SINGLE` co-schedules 100% of submitted work units into strictly `1` `PartitionRecord` regardless of unit count (verified with `>10,000` synthetic units) and rejects conflicting `@AgentRequirement` values on the same key with `400 Bad Request` instead of splitting. |
| **AC-3** | P0 | **Design change + Accepted limitation** | **Citrus reporter redirection & non-destructive CWD diagnostic (`AUTO`):** `IsolatedPartitionRunner` redirects default/relative `citrus.report.directory` and `citrus.message.trace.directory` into `<partitionRoot>/workspace/target/...` via `ThreadScopedProperties`, performs a read-only before/after CWD snapshot (`worker.cwdCheck=FAIL|WARN|OFF`, default `FAIL`), **never deletes** files in worker CWD (preserving shared daemon directories), and fails (`FAIL`) or warns (`WARN`) with observed paths; transient create-then-delete CWD I/O is documented as a best-effort limitation requiring `fork=true`. |
| **AC-4** | P0 | **Already addressed** | **Thread-scoped `basedir`:** Concurrent `DistributionWorkerClient` instances in one JVM place `basedir` and reporter overrides in `ThreadScopedProperties` overlay (never `base()`), verified by parallel in-JVM partition test with zero property cross-talk. |
| **AC-5** | P0 | **Design change** | **Portable child classpath & `@argfile`:** Test classpath precedes `CodeSource`-resolved worker runtime JARs (skipping fallback JUnit/Jackson/SLF4J JARs when already on test CP); `@argfile` escapes `\` -> `\\` and `"` -> `\"` in `Charset.defaultCharset()`, verified with spaces, `#`, quotes, and Windows backslash paths. |
| **AC-6** | P0 | **Design change** | **In-process vs forked JUnit parity:** Both runners call shared `PartitionJUnitLauncher` (including parameterized `methodName(params)` selector support), producing identical `WorkerTestEventPayload` sequences across `@Test`, `@ParameterizedTest`, `@TestFactory`, dynamic filters, skips, and `CitrusExtension`. |
| **AC-7** | P0 | **Design change** | **Attempt + `connectionId` fencing:** `WorkerTestEventPayload` and `PartitionCompletedPayload` carry `attempt`; coordinator fences on `(ASSIGNED, activeConnectionId, attempt)` and sends `PARTITION_CANCEL` on lease expiry. Late attempt-1 events/outputs (even from a reconnected same-`workerId` worker) are dropped. |
| **AC-8** | P0 | **Design change** | **Output & SSE ownership boundaries + dual-lock hygiene:** `DistributedLauncher` filters `test.event`, `partition.completed`, and `partition.lost` by `submittedPartitionIds`, and serializes host output commits across both threads and processes using in-JVM `ReentrantLock` + `FileChannel.lock()` on `<hostBaseDir>/target/.citrus-dist-outputs/commit.lock` (leaving zero dotfiles in `<hostBaseDir>` root). |
| **AC-9** | P0 | **Design change + Accepted limitation** | **Conflict-free staging & journaled rollback publication:** Cross-partition conflicts and Phase 1 staging failures leave `hostBaseDir` untouched; mid-commit `IOException` in Phase 2/3 replays the LIFO undo journal (`DELETE_CREATED_FILE`, `RESTORE_BACKUP`, `DELETE_CREATED_DIR`) under `commit.lock` so pre-existing files and newly created paths roll back cleanly (verified by injected mid-commit `IOException` test; uncatchable `SIGKILL` mid-rename documented as OS limitation). |
| **AC-10** | P0 | **Design change + Accepted limitation** | **Differential output packaging & empty `target/` immunity:** Workers pre-create `<workspaceDir>/target`, hash materialized regular input files under `outputPaths` pre-test, and export only new or content-modified regular files (returning `Optional.empty()` when an output dir has zero modified regular files); input deletions are not propagated. |
| **AC-11** | P1 | **Design change** | **Wire compatibility & capability gate:** Legacy JSON payloads deserialize with safe defaults; workers advertise `citrus.workspace.v1=true` and coordinator requires it on workspace/forked partitions. |
| **AC-12** | P1 | **Design change** | **Archive, path & `AF_UNIX` socket-length hardening:** Rejects `..`, absolute/UNC/drive/ADS/reserved-device paths, symlinks on pack & unpack (`NOFOLLOW_LINKS` prefix walk), and ZIP bombs (`>50,000` entries or `>2 GiB (as built)` uncompressed); POSIX child `-Djava.io.tmpdir` uses `/tmp/cdtmp-<rand>` (`<= 24` bytes) so `AF_UNIX` sockets never exceed 104-byte `sun_path`. |
| **AC-13** | P1 | **Design change** | **Missing input policy:** Missing required inputs fail fast on host before coordinator submission; `?`-suffixed optional inputs skip cleanly when absent. |
| **AC-14** | P1 | **Design change + Accepted limitation** | **256 MiB cap & temp cleanup:** Pre-flight rejects blobs `>256 MiB` with descriptive path/size message; ZIP creation streams to disk without heap-buffering full archive; `try/finally` cleans up all `.tmp-*` files on I/O failure. |
| **AC-15** | P1 | **Design change** | **End-to-end SHA-256 & deterministic ZIPs:** Timestamp/order-invariant ZIPs verified across runs; corrupted cached blobs are evicted and re-downloaded once before failing hard. |
| **AC-16** | P1 | **Design change** | **Loopback handshake & crash detection:** 256-bit `SecureRandom` `HELLO <token>` handshake with `15s` accept / `5s` read / `1 MiB` line bounds; rogue socket rejected without starving valid child; `Runtime.getRuntime().halt(1)` / missing `RESULT` fails partition and fails `DistributedLauncher`. |
| **AC-17** | P1 | **Design change** | **Cancellation & descendant cleanup:** `PARTITION_CANCEL`, `abortConnection()`, `close()`, and shutdown hook kill `ProcessHandle.descendants()` + child JVM (`2s` SIGTERM -> `3s` SIGKILL), skip output upload on cancel/crash, and remove both `partitionRoot` and `partitionTmpDir`. |
| **AC-18** | P1 | **Already addressed** | **Retry & SSE idempotency:** Content-addressed blobs + SSE `afterSeq` resume + `pendingPartitions.remove(partitionId)` guarantee idempotent output staging across disconnects and duplicate events. |
| **AC-19** | P1 | **Design change** | **Single-winner 5-tier replacement, `none` clearing & fork rules:** Higher-priority tier replaces (never merges with) lower tiers (`CLI -D` > explicit `<systemPropertyVariables>` > `project.getProperties()` > `CITRUS_DIST_*` env > defaults); `-Dcitrus.test.distribution.inputs=none` and `-Dcitrus.test.distribution.inputs=` clear inherited paths to `[]` (`./none` escapes literal file `none`); intra-tier ancestor dedup (`src/test,src/test/resources` -> `["src/test"]`); `-Dcitrus.test.distribution.fork=false` overrides POM `fork=true` when `inputs`/`outputs` are empty while non-empty `inputs`/`outputs` always fork. |
| **AC-20** | P1 | **Design change** | **Zero custom Mojo XML:** All module configurations use standard Maven `<properties>`; all modified `pom.xml`s pass standard `./mvnw test` without the Maven extension loaded. |
| **AC-21** | P1 | **Design change + Accepted limitation** | **Boundary-safe `PathRewriter`:** Rewrites `toRealPath()` + `toAbsolutePath()`, `project.build.directory`, Windows slash variants, path lists, and `file://` URIs while leaving `/restdocs`, `/rest-api`, and `/other/...` untouched; encoded/external paths documented as unsupported. |
| **AC-22** | P1 | **Design change** | **Full 84-POM repository audit & wiring:** All 19 configured/annotated modules verified (`citrus-test-distribution-protocol` `<scope>test</scope>` added to `citrus-jbang-connector` and `citrus-quarkus-it`; exact `4 / 31` `@LocalOnly` methods in `JBangTestEngineTest`; `DemoApplicationTest` pinned `@LocalOnly`; `ExpectedCodeGenIT` updated). |
| **AC-23** | P1 | **Already addressed** | **Exact-once `@LocalOnly`:** Class- and method-level `@LocalOnly` produce disjoint `localUnits` and `remoteUnits`, verified on hybrid classes (`CommandParityTest` [1 local / 11 remote] and `JBangTestEngineTest` [4 local / 1 remote]). |
| **AC-24** | P1 | **Design change** | **Measured repo share & SLO gates:** `45 / 65` test-bearing POMs (`69.2%`) stay zero-fork (`0` child processes); forked empty-test startup/teardown `< 1,500 ms` p95; child heap capped at `-Xmx768m`. |
| **AC-25** | P1 | **Design change** | **End-to-end test matrix & rollout gates:** Automated unit + E2E tests covering `AC-1`–`AC-24`, plus passing `./mvnw verify` on all 5 distribution modules and representative modules across Categories A, B, C, and E against an isolated `/tmp`-CWD worker. |
