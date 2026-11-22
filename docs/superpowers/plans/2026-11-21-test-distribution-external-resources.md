# External Test Resources for Citrus Test Distribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable Citrus Test Distribution workers to materialize declared input file-sets in an isolated working directory, rewrite host `basedir` properties, sync declared test outputs back to the host with conflict/rollback guarantees, and roll out external-resource and `@LocalOnly` configurations across all 84 Maven POMs in the Citrus repository.

**Architecture:** Extend the content-addressed SHA-256 blob protocol with `REGULAR_FILE`, `WorkspaceFileSet`, `PartitionPolicy`, `(connectionId, attempt)` lease fencing, and implicit `citrus.workspace.v1` capability routing. Execute zero-workspace partitions in-process via `IsolatedPartitionRunner` (with reporter path redirection and a read-only CWD write guard) and execute partitions with `inputs`/`outputs`/`fork=true` in a short-lived child JVM (`ForkedPartitionRunner` + `ForkedPartitionMain`) sharing `PartitionJUnitLauncher`, then commit verified outputs transactionally on the host via `HostOutputCommitter`.

**Tech Stack:** Java 21, Maven 3.9+, JUnit Platform (`junit-platform-launcher`), Jackson 3 (`tools.jackson.databind`), Vert.x 4 (coordinator HTTP/WebSocket/SSE), Apache License 2.0 headers.

**Spec:** `docs/superpowers/specs/2026-11-21-test-distribution-external-resources-design.md`

## Global Constraints

- **Java & Serialization:** Java 21+ only; use existing `DistributionJson` (`tools.jackson.databind.ObjectMapper`) and standard JDK APIs (`java.nio.file`, `java.net.http.HttpClient`, `ProcessBuilder`, `ProcessHandle`, `FileChannel`) with zero new third-party dependencies.
- **License Headers:** Every new Java and XML/POM file change must preserve or include the standard Apache 2.0 license header used across Citrus.
- **Blob & Extraction Safety:** Maximum compressed blob size `256 MiB` (`268_435_456L` bytes); extraction limits: max `50,000` entries per `DIRECTORY_ZIP`, max `2 GiB` (`2_147_483_648L` bytes) total uncompressed per archive, max `256 MiB` per uncompressed entry; reject all symlinks (`LinkOption.NOFOLLOW_LINKS`), `.` roots, `..` traversal, leading `/` or `\`, Windows drive prefixes (`C:...`), and UNC paths (`\\...`).
- **Short Temp Paths (`AF_UNIX` safety):** Worker partition root prefix must be `"cdist-"` (`Files.createTempDirectory("cdist-")`), and child JVM `-Djava.io.tmpdir` on POSIX when `/tmp` is writable must use `Files.createTempDirectory(Path.of("/tmp"), "cdtmp-")` (`<= 24` bytes) so Unix domain sockets stay well below macOS `104`-byte `sun_path`.
- **Worker Daemon CWD Safety:** `IsolatedPartitionRunner`'s CWD check (`citrus.test.distribution.worker.cwdCheck=FAIL|WARN|OFF`, default `FAIL`) is strictly read-only and must never delete or modify files in the worker's `user.dir`.
- **Single-Winner Precedence:** Configuration tiers (`1: CLI -D userProperties` > `2: Surefire/Failsafe <systemPropertyVariables>` > `3: Maven module <properties>` > `4: CITRUS_DIST_* env` > `5: default`) replace lower tiers rather than merging across tiers; case-insensitive `"none"` (or empty CLI `-Dkey=`) clears lower tiers, while `./none` escapes a literal file named `none`.

## Review Focus

1. **Spaces, backslashes, and `#` characters in `@argfile` classpath paths** (e.g., `/tmp/dir with spaces/repo #1/classes`): unquoted or unescaped `@argfile` tokens split `-cp` or start a `#` comment in the Java launcher; `ForkedPartitionRunner` must wrap the `-cp` argument in double quotes and escape `\` as `\\` and `"` as `\"` (pinned in Task 5).
2. **Sibling directory prefixes and URI-embedded paths during `PathRewriter` host rewriting** (e.g., `hostBaseDir=/work/app` vs `/work/app-other/x` and `jdbc:h2:file:/work/app/target/db`): `PathRewriter` must match `:` before `hostBaseDir` for URI schemes while requiring an end-of-segment boundary (`/`, `\`, whitespace, `:`, `;`, `"`, `'`, or end-of-string) so `/work/app-other` is untouched (pinned in Task 1).
3. **Unexpanded `${citrus.test.distribution.*}` placeholders or blank defaults in inherited `<systemPropertyVariables>`**: root `pom.xml` declares `<citrus.test.distribution.inputs>${citrus.test.distribution.inputs}</citrus.test.distribution.inputs>`; `SurefirePluginMutator` must treat blank values and literal `${...}` placeholders in `<systemPropertyVariables>` as unset so Tier 3 module `<properties>` and Tier 4 `CITRUS_DIST_*` env vars are not shadowed (pinned in Task 7).
4. **Parameterized / overloaded JUnit method selectors (`methodName(java.lang.String,int)`)**: `DiscoveredWorkExtractor` encodes parameter types in `WorkUnitSpec.methodName()`; `PartitionJUnitLauncher` must split `name(params)` into `DiscoverySelectors.selectMethod(testClass, name, params)` instead of looking up a literal method named `"name(params)"` (pinned in Task 4).
5. **Mid-commit `IOException` over pre-existing host output files**: when `<hostBaseDir>/target/docs/index.html` already exists from an earlier build and a multi-file output commit fails mid-way, `HostOutputCommitter` must restore the pre-existing file bytes from `tx-<uuid>/backup` and delete newly created files/directories in reverse order (pinned in Task 6).

---

### Task 1: Protocol Wire Models, Settings, Precedence Parser & Path Rewriter

**Files:**
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/cache/ArtifactKind.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/WorkspaceFileSet.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/PartitionPolicy.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/SubmitWorkRequest.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/PartitionAssignPayload.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/PartitionCompletedPayload.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/config/DistributionSettings.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/config/PathRewriter.java`
- Test: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/config/DistributionSettingsAndPathRewriterTest.java`

**Interfaces:**
- Produces:
  - `ArtifactKind.REGULAR_FILE`
  - `WorkspaceFileSet(String relativePath, ArtifactRef artifact)`
  - `PartitionPolicy { LPT, SINGLE }`
  - `SubmitWorkRequest(String submissionId, String moduleName, List<ArtifactRef> classpathArtifacts, Map<String, String> systemProperties, List<WorkUnitSpec> workUnits, int cachedBlobs, int uploadedBlobs, List<WorkspaceFileSet> inputArtifacts, List<String> outputPaths, boolean forkWorkspace, PartitionPolicy partitionPolicy)` plus backward-compatible 7-arg constructor defaulting `(List.of(), List.of(), false, PartitionPolicy.LPT)`
  - `PartitionAssignPayload(String partitionId, String sessionId, int attempt, List<ArtifactRef> classpathArtifacts, Map<String, String> systemProperties, List<WorkUnitSpec> workUnits, List<WorkspaceFileSet> inputArtifacts, List<String> outputPaths, boolean forkWorkspace)` plus backward-compatible 6-arg constructor defaulting `(List.of(), List.of(), false)`
  - `PartitionCompletedPayload(String partitionId, String sessionId, int attempt, boolean success, String errorMessage, List<WorkspaceFileSet> outputArtifacts)` plus backward-compatible 4-arg constructor defaulting `(1, ..., List.of())`
  - `DistributionSettings`:
    - `WORKSPACE_CAPABILITY_KEY = "citrus.workspace.v1"`
    - `WORKSPACE_TOKEN = "${citrus.dist.workspace}"`
    - `BASEDIR_PROPERTY = "citrus.test.distribution.basedir"`
    - `INPUTS_PROPERTY = "citrus.test.distribution.inputs"` / `INPUTS_ENV = "CITRUS_DIST_INPUTS"`
    - `OUTPUTS_PROPERTY = "citrus.test.distribution.outputs"` / `OUTPUTS_ENV = "CITRUS_DIST_OUTPUTS"`
    - `FORK_PROPERTY = "citrus.test.distribution.fork"` / `FORK_ENV = "CITRUS_DIST_FORK"`
    - `PARTITION_POLICY_PROPERTY = "citrus.test.distribution.partitionPolicy"` / `PARTITION_POLICY_ENV = "CITRUS_DIST_PARTITION_POLICY"`
    - `WORKER_FORK_MODE_PROPERTY = "citrus.test.distribution.worker.forkMode"` / `WORKER_FORK_MODE_ENV = "CITRUS_DIST_WORKER_FORK_MODE"`
    - `WORKER_FORK_JVM_ARGS_PROPERTY = "citrus.test.distribution.worker.forkJvmArgs"` / `WORKER_FORK_JVM_ARGS_ENV = "CITRUS_DIST_WORKER_FORK_JVM_ARGS"`
    - `WORKER_CWD_CHECK_PROPERTY = "citrus.test.distribution.worker.cwdCheck"` / `WORKER_CWD_CHECK_ENV = "CITRUS_DIST_WORKER_CWD_CHECK"`
    - `enum WorkerForkMode { AUTO, ALWAYS }`
    - `enum WorkerCwdCheckMode { FAIL, WARN, OFF }`
    - `record DeclaredPathSpec(String relativePath, boolean optional)`
    - `static List<DeclaredPathSpec> parseDeclaredPaths(String rawValue)` (handles `"none"` -> empty, `"./none"` -> `"none"`, leading `?` -> `optional=true`, `/` normalization, trailing `/` strip, and ancestor deduplication where `src/test/resources` subsumes `src/test/resources/foo.xml` and required beats optional on exact duplicates)
    - `static List<String> parseForkJvmArgs(String rawArgs)` (default `List.of("-Xmx768m", "-XX:ActiveProcessorCount=2")` when null/blank)
  - `PathRewriter`:
    - `static Map<String, String> rewriteHostProperties(Map<String, String> props, Path hostBaseDir)`
    - `static Map<String, String> expandWorkspaceProperties(Map<String, String> props, Path workspaceDir)`

- [ ] **Step 1: Write the failing unit tests in `DistributionSettingsAndPathRewriterTest.java`**

```java
@Test
void shouldParseDeclaredPathsWithNoneEscapeOptionalAndAncestorDedup() {
    assertThat(DistributionSettings.parseDeclaredPaths("NONE")).isEmpty();
    assertThat(DistributionSettings.parseDeclaredPaths("./none"))
            .containsExactly(new DistributionSettings.DeclaredPathSpec("none", false));

    var specs = DistributionSettings.parseDeclaredPaths(
            " ?src/test/resources/sub/a.xml , src\\test\\resources/ , ?pom.xml , pom.xml ");
    assertThat(specs).containsExactly(
            new DistributionSettings.DeclaredPathSpec("src/test/resources", false),
            new DistributionSettings.DeclaredPathSpec("pom.xml", false));
}

@Test
void shouldDeserializeLegacyPayloadsWithSafeDefaults() {
    String legacySubmit = """
            {"submissionId":"s1","moduleName":"m1","classpathArtifacts":[],"systemProperties":{},"workUnits":[],"cachedBlobs":0,"uploadedBlobs":0}
            """;
    SubmitWorkRequest req = DistributionJson.fromJson(legacySubmit, SubmitWorkRequest.class);
    assertThat(req.inputArtifacts()).isEmpty();
    assertThat(req.outputPaths()).isEmpty();
    assertThat(req.forkWorkspace()).isFalse();
    assertThat(req.partitionPolicy()).isEqualTo(PartitionPolicy.LPT);
}

@Test
void shouldRewritePrefixedUrisWithoutCorruptingSiblingDirectoryNames() {
    Path hostBase = Path.of("/work/app");
    Map<String, String> props = Map.of(
            "dbUrl", "jdbc:h2:file:/work/app/target/db",
            "sibling", "/work/app-other/config.xml",
            "pathList", "/work/app/a:/work/app-2/b;/work/app");
    Map<String, String> rewritten = PathRewriter.rewriteHostProperties(props, hostBase);
    assertThat(rewritten.get("dbUrl")).isEqualTo("jdbc:h2:file:${citrus.dist.workspace}/target/db");
    assertThat(rewritten.get("sibling")).isEqualTo("/work/app-other/config.xml");
    assertThat(rewritten.get("pathList"))
            .isEqualTo("${citrus.dist.workspace}/a:/work/app-2/b;${citrus.dist.workspace}");

    Map<String, String> expanded = PathRewriter.expandWorkspaceProperties(rewritten, Path.of("/tmp/cdist-1/workspace"));
    assertThat(expanded.get("dbUrl")).isEqualTo("jdbc:h2:file:/tmp/cdist-1/workspace/target/db");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -Dtest=DistributionSettingsAndPathRewriterTest -pl tools/citrus-test-distribution-protocol`
Expected: FAIL with compilation errors (`WorkspaceFileSet`, `PartitionPolicy`, `PathRewriter`, `parseDeclaredPaths` not found).

- [ ] **Step 3: Implement wire models, `DistributionSettings` extensions, and `PathRewriter`**

Implement all types and methods in the Interfaces block. In `SubmitWorkRequest`, `PartitionAssignPayload`, and `PartitionCompletedPayload`, normalize `null` lists to `List.of()` and `null` `partitionPolicy` to `PartitionPolicy.LPT` in compact canonical constructors (`attempt <= 0` normalized to `1` in `PartitionCompletedPayload`). In `PathRewriter.rewriteHostProperties`, skip rewriting if `hostBaseDir.getNameCount() == 0` (filesystem root `/`), match both native separator and `/` form of `hostBaseDir`, require start-of-string or preceding char in `{' ', '\t', '=', ':', ';', '"', '\''}`, and require end-of-string or following char in `{'/', '\\', ' ', '\t', ':', ';', '"', '\''}`.

- [ ] **Step 4: Run protocol unit tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-protocol`
Expected: PASS (`DistributionSettingsAndPathRewriterTest`, `ClasspathPackagerTest`, `CapabilityMatcherTest`).

- [ ] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution-protocol
git commit -m "feat(distribution-protocol): add workspace wire models, settings parser, and path rewriter"
```

---

### Task 2: Streaming Workspace Packager, Symlink/ZIP-Bomb Hardening & Blob Cache Integrity

**Files:**
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/cache/ClasspathPackager.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/cache/BlobCacheDirectory.java`
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/cache/WorkspacePackager.java`
- Modify: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/cache/ClasspathPackagerTest.java`
- Create: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/cache/WorkspacePackagerTest.java`

**Interfaces:**
- Consumes: `ArtifactKind`, `ArtifactRef`, `WorkspaceFileSet` from Task 1.
- Produces:
  - `ClasspathPackager.MAX_BLOB_BYTES = 256L * 1024 * 1024` (`268_435_456L`)
  - `ClasspathPackager.createDeterministicZipToFile(Path sourceDir, Path targetZip)` -> `String sha256` (streams directly to disk via `DigestOutputStream` + `ZipOutputStream`, rejects symlinks via `Files.isSymbolicLink` / `LinkOption.NOFOLLOW_LINKS`, includes sorted directory entries `rel + "/"` so empty directories survive, uses fixed `EPOCH_TIME` with zero extra fields, and throws `IllegalArgumentException` if compressed size exceeds `MAX_BLOB_BYTES`)
  - `BlobCacheDirectory`:
    - `MAX_ZIP_ENTRIES = 50_000`, `MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES = 2L * 1024 * 1024 * 1024`, `MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES = 256L * 1024 * 1024`
    - `Path putFromFile(String expectedSha256, Path tempSourceFile)` (verifies SHA-256 while streaming/moving into `<cacheDir>/blobs/<prefix>/<sha256>`, enforces `MAX_BLOB_BYTES`)
    - `Path getVerifiedPath(String sha256)` (verifies cached blob SHA-256 on read; if corrupted, deletes blob and returns `null` so caller re-fetches/re-packages)
    - `void extractZipSafely(Path zipBlob, Path targetDir)` (enforces zip-slip containment, symlink entry rejection, and the three ZIP-bomb caps while streaming)
  - `WorkspacePackager`:
    - `record PackagedWorkspaceItem(WorkspaceFileSet fileSet, Path tempBlobFile, byte[] inlineBytes)` (or `BlobCacheDirectory`-backed staging via `packageToCache(BlobCacheDirectory cache, Path baseDir, String relativePath, boolean optional)`)
    - `static String validateRelativePath(String rawPath)`
    - `static Optional<WorkspaceFileSet> packageInputToCache(BlobCacheDirectory cache, Path baseDir, String rawRelativePath, boolean optional)` (throws `IllegalArgumentException` when `!Files.exists(target, LinkOption.NOFOLLOW_LINKS)` and `!optional`; returns `Optional.empty()` when missing and `optional`)
    - `static Map<String, String> snapshotBaselineHashes(Path workspaceDir, List<String> outputPaths)` (maps workspace-relative `/`-path -> SHA-256 of regular files currently under `outputPaths`)
    - `static Optional<WorkspaceFileSet> packageModifiedOutputToCache(BlobCacheDirectory cache, Path workspaceDir, String rawOutputPath, Map<String, String> baselineHashes)` (packages single modified/new `REGULAR_FILE`, or builds deterministic `DIRECTORY_ZIP` containing ONLY regular files under `rawOutputPath` that are absent from `baselineHashes` or have a changed SHA-256; returns `Optional.empty()` if zero new/modified regular files exist)
    - `static void materializeIntoDirectory(BlobCacheDirectory cache, WorkspaceFileSet fileSet, Path targetRoot)`
    - `record MaterializedEntry(String workspaceRelativePath, String sha256, Path extractedFile)`

- [ ] **Step 1: Write failing tests in `WorkspacePackagerTest.java` (and `ClasspathPackagerTest.java`)**

```java
@Test
void shouldRejectTraversalAbsoluteWindowsAndSymlinkInputs(@TempDir Path tempDir) throws Exception {
    BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
    Path baseDir = Files.createDirectories(tempDir.resolve("module"));
    assertThatThrownBy(() -> WorkspacePackager.validateRelativePath("../secret.txt"))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> WorkspacePackager.validateRelativePath("/etc/passwd"))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> WorkspacePackager.validateRelativePath("C:\\temp\\a.txt"))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> WorkspacePackager.validateRelativePath("."))
            .isInstanceOf(IllegalArgumentException.class);

    Path outside = Files.writeString(tempDir.resolve("outside.txt"), "secret");
    Files.createSymbolicLink(baseDir.resolve("link.txt"), outside);
    assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, baseDir, "link.txt", false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Symbolic link");
}

@Test
void shouldFailOnMissingRequiredInputAndSkipMissingOptionalInput(@TempDir Path tempDir) {
    BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
    Path baseDir = tempDir.resolve("module");
    assertThatThrownBy(() -> WorkspacePackager.packageInputToCache(cache, baseDir, "src/test/resources", false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("src/test/resources");
    assertThat(WorkspacePackager.packageInputToCache(cache, baseDir, "src/test/resources", true)).isEmpty();
}

@Test
void shouldPackageOnlyNewOrModifiedOutputFilesAndIgnoreEmptyTargetDir(@TempDir Path tempDir) throws Exception {
    BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
    Path ws = Files.createDirectories(tempDir.resolve("ws"));
    Files.createDirectories(ws.resolve("target/docs"));
    Files.writeString(ws.resolve("target/docs/unchanged.txt"), "v1");
    Files.writeString(ws.resolve("target/docs/modified.txt"), "v1");

    Map<String, String> baseline = WorkspacePackager.snapshotBaselineHashes(ws, List.of("target/docs"));
    assertThat(WorkspacePackager.packageModifiedOutputToCache(cache, ws, "target/docs", baseline)).isEmpty();

    Files.writeString(ws.resolve("target/docs/modified.txt"), "v2");
    Files.writeString(ws.resolve("target/docs/created.txt"), "new");

    Optional<WorkspaceFileSet> out = WorkspacePackager.packageModifiedOutputToCache(cache, ws, "target/docs", baseline);
    assertThat(out).isPresent();

    Path dest = Files.createDirectories(tempDir.resolve("dest"));
    WorkspacePackager.materializeIntoDirectory(cache, out.orElseThrow(), dest);
    assertThat(dest.resolve("target/docs/unchanged.txt")).doesNotExist();
    assertThat(Files.readString(dest.resolve("target/docs/modified.txt"))).isEqualTo("v2");
    assertThat(Files.readString(dest.resolve("target/docs/created.txt"))).isEqualTo("new");
}

@Test
void shouldEvictCorruptedCachedBlobAndEnforceZipEntryLimit(@TempDir Path tempDir) throws Exception {
    BlobCacheDirectory cache = new BlobCacheDirectory(tempDir.resolve("cache"));
    byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
    String sha = ClasspathPackager.sha256Hex(payload);
    Path blob = cache.put(sha, payload);
    Files.writeString(blob, "tampered");
    assertThat(cache.getVerifiedPath(sha)).isNull();
    assertThat(blob).doesNotExist();
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -Dtest=WorkspacePackagerTest -pl tools/citrus-test-distribution-protocol`
Expected: FAIL with compilation error (`WorkspacePackager` not found).

- [ ] **Step 3: Implement `WorkspacePackager` and harden `ClasspathPackager` + `BlobCacheDirectory`**

Implement streaming deterministic ZIP generation to a temporary file inside `<cacheDir>/tmp/` before atomic move into `<cacheDir>/blobs/<prefix>/<sha256>`, symlink rejection via `Files.walkFileTree` without `FOLLOW_LINKS` (`visitFile` checks `attrs.isSymbolicLink()` and `Files.isSymbolicLink`), `BlobCacheDirectory.extractZipSafely` with bounded `CountingInputStream` enforcing `MAX_ZIP_ENTRIES`, `MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES`, and `MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES`, and `getVerifiedPath(sha256)` streaming SHA-256 verification.

- [ ] **Step 4: Run all protocol tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-protocol`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution-protocol
git commit -m "feat(distribution-protocol): add streaming workspace packager and hardened blob extraction"
```

---

### Task 3: Coordinator `PartitionPolicy.SINGLE`, Capability Gating & `(connectionId, attempt)` Lease Fencing

**Files:**
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/broker/LptPartitionScheduler.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/broker/BrokerSessionCoordinator.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/broker/DistributionBrokerVerticle.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/broker/LptPartitionSchedulerTest.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/broker/DistributionBrokerServerTest.java`

**Interfaces:**
- Consumes: `SubmitWorkRequest`, `PartitionAssignPayload`, `PartitionCompletedPayload`, `PartitionPolicy`, `WorkspaceFileSet`, `DistributionSettings.WORKSPACE_CAPABILITY_KEY` from Task 1.
- Produces:
  - `LptPartitionScheduler.schedule(String sessionId, List<WorkUnitSpec> workUnits, Map<String, Long> historicalDurationsMs, int maxPartitionSize, long defaultDurationMs, PartitionPolicy policy)` (keeps 5-arg overload delegating to `PartitionPolicy.LPT`; when `policy == PartitionPolicy.SINGLE`, emits exactly `1` `PartitionRecord` containing all `workUnits` in deterministic order and the union of `requiredCapabilities`, throwing `IllegalArgumentException` if two `WorkUnitSpec`s specify differing values for the same capability key)
  - `BrokerSessionCoordinator`:
    - Adds `DistributionSettings.WORKSPACE_CAPABILITY_KEY = "true"` to each partition's `requiredCapabilities` whenever `!request.inputArtifacts().isEmpty() || !request.outputPaths().isEmpty() || request.forkWorkspace()`.
    - Tracks `String activeConnectionId` on `PartitionRuntime`, set on `dispatchPendingPartitions()`.
    - Fences `handleWorkerMessage` for `TEST_EVENT` and `PARTITION_COMPLETED`: drops message if `pr == null || pr.record.state() != PartitionState.ASSIGNED || !conn.connectionId.equals(pr.activeConnectionId)` (and for `PARTITION_COMPLETED`, `done.attempt() != pr.record.attempt()`).
    - In `sweepExpiredLeases()`, sends `PARTITION_CANCEL` envelope on the expired worker connection (if still open), clears `conn.activePartitionId = null`, clears `pr.activeConnectionId = null`, and transitions `conn.state = WorkerState.IDLE` before calling `requeueOrLosePartition`.
    - Publishes `"outputArtifacts", done.outputArtifacts()` inside `"partition.completed"` SSE `data` map when non-empty.
  - `DistributionBrokerVerticle`: catches `IllegalArgumentException` in `handleSubmitWork` and responds HTTP `400` JSON `{"error": e.getMessage()}`.

- [ ] **Step 1: Write failing tests in `LptPartitionSchedulerTest.java` and `DistributionBrokerServerTest.java`**

```java
// In LptPartitionSchedulerTest.java:
@Test
void shouldCoScheduleAllUnitsUnderSinglePolicyAndRejectConflictingCapabilities() {
    LptPartitionScheduler scheduler = new LptPartitionScheduler();
    List<WorkUnitSpec> units = IntStream.range(0, 25)
            .mapToObj(i -> new WorkUnitSpec("u" + i, "pkg.Test" + i, "m", null,
                    i % 2 == 0 ? Map.of("os", "linux") : Map.of("docker", "true")))
            .toList();
    List<PartitionRecord> single = scheduler.schedule("s1", units, Map.of(), 5, 1000L, PartitionPolicy.SINGLE);
    assertThat(single).hasSize(1);
    assertThat(single.getFirst().workUnits()).hasSize(25);
    assertThat(single.getFirst().requiredCapabilities())
            .containsEntry("os", "linux")
            .containsEntry("docker", "true");

    List<WorkUnitSpec> conflicting = List.of(
            new WorkUnitSpec("u1", "pkg.A", "m", null, Map.of("os", "linux")),
            new WorkUnitSpec("u2", "pkg.B", "m", null, Map.of("os", "windows")));
    assertThatThrownBy(() -> scheduler.schedule("s1", conflicting, Map.of(), 5, 1000L, PartitionPolicy.SINGLE))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("os");
}

// In DistributionBrokerServerTest.java:
@Test
void shouldGateWorkspacePartitionsOnCapabilityAndFenceStaleAttemptCompletions() {
    // 1. Register legacy worker WITHOUT citrus.workspace.v1 -> submit work with forkWorkspace=true -> stays PENDING
    // 2. Connect worker WITH citrus.workspace.v1=true -> receives PARTITION_ASSIGN (attempt=1)
    // 3. Force lease expiry -> coordinator sends PARTITION_CANCEL and reassigns attempt=2
    // 4. Send late PARTITION_COMPLETED(attempt=1, outputArtifacts=[stale]) -> ignored by coordinator
    // 5. Send valid PARTITION_COMPLETED(attempt=2, outputArtifacts=[fresh]) -> accepted and SSE emits fresh outputArtifacts
}
```

- [ ] **Step 2: Run coordinator tests to verify they fail**

Run: `./mvnw test -Dtest=LptPartitionSchedulerTest,DistributionBrokerServerTest -pl tools/citrus-test-distribution-coordinator`
Expected: FAIL.

- [ ] **Step 3: Implement `PartitionPolicy.SINGLE`, `citrus.workspace.v1` gating, and `(connectionId, attempt)` fencing**

Update `LptPartitionScheduler`, `BrokerSessionCoordinator`, and `DistributionBrokerVerticle` per the Interfaces block. Ensure `WorkerTestEventPayload` handling also checks `conn.connectionId.equals(pr.activeConnectionId)` so late test events from an expired attempt are dropped before `store.insertTestExecution` or `eventHub.publish`.

- [ ] **Step 4: Run all coordinator tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-coordinator`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution-coordinator
git commit -m "feat(distribution-coordinator): add SINGLE partition policy, workspace capability gate, and attempt fencing"
```

---

### Task 4: Shared `PartitionJUnitLauncher` & Hardened `IsolatedPartitionRunner`

**Files:**
- Create: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/PartitionJUnitLauncher.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/DistributionWorkerConfig.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/IsolatedPartitionRunner.java`
- Create: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/PartitionJUnitLauncherAndCwdGuardTest.java`

**Interfaces:**
- Consumes: `PartitionAssignPayload`, `WorkUnitSpec`, `WorkspaceFileSet`, `DistributionSettings` (`WorkerForkMode`, `WorkerCwdCheckMode`), `PathRewriter` from Tasks 1–2.
- Produces:
  - `PartitionJUnitLauncher`:
    - `record ExecutionSummary(boolean success, String errorMessage)`
    - `static ExecutionSummary executeWorkUnits(String partitionId, List<WorkUnitSpec> workUnits, ClassLoader testClassLoader, IsolatedPartitionRunner.CancellationToken cancellationToken, Consumer<WorkerTestEventPayload> eventSink)`
    - Handles `CitrusExtension` reset, `CitrusDynamicSourceFilter`, `junit.platform.launcher.interceptors.enabled=false`, and parses `methodName` with parameter types (`"myTest(java.lang.String,int)"` -> `DiscoverySelectors.selectMethod(testClass, "myTest", "java.lang.String,int")`, `"myTest()"` -> `DiscoverySelectors.selectMethod(testClass, "myTest", "")`, bare `"myTest"` -> `selectMethodCompatible`).
  - `DistributionWorkerConfig`:
    - Adds `WorkerForkMode forkMode()` (default `AUTO`), `List<String> forkJvmArgs()` (default `[-Xmx768m, -XX:ActiveProcessorCount=2]`), `WorkerCwdCheckMode cwdCheckMode()` (default `FAIL`), and includes `"citrus.workspace.v1", "true"` in default capabilities when capabilities map is initialized from `fromSettings()` / builder defaults.
  - `IsolatedPartitionRunner.PartitionExecutionResult(boolean success, String errorMessage, List<WorkspaceFileSet> outputArtifacts)` (with 2-arg convenience constructor defaulting `List.of()`).
  - `IsolatedPartitionRunner`:
    - Uses `Files.createTempDirectory("cdist-")` with subdirectories `extracted/` and `workspace/` (pre-creating `workspace/target`).
    - Expands `${citrus.dist.workspace}` to `workspace` and sets thread-scoped `basedir = workspace`, plus redirects `citrus.report.directory` (`<workspace>/target/citrus-reports` when unset, or `<workspace>/<rel>` when relative) and `citrus.message.trace.directory` (`<workspace>/target/citrus-logs/trace/messages` when unset, or `<workspace>/<rel>` when relative).
    - Records read-only shallow snapshot of `Path.of(System.getProperty("user.dir"))` `(name -> mtime, size)` before execution and compares after execution when `cwdCheckMode != OFF`: logs `WARN` on `WARN` or returns `PartitionExecutionResult(false, "Undeclared CWD write detected in worker user.dir ... Set citrus.test.distribution.fork=true or declare citrus.test.distribution.outputs", List.of())` on `FAIL`, **never** deleting or modifying any file in `user.dir`.

- [ ] **Step 1: Write failing tests in `PartitionJUnitLauncherAndCwdGuardTest.java`**

```java
@Test
void shouldExecuteParameterizedMethodSelectorWithParameterTypes() {
    List<WorkerTestEventPayload> events = new CopyOnWriteArrayList<>();
    WorkUnitSpec unit = new WorkUnitSpec("u1", SampleParameterizedFixture.class.getName(),
            "echoValue(java.lang.String)", null, Map.of());
    PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits(
            "p1", List.of(unit), getClass().getClassLoader(), () -> false, events::add);
    assertThat(summary.success()).isTrue();
    assertThat(events).anyMatch(e -> "FINISHED".equals(e.eventType()) && "SUCCESSFUL".equals(e.status()));
}

@Test
void shouldDetectUndeclaredWorkerCwdWriteWithoutDeletingExistingOrCreatedFiles(@TempDir Path tempDir) throws Exception {
    // Package a fixture test class into BlobCacheDirectory that writes a unique file into Path.of("").toAbsolutePath()
    // Run IsolatedPartitionRunner with cwdCheckMode = FAIL
    // Assert result.success() == false, result.errorMessage() mentions citrus.test.distribution.fork=true,
    // AND assert pre-existing files + the test-created file in user.dir were NOT deleted by IsolatedPartitionRunner
    // (clean up only the test's own unique temp filename in test finally block).
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -Dtest=PartitionJUnitLauncherAndCwdGuardTest -pl tools/citrus-test-distribution-worker`
Expected: FAIL with compilation error (`PartitionJUnitLauncher` not found).

- [ ] **Step 3: Implement `PartitionJUnitLauncher`, `DistributionWorkerConfig` updates, and `IsolatedPartitionRunner` hardening**

Extract JUnit execution out of `IsolatedPartitionRunner` into `PartitionJUnitLauncher` so zero JUnit discovery/listener code is duplicated. Update `DistributionWorkerConfig` (`fromSettings()` reads `WORKER_FORK_MODE_*`, `WORKER_FORK_JVM_ARGS_*`, `WORKER_CWD_CHECK_*` and merges `"citrus.workspace.v1", "true"` into default capabilities unless explicitly overridden to `"false"`). Add `basedir`/reporter thread-scoped overrides and the read-only CWD snapshot check in `IsolatedPartitionRunner`.

- [ ] **Step 4: Run worker tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution-worker
git commit -m "feat(distribution-worker): extract shared PartitionJUnitLauncher and add read-only CWD write guard"
```

---

### Task 5: `ForkedPartitionRunner`, `ForkedPartitionMain`, Differential Outputs & Process-Tree Lifecycle

**Files:**
- Create: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/ForkedPartitionMain.java`
- Create: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/ForkedPartitionRunner.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/DistributionWorkerClient.java`
- Modify: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/DistributionWorkerClientTest.java`
- Create: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/ForkedPartitionRunnerTest.java`

**Interfaces:**
- Consumes: `WorkspacePackager`, `BlobCacheDirectory`, `PathRewriter`, `PartitionJUnitLauncher`, `DistributionWorkerConfig`, `IsolatedPartitionRunner.PartitionExecutionResult` from Tasks 1–4.
- Produces:
  - `ForkedPartitionMain`:
    - `record ForkRequest(String partitionId, String sessionId, int attempt, String authToken, Map<String, String> systemProperties, List<WorkUnitSpec> workUnits)`
    - `public static void main(String[] args)`: reads `args[0]` (`ipc/request.json`), connects TCP socket to `127.0.0.1:Integer.parseInt(args[1])`, writes `"HELLO " + authToken + "\n"`, applies `systemProperties` (skipping `java.*` except `java.io.tmpdir` already set on CLI, `os.*`, `user.dir*`), runs `PartitionJUnitLauncher.executeWorkUnits(...)` streaming `"EVENT " + json + "\n"` (flushed), writes `"RESULT " + json + "\n"`, and returns cleanly.
  - `ForkedPartitionRunner`:
    - `PartitionExecutionResult execute(PartitionAssignPayload assignment, IsolatedPartitionRunner.CancellationToken cancellationToken, Consumer<WorkerTestEventPayload> eventSink)`
    - Materializes `classpathArtifacts` into `partitionRoot/extracted`, `inputArtifacts` into `partitionRoot/workspace`, pre-creates `partitionRoot/workspace/target`, allocates short POSIX `/tmp/cdtmp-` for `-Djava.io.tmpdir` (`<= 24` chars when `/tmp` is writable, else `partitionRoot/t`), snapshots `baselineHashes`, writes `ipc/classpath.argfile` with proper Java `@argfile` quoting (`-cp "<escaped joined paths>"`, appending `CodeSource` locations of `ForkedPartitionMain`, `DistributionJson`, `ObjectMapper`, `LauncherFactory`, and `LoggerFactory` only if their SHA/filename is absent from test classpath), enforces `15s` `HELLO <token>` accept/auth timeout, drains child `stdout`/`stderr` into a 50-line ring buffer, kills `process.descendants()` + `process` on cancellation/timeout/shutdown, and packages/uploads differential `outputPaths` (`POST /api/v1/cache/missing` + `PUT /api/v1/cache/blobs/:sha256`) only when `!cancellationToken.isCancellationRequested()`.
  - `DistributionWorkerClient`:
    - Routes to `ForkedPartitionRunner` when `config.forkMode() == WorkerForkMode.ALWAYS || assignment.forkWorkspace() || !assignment.inputArtifacts().isEmpty() || !assignment.outputPaths().isEmpty()`, else `IsolatedPartitionRunner`.
    - Sends `new PartitionCompletedPayload(assign.partitionId(), assign.sessionId(), assign.attempt(), result.success(), result.errorMessage(), result.outputArtifacts())`.
    - Calls `cancelActivePartition()` inside `abortConnection()` and `close()`.

- [ ] **Step 1: Write failing tests in `ForkedPartitionRunnerTest.java` and `DistributionWorkerClientTest.java`**

```java
@Test
void shouldMaterializeRelativeCwdInputsRewriteBasedirAndExportDifferentialOutputs(@TempDir Path tempDir) throws Exception {
    // Fixture reads new File("src/test/resources/greeting.txt"), asserts Path.of("").toRealPath() == Path.of(System.getProperty("basedir")).toRealPath(),
    // asserts System.getProperty("custom.dir") expanded from ${citrus.dist.workspace}/conf,
    // asserts System.getProperty("java.io.tmpdir").length() < 80,
    // and writes target/docs/report.html while leaving target/docs/preexisting.txt untouched.
    // Verify result.success() == true and result.outputArtifacts() contains ONLY target/docs/report.html.
}

@Test
void shouldLaunchChildJvmWhenWorkspaceAndClasspathContainSpacesAndHash(@TempDir Path tempDir) throws Exception {
    // Create extracted classpath dir containing spaces and '#' ("dir #1 with spaces"),
    // verify ForkedPartitionRunner @argfile escaping launches ForkedPartitionMain and passes test.
}

@Test
void shouldKillChildAndDescendantProcessesOnCancellationAndSkipOutputUpload(@TempDir Path tempDir) throws Exception {
    // Fixture spawns a sleeping grandchild process, writes partial file to target/docs/out.txt, and blocks;
    // trigger cancellationToken -> assert both child and grandchild PIDs terminate within 5s,
    // outputArtifacts is empty, and partitionRoot + partitionTmpDir are deleted.
}

@Test
void shouldReportCrashDiagnosticsWhenChildCallsSystemExit(@TempDir Path tempDir) throws Exception {
    // Fixture prints "FATAL BOOM" to stderr and calls System.exit(42);
    // assert result.success() == false and result.errorMessage() contains "42" and "FATAL BOOM".
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./mvnw test -Dtest=ForkedPartitionRunnerTest -pl tools/citrus-test-distribution-worker`
Expected: FAIL with compilation error (`ForkedPartitionRunner` not found).

- [ ] **Step 3: Implement `ForkedPartitionMain`, `ForkedPartitionRunner`, and `DistributionWorkerClient` routing/cancellation**

Implement `@argfile` quoting (`"` around `-cp` path list with `\` -> `\\` and `"` -> `\"`), `ServerSocket(0, 50, InetAddress.getLoopbackAddress())` accepting connections in a loop until a client sends matching `"HELLO " + authToken` within `15_000 ms` (immediately closing unauthenticated probes), `killProcessTree(Process)` killing `process.descendants()` before `process` (`destroy()`, `waitFor(2, SECONDS)`, `destroyForcibly()`), active partition registry cleaned by a single JVM `Runtime.getRuntime().addShutdownHook(...)`, and `DistributionWorkerClient` cancellation on `abortConnection()`/`close()`.

- [ ] **Step 4: Run all worker tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker`
Expected: PASS (`ForkedPartitionRunnerTest`, `DistributionWorkerClientTest`, `PartitionJUnitLauncherAndCwdGuardTest`).

- [ ] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution-worker
git commit -m "feat(distribution-worker): add ForkedPartitionRunner with CWD workspace, differential outputs, and process-tree cleanup"
```

---

### Task 6: Host `DistributedLauncher` Input Shipping, SSE Partition Scoping & Transactional Output Commit

**Files:**
- Modify: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/client/BrokerHttpClient.java`
- Create: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/launcher/HostOutputCommitter.java`
- Modify: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/launcher/DistributedLauncher.java`
- Create: `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/launcher/HostOutputCommitterTest.java`
- Modify: `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/launcher/DistributedLauncherTest.java`

**Interfaces:**
- Consumes: `WorkspacePackager`, `BlobCacheDirectory`, `PathRewriter`, `DistributionSettings`, `SubmitWorkRequest`, `WorkspaceFileSet`, `PartitionPolicy` from Tasks 1–2.
- Produces:
  - `BrokerHttpClient.downloadBlob(String sha256)` -> `byte[]` (`GET /api/v1/cache/blobs/:sha256`)
  - `HostOutputCommitter`:
    - `void recordPartitionOutputs(String partitionId, List<WorkspaceFileSet> outputArtifacts, BlobCacheDirectory cache)` (extracts into `<hostBaseDir>/target/.citrus-dist-outputs/tx-<uuid>/staged/<partitionId>/`, validates paths/symlinks, and checks `Map<String, OutputOrigin> seenFiles`; identical SHA-256 across partitions is idempotent, differing SHA-256 records a deterministic conflict error without touching `<hostBaseDir>`)
    - `Optional<String> conflictError()`
    - `void commitOrRollback()` (executes the 4-phase backup-and-rollback protocol under static `ConcurrentHashMap<Path, ReentrantLock>` + `FileChannel.lock()` on `<hostBaseDir>/target/.citrus-dist-outputs/commit.lock` using `tx-<uuid>/backup` and LIFO `Deque<UndoAction>`)
    - `void discardStaging()` (deletes `tx-<uuid>` cleanly when conflicts or runner errors abort commit)
  - `DistributedLauncher`:
    - Resolves `hostBaseDir` (`citrus.test.distribution.basedir` -> `basedir` -> `user.dir`).
    - Packages declared `inputs` via `DistributionSettings.parseDeclaredPaths` + `WorkspacePackager.packageInputToCache`, failing immediately before work submission when a required input is missing.
    - Computes `outputPaths`, `forkWorkspace` (`DistributionSettings.isForkEnabled() || !inputArtifacts.isEmpty() || !outputPaths.isEmpty()`), `partitionPolicy`, and rewrites system properties via `PathRewriter.rewriteHostProperties`.
    - Filters `test.event`, `partition.completed`, and `partition.lost` in `handleBrokerEvent` by `submittedPartitionIds.contains(event.partitionId())` so sibling modules sharing a reactor `sessionId` never cross-contaminate events or outputs.
    - Fails the test run if `partition.completed` reports `state == FAILED` (even if 0 JUnit `test.event`s arrived, e.g., CWD guard failure or child JVM crash) or if `HostOutputCommitter` reports a cross-partition output conflict.

- [ ] **Step 1: Write failing tests in `HostOutputCommitterTest.java` and `DistributedLauncherTest.java`**

```java
// In HostOutputCommitterTest.java:
@Test
void shouldAllowIdenticalSharedOutputsAndRejectConflictingPartitionOutputsWithoutTouchingHost(@TempDir Path tempDir) throws Exception {
    // Pre-populate hostBaseDir/target/docs/existing.html = "old"
    // Partition p1 produces target/docs/logo.png ("PNG") + target/docs/a.html ("A")
    // Partition p2 produces target/docs/logo.png ("PNG") + target/docs/a.html ("CONFLICT_B")
    // Assert committer.conflictError() mentions "target/docs/a.html", "p1", and "p2",
    // discardStaging() leaves hostBaseDir/target/docs/existing.html == "old" and does NOT create logo.png or a.html.
}

@Test
void shouldRestorePreExistingHostFilesWhenMidCommitWriteFails(@TempDir Path tempDir) throws Exception {
    // Pre-populate hostBaseDir/target/docs/first.html = "original"
    // Stage 2 output files: target/docs/first.html ("updated") and target/docs/sub/second.html ("new")
    // Inject failure on publishing second.html (e.g., pre-existing regular file at hostBaseDir/target/docs/sub blocking directory creation, or fault hook)
    // Assert commitOrRollback() throws IOException, restores target/docs/first.html == "original",
    // removes any newly created files/dirs, and deletes tx-<uuid>.
}

// In DistributedLauncherTest.java:
@Test
void shouldExecuteEndToEndWithDeclaredInputsOutputsBasedirRewriteAndSiblingSessionIsolation(@TempDir Path tempDir) throws Exception {
    // Start embedded DistributionBrokerServer + 2 DistributionWorkerClients
    // Configure module inputs=src/test/resources, outputs=target/citrus-docs
    // Emit a synthetic sibling-module event on the same sessionId with conflicting partitionId -> ignored
    // Verify remote test reads src/test/resources/data.txt from CWD, writes target/citrus-docs/snippet.adoc,
    // and host receives target/citrus-docs/snippet.adoc cleanly with zero untracked dotfiles in hostBaseDir root.
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./mvnw test -Dtest=HostOutputCommitterTest,DistributedLauncherTest -pl tools/citrus-test-distribution`
Expected: FAIL.

- [ ] **Step 3: Implement `BrokerHttpClient.downloadBlob`, `HostOutputCommitter`, and `DistributedLauncher` updates**

Implement the 4-phase commit in `HostOutputCommitter` (`ATOMIC_MOVE` with `COPY_ATTRIBUTES + REPLACE_EXISTING` fallback across filesystems, recording `DELETE_CREATED_FILE`, `RESTORE_BACKUP`, and `DELETE_CREATED_DIR` in a `Deque<UndoAction>` replayed in LIFO order on any `Throwable`). Update `DistributedLauncher` to batch input blob digests alongside classpath digests in `findMissingDigests`, filter all SSE events by `submittedPartitionIds`, gate output collection on `pendingPartitions.remove(event.partitionId())`, and commit outputs only when no conflict occurred.

- [ ] **Step 4: Run all launcher tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution
git commit -m "feat(distribution-launcher): add input shipping, SSE partition scoping, and transactional output committer"
```

---

### Task 7: Maven Extension 5-Tier Precedence & Root `pom.xml` Property Wiring

**Files:**
- Modify: `tools/maven/citrus-test-distribution-maven-extension/src/main/java/org/citrusframework/mvn/distribution/SurefirePluginMutator.java`
- Modify: `tools/maven/citrus-test-distribution-maven-extension/src/main/java/org/citrusframework/mvn/distribution/TestDistributionLifecycleParticipant.java`
- Modify: `tools/maven/citrus-test-distribution-maven-extension/src/test/java/org/citrusframework/mvn/distribution/TestDistributionMavenExtensionTest.java`
- Modify: `pom.xml`

**Interfaces:**
- Consumes: `DistributionSettings` property/env constants from Task 1.
- Produces:
  - `SurefirePluginMutator`:
    - Always injects `citrus.test.distribution.basedir = project.getBasedir().toPath().toAbsolutePath().normalize().toString()` when `project.getBasedir() != null` (unless overridden in CLI `userProperties`).
    - Resolves `citrus.test.distribution.inputs`, `citrus.test.distribution.outputs`, `citrus.test.distribution.fork`, and `citrus.test.distribution.partitionPolicy` using strict 5-tier single-winner replacement precedence:
      1. `cliUserProperties` (`MavenSession.getUserProperties()` — note: key presence with `""` or `"none"` clears lower tiers!)
      2. Explicit `<systemPropertyVariables>` in the plugin DOM (ignoring `null`, blank `""`, or unexpanded `"${citrus.test.distribution.*}"` placeholders; `"none"` clears lower tiers)
      3. `project.getProperties()` (ignoring `null` or blank `""`; `"none"` clears lower tiers)
      4. Environment variables (`CITRUS_DIST_INPUTS`, `CITRUS_DIST_OUTPUTS`, `CITRUS_DIST_FORK`, `CITRUS_DIST_PARTITION_POLICY`)
      5. Default (`""` / unset)
  - Root `pom.xml`:
    - Defines `<citrus.test.distribution.inputs/>`, `<citrus.test.distribution.outputs/>`, `<citrus.test.distribution.fork/>`, `<citrus.test.distribution.partitionPolicy/>` in `<properties>`.
    - Propagates `citrus.test.distribution.basedir` (`${project.basedir}`), `citrus.test.distribution.inputs`, `citrus.test.distribution.outputs`, `citrus.test.distribution.fork`, and `citrus.test.distribution.partitionPolicy` inside `<pluginManagement>` `<systemPropertyVariables>` of both `maven-surefire-plugin` and `maven-failsafe-plugin`.

- [ ] **Step 1: Write failing tests in `TestDistributionMavenExtensionTest.java`**

```java
@Test
void shouldResolveFiveTierSingleWinnerPrecedenceAndSupportNoneAndEmptyCliClear() {
    // 1. Module <properties> sets inputs="src/test/resources", CLI userProperties sets inputs="pom.xml"
    //    -> winner is "pom.xml" (replaces, never merges).
    // 2. Module <properties> sets inputs="src/test/resources", CLI userProperties sets inputs="" (or "NONE")
    //    -> resolved inputs in <systemPropertyVariables> is "none" / "" (cleared).
}

@Test
void shouldNotShadowModulePropertiesWithInheritedPlaceholderInSystemPropertyVariables() {
    // Configure plugin DOM with <systemPropertyVariables><citrus.test.distribution.inputs>${citrus.test.distribution.inputs}</citrus.test.distribution.inputs></systemPropertyVariables>
    // and project.getProperties() with citrus.test.distribution.inputs = "src/test/resources"
    // -> SurefirePluginMutator writes "src/test/resources" and injects citrus.test.distribution.basedir.
}
```

- [ ] **Step 2: Run the extension tests to verify they fail**

Run: `./mvnw test -Dtest=TestDistributionMavenExtensionTest -pl tools/maven/citrus-test-distribution-maven-extension`
Expected: FAIL.

- [ ] **Step 3: Implement 5-tier precedence in `SurefirePluginMutator` + `TestDistributionLifecycleParticipant` and update root `pom.xml`**

Pass `session.getUserProperties()` (distinct from `System.getProperties()`) and an environment lookup `Function<String, String>` (default `System::getenv`, injectable for unit tests) into `SurefirePluginMutator.configureProject(...)` while keeping existing method overloads for binary compatibility. Add the 4 empty `<properties>` defaults and 5 `<systemPropertyVariables>` entries to root `pom.xml` (`maven-surefire-plugin` and `maven-failsafe-plugin`).

- [ ] **Step 4: Run Maven extension tests to verify they pass**

Run: `./mvnw test -pl tools/maven/citrus-test-distribution-maven-extension`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pom.xml tools/maven/citrus-test-distribution-maven-extension
git commit -m "feat(distribution-maven): wire 5-tier workspace property precedence in extension and root pom"
```

---

### Task 8: Citrus Repository Rollout Across All 19 Target Modules & User Manual Documentation

**Files:**
- Modify: `endpoints/citrus-http/pom.xml`
- Modify: `endpoints/citrus-ws/pom.xml`
- Modify: `tools/restdocs/pom.xml`
- Modify: `tools/docs-generator/pom.xml`
- Modify: `tools/test-generator/pom.xml`
- Modify: `tools/test-api-generator/citrus-openapi-codegen/pom.xml`
- Modify: `tools/test-api-generator/citrus-openapi-codegen/src/test/java/org/citrusframework/openapi/generator/ExpectedCodeGenIT.java`
- Modify: `tools/test-api-generator/citrus-openapi-codegen-maven-plugin/pom.xml`
- Modify: `core/citrus-api/pom.xml`
- Modify: `core/citrus-base/pom.xml`
- Modify: `endpoints/citrus-ftp/pom.xml`
- Modify: `endpoints/citrus-camel/pom.xml`
- Modify: `endpoints/citrus-kafka/pom.xml`
- Modify: `endpoints/citrus-ssh/pom.xml`
- Modify: `connectors/citrus-playwright/pom.xml`
- Modify: `connectors/citrus-selenium/pom.xml`
- Modify: `tools/schema-generator/pom.xml`
- Modify: `tools/jbang/pom.xml`
- Modify: `tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/ListTest.java`
- Modify: `tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/RunTest.java`
- Modify: `tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/CommandParityTest.java`
- Modify: `connectors/citrus-jbang-connector/pom.xml`
- Modify: `connectors/citrus-jbang-connector/src/test/java/org/citrusframework/jbang/engine/JBangTestEngineTest.java`
- Modify: `runtime/citrus-quarkus/citrus-quarkus-it/pom.xml`
- Modify: `runtime/citrus-quarkus/citrus-quarkus-it/src/test/java/org/citrusframework/quarkus/it/DemoApplicationTest.java`
- Modify: `src/manual/tools-test-distribution.adoc`

**Interfaces:**
- Consumes: All protocol annotations (`@LocalOnly`), Maven properties (`citrus.test.distribution.inputs`, `citrus.test.distribution.outputs`, `citrus.test.distribution.fork`, `citrus.test.distribution.partitionPolicy`), and runtime workspace support from Tasks 1–7.
- Produces: Complete 84-POM repository rollout + updated AsciiDoc user manual.

- [ ] **Step 1: Update `ExpectedCodeGenIT.java` so generated `.java` lookup works with unpacked classpath archives**

In `tools/test-api-generator/citrus-openapi-codegen/src/test/java/org/citrusframework/openapi/generator/ExpectedCodeGenIT.java` (`testGeneratedFiles` and `testGeneratedSoapFiles`), resolve the `.java` path under `getAbsoluteTargetDirectoryPath("generated-test-sources")` using the package-relative class path (`packagePath + "/" + resourceFile.getName().replace(".class", ".java")`) rather than `resource.getFile().getAbsolutePath().replace("test-classes", "generated-test-sources")`.

- [ ] **Step 2: Configure Category A & Category B Maven `<properties>` across the 18 workspace/fork POMs**

Add module `<properties>` exactly matching Spec Section 5.6:
- Category A:
  - `endpoints/citrus-http/pom.xml`: `<citrus.test.distribution.inputs>src/test/resources</citrus.test.distribution.inputs>`
  - `endpoints/citrus-ws/pom.xml`: `<citrus.test.distribution.inputs>src/test/resources</citrus.test.distribution.inputs>`
  - `tools/restdocs/pom.xml`: `<citrus.test.distribution.inputs>src/test/resources</citrus.test.distribution.inputs>`, `<citrus.test.distribution.outputs>target/citrus-docs</citrus.test.distribution.outputs>`
  - `tools/docs-generator/pom.xml`: `<citrus.test.distribution.inputs>src/test/resources</citrus.test.distribution.inputs>`, `<citrus.test.distribution.outputs>target/docs</citrus.test.distribution.outputs>`, `<citrus.test.distribution.partitionPolicy>SINGLE</citrus.test.distribution.partitionPolicy>`
  - `tools/test-generator/pom.xml`: `<citrus.test.distribution.inputs>src/test/resources</citrus.test.distribution.inputs>`
  - `tools/test-api-generator/citrus-openapi-codegen/pom.xml`: `<citrus.test.distribution.inputs>target/generated-test-sources</citrus.test.distribution.inputs>`
  - `tools/test-api-generator/citrus-openapi-codegen-maven-plugin/pom.xml`: `<citrus.test.distribution.inputs>pom.xml,src/test/resources,target/test-classes/SpringMetaFileGeneratorTest</citrus.test.distribution.inputs>`
- Category B (`<citrus.test.distribution.fork>true</citrus.test.distribution.fork>`):
  - `core/citrus-api/pom.xml`, `core/citrus-base/pom.xml`, `endpoints/citrus-ftp/pom.xml`, `endpoints/citrus-camel/pom.xml`, `endpoints/citrus-kafka/pom.xml`, `endpoints/citrus-ssh/pom.xml`, `connectors/citrus-playwright/pom.xml`, `connectors/citrus-selenium/pom.xml`, `tools/schema-generator/pom.xml`, `tools/jbang/pom.xml`, `connectors/citrus-jbang-connector/pom.xml`.

- [ ] **Step 3: Add `<scope>test</scope>` protocol dependency and `@LocalOnly` annotations to Category C host-coupled tests**

- Add `org.citrusframework:citrus-test-distribution-protocol:${project.version}` (`<scope>test</scope>`) to `connectors/citrus-jbang-connector/pom.xml` and `runtime/citrus-quarkus/citrus-quarkus-it/pom.xml`.
- Annotate with `@LocalOnly`:
  - `tools/jbang`: `ListTest` (class), `RunTest` (class), `CommandParityTest#shouldListWithSortOption` (method).
  - `connectors/citrus-jbang-connector`: `JBangTestEngineTest#testRunFromWorkDirectory`, `#testRunDirectory`, `#testRunSource`, `#testRunNoMatch` (4 methods).
  - `runtime/citrus-quarkus/citrus-quarkus-it`: `DemoApplicationTest` (class).

- [ ] **Step 4: Document External Test Resources in `src/manual/tools-test-distribution.adoc`**

Add a dedicated `=== External Test Resources` section covering:
- Declaring `citrus.test.distribution.inputs`, `citrus.test.distribution.outputs`, optional `?` path prefix, and `"none"` override clearing (5-tier replacement precedence).
- Worker `forkMode` (`AUTO` vs `ALWAYS`), `citrus.test.distribution.fork`, `forkJvmArgs`, `cwdCheck` (`FAIL|WARN|OFF`), and `partitionPolicy` (`LPT` vs `SINGLE`).
- Automatic `basedir` / `${citrus.dist.workspace}` system property rewriting.
- Transactional host output commit, cross-partition conflict failure, and when to pin tests with `@LocalOnly`.

- [ ] **Step 5: Build & verify distribution modules and representative rollout modules (both standard Maven and distributed mode)**

Run:
```bash
./mvnw test-compile -pl endpoints/citrus-http,endpoints/citrus-ws,tools/restdocs,tools/docs-generator,tools/test-generator,tools/test-api-generator/citrus-openapi-codegen,tools/test-api-generator/citrus-openapi-codegen-maven-plugin,tools/jbang,connectors/citrus-jbang-connector,runtime/citrus-quarkus/citrus-quarkus-it -am
./mvnw verify -pl tools/citrus-test-distribution-protocol,tools/citrus-test-distribution-coordinator,tools/citrus-test-distribution-worker,tools/citrus-test-distribution,tools/maven/citrus-test-distribution-maven-extension
./mvnw verify -pl tools/docs-generator,tools/restdocs,tools/schema-generator,tools/test-api-generator/citrus-openapi-codegen,tools/jbang
```
Expected: BUILD SUCCESS with all unit and integration tests passing and zero untracked lock/staging files in `git status`.

- [ ] **Step 6: Commit**

```bash
git add endpoints core connectors runtime tools src/manual/tools-test-distribution.adoc
git commit -m "feat(distribution): roll out external test resources and @LocalOnly across Citrus modules"
```
