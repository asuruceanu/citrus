# Test Distribution Coordinator Terminology Implementation Plan

> **Execution:** One implementer carries out all tasks in a worktree from the Test Distribution repository default branch. Once the full build passes, one fresh reviewer checks the whole branch.

**Goal:** Replace every Test Distribution occurrence of `broker` / `Broker` / `BROKER` with `coordinator` / `Coordinator` / `COORDINATOR` across Java packages, types, methods, CLI commands/options, system properties, environment variables, default cache directory, JSON status/schema outputs, Web UI labels, and user manual documentation.

**Architecture:** Rename protocol, coordinator, worker, and launcher APIs in dependency order. Complete the JBang source rename before building the agent, which depends on JBang. Verify the renamed consumers together from source in a Maven reactor that includes their dependencies. Preserve the wire protocol and scheduling behavior.

**Tech Stack:** Java 21, Maven 3.9+, JUnit Jupiter, Vert.x, Aesh CLI (`citrus-jbang`), Commons CLI (`citrus-agent`), Jackson JSON Schema (`schema-generator`), AsciiDoc.

**Spec:** `docs/superpowers/specs/2026-11-24-test-distribution-coordinator-rename-design.md`

## Global Constraints

- Keep unrelated working-tree edits in the original checkout. Do **not** run `git commit` or `git checkout` during task steps unless explicitly requested by the user.
- Short option for `--coordinator-url` MUST be `-u` across `DistWorkerStart`, `DistStatus`, `DistSessions`, and `CitrusAgentOptions` (`-c` and `-C` are already reserved in `CitrusAppOptions` / `CitrusAgentOptions`).
- Preserve existing default port numbers (`8567` in `DistributionSettings.DEFAULT_COORDINATOR_URL`, `8765` in JBang `Dist*` commands, `8084` in `DistributionCoordinatorConfig` and `CitrusAgentConfiguration`).
- Do **not** keep deprecated `--broker-url`, `-b`, `citrus dist broker`, or `citrus.test.distribution.broker*` fallbacks.
- Do **not** touch non-distribution `broker` usages anywhere in Citrus (JMS, ActiveMQ, Artemis, Kafka, Camel MQTT `brokerUrl`, Knative Eventing `brokerUrl`) or historical benchmark logs in `docs/superpowers/metrics/`.

## Review Focus

1. Short option `-u` followed by a coordinator URL in `DistWorkerStart` and `CitrusAgentOptions` must parse as the coordinator URL and never be misclassified as a cache option value when adjacent to bare `--cache-cleanup` / `--no-cache-cleanup` (pinned in Tasks 1, 5, and 6).
2. `citrus dist status --json` must emit `"coordinatorUrl"` and plain `citrus dist status` must print `"Coordinator: <url> [UP]"` (pinned in Task 5).
3. `DistributionCoordinatorConfig.builder().build()` must default `cacheDir` to `~/.citrus/dist-coordinator-cache` and resolve `citrus.test.distribution.coordinator.minPartitionDurationMs` and `citrus.test.distribution.coordinator.maxWavesPerWorker` from `DistributionSettings` (pinned in Task 2).
4. Omitting `--coordinator-url` / `-u` on `CitrusAgentOptions` in worker mode when `citrus.test.distribution.coordinatorUrl` is set (including with a trailing `/`) must propagate the normalized URL through `DistributionWorkerBridge.buildWorkerConfig(...)`. The explicit CLI URL wins, followed by the system property and then `http://localhost:8084` (pinned in Task 6).
5. `CitrusAgentConfiguration` JSON schema generation (`CitrusSchemaGeneratorTest#verifyAgentConfigurationJson`) must match `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json` with `"coordinatorUrl"` in exact property order and updated `"Coordinator"` descriptions (pinned in Task 6).

---

### Task 1: Protocol Settings, Event Model, and CLI Cache Option Fixtures

**Files:**
- Create: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/CoordinatorEvent.java`
- Delete: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/model/BrokerEvent.java`
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/config/DistributionSettings.java`
- Test: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/config/DistributionSettingsAndPathRewriterTest.java`
- Test: `tools/citrus-test-distribution-protocol/src/test/java/org/citrusframework/distribution/config/WorkerCacheCliOptionsTest.java`

**Interfaces:**
- Consumes: none (leaf protocol module)
- Produces:
  - `org.citrusframework.distribution.model.CoordinatorEvent(long sequence, String type, String sessionId, String partitionId, String workerId, long timestamp, Map<String, Object> data)`
  - `DistributionSettings.COORDINATOR_URL_PROPERTY = "citrus.test.distribution.coordinatorUrl"`
  - `DistributionSettings.COORDINATOR_URL_ENV = "CITRUS_DIST_COORDINATOR_URL"`
  - `DistributionSettings.DEFAULT_COORDINATOR_URL = "http://localhost:8567"`
  - `DistributionSettings.getCoordinatorUrl()`
  - `DistributionSettings.COORDINATOR_MIN_PARTITION_DURATION_MS_PROPERTY = "citrus.test.distribution.coordinator.minPartitionDurationMs"`
  - `DistributionSettings.COORDINATOR_MIN_PARTITION_DURATION_MS_ENV = "CITRUS_DIST_COORDINATOR_MIN_PARTITION_DURATION_MS"`
  - `DistributionSettings.DEFAULT_COORDINATOR_MIN_PARTITION_DURATION_MS = 5_000L`
  - `DistributionSettings.getCoordinatorMinPartitionDurationMs()`
  - `DistributionSettings.COORDINATOR_MAX_WAVES_PER_WORKER_PROPERTY = "citrus.test.distribution.coordinator.maxWavesPerWorker"`
  - `DistributionSettings.COORDINATOR_MAX_WAVES_PER_WORKER_ENV = "CITRUS_DIST_COORDINATOR_MAX_WAVES_PER_WORKER"`
  - `DistributionSettings.DEFAULT_COORDINATOR_MAX_WAVES_PER_WORKER = 2`
  - `DistributionSettings.getCoordinatorMaxWavesPerWorker()`

- [x] **Step 1: Update protocol tests to expect `Coordinator` constants, `CoordinatorEvent`, and `-u` / `--coordinator-url`**

In `DistributionSettingsAndPathRewriterTest.java`, update any `BROKER_*` / `getBroker*` references and add assertions verifying `DistributionSettings.getCoordinatorUrl()`, `DistributionSettings.getCoordinatorMinPartitionDurationMs()`, and `DistributionSettings.getCoordinatorMaxWavesPerWorker()` read `COORDINATOR_URL_PROPERTY` (`"citrus.test.distribution.coordinatorUrl"`), `COORDINATOR_MIN_PARTITION_DURATION_MS_PROPERTY` (`"citrus.test.distribution.coordinator.minPartitionDurationMs"`), and `COORDINATOR_MAX_WAVES_PER_WORKER_PROPERTY` (`"citrus.test.distribution.coordinator.maxWavesPerWorker"`), and verify that a `CoordinatorEvent` created with the record constructor retains its JSON fields and round-trips.

In `WorkerCacheCliOptionsTest.java`, replace `"--broker-url"` / `"-b"` in test value-option sets and argument fixtures with `"--coordinator-url"` / `"-u"`, including an explicit case `WorkerCacheCliOptions.parse(List.of("--no-cache-cleanup", "-u", "http://coordinator:8765"), Set.of("--coordinator-url", "-u"))` verifying `remainingArgs` equals `List.of("-u", "http://coordinator:8765")` and `cacheCleanup` is `Boolean.FALSE`.

- [x] **Step 2: Run protocol tests to verify compilation fails on missing `Coordinator*` symbols**

Run: `./mvnw test -pl tools/citrus-test-distribution-protocol -am`
Expected: FAIL with compilation errors (`cannot find symbol CoordinatorEvent` / `COORDINATOR_URL_PROPERTY`).

- [x] **Step 3: Rename `BrokerEvent` to `CoordinatorEvent` and rename `BROKER_*` settings in `DistributionSettings.java`**

Replace `BrokerEvent.java` with `CoordinatorEvent.java` (preserving the exact record components and JSON field names). In `DistributionSettings.java`, rename the constants and getters to the exact names in **Produces**, and update line 382 Javadoc `"broker-managed sizing"` → `"coordinator-managed sizing"`.

- [x] **Step 4: Run protocol tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-protocol -am`
Expected: PASS (`BUILD SUCCESS`, 0 failures, and zero matches for `rg -i broker tools/citrus-test-distribution-protocol`).

---

### Task 2: Web UI Regression Check and Coordinator Server Package & Types

**Files:**
- Test: `tools/citrus-test-distribution-ui/src/test/java/org/citrusframework/distribution/ui/DistributionUiResourcesTest.java`
- Create (move from `org.citrusframework.distribution.broker`):
  - `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorServer.java`
  - `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorConfig.java`
  - `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorVerticle.java`
  - `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/SessionCoordinator.java`
  - `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/coordinator/CoordinatorEventStreamHub.java`
- Delete: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/broker/*.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/main/java/org/citrusframework/distribution/store/DistributionStore.java`
- Create (move from `org.citrusframework.distribution.broker` test package):
  - `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorServerTest.java`
  - `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/coordinator/DistributionCoordinatorUiTest.java`
  - `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/coordinator/SessionCoordinatorTest.java`
- Delete: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/broker/*.java`
- Modify: `tools/citrus-test-distribution-coordinator/src/test/java/org/citrusframework/distribution/store/JdbcDistributionStoreTest.java`

**Interfaces:**
- Consumes: `CoordinatorEvent` and `DistributionSettings.COORDINATOR_*` / `getCoordinator*()` from Task 1
- Produces:
  - Package `org.citrusframework.distribution.coordinator`
  - `DistributionCoordinatorServer(DistributionCoordinatorConfig config)` (`start()`, `close()`, `port()`, `baseUrl()`, `wsUrl()`, `store()`)
  - `DistributionCoordinatorConfig` (`builder()`, `fromSettings()`, default `Builder.cacheDir = Path.of(System.getProperty("user.home"), ".citrus", "dist-coordinator-cache")`, `MIN_PARTITION_DURATION_MS_PROPERTY = DistributionSettings.COORDINATOR_MIN_PARTITION_DURATION_MS_PROPERTY`, `MIN_PARTITION_DURATION_MS_ENV = DistributionSettings.COORDINATOR_MIN_PARTITION_DURATION_MS_ENV`, `MAX_WAVES_PER_WORKER_PROPERTY = DistributionSettings.COORDINATOR_MAX_WAVES_PER_WORKER_PROPERTY`, `MAX_WAVES_PER_WORKER_ENV = DistributionSettings.COORDINATOR_MAX_WAVES_PER_WORKER_ENV`)
  - `DistributionCoordinatorVerticle`, `SessionCoordinator`, `CoordinatorEventStreamHub`

- [x] **Step 1: Move and update coordinator tests first (including Review Focus config test)**

Move the three test classes from `org.citrusframework.distribution.broker` to `org.citrusframework.distribution.coordinator` as `DistributionCoordinatorServerTest`, `DistributionCoordinatorUiTest`, and `SessionCoordinatorTest`, updating imports/type names to `DistributionCoordinatorServer`, `DistributionCoordinatorConfig`, `SessionCoordinator`, `CoordinatorEventStreamHub`, and `CoordinatorEvent`.

In `DistributionCoordinatorServerTest` (or `SessionCoordinatorTest`), add a test verifying:
```java
assertThat(DistributionCoordinatorConfig.builder().build().cacheDir())
        .isEqualTo(Path.of(System.getProperty("user.home"), ".citrus", "dist-coordinator-cache"));
assertThat(DistributionCoordinatorConfig.MIN_PARTITION_DURATION_MS_PROPERTY)
        .isEqualTo("citrus.test.distribution.coordinator.minPartitionDurationMs");
assertThat(DistributionCoordinatorConfig.MAX_WAVES_PER_WORKER_PROPERTY)
        .isEqualTo("citrus.test.distribution.coordinator.maxWavesPerWorker");
```
Update `DistributionUiResourcesTest` to assert the token-entry controls are absent using their identifiers, such as `commitAuthToken`, rather than the retired label. Keep the assertion for the current authentication message and update comments in `JdbcDistributionStoreTest.java`. Set both sizing properties to non-default values and assert that the built configuration reads them.

- [ ] **Step 2: Run UI + coordinator reactor test to verify compilation fails on unmoved main classes**

Run: `./mvnw test -pl tools/citrus-test-distribution-coordinator -am`
Expected: FAIL with compilation errors on `org.citrusframework.distribution.coordinator.*` production classes and old `DistributionSettings.BROKER_*` references in `org.citrusframework.distribution.broker.*`.

- [x] **Step 3: Move coordinator main classes to `org.citrusframework.distribution.coordinator`**

Move and rename the five existing classes, updating `CoordinatorEvent`, log and error strings, and `DistributionStore` Javadoc. The dashboard already uses “coordinator API” and relies on proxy authentication. Preserve the current dashboard.

- [x] **Step 4: Run UI + coordinator reactor tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-coordinator -am`
Expected: PASS (`BUILD SUCCESS`, 0 failures, and zero matches for `rg -i broker tools/citrus-test-distribution-ui tools/citrus-test-distribution-coordinator`).

---

### Task 3: Worker Config, Blob Client, and Partition Runners

**Files:**
- Create: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/CoordinatorBlobClient.java`
- Delete: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/BrokerBlobClient.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/DistributionWorkerConfig.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/DistributionWorkerClient.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/IsolatedPartitionRunner.java`
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/ForkedPartitionRunner.java`
- Create: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/CoordinatorBlobClientCacheTest.java`
- Delete: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/BrokerBlobClientCacheTest.java`
- Modify: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/ForkedPartitionRunnerTest.java`
- Modify: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/DistributionWorkerClientTest.java`

**Interfaces:**
- Consumes: `DistributionSettings.DEFAULT_COORDINATOR_URL`, `DistributionSettings.getCoordinatorUrl()`, `DistributionCoordinatorServer`, `DistributionCoordinatorConfig` from Tasks 1–2
- Produces:
  - `org.citrusframework.distribution.worker.CoordinatorBlobClient`
  - `DistributionWorkerConfig.coordinatorUrl()` and `DistributionWorkerConfig.Builder.coordinatorUrl(String coordinatorUrl)` (normalizing trailing slashes via `normalizeCoordinatorBaseUrl`)

- [x] **Step 1: Rename `BrokerBlobClientCacheTest` and update worker tests to use `CoordinatorBlobClient`, `coordinatorUrl`, and `DistributionCoordinatorServer`**

Replace `BrokerBlobClientCacheTest.java` with `CoordinatorBlobClientCacheTest.java`, and update `ForkedPartitionRunnerTest.java` and `DistributionWorkerClientTest.java` to reference `CoordinatorBlobClient`, `DistributionWorkerConfig.Builder#coordinatorUrl(...)`, `DistributionWorkerConfig#coordinatorUrl()`, `DistributionCoordinatorServer`, and `DistributionCoordinatorConfig`.

- [ ] **Step 2: Run worker reactor tests to verify compilation fails on old `BrokerBlobClient` / `brokerUrl` methods**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker -am`
Expected: FAIL with compilation errors in `citrus-test-distribution-worker`.

- [x] **Step 3: Rename `BrokerBlobClient` and update `DistributionWorkerConfig`, `DistributionWorkerClient`, `IsolatedPartitionRunner`, and `ForkedPartitionRunner`**

Replace `BrokerBlobClient.java` with `CoordinatorBlobClient.java` (updating exception/log strings from `"broker"` to `"coordinator"` and `config.brokerUrl()` to `config.coordinatorUrl()`). Update `DistributionWorkerConfig` (`coordinatorUrl` field, `coordinatorUrl()`, `Builder.coordinatorUrl(String)`, `normalizeCoordinatorBaseUrl(String)`, `fromSettings()` calling `DistributionSettings.getCoordinatorUrl()`), and update references/comments in `DistributionWorkerClient`, `IsolatedPartitionRunner`, and `ForkedPartitionRunner`.

- [x] **Step 4: Run worker reactor tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker -am`
Expected: PASS (`BUILD SUCCESS`, 0 failures, and zero matches for `rg -i broker tools/citrus-test-distribution-worker`).

---

### Task 4: JUnit Platform Launcher & Maven Lifecycle Extension

**Files:**
- Create: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/CoordinatorHttpClient.java`
- Delete: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/BrokerHttpClient.java`
- Modify: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/DistributedLauncher.java`
- Modify: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/DistributionLauncherInterceptor.java`
- Modify: `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/DistributedLauncherTest.java`
- Modify: `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/DistributionPlacementFollowupTest.java`
- Modify: `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/IndependentJupiterEnginePlacementTest.java`
- Modify: `tools/maven/citrus-test-distribution-maven-extension/src/main/java/org/citrusframework/mvn/distribution/TestDistributionLifecycleParticipant.java`
- Modify: `tools/maven/citrus-test-distribution-maven-extension/src/test/java/org/citrusframework/mvn/distribution/TestDistributionMavenExtensionTest.java`

**Interfaces:**
- Consumes: `CoordinatorEvent`, `DistributionSettings.COORDINATOR_URL_PROPERTY` / `getCoordinatorUrl()`, `DistributionCoordinatorServer`, `DistributionCoordinatorConfig`, `DistributionWorkerConfig#coordinatorUrl` from Tasks 1–3
- Produces:
  - `org.citrusframework.distribution.junit.CoordinatorHttpClient`

- [x] **Step 1: Update JUnit launcher and Maven extension unit tests**

Update `DistributedLauncherTest.java`, `DistributionPlacementFollowupTest.java`, `IndependentJupiterEnginePlacementTest.java`, and `TestDistributionMavenExtensionTest.java`:
- Replace `DistributionBrokerServer` / `DistributionBrokerConfig` / `BrokerHttpClient` / `BROKER_URL_PROPERTY` / `.brokerUrl(...)` with `DistributionCoordinatorServer` / `DistributionCoordinatorConfig` / `CoordinatorHttpClient` / `COORDINATOR_URL_PROPERTY` / `.coordinatorUrl(...)`.
- In `DistributedLauncherTest.java`, rename `"citrus.test.distribution.broker.url"` fixture key to `DistributionSettings.COORDINATOR_URL_PROPERTY` and disambiguate `EXECUTED_IN_COORDINATOR` / `shouldStayOnCoordinator` to `EXECUTED_ON_LAUNCHER_HOST` / `shouldStayOnLauncherHost` so "coordinator" exclusively means the server.

- [ ] **Step 2: Run JUnit + Maven extension reactor tests to verify compilation fails on `CoordinatorHttpClient` / old `Broker*` calls**

Run: `./mvnw test -pl tools/maven/citrus-test-distribution-maven-extension -am`
Expected: FAIL with compilation errors in `citrus-test-distribution` and `citrus-test-distribution-maven-extension`.

- [x] **Step 3: Rename `BrokerHttpClient` to `CoordinatorHttpClient` and update `DistributedLauncher`, `DistributionLauncherInterceptor`, and `TestDistributionLifecycleParticipant`**

Replace `BrokerHttpClient.java` with `CoordinatorHttpClient.java` (using `CoordinatorEvent` and updating Javadoc/exception strings from "broker" to "coordinator", and calling the client side "JUnit launcher" rather than "coordinator").

Apply targeted edits to `DistributedLauncher.java` and `DistributionLauncherInterceptor.java`:
- `BrokerHttpClient` → `CoordinatorHttpClient`, `BrokerEvent` → `CoordinatorEvent`, `handleBrokerEvent` → `handleCoordinatorEvent`, `DistributionSettings.getBrokerUrl()` → `DistributionSettings.getCoordinatorUrl()`, `brokerUrl` → `coordinatorUrl`, and log/exception messages `"broker"` → `"coordinator"`.
- In `DistributionLauncherInterceptor.java`, update the comment that loosely calls the JUnit `LauncherInterceptor` "coordinator" to "launcher".

In `TestDistributionLifecycleParticipant.java`, update `BrokerHttpClient` → `CoordinatorHttpClient`, `activeBrokerClient` → `activeCoordinatorClient`, `DistributionSettings.getBrokerUrl()` → `DistributionSettings.getCoordinatorUrl()`, `brokerUrl` → `coordinatorUrl`, and log/exception strings `"broker"` → `"coordinator"`.

- [x] **Step 4: Run JUnit + Maven extension reactor tests to verify they pass**

Run: `./mvnw test -pl tools/maven/citrus-test-distribution-maven-extension -am`
Expected: PASS (`BUILD SUCCESS`, 0 failures, and zero matches for `rg -i broker tools/citrus-test-distribution tools/maven/citrus-test-distribution-maven-extension`).

---

### Task 5: JBang `citrus dist` CLI and User Manual

**Files:**
- Create: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistCoordinator.java`
- Create: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistCoordinatorStart.java`
- Delete: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistBroker.java`
- Delete: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistBrokerStart.java`
- Modify: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/Dist.java`
- Modify: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistWorkerStart.java`
- Modify: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistStatus.java`
- Modify: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistSessions.java`
- Test: `tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/DistCommandTest.java`
- Modify: `src/manual/tools-test-distribution.adoc`

**Interfaces:**
- Consumes: `DistributionCoordinatorServer`, `DistributionCoordinatorConfig`, `DistributionWorkerConfig#coordinatorUrl` from Tasks 2–3
- Produces:
  - CLI command `citrus dist coordinator start` (`DistCoordinator`, `DistCoordinatorStart`)
  - CLI option `--coordinator-url` / `-u` on `dist worker start`, `dist status`, and `dist sessions`
  - `dist status --json` key `"coordinatorUrl"` and text line `"Coordinator: <url> [...]"`

- [x] **Step 1: Update `DistCommandTest.java` with `coordinator` commands, `-u` / `--coordinator-url`, and JSON/text status checks**

In `DistCommandTest.java`:
- Update all `dist broker` → `dist coordinator`, `--broker-url` → `--coordinator-url`, `DistributionBrokerServer` / `DistributionBrokerConfig` → `DistributionCoordinatorServer` / `DistributionCoordinatorConfig`, and `"Citrus Test Distribution Broker listening on"` → `"Citrus Test Distribution Coordinator listening on"`.
- In `shouldQueryLiveBrokerSessionsAndStatus` (rename to `shouldQueryLiveCoordinatorSessionsAndStatus`): assert `--json` output `.contains("\"coordinatorUrl\":\"" + coordinator.baseUrl() + "\"")`, and test plain `main.execute("dist", "status", "-u", coordinator.baseUrl())` asserting `.contains("Coordinator: " + coordinator.baseUrl() + " [UP]")`.
- Add `Arguments.of(List.of("--no-cache-cleanup", "-u", "http://coordinator:9000"), List.of("-u", "http://coordinator:9000", "--cache-cleanup=false"))` to `validWorkerStartArgs()`.

- [ ] **Step 2: Run `DistCommandTest` to verify compilation/test failure before implementation**

Run: `./mvnw verify -pl tools/jbang -am -Dtest=DistCommandTest -Dsurefire.failIfNoSpecifiedTests=false -Dskip.integration.tests=true`
Expected: FAIL with compilation errors in `tools/jbang`.

- [x] **Step 3: Implement `DistCoordinator`, `DistCoordinatorStart`, `Dist*` option updates, and `src/manual/tools-test-distribution.adoc`**

- Replace `DistBroker.java` with `DistCoordinator.java` (`@CommandDefinition(name = "coordinator", description = "Manage Citrus Test Distribution coordinator", generateHelp = true, groupCommands = {DistCoordinatorStart.class})`).
- Replace `DistBrokerStart.java` with `DistCoordinatorStart.java` (`@CommandDefinition(name = "start", description = "Start Citrus Test Distribution coordinator and web UI", generateHelp = true)`, shutdown thread `"citrus-dist-coordinator-shutdown"`, `"Citrus Test Distribution Coordinator listening on ..."`, `"Failed to start Citrus Test Distribution Coordinator: ..."`).
- Update `Dist.java` (`DistCoordinator.class`, description `"Manage Citrus Test Distribution coordinator, workers, and sessions"`).
- Update `DistWorkerStart.java`: `VALUE_OPTIONS = Set.of("--coordinator-url", "-u", "--worker-id", "-w", "--auth-token", "--capabilities", "-C", "--duration-seconds")`, `@Option(name = "coordinator-url", shortName = 'u', defaultValue = "http://localhost:8765", description = "Coordinator base URL")`, `String coordinatorUrl`, `.coordinatorUrl(coordinatorUrl)`, `config.coordinatorUrl()`, and command description `"Start a Citrus Test Distribution worker connected to a coordinator"`.
- Update `DistStatus.java`: `@Option(name = "coordinator-url", shortName = 'u', defaultValue = "http://localhost:8765", description = "Coordinator base URL")`, `summary.put("coordinatorUrl", base)`, `"Coordinator: " + base + ...`, `"Failed to reach coordinator at " + coordinatorUrl + ...`, and command description.
- Update `DistSessions.java`: `@Option(name = "coordinator-url", shortName = 'u', defaultValue = "http://localhost:8765", description = "Coordinator base URL")`, `"Coordinator returned HTTP ..."`, `"Failed to query sessions from " + coordinatorUrl + ...`, and command description.
- Update `src/manual/tools-test-distribution.adoc`: replace all `citrus dist broker start`, `--broker-url`, `-b`, `citrus.test.distribution.brokerUrl`, `CITRUS_DIST_BROKER_URL`, `citrus.test.distribution.broker.*`, `CITRUS_DIST_BROKER_*`, `~/.citrus/dist-broker-cache`, `DistributionBrokerServer`, and `DistributionBrokerConfig` references with their `coordinator` / `-u` equivalents, and describe the interceptor as part of the JUnit Platform launcher.

- [x] **Step 4: Verify the JBang CLI in the reactor**

Run the same focused verification command from Step 2 and require `DistCommandTest` to pass.

---

### Task 6: Citrus Agent API, Schema Control Fixture, and Agent Worker CLI

**Files:**
- Modify: `core/citrus-api/src/main/java/org/citrusframework/api/agent/CitrusAgentConfiguration.java`
- Modify: `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json`
- Modify: `tools/agent/src/main/java/org/citrusframework/agent/CitrusAgentOptions.java`
- Modify: `tools/agent/src/main/java/org/citrusframework/agent/DistributionWorkerBridge.java`
- Modify: `tools/agent/src/main/java/org/citrusframework/agent/CitrusAgentServer.java`
- Test: `tools/agent/src/test/java/org/citrusframework/agent/CitrusAgentWorkerOptionsTest.java`
- Test: `tools/schema-generator/src/test/java/org/citrusframework/dsl/schema/generator/CitrusSchemaGeneratorTest.java`

**Interfaces:**
- Consumes: `DistributionWorkerConfig#coordinatorUrl` and `DistributionSettings.getCoordinatorUrl()` from Tasks 1 & 3
- Produces:
  - `CitrusAgentConfiguration.getCoordinatorUrl()` / `CitrusAgentConfiguration.setCoordinatorUrl(String coordinatorUrl)` (default `"http://localhost:8084"`)
  - `CitrusAgentOptions`: `-u` / `--coordinator-url` (`"Test distribution coordinator URL"`)

- [x] **Step 1: Update `CitrusAgentWorkerOptionsTest` and `citrus-agent-configuration.json` control fixture (including Review Focus tests)**

In `CitrusAgentWorkerOptionsTest.java`, replace `--broker-url` / `getBrokerUrl()` / `brokerUrl()` with `--coordinator-url` / `getCoordinatorUrl()` / `coordinatorUrl()`, and add two tests:
1. `shouldParseShortCoordinatorUrlFlagAfterBareCacheCleanup`: `parse("--no-cache-cleanup", "-u", "http://coordinator:9000/")` asserts `configuration.getCacheCleanup()` is `false`, `configuration.getCoordinatorUrl()` is `"http://coordinator:9000/"`, and `DistributionWorkerBridge.buildWorkerConfig(configuration).coordinatorUrl()` is `"http://coordinator:9000"`.
2. `shouldHonorCoordinatorUrlSystemPropertyWhenCliFlagOmitted`: set `DistributionSettings.COORDINATOR_URL_PROPERTY` to `"http://coordinator-prop:9100/"`, parse `--mode worker` without a URL flag, and assert `DistributionWorkerBridge.buildWorkerConfig(configuration).coordinatorUrl()` is `"http://coordinator-prop:9100"`. Also cover an explicit CLI URL equal to the built-in default overriding the property and the `8084` fallback with the property unset. Restore the original property after each test.

In `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json`:
- Update `"authToken"` description to `"Shared authentication token for the Citrus Test Distribution Coordinator."`
- Remove `"brokerUrl"` and add `"coordinatorUrl":{"type":"string","title":"CoordinatorUrl","description":"Citrus Test Distribution Coordinator URL when running in worker mode.","default":"http://localhost:8084"}` in alphabetical key position right after `"configClass"`.

- [ ] **Step 2: Run agent and schema-generator tests to verify failure before implementation**

Run: `./mvnw verify -pl tools/agent -am -Dtest=CitrusAgentWorkerOptionsTest -Dsurefire.failIfNoSpecifiedTests=false -Dskip.integration.tests=true`
Expected: FAIL with compilation errors on `getCoordinatorUrl()` / `coordinatorUrl()`.

- [x] **Step 3: Update `CitrusAgentConfiguration`, `CitrusAgentOptions`, `DistributionWorkerBridge`, and `CitrusAgentServer`**

- In `CitrusAgentConfiguration.java`: rename `brokerUrl` → `coordinatorUrl`, `getBrokerUrl()` → `getCoordinatorUrl()`, `setBrokerUrl(String)` → `setCoordinatorUrl(String)`, track whether a URL was configured while preserving the public `8084` default, and update `@SchemaProperty` descriptions on `coordinatorUrl` (`"Citrus Test Distribution Coordinator URL when running in worker mode."`) and `authToken` (`"Shared authentication token for the Citrus Test Distribution Coordinator."`).
- In `CitrusAgentOptions.java`: replace `-b` / `broker-url` with `-u` / `coordinator-url` (`"Test distribution coordinator URL"`, calling `configuration.setCoordinatorUrl(...)`) and update `auth-token` description to `"Test distribution coordinator auth token"`.
- Resolve the effective worker URL in `DistributionWorkerBridge` as explicit agent configuration, then the coordinator URL system property, then `http://localhost:8084`. Use the worker builder to normalize the selected URL and log that effective URL at startup.

- [x] **Step 4: Run `CitrusAgentWorkerOptionsTest` and `CitrusSchemaGeneratorTest#verifyAgentConfigurationJson`**

Run: `./mvnw verify -pl tools/agent,tools/schema-generator -am -Dtest=CitrusAgentWorkerOptionsTest,CitrusSchemaGeneratorTest#verifyAgentConfigurationJson -Dsurefire.failIfNoSpecifiedTests=false -Dskip.integration.tests=true`
Expected: PASS (`BUILD SUCCESS`, 0 failures, and zero matches for `rg -i broker core/citrus-api/src/main/java/org/citrusframework/api/agent tools/agent tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json`).

---

### Execution record

The protocol regressions first failed to compile against the original names. The protocol suite passed after the rename. The separate pre-rename checks listed for Tasks 2–6 were consolidated into that protocol check and the affected-suite reactor run. The affected suites passed together in the 66-module Maven reactor, using the JBang and agent sources from this worktree. The passing run reported 1,164 tests, two skipped, and no failures or errors. It included the JBang connector and Docker Compose checks used to verify the worktree path workaround.

Unfiltered upstream verification encountered local path issues. The passing run used a dot-free path under the Docker-shared user directory, selected all test classes in the affected modules, and disabled upstream Failsafe integration tests and Javadoc generation.

### Final verification and independent review

- [x] **Build the affected modules and their dependencies from source**

Run Maven `verify` for `tools/maven/citrus-test-distribution-maven-extension,tools/agent,tools/jbang,tools/schema-generator` with `-am`. Include the full affected test suites and retain the build summary.

- [x] **Audit the renamed surfaces**

Run: `rg -i broker -g '!**/target/**' tools/citrus-test-distribution-protocol tools/citrus-test-distribution-ui tools/citrus-test-distribution-coordinator tools/citrus-test-distribution-worker tools/citrus-test-distribution tools/maven/citrus-test-distribution-maven-extension tools/agent core/citrus-api/src/main/java/org/citrusframework/api/agent tools/jbang/src/main/java/org/citrusframework/jbang/cli tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/DistCommandTest.java tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json src/manual/tools-test-distribution.adoc`
Expected: zero matches (exit code 1).

- [x] **Have one fresh reviewer check the whole branch once the build passes**

Provide the branch base, full diff, plan, and build evidence. Resolve actionable findings and rerun affected checks.

The reviewer found one regression-test gap. The protocol test now verifies the complete JSON payload for a partition assignment, including the partition and worker IDs. All 151 protocol tests passed again after this test change.
