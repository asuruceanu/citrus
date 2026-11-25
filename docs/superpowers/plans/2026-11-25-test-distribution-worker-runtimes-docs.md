# Test Distribution Worker Run-times: Documentation and Configuration Consistency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Document both Citrus Test Distribution worker run-times completely and fix three small Citrus-native configuration inconsistencies so the documented behaviour matches the runtime.

**Architecture:** Add a validated `citrus.agent.mode` / `CITRUS_AGENT_MODE` setting in `core/citrus-api`; wire CLI validation plus coordinator URL and auth-token fallbacks into `tools/agent`; remove the hardcoded coordinator URL default in `tools/jbang`; then expand the AsciiDoc manual and `tools/agent/README.adoc`. Each Java change is covered by unit tests before the docs are written.

**Tech Stack:** Java 21+, Maven 3.9+, JUnit Jupiter, AssertJ, Aesh, Vert.x, AsciiDoc.

**Spec:** `docs/superpowers/specs/2026-11-25-test-distribution-worker-runtimes-docs-design.md`

## Global Constraints

- Java 21+, Maven 3.9+. Unit tests end in `Test.java`, integration tests in `IT.java`.
- Apache 2.0 license header on every new Java file.
- No third-party product names, branding, or example values in specs, plans, or documentation. Describe Citrus in Citrus terms.
- No new environment aliases. Use only existing `CITRUS_DIST_*`, `CITRUS_AGENT_*`, and `citrus.*` settings.
- `citrus.agent.mode` / `CITRUS_AGENT_MODE` accepts `standalone` and `worker`, case-insensitive and trimmed. Blank falls back to `standalone`. Any other value throws `IllegalArgumentException` naming the value and its source.
- CLI `-m` / `--mode` overrides `citrus.agent.mode` / `CITRUS_AGENT_MODE`.
- `CitrusAgentConfiguration.DEFAULT_COORDINATOR_URL` stays `"http://localhost:8084"`.
- Coordinator URL and cache settings resolve: JVM system property -> `CITRUS_DIST_*` environment variable -> runtime default.
- Pool ID, registration key, worker ID, and capabilities resolve: `CITRUS_DIST_WORKER_*` environment variable -> `citrus.test.distribution.worker.*` JVM system property -> runtime default.
- `citrus dist worker start` fallback coordinator URL is `http://localhost:8765`; the Coordinator default port is `8765`.
- Short option `-p` on `citrus-agent` is reserved for `--package`; agent pool ID is always `--pool`.
- `CitrusAgentConfiguration.setMode(String)` already carries `@SchemaProperty`; no schema-generator control JSON change is needed.
- Manual build verification: `./mvnw process-resources -Pbuild-manual -N`.

## Review Focus

These inputs and failure modes are implied by the spec but not fully covered by any single task's happy-path test. Each is pinned to a task below.

1. `CITRUS_AGENT_MODE=WORKER` (mixed case, surrounding whitespace) must still select worker mode — pinned in Task 1's `shouldParseModeCaseInsensitivelyAndTrim`.
2. An invalid mode from CLI, environment, or system property must fail startup with exit code 1 and one clear message — pinned in Task 1's `shouldRejectInvalidMode` and Task 2's `shouldRejectInvalidCliModeAtStartup`.
3. A blank `CITRUS_AGENT_MODE` must fall back to `standalone` — pinned in Task 1's `shouldFallBackToStandaloneForBlankMode`.
4. `CITRUS_DIST_COORDINATOR_URL` must win when CLI `-u` / `--coordinator-url` is absent — pinned in Task 2's `shouldResolveAgentCoordinatorUrlFromEnvironmentWhenCliUrlAbsent` and Task 3's `shouldResolveWorkerCoordinatorUrlFromPropertyWhenCliUrlAbsent`.
5. An unset `--auth-token` must preserve `citrus.test.distribution.authToken` / `CITRUS_DIST_AUTH_TOKEN` — pinned in Task 2's `shouldPreserveInheritedAuthTokenWhenCliTokenAbsent`.

---

### Task 1: Add validated `citrus.agent.mode` / `CITRUS_AGENT_MODE` in `core/citrus-api`

**Files:**
- Modify: `core/citrus-api/src/main/java/org/citrusframework/api/agent/CitrusAgentSettings.java`
- Modify: `core/citrus-api/src/main/java/org/citrusframework/api/agent/CitrusAgentConfiguration.java:208-246`
- Create: `core/citrus-api/src/test/java/org/citrusframework/api/agent/CitrusAgentSettingsTest.java`

**Interfaces:**
- Consumes: existing `CitrusSettings.getPropertyEnvOrDefault(String propKey, String envKey, String defaultValue)`.
- Produces (later tasks rely on these exact names and types):
  - `CitrusAgentSettings.MODE_PROPERTY` = `"citrus.agent.mode"`
  - `CitrusAgentSettings.MODE_ENV` = `"CITRUS_AGENT_MODE"`
  - `CitrusAgentSettings.MODE_DEFAULT` = `"standalone"`
  - `public static String CitrusAgentSettings.parseMode(String raw, String source)` — returns lowercase `standalone` / `worker`, throws `IllegalArgumentException` for anything else.
  - `public static String CitrusAgentSettings.getMode()` — resolves property -> env -> default via `parseMode`.
  - `CitrusAgentConfiguration.fromEnvVars(TestSourceProvider)` sets mode from `CitrusAgentSettings.getMode()`.

- [ ] **Step 1: Write the failing test file**

Create `core/citrus-api/src/test/java/org/citrusframework/api/agent/CitrusAgentSettingsTest.java` with the Apache 2.0 header and these tests:

```java
@ExtendWith(SystemStubsExtension.class)
class CitrusAgentSettingsTest {

    @Test
    void shouldParseModeCaseInsensitivelyAndTrim() {
        assertThat(CitrusAgentSettings.parseMode(" WORKER ", "test")).isEqualTo("worker");
        assertThat(CitrusAgentSettings.parseMode("Standalone", "test")).isEqualTo("standalone");
    }

    @Test
    void shouldFallBackToStandaloneForBlankMode() {
        assertThat(CitrusAgentSettings.parseMode(null, "test")).isEqualTo("standalone");
        assertThat(CitrusAgentSettings.parseMode("   ", "test")).isEqualTo("standalone");
    }

    @Test
    void shouldRejectInvalidMode() {
        assertThatThrownBy(() -> CitrusAgentSettings.parseMode("bogus", "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bogus")
                .hasMessageContaining("test");
    }

    @Test
    @SetEnvironmentVariable(key = "CITRUS_AGENT_MODE", value = " WORKER ")
    void shouldResolveModeFromEnvironmentIgnoringCase() {
        assertThat(CitrusAgentSettings.getMode()).isEqualTo("worker");
    }

    @Test
    @SetEnvironmentVariable(key = "CITRUS_AGENT_MODE", value = "worker")
    void shouldPopulateModeFromEnvironmentInAgentConfiguration() {
        TestSourceProvider provider = source -> null; // extends Function<String, TestSource>
        assertThat(CitrusAgentConfiguration.fromEnvVars(provider).getMode()).isEqualTo("worker");
    }
}
```

Imports: `uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension`, `uk.org.webcompere.systemstubs.jupiter.SetEnvironmentVariable`, `org.citrusframework.TestSourceProvider`. `system-stubs-jupiter` is already a test dependency of `core/citrus-api`; no pom change is needed.

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw test -Dtest=CitrusAgentSettingsTest -pl core/citrus-api`
Expected: FAIL with compilation errors (`cannot find symbol MODE_PROPERTY`, `parseMode`, `getMode`).

- [ ] **Step 3: Implement the setting**

In `CitrusAgentSettings.java`, add beside the other property/env/default constant pairs:

```java
@CitrusConfigProperty(description = "Execution mode for the agent (standalone or worker).", defaultValue = "standalone")
private static final String MODE_PROPERTY = AGENT_PROPERTY_PREFIX + "mode";
private static final String MODE_ENV = AGENT_ENV_PREFIX + "MODE";
private static final String MODE_DEFAULT = "standalone";
```

Add `public static String parseMode(String raw, String source)`: trim; blank/null returns `MODE_DEFAULT`; `standalone` or `worker` (case-insensitive) returns lowercase; otherwise throw `IllegalArgumentException("Unsupported agent mode '" + raw.trim() + "' for " + source + " - expected 'standalone' or 'worker'"`.

Add `public static String getMode()` returning `parseMode(CitrusSettings.getPropertyEnvOrDefault(MODE_PROPERTY, MODE_ENV, MODE_DEFAULT), MODE_PROPERTY + "/" + MODE_ENV)`.

In `CitrusAgentConfiguration.fromEnvVars(TestSourceProvider)`, add `configuration.setMode(CitrusAgentSettings.getMode());` next to the other `set...` calls before `return configuration;`.

- [ ] **Step 4: Run tests and verify they pass**

Run: `./mvnw test -pl core/citrus-api`
Expected: PASS, all module tests green.

- [ ] **Step 5: Install the updated module for later tasks**

Run: `./mvnw install -DskipTests -pl core/citrus-api -am`
Expected: BUILD SUCCESS, `core/citrus-api` installed to the local Maven repository.

- [ ] **Step 6: Commit**

```bash
git add core/citrus-api/src/main/java/org/citrusframework/api/agent/CitrusAgentSettings.java \
        core/citrus-api/src/main/java/org/citrusframework/api/agent/CitrusAgentConfiguration.java \
        core/citrus-api/src/test/java/org/citrusframework/api/agent/CitrusAgentSettingsTest.java
git commit -m "feat(agent): add validated citrus.agent.mode setting"
```

---

### Task 2: Wire mode validation, coordinator URL fallback, and auth-token fallback in `tools/agent`

**Files:**
- Modify: `tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/config/DistributionSettings.java`
- Modify: `tools/agent/src/main/java/org/citrusframework/agent/CitrusAgentOptions.java:51-60`
- Modify: `tools/agent/src/main/java/org/citrusframework/agent/DistributionWorkerBridge.java:49-68`
- Modify: `tools/agent/src/test/java/org/citrusframework/agent/CitrusAgentWorkerOptionsTest.java`

**Interfaces:**
- Consumes from Task 1: `CitrusAgentSettings.parseMode(String raw, String source)`. `CitrusAgentConfiguration.fromEnvVars(...)` now sets mode, so `CitrusAgentServer.parseOptions(...)` picks up `CITRUS_AGENT_MODE` / `-Dcitrus.agent.mode` automatically.
- Produces:
  - `DistributionSettings.resolveCoordinatorUrl(UnaryOperator<String> prop, UnaryOperator<String> env, String defaultValue)` and `DistributionSettings.resolveCoordinatorUrl(String defaultValue)` — consumed by Task 3.
  - `DistributionWorkerBridge.buildWorkerConfig(CitrusAgentConfiguration)` (existing signature, unchanged behaviour for callers).
  - `DistributionWorkerBridge.buildWorkerConfig(CitrusAgentConfiguration, UnaryOperator<String> env)` — package-private test seam; the one-argument form delegates with `System::getenv`.

- [ ] **Step 1: Write the failing tests**

Add these tests to `CitrusAgentWorkerOptionsTest.java`:

```java
@Test
void shouldRejectInvalidCliModeAtStartup() {
    assertThat(CitrusAgentServer.parseOptions(new String[] {"--mode", "bogus"}, System.err)).isEmpty();
}

@Test
void shouldResolveAgentCoordinatorUrlFromEnvironmentWhenCliUrlAbsent() {
    UnaryOperator<String> env = key -> DistributionSettings.COORDINATOR_URL_ENV.equals(key)
            ? "http://coordinator-env:9100"
            : null;
    var config = DistributionWorkerBridge.buildWorkerConfig(parse("--mode", "worker"), env);
    assertThat(config.coordinatorUrl()).isEqualTo("http://coordinator-env:9100");
    assertThat(DistributionWorkerBridge.buildWorkerConfig(
            parse("--mode", "worker", "--coordinator-url", "http://cli-url:9101"), env).coordinatorUrl())
            .isEqualTo("http://cli-url:9101");
}

@Test
@ResourceLock(Resources.SYSTEM_PROPERTIES)
void shouldPreserveInheritedAuthTokenWhenCliTokenAbsent() {
    String key = DistributionSettings.AUTH_TOKEN_PROPERTY;
    String previous = System.setProperty(key, "inherited-token");
    try {
        assertThat(DistributionWorkerBridge.buildWorkerConfig(parse("--mode", "worker")).authToken())
                .isEqualTo("inherited-token");
        assertThat(DistributionWorkerBridge.buildWorkerConfig(
                parse("--mode", "worker", "--auth-token", "cli-token")).authToken())
                .isEqualTo("cli-token");
    } finally {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }
}
```

`DistributionSettings.AUTH_TOKEN_PROPERTY` is `"citrus.test.distribution.authToken"`; `DistributionSettings.AUTH_TOKEN_ENV` is `"CITRUS_DIST_AUTH_TOKEN"`. Do not introduce new constants.

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw test -Dtest=CitrusAgentWorkerOptionsTest -pl tools/agent`
Expected: FAIL — `parseOptions` with `bogus` returns a configuration instead of empty; env URL resolves to `http://localhost:8084`; inherited token is empty.

- [ ] **Step 3: Add reusable coordinator URL resolution in `DistributionSettings`**

Add next to `getCoordinatorUrl()`:

```java
public static String resolveCoordinatorUrl(UnaryOperator<String> prop, UnaryOperator<String> env,
                                           String defaultValue) {
    String value = getString(prop, env, COORDINATOR_URL_PROPERTY, COORDINATOR_URL_ENV, defaultValue);
    return value == null || value.isBlank() ? defaultValue : value;
}

public static String resolveCoordinatorUrl(String defaultValue) {
    return resolveCoordinatorUrl(System::getProperty, System::getenv, defaultValue);
}
```

The private `getString(UnaryOperator<String>, UnaryOperator<String>, String, String, String)` overload already exists in this class. Keep `getCoordinatorUrl()` unchanged.

- [ ] **Step 4: Implement mode validation**

In `CitrusAgentOptions.java`, change the `-m` / `--mode` option body to call `CitrusAgentSettings.parseMode(value, "-m/--mode")` and `configuration.setMode(...)` with the result, keeping the existing `CitrusRuntimeException` for a missing value. Import `CitrusAgentSettings` from `org.citrusframework.api.agent`.

- [ ] **Step 5: Implement coordinator URL and auth-token fallbacks**

In `DistributionWorkerBridge.java`:
- Add `static DistributionWorkerConfig buildWorkerConfig(CitrusAgentConfiguration configuration, UnaryOperator<String> env)`.
- Make the existing one-argument method delegate to it with `System::getenv`.
- Replace `DistributionSettings.getString(DistributionSettings.COORDINATOR_URL_PROPERTY, null, CitrusAgentConfiguration.DEFAULT_COORDINATOR_URL)` with `DistributionSettings.resolveCoordinatorUrl(System::getProperty, env, CitrusAgentConfiguration.DEFAULT_COORDINATOR_URL)`.
- Keep `.coordinatorUrl(configuration.getCoordinatorUrl(fallbackUrl))`.
- Replace unconditional `.authToken(configuration.getAuthToken())` with a guarded call: only set the token when `StringUtils.hasText(configuration.getAuthToken())`, so an unset CLI token leaves `DistributionWorkerConfig.builder()`'s inherited value intact.

- [ ] **Step 6: Run tests and verify they pass**

Run: `./mvnw test -Dtest=CitrusAgentWorkerOptionsTest -pl tools/agent`
Expected: PASS. Then run `./mvnw test -pl tools/agent` — all module tests green.

- [ ] **Step 7: Commit**

```bash
git add tools/agent/src/main/java/org/citrusframework/agent/CitrusAgentOptions.java \
        tools/agent/src/main/java/org/citrusframework/agent/DistributionWorkerBridge.java \
        tools/agent/src/test/java/org/citrusframework/agent/CitrusAgentWorkerOptionsTest.java
git commit -m "fix(agent): validate mode and honor coordinator and auth token settings"
```

---

### Task 3: Honor coordinator URL settings in `citrus dist worker start`

**Files:**
- Modify: `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistWorkerStart.java:57,173-178`
- Modify: `tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/DistCommandTest.java`

**Interfaces:**
- Consumes: `DistributionSettings.COORDINATOR_URL_PROPERTY` and `DistributionSettings.COORDINATOR_URL_ENV` (existing).
- Produces:
  - `DistributionSettings.resolveCoordinatorUrl(String defaultValue)` — resolves system property -> `CITRUS_DIST_COORDINATOR_URL` -> `defaultValue`.
  - `DistWorkerStart.DEFAULT_COORDINATOR_URL = "http://localhost:8765"` (static final) used when no other setting is present.

- [ ] **Step 1: Write the failing test**

Add to `DistCommandTest.java`. The existing `bindWorkerStart` helper always injects `--coordinator-url`, so bind `CapturingWorkerStart` directly with empty args:

```java
@Test
@ResourceLock(Resources.SYSTEM_PROPERTIES)
void shouldResolveWorkerCoordinatorUrlFromPropertyWhenCliUrlAbsent() {
    String key = DistributionSettings.COORDINATOR_URL_PROPERTY;
    String previous = System.setProperty(key, "http://worker-prop:9200/");
    try {
        CapturingWorkerStart.captured = null;
        CommandResult result = AeshRuntimeRunner.builder()
                .command(CapturingWorkerStart.class)
                .args(new String[0])
                .execute();
        assertThat(result).isEqualTo(CommandResult.SUCCESS);
        assertThat(CapturingWorkerStart.captured).isNotNull();
        assertThat(CapturingWorkerStart.captured.coordinatorUrl()).isEqualTo("http://worker-prop:9200");
    } finally {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }
}
```

`CapturingWorkerStart`, `AeshRuntimeRunner`, `CommandResult`, and `DistributionSettings` are already imported or declared in `DistCommandTest.java`.

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw test -Dtest=DistCommandTest#shouldResolveWorkerCoordinatorUrlFromPropertyWhenCliUrlAbsent -pl tools/jbang`
Expected: FAIL — the built config URL is `http://localhost:8765` because Aesh injected the hardcoded default before the system property could be consulted.

- [ ] **Step 3: Remove the hardcoded Aesh default**

In `DistWorkerStart.java`, add `static final String DEFAULT_COORDINATOR_URL = "http://localhost:8765";` and change the option at line 57 to `@Option(name = "coordinator-url", shortName = 'u', description = "Coordinator base URL (default: http://localhost:8765)")` with no `defaultValue`. In `buildWorkerConfig()`, resolve:

```java
String effectiveUrl = (coordinatorUrl != null && !coordinatorUrl.isBlank())
        ? coordinatorUrl
        : DistributionSettings.resolveCoordinatorUrl(DEFAULT_COORDINATOR_URL);
```

and pass `.coordinatorUrl(effectiveUrl)` to the builder. Note `DistributionSettings` is a dependency of `tools/jbang`, so `tools/citrus-test-distribution-protocol` must be installed locally: run `./mvnw install -DskipTests -pl tools/citrus-test-distribution-protocol -am` before running the tests if the module was not already installed.

- [ ] **Step 4: Run tests and verify they pass**

Run: `./mvnw test -Dtest=DistCommandTest -pl tools/jbang`
Expected: PASS. Then `./mvnw test -pl tools/jbang` — all module tests green.

- [ ] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution-protocol/src/main/java/org/citrusframework/distribution/config/DistributionSettings.java \
        tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistWorkerStart.java \
        tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/DistCommandTest.java
git commit -m "fix(dist): honor coordinator URL settings in worker start"
```

---

### Task 4: Expand the worker documentation

**Files:**
- Modify: `src/manual/tools-test-distribution.adoc:455-541` and `src/manual/tools-test-distribution.adoc:752-822`
- Modify: `src/manual/agent.adoc:123-163` and append a worker mode section near the end
- Modify: `tools/agent/README.adoc`

**Interfaces:**
- Consumes: the exact settings and behaviour from Tasks 1–3 and the existing runtime behaviour recorded in the spec. No Java changes in this task.

- [ ] **Step 1: Rewrite `[[tools-test-distribution-workers]]`**

Replace the current short section in `src/manual/tools-test-distribution.adoc` with a complete operator reference containing, in order:
1. What a worker does and the two supported run-times: `citrus dist worker start` and `citrus-agent --mode worker`.
2. Quick-start shell snippets for both, with `--pool`, `--registration-key`, `--coordinator-url`, and `-u` / `--coordinator-url` as appropriate. Use `http://localhost:8765` as the Coordinator URL example. State that `citrus-agent`'s built-in fallback is `http://localhost:8084` and that `-u` / `--coordinator-url`, `citrus.test.distribution.coordinatorUrl`, or `CITRUS_DIST_COORDINATOR_URL` must be set when targeting a Coordinator on the default port `8765`.
3. A CLI option table for `citrus dist worker start`: `-u`/`--coordinator-url`, `-w`/`--worker-id`, `--auth-token`, `-p`/`--pool`, `-k`/`--registration-key`, `-C`/`--capabilities`, `--cache-dir`, `--[no-]cache-cleanup[=true|false]`, `--cache-free-space-buffer-size`, `--duration-seconds`.
4. A CLI option table for `citrus-agent` worker mode: `-P`/`--port`, `-m`/`--mode`, `-u`/`--coordinator-url`, `-t`/`--auth-token`, `--pool`, `-k`/`--registration-key`, `-C`/`--capabilities`, and the same cache options. Add a note that `-p` is reserved for `--package`, so `--pool` must be spelled out.
5. An environment variable / system property reference table with exact precedence text:
   - Coordinator URL and cache settings: JVM system property -> `CITRUS_DIST_*` -> runtime default.
   - Pool ID, registration key, worker ID, and capabilities: `CITRUS_DIST_WORKER_*` -> `citrus.test.distribution.worker.*` system property -> runtime default.
   - Include `citrus.agent.mode` / `CITRUS_AGENT_MODE` with values `standalone` (default) and `worker`.
6. A capabilities section listing auto-detected keys: `os`, `arch`, `jdk`, `cpuCores`, `memoryMb`, `citrus.workspace.v1`, `citrus.workspace.quarkus.v1`, plus the protocol marker `citrus-worker=true`; explain that `-C` / `CITRUS_DIST_WORKER_CAPABILITIES` entries are merged on top.
7. A local file cache section covering `--cache-dir` (default `${user.home}/.citrus-test-distribution-agent/file-cache`), cleanup, free-space buffer, exclusive `.lock` file, and LRU eviction.
8. A lifecycle section: hourly registration renewal, rejection codes and exit behaviour (`citrus dist worker start` exits nonzero; `citrus-agent` logs one warning and keeps its HTTP server running), and `SIGTERM` drain (30 seconds for `citrus dist worker start`, 15 seconds for the agent bridge).

- [ ] **Step 2: Extend `[[tools-test-distribution-external-resources-worker]]`**

Add a `citrus.test.distribution.coordinatorUrl` row at the top of the worker settings table with `CITRUS_DIST_COORDINATOR_URL` listed as its environment variable and `http://localhost:8765` as the worker default.

- [ ] **Step 3: Update `src/manual/agent.adoc`**

Add a `CITRUS_AGENT_MODE` row to the `.Environment variables` table at lines 123–163 (values `standalone` / `worker`, default `standalone`). Append a `[[agent-worker-mode]]` `== Test Distribution Worker Mode` section near the end of the file covering: `citrus-agent --mode worker`, `CITRUS_AGENT_MODE=worker` for containers, `-u` / `--coordinator-url`, `--pool` (with the `-p` note), `--registration-key`, `--auth-token`, and a cross-reference to `[[tools-test-distribution-workers]]`.

- [ ] **Step 4: Replace `tools/agent/README.adoc`**

Replace the stub with a short accurate README: what `citrus-agent` is (a Vert.x-based test runner server, not a Quarkus web application), how to start it in standalone and worker mode, and links to `src/manual/agent.adoc` and `src/manual/tools-test-distribution.adoc`.

- [ ] **Step 5: Verify the manual builds**

Run: `./mvnw process-resources -Pbuild-manual -N`
Expected: BUILD SUCCESS, no AsciiDoc errors.

- [ ] **Step 6: Commit**

```bash
git add src/manual/tools-test-distribution.adoc src/manual/agent.adoc tools/agent/README.adoc
git commit -m "docs: document test distribution worker run-times"
```
