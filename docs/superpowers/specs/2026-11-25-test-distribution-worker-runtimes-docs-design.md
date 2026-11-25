# Test Distribution Worker Run-times: Documentation and Configuration Consistency

Date: 2026-11-25
Status: Draft — awaiting approval

## Intent and why

Citrus Test Distribution has two supported ways to run a worker:

- `citrus dist worker start` — the lightweight JBang command.
- `citrus-agent --mode worker` — the `citrus-agent` server with worker mode enabled.

Today the user manual covers both in one short overview section, `src/manual/agent.adoc` does not mention `--mode worker` at all, and `tools/agent/README.adoc` is a five-line stub. Operators have no complete reference for CLI flags, environment variables, defaults, capability detection, local file cache behaviour, lifecycle, or container deployment.

A few small configuration inconsistencies also get in the way of running workers from containers, where command-line flags are awkward:

- `citrus-agent` can only select worker mode from the command line. There is no environment variable or system property for it.
- `citrus dist worker start` hardcodes `http://localhost:8765` as the Aesh option default, so `citrus.test.distribution.coordinatorUrl` and `CITRUS_DIST_COORDINATOR_URL` are silently ignored when `-u` / `--coordinator-url` is omitted.
- `DistributionWorkerBridge` resolves the coordinator URL without consulting `CITRUS_DIST_COORDINATOR_URL`, and unconditionally sets an empty auth token, wiping out `citrus.test.distribution.authToken` / `CITRUS_DIST_AUTH_TOKEN`.

The goal is a clear, complete operator reference for both run-times, backed by consistent Citrus-native configuration.

## Goals

1. Document both worker run-times with enough detail to deploy and operate them without reading source code: quick start, CLI options, environment variables and system properties, precedence rules, capabilities, local file cache, lifecycle and exit behaviour, and container deployment.
2. Make worker mode selectable via environment variable / system property for `citrus-agent`, with validation identical to CLI `--mode`.
3. Fix the two coordinator URL / auth token fallback bugs so Citrus-native environment variables and system properties work as documented.
4. Verify the manual still builds after the AsciiDoc changes.

## Non-goals

- No new executable JAR. The two existing run-times are the distribution story.
- No autoscaling logic, `minSize` / `maxSize`, or external autoscaler `/status` endpoint.
- No new TLS, `--allow-untrusted`, or multi-JDK selection work.
- No renaming or removal of pre-existing environment aliases already present in `DistributionSettings`; they are existing behaviour and out of scope.
- No changes to `citrus-agent`'s built-in coordinator URL fallback of `http://localhost:8084`; it stays for backward compatibility.

## Anti-goals

- Adding third-party product names, branding, or example values to Citrus documentation or spec documents. The example that motivated the documentation work is a structural reference only. Documentation and specs describe Citrus in Citrus terms.
- Expanding the configuration surface with new aliases. Prefer the existing `CITRUS_DIST_*`, `CITRUS_AGENT_*`, and `citrus.*` settings.

## Constraints

- Java 21+, Maven 3.9+, JUnit Jupiter. Unit tests end in `Test.java`; integration tests in `IT.java`.
- Apache 2.0 license header on new Java files.
- `CitrusAgentConfiguration.setMode(String)` already carries `@SchemaProperty`; no schema-generator control JSON change is needed.
- Docs are AsciiDoc under `src/manual/`, included from `src/manual/index.adoc`. Manual build check: `./mvnw process-resources -Pbuild-manual -N`.
- `citrus-agent`'s short option `-p` is reserved for `--package`; agent pool ID uses the full `--pool` flag.
- Precedence rules that must stay consistent with existing code:
  - Coordinator URL and cache settings: system property (`-Dcitrus.*`) -> `CITRUS_DIST_*` environment variable -> runtime default.
  - Pool ID, registration key, worker ID, and capabilities: `CITRUS_DIST_WORKER_*` environment variable -> `citrus.test.distribution.worker.*` system property -> runtime default.
- Defaults by component: `citrus dist worker start` uses `http://localhost:8765`; the Coordinator default port is `8765`; `citrus-agent` keeps its legacy `http://localhost:8084` fallback.

## Approaches considered

### Approach A: Documentation only

Rewrite the worker section of `tools-test-distribution.adoc`, add worker mode to `agent.adoc`, and refresh `tools/agent/README.adoc`. No Java changes.

- Trade-off: cheapest, but container users still cannot select worker mode via environment variable, and the two configuration bugs remain. Documentation would have to describe caveats instead of clean behaviour.
- Rejected: the inconsistencies are small and worth fixing alongside the docs.

### Approach B: Documentation plus small configuration consistency fixes (chosen)

Write the documentation and fix the three small configuration issues above. All changes stay inside existing classes and add no new public configuration surface beyond `citrus.agent.mode` / `CITRUS_AGENT_MODE`.

- Trade-off: touches five Java classes and their unit tests in addition to AsciiDoc files, but the documented behaviour then matches the runtime without caveats.
- Chosen: this is the smallest change that makes the documentation truthful.

### Approach C: Dedicated standalone executable worker JAR

Build a shaded, executable worker JAR with a `Main-Class`.

- Trade-off: new packaging and release surface, plus a real classpath hazard. `ForkedPartitionRunner.childClasspath(...)` puts the code source of `ForkedPartitionMain` and `DistributionJson` first on the forked child JVM classpath so the module under test cannot shadow them. In a flat fat JAR that code source would be the fat JAR itself, putting every bundled library ahead of the module under test's own versions.
- Rejected: out of scope and risks breaking forked partition execution.

## Design decisions

These are the builder's calls, recorded so implementation does not need to revisit them.

### 1. Worker mode setting (`citrus-agent`)

Add `citrus.agent.mode` / `CITRUS_AGENT_MODE` to `CitrusAgentSettings` with default `standalone`, and read it in `CitrusAgentConfiguration.fromEnvVars(...)`. Accepted values: `standalone` and `worker`, case-insensitive and trimmed. Blank falls back to `standalone`; any other value throws `IllegalArgumentException` with a message naming the bad value and its source. Apply the same validation to CLI `-m` / `--mode`. Because `CitrusAgentServer.parseOptions(...)` already catches `IllegalArgumentException`, bad values from any source exit with `STARTUP_FAILURE_EXIT_CODE = 1` and the same user-facing error. CLI `--mode` still wins over the environment variable.

### 2. Coordinator URL fallback

In `DistWorkerStart`, remove the Aesh `defaultValue` on `--coordinator-url` and resolve the value from the command line first, then `citrus.test.distribution.coordinatorUrl`, then `CITRUS_DIST_COORDINATOR_URL`, then `http://localhost:8765`. In `DistributionWorkerBridge`, resolve the fallback with the same Citrus-native precedence using `citrus-agent`'s legacy `http://localhost:8084` default. No new environment alias is introduced.

### 3. Auth token fallback

In `DistributionWorkerBridge.buildWorkerConfig(...)`, only set the auth token on the worker builder when the CLI value is non-blank. When it is not supplied, let `DistributionWorkerConfig.builder()` keep the value from `citrus.test.distribution.authToken` / `CITRUS_DIST_AUTH_TOKEN`.

### 4. Documentation layout

- `src/manual/tools-test-distribution.adoc`: expand `[[tools-test-distribution-workers]]` into a full operator reference for both run-times, and add the coordinator URL row to `[[tools-test-distribution-external-resources-worker]]`.
- `src/manual/agent.adoc`: add the `CITRUS_AGENT_MODE` row to the environment variables table and a `== Test Distribution Worker Mode` section near the end.
- `tools/agent/README.adoc`: replace the stub with a short, accurate quick start that links to the manual.

### 5. Verification

Unit tests first for each Java change. Focused test runs during development, full module test suites before commit, then `./mvnw process-resources -Pbuild-manual -N` to prove the manual builds.

## What is left to the builder

The implementation plan will break this into small, independently testable tasks, each with exact file paths, method signatures, test cases, and verification commands. Until that plan is approved, no implementation work should happen.
