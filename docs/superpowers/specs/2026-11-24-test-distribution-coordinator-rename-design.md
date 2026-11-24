# Citrus Test Distribution: Replace `broker` Terminology with `coordinator`

**Date:** 2026-11-24

**Status:** Approved for Native execution

## Intent and Why

Citrus is a messaging integration testing framework where "broker" already refers to JMS, Kafka, MQTT, and Knative Eventing message brokers. In Citrus Test Distribution, naming remained split between `coordinator` (the Maven module `citrus-test-distribution-coordinator`, `ROLE_COORDINATOR`, `OWNER_COORDINATOR`, and manual section headings) and `broker` (CLI commands/options, system properties, environment variables, Java packages/types, UI labels, and examples). Standardizing exclusively on `coordinator` eliminates terminology confusion before Test Distribution ships upstream.

## Goals, Non-Goals, and Anti-Goals

- **Goals:**
  - Replace every Test Distribution occurrence of `broker` / `Broker` / `BROKER` with `coordinator` / `Coordinator` / `COORDINATOR` across Java packages, types, methods, CLI commands/flags, system properties, environment variables, default cache directory, JSON status/schema fields, UI strings, and `src/manual/tools-test-distribution.adoc`.
  - Resolve the agent worker URL from an explicit CLI URL first, then the coordinator URL system property, and finally `http://localhost:8084`. Normalize the selected URL before connecting.
  - Disambiguate comments and test names in `citrus-test-distribution` that loosely referred to the client-side JUnit `LauncherInterceptor` host as a "coordinator" (referring to that side as the "launcher" or "launcher host" instead).
- **Non-Goals:**
  - Changing HTTP/SSE wire endpoints, scheduling behavior, JDBC schema, or default port numbers (`8567` in `DistributionSettings`, `8765` in JBang CLI commands, `8084` in `CitrusAgentConfiguration`).
  - Rewriting historical benchmark logs under `docs/superpowers/metrics/`.
- **Anti-Goals:**
  - Touching non-distribution `broker` usages anywhere in Citrus (JMS, ActiveMQ, Artemis, Kafka, Camel MQTT `brokerUrl`, or Knative Eventing `brokerUrl`).
  - Leaving deprecated `broker` fallback flags or property aliases in an unreleased feature.

## Constraints

- In `CitrusAppOptions` / `CitrusAgentOptions`, `-c` (`--config`) and `-C` (`--capabilities`) are already assigned, so `--coordinator-url` uses short option `-u` consistently across `citrus-jbang` (`dist worker start`, `dist status`, `dist sessions`) and `citrus-agent`.
- Carry out the rename in an isolated worktree and preserve unrelated edits in the original checkout.
- `tools/schema-generator` validates `CitrusAgentConfiguration` against `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json`; both must stay in sync.

## Approaches Considered

1. **Approach A — Clean Cutover Across Code, CLI, Config, UI, and Docs (Selected):** Rename all external surfaces and internal Java packages/classes cleanly with no legacy `broker` aliases. Chosen because Test Distribution is still unreleased (`5.3.0-SNAPSHOT`) and the goal is complete removal of `broker` terminology.
2. **Approach B — Full Rename + Runtime `broker` Fallbacks:** Rename everything while retaining silent CLI/property fallbacks for `--broker-url` and `brokerUrl`. Rejected because it preserves `broker` constants, flags, and tests in an unreleased feature.
3. **Approach C — External Surface Only:** Rename CLI, properties, UI, and docs while leaving Java packages/classes (`DistributionBrokerServer`, `BrokerEvent`, etc.) unchanged. Rejected because `DistributionBrokerServer` and `CitrusAgentConfiguration#getBrokerUrl` are public embeddable APIs and would stay split from the module and CLI naming.

## Decided Naming Mapping

### CLI (`tools/jbang` & `tools/agent`)
- `citrus dist broker` / `citrus dist broker start` → `citrus dist coordinator` / `citrus dist coordinator start` (`DistCoordinator`, `DistCoordinatorStart`, shutdown thread `citrus-dist-coordinator-shutdown`, console banner `Citrus Test Distribution Coordinator listening on ...`)
- `-b` / `--broker-url` → `-u` / `--coordinator-url` across `DistWorkerStart` (including `VALUE_OPTIONS`), `DistStatus`, `DistSessions`, `CitrusAgentOptions`, and `WorkerCacheCliOptionsTest`
- `citrus dist status`: JSON field `"brokerUrl"` → `"coordinatorUrl"`; text output `"Broker: ..."` → `"Coordinator: ..."`

### System Properties, Environment Variables, and Default Paths
- `DistributionSettings`:
  - `BROKER_URL_PROPERTY` / `BROKER_URL_ENV` / `DEFAULT_BROKER_URL` / `getBrokerUrl()` → `COORDINATOR_URL_PROPERTY` (`citrus.test.distribution.coordinatorUrl`), `COORDINATOR_URL_ENV` (`CITRUS_DIST_COORDINATOR_URL`), `DEFAULT_COORDINATOR_URL`, `getCoordinatorUrl()`
  - `BROKER_MIN_PARTITION_DURATION_MS_*` / `getBrokerMinPartitionDurationMs()` → `COORDINATOR_MIN_PARTITION_DURATION_MS_PROPERTY` (`citrus.test.distribution.coordinator.minPartitionDurationMs`), `COORDINATOR_MIN_PARTITION_DURATION_MS_ENV` (`CITRUS_DIST_COORDINATOR_MIN_PARTITION_DURATION_MS`), `DEFAULT_COORDINATOR_MIN_PARTITION_DURATION_MS`, `getCoordinatorMinPartitionDurationMs()`
  - `BROKER_MAX_WAVES_PER_WORKER_*` / `getBrokerMaxWavesPerWorker()` → `COORDINATOR_MAX_WAVES_PER_WORKER_PROPERTY` (`citrus.test.distribution.coordinator.maxWavesPerWorker`), `COORDINATOR_MAX_WAVES_PER_WORKER_ENV` (`CITRUS_DIST_COORDINATOR_MAX_WAVES_PER_WORKER`), `DEFAULT_COORDINATOR_MAX_WAVES_PER_WORKER`, `getCoordinatorMaxWavesPerWorker()`
- Default coordinator blob cache directory: `~/.citrus/dist-broker-cache` → `~/.citrus/dist-coordinator-cache`

### Java Packages, Types, and Schema
- `tools/citrus-test-distribution-coordinator`:
  - Package `org.citrusframework.distribution.broker` → `org.citrusframework.distribution.coordinator` (main and test)
  - `DistributionBrokerServer` → `DistributionCoordinatorServer`
  - `DistributionBrokerConfig` → `DistributionCoordinatorConfig`
  - `DistributionBrokerVerticle` → `DistributionCoordinatorVerticle`
  - `BrokerSessionCoordinator` → `SessionCoordinator` (avoids redundant `CoordinatorSessionCoordinator`)
  - `BrokerEventStreamHub` → `CoordinatorEventStreamHub`
  - Tests: `DistributionCoordinatorServerTest`, `DistributionCoordinatorUiTest`, `SessionCoordinatorTest`, plus comment updates in `DistributionStore` and `JdbcDistributionStoreTest`
- `tools/citrus-test-distribution-protocol`: `BrokerEvent` → `CoordinatorEvent`
- `tools/citrus-test-distribution-worker`:
  - `BrokerBlobClient` → `CoordinatorBlobClient` (`CoordinatorBlobClientCacheTest`)
  - `DistributionWorkerConfig`: `brokerUrl` / `brokerUrl()` / `Builder.brokerUrl(...)` / `normalizeBrokerBaseUrl(...)` → `coordinatorUrl` / `coordinatorUrl()` / `Builder.coordinatorUrl(...)` / `normalizeCoordinatorBaseUrl(...)`
  - Update callers/comments in `DistributionWorkerClient`, `IsolatedPartitionRunner`, `ForkedPartitionRunner`, and worker tests
- `tools/citrus-test-distribution` & `tools/maven/citrus-test-distribution-maven-extension`:
  - `BrokerHttpClient` → `CoordinatorHttpClient`
  - Update `DistributedLauncher`, `DistributionLauncherInterceptor`, `TestDistributionLifecycleParticipant`, and their unit tests (`DistributedLauncherTest`, `DistributionPlacementFollowupTest`, `IndependentJupiterEnginePlacementTest`, `TestDistributionMavenExtensionTest`)
- `core/citrus-api`, `tools/agent`, & `tools/schema-generator`:
  - `CitrusAgentConfiguration`: `brokerUrl` / `getBrokerUrl()` / `setBrokerUrl(String)` → `coordinatorUrl` / `getCoordinatorUrl()` / `setCoordinatorUrl(String)` and `@SchemaProperty` descriptions
  - Update `DistributionWorkerBridge`, `CitrusAgentServer`, `CitrusAgentWorkerOptionsTest`, and `tools/schema-generator/src/test/resources/control/citrus-agent-configuration.json`
- `tools/citrus-test-distribution-ui` & Documentation:
  - Preserve the current proxy-authenticated UI and update the UI regression test to check that token-entry controls remain absent.
  - `src/manual/tools-test-distribution.adoc`: update all CLI commands, flags, property/env tables, cache directory paths, class references, and JUnit launcher wording

## Left to the Builder

- Local variable, helper method, log message, and Javadoc wording updates inside touched files to match `coordinator` terminology cleanly.
- Running module compilation, unit tests, schema-generator verification, and a final case-insensitive `broker` audit across all Test Distribution paths to verify zero residual matches.
