# Citrus Test Distribution: Support `citrus-testng` on Workers via `testng-engine`

**Date:** 2026-11-24

**Status:** Approved

## Intent and Why

`runtime/citrus-testng` is the only module in the 105-module Citrus reactor excluded from distributed test runs because it overrides `<surefire.provider>surefire-testng</surefire.provider>`, bypassing the JUnit Platform `Launcher` and `DistributionLauncherInterceptor`. Running `runtime/citrus-testng` through the official JUnit Platform `org.junit.support:testng-engine` bridge enables distributed execution of all `citrus-testng` unit, integration, acceptance, and expected-failure tests on Citrus Test Distribution workers without coupling the distribution runtime to TestNG APIs.

## Goals, Non-Goals, and Anti-Goals

- **Goals:**
  - Execute `runtime/citrus-testng` unit (`*Test`), integration (`*IT`), acceptance (`*AT`), and expected-failure (`ShouldFailGroup`) tests on `surefire-junit-platform` via `org.junit.support:testng-engine:1.1.0`, both locally and on Citrus Test Distribution workers (`AUTO` and `ALWAYS` fork modes). Check the actual shared-JVM and forked paths and verify distinct test membership for each Maven execution, including the expected-failure group. Disable the live run’s cold-unit threshold so the single-test acceptance execution runs remotely.
  - Ensure `IsolatedPartitionRunner.PartitionClassLoader` loads `org.junit.support.*` child-first from the partition classpath alongside `org.testng.*`.
  - Ensure `DiscoveredWorkExtractor.partiallySelectedClasses` counts discovery-time empty `MethodSource` containers (such as TestNG `@DataProvider` / `invocationCount` methods and Jupiter `@ParameterizedTest` methods) so group-filtered or `-Dtest`-filtered classes split into method work units accurately.
  - Document how TestNG modules run on Citrus Test Distribution in `src/manual/tools-test-distribution.adoc`.
- **Non-Goals:**
  - Automatically rewriting `surefire-testng` provider dependencies in `citrus-test-distribution-maven-extension` for downstream projects.
  - Modifying historical benchmark reports under `docs/superpowers/metrics/`.
- **Anti-Goals:**
  - Altering `citrus-testng`'s published `compile`/`runtime` transitive dependencies.
  - Introducing compile or runtime `org.testng.*` dependencies into `citrus-test-distribution` or `citrus-test-distribution-worker`.

## Constraints

- Implement in the isolated worktree `/Users/asuruceanu/.local/share/opencode/worktree/46a00c/citrus-testng-distribution` on branch `feature/test-distribution-testng` (based on `incubator/test-distribution`).
- Keep root JUnit (`6.1.3`), TestNG (`7.12.0`), and Maven Surefire/Failsafe (`3.5.6`) versions unchanged.

## Approaches Considered

1. **Approach A — JUnit Platform `testng-engine` Bridge + Engine-Agnostic Distribution Fixes (Selected):** Manage `org.junit.support:testng-engine:1.1.0` in root `pom.xml`, add it in `<scope>test</scope>` to `runtime/citrus-testng/pom.xml`, drop `<surefire.provider>surefire-testng</surefire.provider>`, and fix `PartitionClassLoader` delegation plus `DiscoveredWorkExtractor.countTests`. Chosen because it reuses the JUnit Platform distribution pipeline end-to-end with zero TestNG coupling in distribution modules and zero transitive impact on `citrus-testng` consumers.
2. **Approach B — Dedicated TestNG Distribution Adapter on `surefire-testng`:** Keep `surefire-testng` and add a TestNG listener/interceptor and worker launcher. Rejected because it duplicates discovery, group filtering, worker execution, and Surefire result replay while coupling the worker to TestNG.
3. **Downstream Auto-Wiring (Scoped Out):** Have `SurefirePluginMutator` or `citrus-testng` runtime scope inject `testng-engine` automatically for external TestNG builds. Rejected so `citrus-testng`'s published classpath stays clean and external TestNG users opt in explicitly via `surefire-junit-platform` + `testng-engine`.

## Decided Design

- **Maven Configuration (`pom.xml`, `runtime/citrus-testng/pom.xml`):**
  - Define `<testng.engine.version>1.1.0</testng.engine.version>` and manage `org.junit.support:testng-engine` in root `pom.xml` `<dependencyManagement>`; update the `<surefire.provider>` comment in root `pom.xml` to reference only `runtime/citrus-junit`.
  - Add `org.junit.support:testng-engine` (`<scope>test</scope>`) to `runtime/citrus-testng/pom.xml` and remove `<surefire.provider>surefire-testng</surefire.provider>`.
  - Retain `runtime/citrus-testng`'s Failsafe executions (`default` excluding `ShouldFailGroup`, `acceptance-tests` for `*AT.java`, and `failure-tests` selecting `ShouldFailGroup`), relying on `testng-engine` mapping `@Test(groups = ...)` to JUnit Platform tags.
- **Worker Classloading (`tools/citrus-test-distribution-worker`):**
  - In `IsolatedPartitionRunner.PartitionClassLoader`, narrow shared JUnit prefixes from `org.junit.` / `org/junit/` to `org.junit.platform.` / `org/junit/platform/` and `org.junit.jupiter.` / `org/junit/jupiter/` (retaining `META-INF/services/org.junit.`), so `org.junit.support.testng.engine.*` always loads from the partition classloader alongside `org.testng.*`.
- **Work Extraction (`tools/citrus-test-distribution`):**
  - In `DiscoveredWorkExtractor.countTests`, count discovery-time leaf `MethodSource` containers (`plan.getChildren(node).isEmpty()`) alongside `node.isTest()`, ensuring `partiallySelectedClasses` detects pruned classes that contain data-driven or parameterized test methods.
- **Multi-source Citrus tests:** For multi-source methods, multiply the existing TestNG invocation count by the resolved source count and return the original method instance once. This preserves explicit repeats per source. The bridge uses invocation count to classify the method as a container. Citrus selects the source using the current invocation index.
- **Documentation (`src/manual/tools-test-distribution.adoc`):**
  - Add a brief note explaining how TestNG suites run on Citrus Test Distribution using `surefire-junit-platform` and `org.junit.support:testng-engine`.

## Left to the Builder

- Reproduce the provider change with the actual `CitrusXmlAnnotationIT` fixture and test that each resolved source executes exactly once under both Citrus TestNG support classes with the default invocation count.
- Unit and integration tests in `citrus-test-distribution-worker`, `citrus-test-distribution`, and `citrus-testng` covering `PartitionClassLoader` isolation for `org.junit.support.*`, `DiscoveredWorkExtractor` partial-class splitting with container `MethodSource` descriptors, multi-source `@CitrusTestSource` execution under `testng-engine`, and distributed TestNG execution (including groups and `@DataProvider`).
- End-to-end verification of `./mvnw verify -pl runtime/citrus-testng` both locally (distribution disabled) and distributed against a coordinator and worker (`AUTO` and `ALWAYS` fork modes). Check the actual shared-JVM and forked paths and verify distinct test membership for each Maven execution, including the expected-failure group. Disable the live run’s cold-unit threshold so the single-test acceptance execution runs remotely.
