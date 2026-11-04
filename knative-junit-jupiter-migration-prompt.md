# Prompt: migrate citrus-knative tests from TestNG to JUnit Jupiter (#1873)

Use this prompt with Claude (paste into chat with repo access to citrusframework/citrus).

---

## Task

Migrate `connectors/citrus-knative` tests from TestNG to JUnit Jupiter, following issue #1873 and the merged sibling PRs (#1897 binary, #1898 yaml, #1900 assertj, #1899 text, jbang-connector).

Branch from `main` as `feature/1873-junit-jupiter-knative`. Commit as `test(#1873): migrate citrus-knative tests from TestNG to JUnit Jupiter`. Do NOT touch production code.

## Scope (37 test files: 27 unit tests + 8 ITs + 1 IT base class + 1 Spring config)

```
connectors/citrus-knative/src/test/java/org/citrusframework/knative/actions/KnativeActionBuilderTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/integration/AbstractKnativeIT.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/integration/CreateKnativeBrokerIT.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/integration/CreateKnativeTriggerIT.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/integration/DeleteKnativeBrokerIT.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/integration/DeleteKnativeResourceIT.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/integration/DeleteKnativeTriggerIT.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/integration/KnativeServiceConfiguration.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/integration/ReceiveEventIT.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/integration/SendEventIT.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/integration/VerifyKnativeBrokerIT.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/AbstractXmlActionTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/CreateBrokerTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/CreateChannelTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/CreateSubscriptionTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/CreateTriggerTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/DeleteBrokerTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/DeleteChannelTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/DeleteResourceTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/DeleteSubscriptionTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/DeleteTriggerTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/KnativeTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/SendReceiveEventTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/xml/VerifyBrokerTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/AbstractYamlActionTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/CreateBrokerTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/CreateChannelTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/CreateSubscriptionTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/CreateTriggerTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/DeleteBrokerTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/DeleteChannelTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/DeleteResourceTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/DeleteSubscriptionTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/DeleteTriggerTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/KnativeTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/SendReceiveEventTest.java
connectors/citrus-knative/src/test/java/org/citrusframework/knative/yaml/VerifyBrokerTest.java
```

## Reference pattern (copy exactly from jbang-connector commit `ce22324613`)

POM (`connectors/citrus-jbang-connector/pom.xml` at commit `ce22324613`):

- Add `maven-surefire-plugin` + `maven-failsafe-plugin` blocks with `surefire-junit-platform` provider dependency (versions `${maven.surefire.plugin.version}` / `${maven.failsafe.plugin.version}`).
- Test deps: remove `org.citrusframework:citrus-testng`, add `org.citrusframework:citrus-junit-jupiter` (test scope). Add `org.junit.platform:junit-platform-launcher` (test scope). Do NOT add `junit-jupiter-params` (unused).
- Verify against `core/citrus-base/pom.xml` for exact plugin/dependency shape.

`src/test/resources/junit-platform.properties` (copy from jbang-connector):

```
# Test classes run in parallel; the methods of one class share a thread.
# Run sequentially with -Djunit.jupiter.execution.parallel.enabled=false
junit.jupiter.execution.parallel.enabled = true
junit.jupiter.execution.parallel.mode.default = same_thread
junit.jupiter.execution.parallel.mode.classes.default = concurrent
```

Include the license header plus the two-line comment from the sibling files.

Abstract base classes (copy shape from jbang-connector `AbstractXmlActionTest` / `AbstractYamlActionTest` at `ce22324613`, which itself mirrors validation-assertj):

- `@ExtendWith(MockitoExtension.class)` (`org.mockito.junit.jupiter.MockitoExtension`), extend `org.citrusframework.junit.jupiter.AbstractUnitTest`.
- Replace `@BeforeClass setupMocks()` with a `@BeforeEach @Override prepareTest()` override: create the `Citrus` instance, init the mock server + clients, then call `super.prepareTest()`. Plain `@BeforeEach setupMocks()` does NOT work: the superclass `@BeforeEach` runs first and `createTestContext()` NPEs on the still-null mock (this exact failure was hit and fixed in the jbang migration).
- Use `lenient()` (`org.mockito.Mockito.lenient`) for the `citrusContext` stubbings in `createTestContext()`.

## Conversion rules

1. Base classes (`xml/AbstractXmlActionTest.java`, `yaml/AbstractYamlActionTest.java`):
   - `org.citrusframework.testng.AbstractTestNGUnitTest` → `org.citrusframework.junit.jupiter.AbstractUnitTest`.
   - `@BeforeClass setupMocks()` (instance method: `MockitoAnnotations.openMocks`, `k8sServer.init()`, `createClient()`, `adapt(KnativeClient.class)`) → fold into the `@BeforeEach @Override prepareTest()` override described above. Drop `MockitoAnnotations` (handled by the extension).
   - `@AfterClass(alwaysRun = true) stop()` (`k8sServer.destroy()`) → `@AfterEach` (`org.junit.jupiter.api.AfterEach`). Instance fields are safe: JUnit's default per-method lifecycle creates a fresh instance (and fresh mock server) per test.
   - `createTestContext()` override: keep logic (including the `k8sClient`/`knativeClient` bindings), switch stubbings to `lenient()`.
2. `SendReceiveEventTest` (xml + yaml):
   - `@BeforeClass @Override setupMocks()` (`super.setupMocks()` + `CitrusAnnotations.injectAll(this)`) → `@BeforeEach @Override prepareTest()` (`super.prepareTest()` + `CitrusAnnotations.injectAll(this)`).
   - `@AfterClass(alwaysRun = true) shutdown()` (`knativeBroker.stop()`) → `@AfterEach`. The broker field is per-test-instance, so per-test shutdown is correct.
3. ITs (`integration/*IT.java`, `AbstractKnativeIT.java`):
   - `AbstractKnativeIT extends org.citrusframework.testng.spring.TestNGCitrusSpringSupport` → plain class annotated with `@CitrusSpringSupport` (`org.citrusframework.junit.jupiter.spring.CitrusSpringSupport`), keeping `@ContextConfiguration(classes = KnativeServiceConfiguration.class)`. Cf. assertj `AssertIT` / `HamcrestCoexistenceIT` (same `@CitrusSpringSupport` + `@ContextConfiguration` shape; `@CitrusTest` methods and `TestActionSupport` `when`/`then` DSL stay as is).
   - `org.testng.annotations.Test` → `org.junit.jupiter.api.Test` on every IT method.
   - `@AfterClass(alwaysRun = true) shutdown()` (`SendEventIT`, `ReceiveEventIT`) → `@AfterEach` (instance `knativeBroker` field; do not use `@AfterAll` without making the field static).
4. `integration/KnativeServiceConfiguration.java`: `org.testng.Assert` → `org.junit.jupiter.api.Assertions`; fix argument order to `assertEquals(expected, actual)` (7 call sites, currently actual-first, e.g. `Assert.assertEquals(message.getHeader("Ce-Id"), "say-hello")`).
5. Unit tests (`actions/KnativeActionBuilderTest`, all `Create*Test`, `Delete*Test`, `KnativeTest`, `VerifyBrokerTest`, `SendReceiveEventTest` bodies):
   - `org.testng.annotations.Test` → `org.junit.jupiter.api.Test`.
   - `org.testng.Assert` → `org.junit.jupiter.api.Assertions`; fix order to `assertEquals(expected, actual)` (e.g. `Assert.assertEquals(result.getName(), "CreateBrokerTest")` → `Assertions.assertEquals("CreateBrokerTest", result.getName())`, `Assert.assertEquals(result.getActionCount(), 1L)` → `Assertions.assertEquals(1L, result.getActionCount())`, `Assert.assertEquals(broker.getMetadata().getLabels().get("app"), "citrus")` → expected first).
6. This module has no `expectedExceptions`, `expectedExceptionsMessageRegExp`, `SkipException`, or try/catch-`fail` patterns — if you add any failure assertion, use `assertThrows`.

## Verify

- `grep -rn "org.testng\|TestNG\|SkipException" connectors/citrus-knative/src/test --include="*.java"` must return nothing.
- `./mvnw test -pl connectors/citrus-knative -am` green (unit tests, incl. mock-server-backed XML/YAML loader tests).
- `./mvnw verify -pl connectors/citrus-knative` green (failsafe runs the `*IT` classes: mock Kubernetes server + local `HttpServer` brokers, no external cluster needed).
- No `*IT.java` naming changes; failsafe picks them up via the new `maven-failsafe-plugin` block.

## Constraints

- Test-only diff. No production code, no new dependencies beyond the template.
- Keep commit/branch naming exactly as above to match the #1873 series.
- PR body must list verification steps (`./mvnw test -pl connectors/citrus-knative` and `./mvnw verify -pl connectors/citrus-knative` output).
