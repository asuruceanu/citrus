# Prompt: migrate citrus-kubernetes tests from TestNG to JUnit Jupiter (#1873)

Use this prompt with Claude (paste into chat with repo access to citrusframework/citrus).

---

## Task

Migrate `connectors/citrus-kubernetes` tests from TestNG to JUnit Jupiter, following issue #1873 and the merged sibling PRs (#1897 binary, #1898 yaml, #1900 assertj, #1899 text, jbang-connector PR #1911, knative PR #1912, docker, agent-connector).

Branch from `main` as `feature/1873-junit-jupiter-kubernetes`. Commit as `test(#1873): migrate citrus-kubernetes tests from TestNG to JUnit Jupiter`. Do NOT touch production code.

## Scope (83 test files: 75 unit + 17 ITs + 1 IT base + 1 Spring config + 1 Foo helper + base classes)

```
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/KubernetesActorTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/UnitTestSupport.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/actions/KubernetesActionBuilderTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/actions/KubernetesExecuteActionTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/actions/dsl/KubernetesTestActionBuilderTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/config/CredentialValidatorTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/config/annotation/KubernetesBadClientConfigParserTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/config/annotation/KubernetesClientConfigParserTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/config/xml/CreatePodActionParserTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/config/xml/CreateServiceActionParserTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/config/xml/KubernetesBadClientParserTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/config/xml/KubernetesClientParserTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/config/xml/KubernetesExecuteActionParserTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/endpoint/KubernetesEndpointComponentTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/endpoint/builder/KubernetesEndpointsTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/functions/ResolveExternalServiceUrlFunctionTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/functions/ResolveServiceUrlFunctionTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/functions/ServiceClusterIpFunctionTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/AbstractKubernetesIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/Foo.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesCreateAnnotationsIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesCreateConfigMapsIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesCreateCustomResourcesIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesCreateIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesCreateLabelsIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesCreateResourceIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesCreateSecretsIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesCreateServiceIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesDeleteConfigMapsIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesDeleteCustomResourcesIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesDeleteResourcesIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesDeleteSecretsIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesDeleteServicesIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesExecuteIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesSendReceiveIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesServiceConfiguration.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesVerifyCustomResourcesIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/integration/KubernetesVerifyPodsIT.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/AbstractXmlActionTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/AgentConnectTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/AgentDisconnectTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/CreateAnnotationsTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/CreateConfigMapTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/CreateCustomResourceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/CreateLabelsTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/CreateResourceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/CreateSecretTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/CreateServiceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/DeleteConfigMapTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/DeleteCustomResourceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/DeleteResourceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/DeleteSecretTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/DeleteServiceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/KubernetesTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/ServiceConnectTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/ServiceDisconnectTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/VerifyCustomResourceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/xml/VerifyPodTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/AbstractYamlActionTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/AgentConnectTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/AgentDisconnectTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/CreateAnnotationsTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/CreateConfigMapTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/CreateCustomResourceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/CreateLabelsTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/CreateResourceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/CreateSecretTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/CreateServiceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/DeleteConfigMapTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/DeleteCustomResourceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/DeleteResourceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/DeleteSecretTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/DeleteServiceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/KubernetesTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/ServiceConnectTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/ServiceDisconnectTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/VerifyCustomResourceTest.java
connectors/citrus-kubernetes/src/test/java/org/citrusframework/kubernetes/yaml/VerifyPodTest.java
```

## Reference pattern (copy exactly from knative commit `84b0fd819f`, PR #1912)

POM (`connectors/citrus-knative/pom.xml` at commit `84b0fd819f`):

- Add `maven-surefire-plugin` + `maven-failsafe-plugin` blocks inside the existing `<build>` section (kubernetes already has a failsafe block with `citrus.kuberenetes.it.enabled` systemProperties — merge the `surefire-junit-platform` provider dependency into it, do not replace the block).
- Test deps: remove `org.citrusframework:citrus-testng`, add `org.citrusframework:citrus-junit-jupiter` (test scope). Add `org.junit.platform:junit-platform-launcher` (test scope). Do NOT add `junit-jupiter-params` (unused). Keep `citrus-test-support` (still needed for `UnitTestSupport`/`AbstractBeanDefinitionParserTest` JUnit bases — cf. knative diff where it stayed).
- Verify against `core/citrus-base/pom.xml` for exact plugin/dependency shape.

`src/test/resources/junit-platform.properties` (copy from knative commit `84b0fd819f`, including license header):

```
# Test classes run in parallel; the methods of one class share a thread.
# Run sequentially with -Djunit.jupiter.execution.parallel.enabled=false
junit.jupiter.execution.parallel.enabled = true
junit.jupiter.execution.parallel.mode.default = same_thread
junit.jupiter.execution.parallel.mode.classes.default = concurrent
```

Abstract base classes (copy shape from knative `AbstractXmlActionTest` / `AbstractYamlActionTest` at `84b0fd819f`):

- `@ExtendWith(MockitoExtension.class)` (`org.mockito.junit.jupiter.MockitoExtension`), extend `org.citrusframework.junit.jupiter.AbstractUnitTest`.
- Replace `@BeforeClass setupMocks()` with a `@BeforeEach @Override prepareTest()` override: create the `Citrus` instance, init the mock server + clients, then call `super.prepareTest()`. Plain `@BeforeEach setupMocks()` does NOT work: the superclass `@BeforeEach` runs first and `createTestContext()` NPEs on the still-null mock (this exact failure was hit and fixed in the jbang/knative migrations).
- Use `lenient()` (`org.mockito.Mockito.lenient`) for the `citrusContext` stubbings in `createTestContext()`. Drop `MockitoAnnotations` (handled by the extension).

## Conversion rules

1. Base classes (`xml/AbstractXmlActionTest.java`, `yaml/AbstractYamlActionTest.java`):
   - `org.citrusframework.testng.AbstractTestNGUnitTest` → `org.citrusframework.junit.jupiter.AbstractUnitTest`.
   - `@BeforeClass setupMocks()` (instance method: `MockitoAnnotations.openMocks`, `k8sServer.init()`, `createClient()`) → fold into the `@BeforeEach @Override prepareTest()` override described above. Drop `MockitoAnnotations` (handled by the extension).
   - `@AfterClass(alwaysRun = true) stop()` (`k8sServer.destroy()`) → `@AfterEach` (`org.junit.jupiter.api.AfterEach`). Instance fields are safe: JUnit's default per-method lifecycle creates a fresh instance (and fresh mock server) per test.
   - `createTestContext()` override: keep logic (including the `k8sClient` binding), switch stubbings to `lenient()`.
2. `UnitTestSupport.java` (hand-rolled base: fields `testContextFactory`, `context`, `@BeforeMethod prepareTest()`):
   - Preferred: delete it and extend `AbstractUnitTest` in the 4 subclasses (`actions/dsl/KubernetesTestActionBuilderTest`, `functions/ResolveExternalServiceUrlFunctionTest`, `functions/ResolveServiceUrlFunctionTest`, `functions/ServiceClusterIpFunctionTest`) — `AbstractUnitTest` already provides both fields plus `@BeforeEach prepareTest()`. Alternatively convert its `@BeforeMethod` → `@BeforeEach` (`org.junit.jupiter.api.BeforeEach`). Do not keep any `org.testng` import either way.
   - `functions/*Test.java`: `@BeforeClass` setup (mock server init) → same `@BeforeEach @Override prepareTest()` pattern; JUnit per-method instance lifecycle makes per-test setup correct. If a function test instead uses `UnitTestSupport` without a mock server, simply convert `@BeforeClass` → `@BeforeEach` (`org.junit.jupiter.api.BeforeEach`).
3. `KubernetesActorTest.java`:
   - `@Test(singleThreaded = true)` on the class → drop `singleThreaded` (no JUnit equivalent; the parallel config scopes classes to one thread via `same_thread` methods mode). Convert the two methods `shouldVerifyConnectedState` / `shouldVerifyDisconnectedState` (currently implicit TestNG tests via class-level `@Test`) to method-level `org.junit.jupiter.api.Test`.
   - `@BeforeClass setupMocks()` / `@AfterClass(alwaysRun = true) stop()` (mock server) → `@BeforeEach` / `@AfterEach`.
4. `actions/KubernetesExecuteActionTest.java`, `config/annotation/*ParserTest.java`:
   - `extends AbstractTestNGUnitTest` → `extends AbstractUnitTest`, `@ExtendWith(MockitoExtension.class)` where `@Mock` fields exist.
   - `@BeforeClass setupMocks()/setup()` (`MockitoAnnotations.openMocks`) → `@BeforeEach setup()` (drop `MockitoAnnotations`). `@BeforeMethod setMocks()` → `@BeforeEach` (note: subclass `@BeforeEach` runs after the superclass `prepareTest()`, so `context` is already available — this matches the knative fix).
   - `config/xml/*ParserTest.java`: `extends AbstractBeanDefinitionParserTest` (`org.citrusframework.testng.AbstractBeanDefinitionParserTest`) → `org.citrusframework.junit.jupiter.AbstractBeanDefinitionParserTest` (check the JUnit package name in the already-migrated json/xml module first; import stays analogous). `KubernetesBadClientParserTest`'s `@BeforeClass(alwaysRun = true, dependsOnMethods = "springTestContextPrepareTestInstance") parseBeanDefinitions()` override has no JUnit equivalent — drop the annotation, keep the empty override as-is (documenting that it must stay overridden so the test fails when loading, not before it starts).
5. `CredentialValidatorTest.java`:
   - `@DataProvider` (TestNG) → JUnit 5 `@MethodSource` (`org.junit.jupiter.api.params.ParameterizedTest`, `org.junit.jupiter.params.provider.MethodSource`). Convert `primeNumbers()` (static `Object[][]`) to a static method returning `Stream<Arguments>` or `List<Arguments>`, rename to `isValidData()` (matching the provider constant), with `@ParameterizedTest(name = "{0}...")` + `@MethodSource("isValidData")`. This requires adding `org.junit.jupiter:junit-jupiter-params` (test scope) — justified here (unlike the sibling modules, this one actually uses it; assert in the PR body that it is used).
   - Fix `assertEquals` order: `assertEquals(CredentialValidator.isValid(...), expectedResult)` → `Assertions.assertEquals(expectedResult, CredentialValidator.isValid(...))`.
6. ITs (`integration/*IT.java`, `AbstractKubernetesIT.java`):
   - `AbstractKubernetesIT extends org.citrusframework.testng.spring.TestNGCitrusSpringSupport` → plain class annotated with `@CitrusSpringSupport` (`org.citrusframework.junit.jupiter.spring.CitrusSpringSupport`), keeping `@ContextConfiguration(classes = KubernetesServiceConfiguration.class)`. Cf. knative `AbstractKnativeIT` at `84b0fd819f` (same `@CitrusSpringSupport` + `@ContextConfiguration` shape; `@CitrusTest` methods and `TestActionSupport` `when`/`then` DSL stay as is).
   - `org.testng.annotations.Test` → `org.junit.jupiter.api.Test` on every IT method.
   - `integration/KubernetesServiceConfiguration.java`: `org.testng.Assert` → `org.junit.jupiter.api.Assertions`; fix argument order to `assertEquals(expected, actual)`.
   - `integration/Foo.java`: test helper with no TestNG annotations — leave as is unless it imports `org.testng`.
7. Unit tests (all remaining `actions/*`, `endpoint/*`, `xml/*`, `yaml/*`, `config/*` bodies, incl. `AgentDisconnectTest` / `ServiceDisconnectTest` `@BeforeClass setup()` mock-opening):
   - `org.testng.annotations.Test` → `org.junit.jupiter.api.Test`.
   - `org.testng.Assert` / static `org.testng.Assert.*` → `org.junit.jupiter.api.Assertions` / static `org.junit.jupiter.api.Assertions.*`; fix order to `assertEquals(expected, actual)` (e.g. `Assert.assertEquals(result.getName(), "ServiceDisconnectTest")` → `Assertions.assertEquals("ServiceDisconnectTest", result.getName())`, `Assert.assertEquals(result.getActionCount(), 1L)` → `Assertions.assertEquals(1L, ...)`, `Assert.assertEquals(clients.size(), 3)` → `Assertions.assertEquals(3, clients.size())`). Note the `config/xml/KubernetesClientParserTest` case: `assertEquals(clients.size(), 3)` currently passes under TestNG actual-first; under JUnit it must be `assertEquals(3, clients.size())` or the failure message swaps.
   - `AgentDisconnectTest` / `ServiceDisconnectTest` (`xml` + `yaml`): `@BeforeClass setup() { MockitoAnnotations.openMocks(this); }` → `@BeforeEach setup()` with `@ExtendWith(MockitoExtension.class)` (drop `MockitoAnnotations`).
   - This module has no `expectedExceptions`, `expectedExceptionsMessageRegExp`, `SkipException`, or try/catch-`fail` patterns — if you add any failure assertion, use `assertThrows` (TestNG `expectThrows` in `KubernetesBadClientParserTest` / `KubernetesBadClientConfigParserTest` → `Assertions.assertThrows`, signature is the same).
8. Note on `citrus.kuberenetes.it.enabled`: the pom property defaults to `false`; the ITs are only enabled when an external cluster is reachable. The unit tests (mock Kubernetes server) must run unconditionally. Ensure the failsafe `systemProperties` block survives the POM merge (see Reference pattern above).

## Verify

- `grep -rn "org.testng\|TestNG\|SkipException" connectors/citrus-kubernetes/src/test --include="*.java"` must return nothing.
- `./mvnw test -pl connectors/citrus-kubernetes -am` green (unit tests, incl. mock-server-backed XML/YAML loader tests and the parameterized `CredentialValidatorTest`).
- `./mvnw verify -pl connectors/citrus-kubernetes` — the `*IT` classes are skipped without a reachable cluster (`citrus.kuberenetes.it.enabled=false`); a successful verify with ITs skipped is the expected result here (contrast with knative, whose ITs use a mock server and run). Ensure failsafe picks the ITs up via the new `maven-failsafe-plugin` block; do not rename any `*IT.java`.
- No `junit-jupiter-params` import leftovers beyond `CredentialValidatorTest`; every other test uses plain `org.junit.jupiter.api.Test`.

## Constraints

- Test-only diff. No production code, no new dependencies beyond the POM template (plus `junit-jupiter-params`, justified above for the data provider).
- Keep commit/branch naming exactly as above to match the #1873 series.
- PR body must list verification steps (`./mvnw test -pl connectors/citrus-kubernetes -am` and `./mvnw verify -pl connectors/citrus-kubernetes` output, noting ITs skipped by default).
