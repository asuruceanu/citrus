# Prompt: migrate citrus-openapi tests from TestNG to JUnit Jupiter (#1873)

Use this prompt with Claude (paste into chat with repo access to citrusframework/citrus).

---

## Task

Migrate `connectors/citrus-openapi` tests from TestNG to JUnit Jupiter, following issue #1873 and the merged sibling PRs (#1897 binary, #1898 yaml, #1900 assertj, #1899 text, jbang-connector PR #1911, knative PR #1912, docker, agent-connector, kubernetes PR #1917).

Branch from `main` as `feature/1873-junit-jupiter-openapi`. Commit as `test(#1873): migrate citrus-openapi tests from TestNG to JUnit Jupiter`. Do NOT touch production code.

## Scope

Test files (43 Java test files + 1 helper class):

```
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/OpenApiMessageTypeTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/OpenApiRepositoryTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/OpenApiSettingsTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/OpenApiSpecificationTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/OpenApiTestDataGeneratorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/OpenApiUtilsTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/SampleOpenApiProcessor.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/actions/OpenApiActionBuilderTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/actions/OpenApiClientActionBuilderTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/actions/OpenApiClientRequestMessageBuilderTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/actions/OpenApiPayloadBuilderTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/actions/OpenApiServerActionBuilderTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/groovy/AbstractGroovyActionDslTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/groovy/OpenApiClientTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/groovy/OpenApiServerTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/integration/OpenApiClientApiContextPathIT.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/integration/OpenApiClientIT.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/integration/OpenApiServerIT.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/model/OasModelHelperTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/model/OperationPathAdapterTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/model/v2/Oas20ModelHelperTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/model/v3/Oas30ModelHelperTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/OasRandomConfigurationTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomArrayGeneratorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomCompositeGeneratorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomContextTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomElementTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomEnumGeneratorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomGeneratorBuilderTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomGeneratorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomModelBuilderTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomNumberGeneratorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomObjectGeneratorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/random/RandomStringGeneratorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/validation/OpenApiMessageValidationContextBuilderTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/validation/OpenApiOperationToMessageHeadersProcessorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/validation/OpenApiRequestValidatorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/validation/OpenApiResponseValidatorTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/xml/AbstractXmlActionTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/xml/OpenApiClientTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/xml/OpenApiServerTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/yaml/AbstractYamlActionTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/yaml/OpenApiClientTest.java
connectors/citrus-openapi/src/test/java/org/citrusframework/openapi/yaml/OpenApiServerTest.java
```

## Reference pattern (copy exactly from kubernetes commit `627708565f`, PR #1917; cf. knative `84b0fd819f`, docker `0af68044fd`)

POM (`connectors/citrus-kubernetes/pom.xml` at `627708565f`):

- Add `maven-surefire-plugin` + `maven-failsafe-plugin` blocks inside a new `<build>` section (openapi has no `<build>` yet — no failsafe systemProperties to preserve; no `jaxb2` block to keep separate, just add these two plugins).
- Test deps: remove `org.citrusframework:citrus-testng`, add `org.citrusframework:citrus-junit-jupiter` (test scope). Add `org.junit.platform:junit-platform-launcher` (test scope). Add `junit-jupiter-params` ONLY if you use `@ParameterizedTest` (see CredentialValidatorTest exception below — add it here too since openapi has real `@DataProvider` usage, see conversion rules #3; justify in PR body as used).
- Keep `citrus-test-support` (still needed for `AbstractUnitTest`/`AbstractBeanDefinitionParserTest` etc. JUnit bases — cf. knative/docker/k8s diffs where it stayed).
- Verify against `runtime/citrus-junit-jupiter/pom.xml` for failsafe `surefire-junit-platform` shape.

`src/test/resources/junit-platform.properties` (copy from knative `84b0fd819f` / kubernetes `627708565f`, including license header):

```
# Test classes run in parallel; the methods of one class share a thread.
# Run sequentially with -Djunit.jupiter.execution.parallel.enabled=false
junit.jupiter.execution.parallel.enabled = true
junit.jupiter.execution.parallel.mode.default = same_thread
junit.jupiter.execution.parallel.mode.classes.default = concurrent
```

Abstract base classes (copy shape from kubernetes/knative `AbstractXmlActionTest` / `AbstractYamlActionTest` at `627708565f` / `84b0fd819f`; also `AbstractGroovyActionDslTest`):

- `@ExtendWith(MockitoExtension.class)` (`org.mockito.junit.jupiter.MockitoExtension`), extend `org.citrusframework.junit.jupiter.AbstractUnitTest`.
- For xml/yaml: add `@Isolated("XmlTestLoader uses a static non-thread-safe JAXB Unmarshaller cache")` (knative/k8s pattern).
- Replace `@BeforeClass setupMocks()` with a `@BeforeEach @Override prepareTest()` override: create the `Citrus` instance (and any server/testloader setup), then call `super.prepareTest()`. Plain `@BeforeEach setupMocks()` does NOT work: the superclass `@BeforeEach` runs first and `createTestContext()` NPEs on the still-null mock (this exact failure was hit and fixed in the k8s migration).
- Use `lenient()` (`org.mockito.Mockito.lenient`) for the `citrusContext` stubbings in `createTestContext()`. Drop `MockitoAnnotations` (handled by the extension).
- Walk every subclass of these bases: if a subclass has `@Mock` fields, it needs its own `@ExtendWith(MockitoExtension.class)` — MockitoExtension is NOT inherited (this caused `UnnecessaryStubbing`/null-mock failures in the k8s migration). If a subclass `@BeforeClass setup()` was empty after removing `openMocks`, delete it. If a subclass `@BeforeClass setup()` had real body beyond `openMocks`, convert to `@BeforeEach` (note: subclass `@BeforeEach` runs after superclass `prepareTest()`, so `context` is available — this matches the knative fix).
- `groovy/OpenApiClientTest.java` and `yaml/OpenApiClientTest.java` / `xml/OpenApiClientTest.java` have `@BeforeClass setupEndpoints` (httpServer init) + `@AfterClass cleanupEndpoints` — convert to `@BeforeEach`/`@AfterEach` and make them instance methods; JUnit per-method instance lifecycle gives a fresh server per test (see k8s `KubernetesActorTest` pattern). If server lifecycle must be shared, document and use static server cautiously.
- `groovy/AbstractGroovyActionDslTest.java` `createTestContext` has an extra `bind(any(String.class), any())` stubbing — convert it to `lenient()` as well.

## Why this module is special (read carefully)

Unlike kubernetes/docker/knative, `connectors/citrus-openapi` has **real `@DataProvider` usage in ITs** and a **`@BeforeTest` on static config**, plus TestNG static imports that need `assertThrows` mapping. You MUST handle these:

1. `@DataProvider` → `@MethodSource` / `@ParameterizedTest`:
   - `integration/OpenApiClientIT.java`: `validationConfigurations()` (returns `Object[][]` with `OpenApiSpecification, Boolean, boolean` — note `Boolean` can be `null`) used by `shouldProperlyValidateSendRequest`, `shouldProperlyValidateReceiveResponse` (the latter has NO `@CitrusTest`! check what it does — it uses `when/then` directly; under JUnit it needs `@Test` only, no runner param unless it uses `variable()` / `when()`).
   - `integration/OpenApiServerIT.java`: same `validationConfigurations()` + `aliases()` (returns `Object[][]` of one `String`).
   - `integration/OpenApiClientApiContextPathIT.java`: `openApiSpecifications()` (returns `Object[][]` of `OpenApiSpecification, String`).
   - `OpenApiTestDataGeneratorTest.java`: MANY `@DataProvider`s (e.g. `testRandomNumber`, `testRandomNumberLong`, etc.) — convert ALL of them. `Stream<Arguments>` or `List<Arguments>` returning static method. Note some providers return `Object[][]` with varying types (`int`, `long`, `boolean`, `null`) — use `Arguments.of(...)` with explicit types.
   - `random/*Test.java`: grep for `DataProvider` — ALL of them (Random*GeneratorTest, Random*Test) have providers; convert all.
   - TestNG `@Test(dataProvider = "x")` → JUnit `@ParameterizedTest(name = "{0}...") @MethodSource("x")`. Provider methods rename to match constant/method name (e.g. `validationConfigurations` → `validationConfigurations()` returning `Stream<Arguments>` or `List<Arguments>`). `@DataProvider` name attribute → `@MethodSource` value; method referenced by `dataProvider` may be static or instance — make provider `static` and match the name.
2. `@BeforeTest` → `@BeforeAll`:
   - `integration/OpenApiClientIT.java`, `OpenApiServerIT.java`: `@BeforeTest void beforeTest()` mutates `petstoreSpecWithValidationDisabled` static final field (sets validation disabled). Convert to `@BeforeAll static void beforeTest()` (instance `@BeforeAll` is not allowed in JUnit 5; k8s `AbstractKubernetesIT` docker pattern used `@BeforeAll static`).
   - `OpenApiClientApiContextPathIT` has no `@BeforeTest` but has `private static final OpenApiSpecification` fields initialized from `OpenApiSpecification.from(Resources.create(...))` — keep as-is (no lifecycle annotation).
3. `TestNGCitrusSpringSupport` → `@CitrusSpringSupport`:
   - `integration/OpenApiClientIT`, `OpenApiServerIT`, `OpenApiClientApiContextPathIT`: extend `TestNGCitrusSpringSupport` + `@ContextConfiguration(classes = Config.class)` + `@DirtiesContext` → plain class with `@CitrusSpringSupport @ContextConfiguration(classes = {CitrusSpringConfig.class, Config.class})` (cf. knative `AbstractKnativeIT` / docker `AbstractDockerIT`). Keep `@DirtiesContext`.
   - `OpenApiClientIT` has class-level `@Test` (TestNG class-level marker, no args) → drop it; method-level `@Test` stays (`shouldExecuteGetPetById`, etc. already have `@Test` + `@CitrusTest` on methods). `OpenApiClientApiContextPathIT` and `OpenApiServerIT` have same class-level `@Test` → drop, methods already have `@Test`.
   - Methods with `@CitrusTest` that use `variable()`/`when()`/`then()`/`assertThrows(() -> then(...))`: convert to `@CitrusResource GherkinTestActionRunner runner` + `runner.when/then/...`. NOTE: `shouldProperlyValidateReceiveResponse` in OpenApiClientIT has NO `@CitrusTest` but uses `when/then/variable` — under JUnit that without runner will still resolve via `TestActionSupport`? Check knative pattern: they added runner to `@CitrusTest` methods. For non-`@CitrusTest` methods that use `when/then`, verify if `TestActionSupport.when/then` still work standalone (citrus `TestActionSupport` has default methods using `GherkinTestActionRunner`? No — historically `TestActionSupport.when/then` delegate to a runner field injected by `@CitrusResource`? Actually `TestActionSupport` methods create actions via `TestActionSupport` itself — they should work without runner param. In k8s `KubernetesCreateAnnotationsIT`, methods converted to `runner.given/when/then`. So if a method uses `given/when/then` it probably should get a runner param; if it uses `variable/when/then` without a runner it may fail. Follow the knative/k8s rule: every method that calls `when/then/given` gets a `@CitrusResource GherkinTestActionRunner runner` param, and replace bare calls with `runner.` as in k8s PR #1917.
   - `OpenApiServerIT.getPetById` has `@CitrusTest` but NO `@Test` on the method! (check: it has `@CitrusTest public void getPetById()` — no `@Test`). Under TestNG class-level `@Test` made it a test. Under JUnit it MUST get `@Test` on the method.
4. `assertThrows` (TestNG): `OpenApiClientIT` / `OpenApiServerIT` `import static org.testng.Assert.assertThrows` — in TestNG `assertThrows` with 1 arg lambda takes `(ThrowingRunnable)`; same in JUnit, plus 2-arg form with expected exception. Swap import to `org.junit.jupiter.api.Assertions.assertThrows`.
5. `@BeforeMethod`/`@AfterMethod` → `@BeforeEach`/`@AfterEach`:
   - `OpenApiRepositoryTest.beforeMethodSetup`, `OpenApiSettingsTest.beforeMethod`, `OpenApiUtilsTest.beforeMethod/afterMethod`: `OpenApiUtilsTest.beforeMethod` does `mockCloseable = MockitoAnnotations.openMocks(this)` + `@AfterMethod mockCloseable.close()` — replace with `@ExtendWith(MockitoExtension.class)` + `@Mock private SystemProvider systemProvider` (if used) or `@BeforeEach` assignment. `OpenApiSettingsTest.beforeMethod` does `systemProvider = mock()` — convert to `@ExtendWith(MockitoExtension.class) @Mock` or `@BeforeEach systemProvider = mock()`. Prefer `@Mock` + extension; MockitoExtension handles close, drop `AutoCloseable` field + afterMethod.
6. `@BeforeClass` → `@BeforeEach`:
   - `xml/OpenApiClientTest.setupEndpoints` (creates httpServer/httpClient) + `@AfterClass cleanupEndpoints` — see groovy/xml/yaml rule above.
   - `groovy/OpenApiClientTest`, `yaml/OpenApiClientTest`, `xml/OpenApiClientTest` have `@BeforeClass/@AfterClass` — convert.
   - `actions/OpenApiClientRequestMessageBuilderTest.setup()` (`extends AbstractTestNGUnitTest`, `@Mock` fields, `@BeforeClass` openMocks + when-stubbing) — extend `AbstractUnitTest`, `@ExtendWith(MockitoExtension.class)`, `@BeforeEach setup()` with lenient stubbing if needed, pub.
   - `actions/OpenApiPayloadBuilderTest.setUp()` (`@BeforeClass context = new TestContext()`) → `@BeforeEach`.
   - `OpenApiTestDataGeneratorTest.beforeClass()` (`@BeforeClass public static`) → `@BeforeAll static`.
7. `expectThrows` / `try/catch + fail` patterns: grep. Module's `OpenApiRepositoryTest` uses `assertThrows(fixture::initialize)` (TestNG static) — swap import. If any try/catch with `fail()` exists, convert to `assertThrows`. This module also has `assertThrows(TestCaseFailedException.class, () -> then(...))` — two-arg form maps directly to JUnit `Assertions.assertThrows`.

## Conversion rules (generic, applies to all remaining files)

1. Base classes (`xml/AbstractXmlActionTest.java`, `yaml/AbstractYamlActionTest.java`, `groovy/AbstractGroovyActionDslTest.java`): as above.
2. `actions/OpenApiClientRequestMessageBuilderTest.java`: `extends AbstractTestNGUnitTest` → `extends AbstractUnitTest`, `@ExtendWith(MockitoExtension.class)` (has `@Mock` fields). `@BeforeClass setup()` → `@BeforeEach setup()` (drop `MockitoAnnotations`). Inner `Assert.assertEquals` inside `doAnswer` lambda → JUnit `Assertions`.
3. Unit tests (all remaining `actions/*`, `model/*`, `random/*`, `validation/*`, root `OpenApi*Test`):
   - `org.testng.annotations.Test` → `org.junit.jupiter.api.Test`.
   - `org.testng.Assert` / static `org.testng.Assert.*` → `org.junit.jupiter.api.Assertions` / static `org.junit.jupiter.api.Assertions.*`; fix order to `assertEquals(expected, actual)` (e.g. `Assert.assertEquals(result.getName(), "X")` → `Assertions.assertEquals("X", result.getName())`). NOTE: some files have reversed asserts (`assertEquals(fixture.getRootContextPath(), ROOT)` → `assertEquals(ROOT, fixture.getRootContextPath())`).
   - `@BeforeMethod` → `@BeforeEach` (`org.junit.jupiter.api.BeforeEach`); `@AfterMethod` → `@AfterEach` (`org.junit.jupiter.api.AfterEach`).
   - `assertThrows` from TestNG static import → `Assertions.assertThrows` (`org.junit.jupiter.api.Assertions.assertThrows`). Signature identical (both 1-arg and 2-arg forms exist).
   - `@Mock` fields with `MockitoAnnotations.openMocks` in `@BeforeClass`/`@BeforeMethod`: add `@ExtendWith(MockitoExtension.class)`, delete the `openMocks` line, delete the setup method if it becomes empty.
   - `MockitoAnnotations` import: drop (handled by extension).
   - `OpenApiRepositoryTest.withOpenApiSpecifications_shouldExecuteReplacementWithoutInterruption` uses `runAsync` + `assertThat` + `mock()` — keep logic, it does NOT need MockitoExtension (mocks created via `mock()` not `@Mock`).
4. `CredentialValidatorTest`-style params: `junit-jupiter-params` is already in pom template if used; openapi DOES use `@ParameterizedTest` (DataProvider ITs) so params is justified (unlike knative/docker/k8s where only kubernetes had CredentialValidatorTest). Assert in PR body it is used.
5. ITs: see special section above. `integration/` classes use `@Test` on class + `@DirtiesContext` + static spec fields — keep `@DirtiesContext`, drop class `@Test`, add runner params.
6. `SampleOpenApiProcessor.java`: helper, no TestNG imports — leave as is unless it imports `org.testng`.

## Verify

- `grep -rn "org.testng\|TestNG\|SkipException" connectors/citrus-openapi/src/test --include="*.java"` must return nothing.
- `./mvnw test -o -pl connectors/citrus-openapi -am` green (unit tests incl. xml/yaml/groovy loader tests, parameterized `OpenApiTestDataGeneratorTest`, `OpenApiRepositoryTest` concurrency test).
- `./mvnw verify -o -pl connectors/citrus-openapi` — ITs use in-memory HttpServer + Spring (no external cluster) and must RUN (do not skip). Ensure failsafe picks the ITs up via the new `maven-failsafe-plugin` block; do not rename any `*IT.java`. If ITs fail due to parallel execution (ports, shared specs), add `@Isolated` (cf. k8s `AbstractKubernetesIT`) — but prefer `@Isolated` only where needed (ITs use `@DirtiesContext` + `SocketUtils.findAvailableTcpPort`, so ports should be isolated).
- No `junit-jupiter-params` import leftovers beyond `ParameterizedTest` methods + DataProvider ITs.

## Constraints

- Test-only diff. No production code, no new dependencies beyond the POM template (plus `junit-jupiter-params`, justified above for the data providers).
- Keep commit/branch naming exactly as above to match the #1873 series.
- PR body must list verification steps (`./mvnw test -o -pl connectors/citrus-openapi -am` and `./mvnw verify -o -pl connectors/citrus-openapi` output, noting ITs ran with in-memory HttpServer).
