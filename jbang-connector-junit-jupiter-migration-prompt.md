# Prompt: migrate citrus-jbang-connector tests from TestNG to JUnit Jupiter (#1873)

Use this prompt with Claude (paste into chat with repo access to citrusframework/citrus).

---

## Task

Migrate `connectors/citrus-jbang-connector` unit tests from TestNG to JUnit Jupiter, following issue #1873 and the merged sibling PRs (#1897 binary, #1898 yaml, #1900 assertj, #1899 text, citrus-base, citrus-spring).

Branch from `main` as `feature/1873-junit-jupiter-jbang-connector`. Commit as `test(#1873): migrate citrus-jbang-connector tests from TestNG to JUnit Jupiter`. Do NOT touch production code.

## Scope (9 test files, no ITs in this module)

```
connectors/citrus-jbang-connector/src/test/java/org/citrusframework/jbang/JBangSettingsTest.java
connectors/citrus-jbang-connector/src/test/java/org/citrusframework/jbang/UnitTestSupport.java
connectors/citrus-jbang-connector/src/test/java/org/citrusframework/jbang/actions/JBangActionBuilderTest.java
connectors/citrus-jbang-connector/src/test/java/org/citrusframework/jbang/actions/JBangActionTest.java
connectors/citrus-jbang-connector/src/test/java/org/citrusframework/jbang/engine/JBangTestEngineTest.java
connectors/citrus-jbang-connector/src/test/java/org/citrusframework/jbang/xml/AbstractXmlActionTest.java
connectors/citrus-jbang-connector/src/test/java/org/citrusframework/jbang/xml/JBangTest.java
connectors/citrus-jbang-connector/src/test/java/org/citrusframework/jbang/yaml/AbstractYamlActionTest.java
connectors/citrus-jbang-connector/src/test/java/org/citrusframework/jbang/yaml/JBangTest.java
```

## Reference pattern (copy exactly from validation-text PR #1899)

POM (`validation/citrus-validation-text/pom.xml` at commit `26509c2009`):

- Add `maven-surefire-plugin` + `maven-failsafe-plugin` blocks with `surefire-junit-platform` provider dependency (copy versions `${maven.surefire.plugin.version}` / `${maven.failsafe.plugin.version}`).
- Test deps: remove `org.citrusframework:citrus-testng`, add `org.citrusframework:citrus-junit-jupiter` (test scope). Add `org.junit.platform:junit-platform-launcher` (test scope). Do NOT add `junit-jupiter-params` (removed in #1899 review — it is unused).
- Verify against `core/citrus-base/pom.xml` for exact plugin/dependency shape.

`src/test/resources/junit-platform.properties` (copy from validation-text):

```
junit.jupiter.execution.parallel.enabled = true
junit.jupiter.execution.parallel.mode.default = same_thread
junit.jupiter.execution.parallel.mode.classes.default = concurrent
```

Include the two-line comment header from the sibling files explaining sequential override via `-Djunit.jupiter.execution.parallel.enabled=false`.

## Conversion rules

1. Base classes:
   - `org.citrusframework.testng.AbstractTestNGUnitTest` → `org.citrusframework.junit.jupiter.AbstractUnitTest` (`xml/AbstractXmlActionTest.java`, `yaml/AbstractYamlActionTest.java`). Keep `setupMocks()` logic, keep `createTestContext()` override as is.
   - `UnitTestSupport.java` is a hand-rolled base (fields `testContextFactory`, `context`, `@BeforeMethod prepareTest()`). Either delete it and extend `AbstractUnitTest` in `JBangActionTest` (preferred — `AbstractUnitTest` already provides both fields plus `@BeforeEach prepareTest()`), or convert its `@BeforeMethod` → `@BeforeEach` (`org.junit.jupiter.api.BeforeEach`). Do not keep any `org.testng` import.
2. Annotations:
   - `org.testng.annotations.Test` → `org.junit.jupiter.api.Test`.
   - `@BeforeClass` (non-static, e.g. `setupMocks`) → `@BeforeEach`. `@BeforeClass public static beforeClass()` network guard → `@BeforeAll` (`org.junit.jupiter.api.BeforeAll`, must stay static) — or `@BeforeEach` if simpler; keep static + `@BeforeAll` to preserve once-per-class semantics.
   - `@BeforeMethod` → `@BeforeEach`.
3. Assertions: `org.testng.Assert` → `org.junit.jupiter.api.Assertions`. Fix argument order to `assertEquals(expected, actual)` — TestNG call sites may have them swapped; the #1873 series corrected these (see citrus-base `40bf93e9dd`).
4. Skips: `org.testng.SkipException` has no JUnit equivalent. Replace `throw new SkipException("... network ...")` with `org.junit.jupiter.api.Assumptions.abort("...")` (same message). Applies to `actions/JBangActionTest#beforeClass`, `xml/JBangTest#beforeClass`, `yaml/JBangTest#beforeClass`.
5. Expected exceptions → `assertThrows` (cf. #1899 review):
   - `actions/JBangActionTest`: 3 cases (`testJBangCommandValidateExitCode`, `testUnknownApp`, `testUnknownCommand`). For `testJBangCommandValidateExitCode`, also assert the message: capture the exception and `Assertions.assertTrue(e.getMessage().contains("Expected exit code"))` (original `expectedExceptionsMessageRegExp = "Error while running JBang script or file. Expected exit code -1, but was 0"` — use `contains` on the stable substring, not a full regexp match).
   - `engine/JBangTestEngineTest#testRunNoMatch` (`expectedExceptions = CitrusRuntimeException.class`) → `Assertions.assertThrows(CitrusRuntimeException.class, () -> runTestEngine(configuration))`.
   - No try/catch + `Assertions.fail()` in this module — if you add any, use `assertThrows` instead.

## Per-file checklist

- `JBangSettingsTest`: pure asserts. Swap import, fix `assertEquals(expected, actual)` order (e.g. `Assert.assertEquals(result, List.of(...))` → `Assertions.assertEquals(List.of(...), result)`).
- `JBangActionBuilderTest`: same, 3 asserts.
- `JBangTestEngineTest`: swap import, convert `testRunNoMatch`, fix `assertEquals(JBangTestEngine.class, engine.getClass())` order.
- `actions/JBangActionTest`: extends `UnitTestSupport` → extend `AbstractUnitTest` (delete `UnitTestSupport` if unused afterwards). Convert `@BeforeClass` guard to `@BeforeAll` + `Assumptions.abort`. Convert 3 `expectedExceptions`. Fix `assertEquals`/`assertTrue`/`assertFalse` imports.
- `xml/AbstractXmlActionTest`, `yaml/AbstractYamlActionTest`: swap base class + `@BeforeClass` → `@BeforeEach` on `setupMocks` (`MockitoAnnotations.openMocks(this)` stays).
- `xml/JBangTest`, `yaml/JBangTest`: swap imports, `@BeforeClass` guard → `@BeforeAll` + `Assumptions.abort`. Fix ~15 `assertEquals` orders per file (note `assertEquals(result.getActionCount(), 3L)` → `assertEquals(3L, result.getActionCount())`, `assertEquals(action.getArgs().size(), 1L)` → `assertEquals(1L, ...)`).

## Verify

- `grep -r "org.testng\|TestNG\|SkipException" connectors/citrus-jbang-connector/src/test --include="*.java"` must return nothing.
- `./mvnw test -pl connectors/citrus-jbang-connector -am` green. Note: JBang execution tests require network (`TestUtils.isNetworkReachable()`); offline they abort via assumptions, which counts as skipped, not failed.
- `./mvnw verify -pl connectors/citrus-jbang-connector -Dit.test=JBangTest` if ITs exist later; currently no `*IT.java` in this module.

## Constraints

- Test-only diff. No production code, no new dependencies beyond the template.
- Keep commit/branch naming exactly as above to match the #1873 series.
- PR body must list verification steps (`./mvnw test -pl connectors/citrus-jbang-connector` output).
