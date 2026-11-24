# Citrus TestNG Test Distribution Support Implementation Plan

> Execute the tasks in one session and request a fresh whole-branch review after verification.

**Goal:** Enable `runtime/citrus-testng` unit, integration, acceptance, and expected-failure tests to execute on Citrus Test Distribution workers through `org.junit.support:testng-engine` on `surefire-junit-platform`.

**Architecture:** Manage `org.junit.support:testng-engine:1.1.0` in the root POM, add it in `test` scope to `runtime/citrus-testng`, and drop `<surefire.provider>surefire-testng</surefire.provider>` so TestNG tests run through JUnit Platform `Launcher` and `DistributionLauncherInterceptor`. Make three targeted fixes so JUnit Platform + worker execution handles every TestNG construct cleanly: child-first partition classloading for `org.junit.support.*`, discovery-time counting of empty `MethodSource` containers in `DiscoveredWorkExtractor`, and `ITestNGMethod.setInvocationCount` synchronization for multi-source `@CitrusTestSource` methods in Citrus's TestNG method interceptors.

**Tech Stack:** Java 21, Maven 3.9+, Maven Surefire/Failsafe 3.5.6 (`surefire-junit-platform`), JUnit Platform / Jupiter 6.1.3, TestNG 7.12.0, `org.junit.support:testng-engine` 1.1.0, AssertJ.

**Spec:** `docs/superpowers/specs/2026-11-24-testng-test-distribution-design.md`

## Global Constraints

- Implement in isolated worktree `/Users/asuruceanu/.local/share/opencode/worktree/46a00c/citrus-testng-distribution` on branch `feature/test-distribution-testng` (based on `incubator/test-distribution`).
- Keep root JUnit (`6.1.3`), TestNG (`7.12.0`), and Maven Surefire/Failsafe (`3.5.6`) versions unchanged.
- Do not alter `citrus-testng`'s published `compile` or `runtime` transitive dependencies (`org.junit.support:testng-engine` must be `<scope>test</scope>`).
- Do not introduce `org.testng.*` compile/runtime dependencies into `citrus-test-distribution` or `citrus-test-distribution-worker`.
- Do not automatically rewrite `surefire-testng` provider dependencies in `citrus-test-distribution-maven-extension` or modify historical benchmark files under `docs/superpowers/metrics/`.
- All new Java files must carry the standard Citrus Apache 2.0 license header.

## Review Focus

1. **Worker parent classloader also contains `org.junit.support.*` classes/resources (e.g. in-JVM `ForkMode.AUTO` tests)** — `PartitionClassLoader` must still load `org.junit.support.*` child-first from the partition classpath alongside `org.testng.*`, while continuing to share `org.junit.platform.*`, `org.junit.jupiter.*`, and `META-INF/services/org.junit.*` with the worker. *(Pinned in Task 1)*
2. **Tag/group-filtered class whose excluded method is a discovery-time empty `MethodSource` container (`@DataProvider`, `invocationCount > 1`, `@TestTemplate`, `@ParameterizedTest`)** — `DiscoveredWorkExtractor.partiallySelectedClasses` must detect the excluded container method and split the class into method-level `WorkUnitSpec`s instead of emitting a whole-class `WorkUnitSpec` that runs the excluded method on the worker. *(Pinned in Task 2)*
3. **Multi-source `@CitrusTestSource` (`names`, `sources`, `packageScan`) in `TestNGCitrusSupport` / `TestNGCitrusSpringSupport` under `testng-engine`** — Citrus XML sources must execute exactly once with accurate JUnit reporting. TestNG multiplies duplicate method instances by invocation count, so use one scheduling mechanism. *(Pinned in Task 3)*
4. **Distributed TestNG `@DataProvider` dynamic `[invoc:N]` events and mixed-group (`ShouldFailGroup`) classes on workers across `WorkerForkMode.AUTO` and `WorkerForkMode.ALWAYS`** — dynamic invocations must resolve to the parent `WorkUnitSpec` without triggering unmatched-unit errors and replay cleanly on the host, while group-excluded methods in mixed classes never run on the worker. *(Pinned in Task 4)*
5. **Published `citrus-testng` dependency tree and all four Maven test executions (`surefire:test`, `failsafe:default`, `failsafe:acceptance-tests`, `failsafe:failure-tests`)** — `citrus-testng` must keep `testng-engine` out of `compile`/`runtime` scope while passing all executions both locally and against a live coordinator + worker. *(Pinned in Tasks 3 & 4)*

---

### Task 1: Isolate `org.junit.support.*` in `IsolatedPartitionRunner.PartitionClassLoader`

**Files:**
- Modify: `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/IsolatedPartitionRunner.java:1085-1096,1295-1305`
- Test: `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/PartitionClassLoaderTest.java`

**Interfaces:**
- Consumes: package-private `IsolatedPartitionRunner.PartitionClassLoader(URL[] urls, ClassLoader parent)`
- Produces: narrowed parent-first JUnit prefixes (`org.junit.platform.`, `org.junit.jupiter.` for classes; `org/junit/platform/`, `org/junit/jupiter/`, `META-INF/services/org.junit.` for resources) so `org.junit.support.*` loads child-first from partition URLs

- [x] **Step 1: Write the failing test in `PartitionClassLoaderTest`**

Add `shouldIsolateJUnitSupportWhileSharingPlatformAndJupiter` in `tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/PartitionClassLoaderTest.java`:
- Compile or generate minimal valid class bytes for `org.junit.support.probe.SupportProbe` (using `javax.tools.ToolProvider.getSystemJavaCompiler()`) into two separate directories (`parentDir` and `partitionDir`), plus `org/junit/support/probe/source.txt` containing `"parent"` in `parentDir` and `"partition"` in `partitionDir`.
- Construct `URLClassLoader parentWithSupport = new URLClassLoader(new URL[]{ parentDir.toUri().toURL() }, WORKER)` and `IsolatedPartitionRunner.PartitionClassLoader loader = new IsolatedPartitionRunner.PartitionClassLoader(new URL[]{ partitionDir.toUri().toURL() }, parentWithSupport)`.
- Assert:
  ```java
  Class<?> partitionLoaded = Class.forName("org.junit.support.probe.SupportProbe", false, loader);
  Class<?> parentLoaded = Class.forName("org.junit.support.probe.SupportProbe", false, parentWithSupport);
  assertThat(partitionLoaded).isNotSameAs(parentLoaded);
  assertThat(partitionLoaded.getClassLoader()).isSameAs(loader);
  try (InputStream in = loader.getResourceAsStream("org/junit/support/probe/source.txt")) {
      assertThat(in).isNotNull();
      assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("partition");
  }
  assertThat(Class.forName(TestEngine.class.getName(), false, loader)).isSameAs(TestEngine.class);
  assertThat(Class.forName(Test.class.getName(), false, loader)).isSameAs(Test.class);
  ```

- [x] **Step 2: Run test to verify it fails**

Run: `./mvnw test -Dtest=PartitionClassLoaderTest#shouldIsolateJUnitSupportWhileSharingPlatformAndJupiter -pl tools/citrus-test-distribution-worker`
Expected: FAIL — `partitionLoaded` is the same class as `parentLoaded` (`parentWithSupport`) because `shouldDelegateToParent` currently matches `name.startsWith("org.junit.")`.

- [x] **Step 3: Narrow JUnit parent delegation in `IsolatedPartitionRunner.PartitionClassLoader`**

In `tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/IsolatedPartitionRunner.java`:
- Replace `"org/junit/"` in `SHARED_RESOURCE_PREFIXES` with `"org/junit/platform/"` and `"org/junit/jupiter/"` (keeping `"META-INF/services/org.junit."` intact).
- In `shouldDelegateToParent(String name)`, replace `name.startsWith("org.junit.")` with `name.startsWith("org.junit.platform.") || name.startsWith("org.junit.jupiter.")`.

- [x] **Step 4: Run worker unit tests to verify they pass**

Run: `./mvnw test -pl tools/citrus-test-distribution-worker`
Expected: PASS (`0` failures).

- [x] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution-worker/src/main/java/org/citrusframework/distribution/worker/IsolatedPartitionRunner.java \
        tools/citrus-test-distribution-worker/src/test/java/org/citrusframework/distribution/worker/PartitionClassLoaderTest.java
git commit -m "fix(test-distribution-worker): load org.junit.support child-first in PartitionClassLoader"
```

---

### Task 2: Count Discovery-Time Empty `MethodSource` Containers in `DiscoveredWorkExtractor`

**Files:**
- Modify: `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/DiscoveredWorkExtractor.java:151-160`
- Test: `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/DiscoveredWorkExtractorTest.java`

**Interfaces:**
- Consumes: `DiscoveredWorkExtractor.partiallySelectedClasses(Launcher launcher, TestPlan filteredPlan)` and `DiscoveredWorkExtractor.extract(TestPlan plan, long defaultDurationMs, Set<String> partiallySelectedClassIds)`
- Produces: `partiallySelectedClasses(...)` detects filtered out discovery-time empty `MethodSource` containers (such as TestNG `@DataProvider` / `invocationCount` methods and Jupiter `@TestTemplate` / `@ParameterizedTest` methods)

- [x] **Step 1: Write the failing test in `DiscoveredWorkExtractorTest`**

Add `splitsClassesWithExcludedTemplateMethods` in `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/DiscoveredWorkExtractorTest.java`:
- Define static fixture `MixedTemplateClass` with two `@TestTemplate` methods (`fastTemplate()` and `@Tag("slow") slowTemplate()`) backed by a `@ExtendWith(SingleInvocationProvider.class)` `TestTemplateInvocationContextProvider` (at discovery time, JUnit Platform represents `@TestTemplate` methods as `Type.CONTAINER` with `MethodSource` and empty children — identical to `testng-engine` data-driven/multi-invocation methods).
- Discover `MixedTemplateClass` with `TagFilter.excludeTags("slow")`:
  ```java
  Launcher launcher = LauncherFactory.create();
  TestPlan plan = launcher.discover(LauncherDiscoveryRequestBuilder.request()
          .selectors(DiscoverySelectors.selectClass(MixedTemplateClass.class))
          .filters(TagFilter.excludeTags("slow"))
          .build());
  Set<String> partial = DiscoveredWorkExtractor.partiallySelectedClasses(launcher, plan);
  DiscoveredWorkExtractor.Extraction extraction = DiscoveredWorkExtractor.extract(plan, 1000L, partial);
  assertThat(partial).isNotEmpty();
  assertThat(extraction.remote()).singleElement().satisfies(unit -> {
      assertThat(unit.className()).isEqualTo(MixedTemplateClass.class.getName());
      assertThat(unit.methodName()).isEqualTo("fastTemplate()");
  });
  ```

- [x] **Step 2: Run test to verify it fails**

Run: `./mvnw test -Dtest=DiscoveredWorkExtractorTest#splitsClassesWithExcludedTemplateMethods -pl tools/citrus-test-distribution`
Expected: FAIL — `partial` is empty and `unit.methodName()` is `null` because `countTests` counts `0` for both `planned` and `available`.

- [x] **Step 3: Count leaf `MethodSource` containers in `DiscoveredWorkExtractor.countTests`**

In `tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/DiscoveredWorkExtractor.java`, update `countTests(TestPlan plan, TestIdentifier node, Map<String, Integer> counts)` so a node contributes `1` when `node.isTest()` OR when `node.getSource().orElse(null) instanceof MethodSource && plan.getChildren(node).isEmpty()`.

- [x] **Step 4: Run `DiscoveredWorkExtractorTest` to verify it passes**

Run: `./mvnw test -Dtest=DiscoveredWorkExtractorTest -pl tools/citrus-test-distribution`
Expected: PASS (`0` failures).

- [x] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution/src/main/java/org/citrusframework/distribution/junit/DiscoveredWorkExtractor.java \
        tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/DiscoveredWorkExtractorTest.java
git commit -m "fix(test-distribution): count container test methods when detecting pruned classes"
```

---

### Task 3: Bridge `runtime/citrus-testng` onto JUnit Platform via `org.junit.support:testng-engine`

**Files:**
- Modify: `pom.xml:315-321,634-640`
- Modify: `runtime/citrus-testng/pom.xml:25-65`
- Modify: `runtime/citrus-testng/src/main/java/org/citrusframework/testng/TestNGCitrusMethodInterceptor.java`
- Modify: `runtime/citrus-testng/src/main/java/org/citrusframework/testng/spring/TestNGCitrusSpringMethodInterceptor.java`
- Create: `runtime/citrus-testng/src/test/java/org/citrusframework/testng/TestNGCitrusMethodInterceptorTest.java`, `PlainSourceFixture.java`, and `SpringSourceFixture.java`
- Create: `runtime/citrus-testng/src/test/resources/org/citrusframework/testng/main/second-echo.citrus.it.yaml`

**Interfaces:**
- Consumes: `org.junit.support:testng-engine:1.1.0` (`[engine:testng]`), `ITestNGMethod.setInvocationCount(int)`
- Produces: `runtime/citrus-testng` executing on `surefire-junit-platform` + `testng-engine` (`<scope>test</scope>`), with `TestNGCitrusMethodInterceptor` and `TestNGCitrusSpringMethodInterceptor` scheduling one method instance and using TestNG invocation count to run each resolved source once

- [x] **Step 1: Configure `testng-engine` in `pom.xml` and `runtime/citrus-testng/pom.xml`, and write the failing multi-source interceptor test**

1. In root `pom.xml`:
   - Update line 316 comment from `(runtime/citrus-testng, runtime/citrus-junit) override this property` to `(runtime/citrus-junit overrides this property)`.
   - Add `<testng.engine.version>1.1.0</testng.engine.version>` next to `<testng.version>7.12.0</testng.version>`.
   - Add `<dependencyManagement>` entry for `org.junit.support:testng-engine:${testng.engine.version}` next to `org.testng:testng`.
2. In `runtime/citrus-testng/pom.xml`:
   - Remove `<properties><surefire.provider>surefire-testng</surefire.provider></properties>`.
   - Add `<dependency><groupId>org.junit.support</groupId><artifactId>testng-engine</artifactId><scope>test</scope></dependency>` and `<dependency><groupId>org.junit.platform</groupId><artifactId>junit-platform-launcher</artifactId><scope>test</scope></dependency>`.
3. Use the existing XML sources in `CitrusXmlAnnotationIT` as the Spring regression baseline. Add a focused launcher regression covering both support base classes and recording the exact source names, invocation results, and container failures.

- [x] **Step 2: Record the baseline with the provider switched**

Run the real XML integration fixture and the focused regression. The provider-only baseline passed all seven cases in `CitrusXmlAnnotationIT`. The focused regression ran both sources but reported no distinct invocation IDs for either interceptor.

- [x] **Step 3: Schedule multi-source methods through TestNG invocation count**

In both interceptors, retain a single `IMethodInstance` per input and use `setInvocationCount` to schedule the source count multiplied by the existing invocation count. Preserve an explicit invocation count per source, matching the previous TestNG scheduling behavior. Verify each source runs once with the default count and repeat counts produce distinct successful launcher results.

- [x] **Step 4: Run `runtime/citrus-testng` full `verify` lifecycle and check runtime dependency scope**

Run: `./mvnw verify -pl runtime/citrus-testng`
Expected: PASS — Surefire unit tests and all three Failsafe executions (`default-cli`/`integration-tests`, `acceptance-tests`, and `failure-tests`) pass on `surefire-junit-platform` + `testng-engine` (`0` failures).

Run: `./mvnw dependency:list -DincludeScope=runtime -pl runtime/citrus-testng`
Expected: Output does NOT contain `testng-engine` or `junit-platform`.

- [x] **Step 5: Commit**

```bash
git add pom.xml \
        runtime/citrus-testng/pom.xml \
        runtime/citrus-testng/src/main/java/org/citrusframework/testng/TestNGCitrusMethodInterceptor.java \
        runtime/citrus-testng/src/main/java/org/citrusframework/testng/spring/TestNGCitrusSpringMethodInterceptor.java \
        runtime/citrus-testng/src/test
git commit -m "feat(testng): run citrus-testng on JUnit Platform via testng-engine"
```

---

### Task 4: End-to-End TestNG Worker Distribution Coverage, Live Worker Verification, and Documentation

**Files:**
- Modify: `tools/citrus-test-distribution/pom.xml`
- Create: `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/TestNgDistributionTest.java`
- Modify: `runtime/citrus-testng/src/test/java/org/citrusframework/acceptance/CitrusSpringConfigAT.java`
- Modify: `src/manual/tools-test-distribution.adoc:28-64,705-742`

**Interfaces:**
- Consumes: `DistributionCoordinatorServer`, `DistributionWorkerClient`, `DistributionWorkerConfig`, `DistributionSettings.WorkerForkMode` (`WorkerForkMode.AUTO`, `WorkerForkMode.ALWAYS`), `DistributedLauncher` (via `LauncherFactory`), `org.testng:testng` (`test`), `org.junit.support:testng-engine` (`test`)
- Produces: automated end-to-end regression suite `TestNgDistributionTest` covering TestNG worker execution (`AUTO` and `ALWAYS` fork modes, `@DataProvider` invocations, and `TagFilter` group splitting) plus user guide documentation in `src/manual/tools-test-distribution.adoc`

- [x] **Step 1: Add `test`-scope `testng` and `testng-engine` to `tools/citrus-test-distribution/pom.xml` and write `TestNgDistributionTest`**

1. In `tools/citrus-test-distribution/pom.xml`, add `<scope>test</scope>` dependencies on `org.testng:testng` and `org.junit.support:testng-engine`.
2. Create `tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/TestNgDistributionTest.java` (naming inner TestNG fixture classes `*NgFixture` so Maven Surefire never selects them directly):
   - `executesTestNgDataProviderAndPrunedGroupsOnWorkerInAutoAndAlwaysForkModes`: parameterized over `WorkerForkMode.AUTO` and `WorkerForkMode.ALWAYS`, starts an embedded `DistributionCoordinatorServer` + `DistributionWorkerClient`, sets `citrus.test.distribution.enabled=true`, `minRemoteDurationMs=0`, `minRemoteColdUnits=1`, `fallbackLocal=false`, and executes `LauncherDiscoveryRequest` selecting `DataProviderNgFixture` (`@DataProvider` returning 3 rows) and `MixedGroupNgFixture` (one default `@org.testng.annotations.Test` method + one `@org.testng.annotations.Test(groups = "should-fail")` method that fails if invoked) with `TagFilter.excludeTags("should-fail")`.
   - Assert: `0` test failures, `4` passed test executions (`3` data-provider invocations + `1` non-excluded method), `MixedGroupNgFixture`'s excluded method was never invoked on the worker, and the coordinator recorded completed remote partitions on the worker. Verify the partition actually executes in the shared worker JVM for AUTO and in a separate JVM for ALWAYS.

- [x] **Step 2: Run `tools/citrus-test-distribution` test suite**

Run: `./mvnw test -pl tools/citrus-test-distribution -am`
Expected: PASS (`0` failures, including `TestNgDistributionTest` in both `AUTO` and `ALWAYS` fork modes).

- [x] **Step 3: Document TestNG support in `src/manual/tools-test-distribution.adoc`**

Update `src/manual/tools-test-distribution.adoc` (under **Quickstart with Maven** and **Test Suite Compatibility**) to note that TestNG test suites run on Citrus Test Distribution when executed on `surefire-junit-platform` with `org.junit.support:testng-engine` in `test` scope, mapping TestNG `@Test(groups = ...)` to JUnit Platform tags.

- [x] **Step 4: Verify live distributed `./mvnw verify -pl runtime/citrus-testng` with the Maven extension**

1. Install the modified modules into the local repository without skipping compilation:
   `./mvnw install -DskipTests -pl runtime/citrus-testng,tools/citrus-test-distribution-protocol,tools/citrus-test-distribution-coordinator,tools/citrus-test-distribution-worker,tools/citrus-test-distribution,tools/maven/citrus-test-distribution-maven-extension -am`
2. Register the installed extension in the worktree’s Maven extension configuration for the verification run. Start the local coordinator and worker once per worker mode and run:
   `./mvnw verify -pl runtime/citrus-testng -Dcitrus.test.distribution.enabled=true -Dcitrus.test.distribution.coordinatorUrl=... -Dcitrus.test.distribution.fallbackLocal=false -Dcitrus.test.distribution.minRemoteDurationMs=0 -Dcitrus.test.distribution.minRemoteColdUnits=0`
   Expected: `BUILD SUCCESS`, all 4 executions (`surefire:test`, `failsafe:integration-test`, `failsafe:acceptance-tests`, `failsafe:failure-tests`) pass with remote worker partition assignments and zero failures. Inspect each execution’s reports and coordinator results to show that it ran tests remotely. Verify a known `ShouldFailGroup` test passes in the failure execution and is absent from the default integration execution. Disable both size and duration thresholds so the single-test acceptance execution reaches a worker. Keep the Spring acceptance assertion scoped to its configured action listeners by excluding the runtime endpoint collector.

- [x] **Step 5: Commit**

```bash
git add tools/citrus-test-distribution/pom.xml \
        tools/citrus-test-distribution/src/test/java/org/citrusframework/distribution/junit/TestNgDistributionTest.java \
        src/manual/tools-test-distribution.adoc
git commit -m "test(test-distribution): add TestNG worker distribution coverage and docs"
```

## Verification results

| Run | Unit | Integration | Acceptance | Expected failure | Total |
| --- | ---: | ---: | ---: | ---: | ---: |
| Local | 105 | 101 | 1 | 5 | 212 |
| Live worker, `AUTO` | 105 | 101 | 1 | 5 | 212 |
| Live worker, `ALWAYS` | 105 | 101 | 1 | 5 | 212 |

Every test passed in all three runs. Both distributed runs recorded every test on the designated worker.
`CitrusSpringConfigAT` ran remotely in the acceptance execution. `FailTestNgIT` passed in the expected-failure
execution and was absent from the default integration execution. The automated regression checked worker process IDs
to verify that `AUTO` used the shared worker JVM and `ALWAYS` forked.

The worker and distribution test suites also passed with 145 and 68 tests reported, respectively, including two
worker skips. The `citrus-testng` runtime dependency listing contained neither the TestNG bridge nor JUnit Platform.
