# Full-repository test distribution benchmark after partition overhead reduction

## 105-module verify with eight workers

On 2026-11-25 (Europe/Chisinau), all 105 modules passed Maven 3.9.8 `verify` in one serial reactor in **30m 04s** (1,804.37s, measured by the run harness). The run used Temurin Java 21 on macOS, commit `69cebd1454bc`, and the Kafka timeout and Cucumber Groovy port fixes recorded with this benchmark entry. All eight workers used `ALWAYS` fork mode and reused their caches from the earlier eight-worker run. The coordinator recorded 209 completed partitions, zero retries, 18,986 worker passes, and three worker skips. Both the Kafka timeout test and the Cucumber Groovy `Inline config` scenario passed on workers. [Run dashboard](http://127.0.0.1:54592/ui/index.html#session/sess-89a3fadf-b73/waterfall) · [Run audit](/tmp/citrus-full-verify-fixed.YYOeBP/final-audit.json)

### Comparison with GitHub Actions

Both totals below come from Surefire and Failsafe `TEST-*.xml` reports. The GitHub Actions baseline used Ubuntu, Java 21, and commit `6fc097db1c`. [CI report](https://github.com/asuruceanu/citrus/actions/runs/45484682676/job/138774535801) · [Per-module comparison](/tmp/citrus-full-verify-fixed.YYOeBP/comparison.json)

| Run | Surefire passed | Failsafe passed | Total passed | Failed | Skipped |
|---|---:|---:|---:|---:|---:|
| GitHub Actions | 17,860 | 1,023 | 18,883 | 0 | 9 |
| Eight-worker verify | 17,878 | 1,126 | **19,004** | **0** | **3** |
| Difference | +18 | +103 | **+121** | 0 | **−6** |

TestNG Failsafe XML accounts for 100 of the additional passes. The CI Maven log records 101 passes in the default TestNG integration execution, and its uploaded TestNG Failsafe XML contains six passes across the module. The worker run reports 106 TestNG Failsafe passes in XML. The other 21 additional passes appear in the TestNG, Kafka, Test Distribution launcher, and worker Surefire reports and the Maven plugin integration Failsafe reports. The Test Distribution worker module reports eight platform-dependent skips on Linux and two on macOS. Both runs skipped the same Docker integration test. [CI runtime job](https://github.com/asuruceanu/citrus/actions/runs/45484682676/job/138768118889)

### Command and worker settings

The run used the CI Maven flags and `playwright-runtimes` profile with the Test Distribution Maven extension loaded. The local thresholds were zero, and local fallback was disabled.

```sh
./mvnw --no-transfer-progress --fail-at-end verify \
  -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Pplaywright-runtimes \
  -Dcitrus.test.distribution.enabled=true \
  -Dcitrus.test.distribution.coordinatorUrl=http://127.0.0.1:54592 \
  -Dcitrus.test.distribution.fallbackLocal=false \
  -Dcitrus.test.distribution.minRemoteColdUnits=0 \
  -Dcitrus.test.distribution.minRemoteDurationMs=0 \
  -Dcitrus.test.distribution.projectKey=verify8-yyoebp-verify
```

The harness temporarily applied `SINGLE` to the previously retried modules that needed it, including JMS. HTTP, SOAP, and REST Docs received `src/test/resources` in their worker workspaces. The harness restored the module POMs, Maven extension configuration, and Docker VM async I/O setting after the run. [Worker overrides](/tmp/citrus-full-verify-fixed.YYOeBP/worker-overrides.json) · [Maven log](/tmp/citrus-full-verify-fixed.YYOeBP/verify.log)

## 104-module Maven 3.9.8 warm benchmark

Measured on 2026-11-24 (Europe/Chisinau) at `d53a0b0dfc` with the four worker-parity test edits applied. Both runs used Java 21, Maven 3.9.8 in serial reactor mode, and the same 104-module selection. The main pass selected 98 modules and the follow-on selected the six Test Distribution modules. Only `runtime/citrus-testng` was excluded. The warm run reused the coordinator database and blob cache and all six worker caches. All six workers ran in `ALWAYS` fork mode and local fallback was disabled.

| Phase | Modules | Tests per run | Passed | Skipped | Failures / errors | Six-worker warm time | Clean local time |
|---|---:|---:|---:|---:|---:|---:|---:|
| Main | 98 | 17,290 | 17,290 | 0 | 0 / 0 | 981.67s | 537.03s |
| Test Distribution follow-on | 6 | 468 | 466 | 2 | 0 / 0 | 76.33s | 67.89s |
| **Combined** | **104** | **17,758** | **17,756** | **2** | **0 / 0** | **1,058.00s** | **604.92s** |

The six-worker warm run took 453.08s longer than the clean local run, a 74.9% increase over local. The main pass accounted for 444.64s of the difference, and the follow-on accounted for 8.44s. The table sums the two Maven command runtimes measured by the harness's monotonic process timer.

The clean local baseline ran under `caffeinate -is` with the same Maven arguments as the earlier baseline. The macOS power log recorded no sleep or wake events during the rerun. This supersedes the earlier local timing, which included a sleep interruption. Both warm Maven invocations and both clean local Maven invocations exited zero. [Sleep and timing check](/private/var/folders/h5/j6sjlnjn40d1vh48jv0pk5kw0000gn/T/citrus-partition-bench.m4dOSuIZ/measured/local-104-caffeinated-dXpHxh9i/sleep-and-timing-check.json)

### Worker placement

The main session matched 16,896 test records to their assigned workers across 128 completed partitions. It recorded another 389 passes under `local-host`. Maven recorded five additional JUnit 4 passes that did not appear in the coordinator session. The follow-on matched all 468 records, including the two skips, to their assigned workers across seven completed partitions. Every assigned record matched its partition ID and attempt. Both phases used all six workers and had zero retries. The coordinator served 5,823 of 5,824 blob requests from cache in the main pass and all 369 in the follow-on.

| Suite or module | Tests verified on workers | Workers |
|---|---:|---|
| Kubernetes | 99 | 1, 2, 3, 4, 5, 6 |
| Knative | 27 | 2, 5, 6 |
| Selenium | 306 | 1, 2, 3, 4, 5 |
| JBang CLI `RunTest` | 8 | 2, 3 |
| JBang connector `JBangTestEngineTest` | 5 | 1 |
| Quarkus `DemoApplicationTest` | 1 | 3 |
| Six Test Distribution modules | 468 | 1, 2, 3, 4, 5, 6 |

`JBangTestEngineTest#testRunDirectory()` passed on `worker-1` and in the local baseline. [Main warm dashboard](http://127.0.0.1:18080/ui/index.html#session/sess-420e63ae-e9c/waterfall) · [Follow-on dashboard](http://127.0.0.1:18080/ui/index.html#session/sess-55c2de96-968/waterfall)

### Local parity

All 104 Maven modules matched on test totals and outcomes, including the 68 modules with tests. All 17,758 test cases also matched by module, class, normalized test identity, and outcome. The comparison used fresh Surefire XML and recovered blank test names in the warm XML from the coordinator's JUnit identities. The two runs skipped the same macOS-disabled worker recovery cases. The Maven console grouped four `ReceiveMessageActionTest` cases under different class labels, and the XML matched those cases individually.

[Comparison data](/private/var/folders/h5/j6sjlnjn40d1vh48jv0pk5kw0000gn/T/citrus-partition-bench.m4dOSuIZ/measured/local-104-caffeinated-dXpHxh9i/comparison-104.json)

## Maven reactor parallelism with six workers

The experiment used the same `d53a0b0dfc` revision, Java 21, 95-module selection, and six forked workers as the serial warm baseline. Both parallel runs reused the blob caches and used separate fresh project history keys. All three runs used the 1,000 ms default estimate for the same 1,250 work units. Reported overlap is time with active worker partitions from at least two Maven modules.

| Reactor setting | Maven wall | Change vs serial | Tests | Failures / errors / skips | Completed partitions | Worker overlap between modules |
|---|---:|---:|---:|---:|---:|---:|
| Serial | 18m 08s | — | 16,858 | 0 / 0 / 0 | 143 | 0.0s |
| `-T 2` | 14m 16s | 3m 52s faster (21.3%) | 16,858 | 0 / 0 / 0 | 143 | 546.5s |
| `-T 4` | 13m 39s | 4m 29s faster (24.7%) | 16,858 | 0 / 0 / 0 | 143 | 641.2s |

Maven confirmed the requested thread counts. All 95 selected modules passed in both parallel runs. All three runs matched the 16,853 worker records by module, work unit, JUnit ID, and outcome. Both parallel runs completed with zero retries and verified the JBang CLI (8/8), JBang connector (5/5), and Quarkus (1/1) suites on workers.

`-T 4` was 37s faster than `-T 2` (4.3%). Partitions from up to four modules ran concurrently with `-T 4`, and three or more overlapped for 418.6s. Both parallel runs peaked at six active workers. Blob hits were 6,805/6,808 at `-T 2` and 6,807/6,808 at `-T 4`.

Camel's module wall time was 286s serial, 279s at `-T 2`, and 319s at `-T 4`. Its worker dispatch span was 283.7s, 274.9s, and 308.1s respectively, with 10 partitions in each run. Spring Integration took 19.1s serial, 272s at `-T 2`, and 76s at `-T 4`. Each setting has one measured run, so the observed gain includes variation in individual module times.

[`-T 2` dashboard waterfall](http://127.0.0.1:18080/ui/index.html#session/sess-f979683c-e86/waterfall) · [Comparison data](/private/var/folders/h5/j6sjlnjn40d1vh48jv0pk5kw0000gn/T/citrus-partition-bench.m4dOSuIZ/measured/warm-six-workers-t2-20261124-200052/t2-comparison.json)

[`-T 4` dashboard waterfall](http://127.0.0.1:18080/ui/index.html#session/sess-d5cf2135-445/waterfall) · [Comparison data](/private/var/folders/h5/j6sjlnjn40d1vh48jv0pk5kw0000gn/T/citrus-partition-bench.m4dOSuIZ/measured/warm-six-workers-t4-20261124-201545/t4-comparison.json)

## Maven 4 trial and connector worker checks

Maven 4.0.0-rc-7 ran serially without `-T`. The first attempt stopped with offline dependency-resolution errors in OpenAPI and Cucumber Testcontainers. The coordinator recorded zero test failures among 16,111 tests. Targeted Maven 4 preflight checks passed after the dependencies were fetched with published checksums. The full retry included Selenium and was stopped on request before completion. These attempts provide no completed Maven 4 timing comparison.

The Maven 3.9.8 wrapper and Maven extension configuration were verified after the stop. The temporary Maven 4 runtime was removed, and the Docker VM async I/O limit returned to 65,536.

| Connector | Maven 3.9.8 tests passed on worker | Completed partitions | Worker |
|---|---:|---:|---|
| Selenium | 306 | 2 | `worker-1` |
| Kubernetes | 99 | 2 | `worker-1` |
| Knative | 27 | 2 | `worker-1` |

The connector checks ran Maven's `test` phase with local fallback disabled. Test records matched their completed partitions and worker IDs. Kubernetes live-cluster integration tests are separately opt-in and the configured cluster endpoint refused the readiness connection. Selenium, Kubernetes, and Knative are already registered in the checked-in Maven reactor. Their omission came from the benchmark's exclusion list.

[Selenium worker session](http://127.0.0.1:18080/ui/index.html#session/sess-17ca7421-a04/waterfall) · [Kubernetes worker session](http://127.0.0.1:18080/ui/index.html#session/sess-25ca9e52-c3d/waterfall) · [Knative worker session](http://127.0.0.1:18080/ui/index.html#session/sess-42c4e8f3-517/waterfall)

### Benchmark commands with the connectors and Test Distribution modules

The warm pass selects 98 modules, including Selenium, Kubernetes, and Knative. A separate distributed pass adds the six Test Distribution modules, bringing coverage to 104 of 105 reactor modules. Only `runtime/citrus-testng` remains excluded. The historical runs below retain their original selections. Use the benchmark checkout at `d53a0b0dfc` with the worker-parity test fixes applied, six workers connected, and the Test Distribution Maven extension loaded.

```sh
./mvnw -B -o test -fae \
  -pl '!runtime/citrus-testng,!tools/citrus-test-distribution-protocol,!tools/citrus-test-distribution-ui,!tools/citrus-test-distribution-coordinator,!tools/citrus-test-distribution-worker,!tools/citrus-test-distribution,!tools/maven/citrus-test-distribution-maven-extension' \
  -Dcitrus.test.distribution.enabled=true \
  -Dcitrus.test.distribution.brokerUrl=http://127.0.0.1:18080 \
  -Dcitrus.test.distribution.fallbackLocal=false \
  -Dmaven.test.failure.ignore=true
```

After the warm pass, keep the extension loaded and the six workers connected for this follow-on command. The command disables local fallback and history-based local execution thresholds. The separate project key keeps the Test Distribution module history scoped to this phase.

```sh
./mvnw -B -o test -fae \
  -pl tools/citrus-test-distribution-protocol,tools/citrus-test-distribution-ui,tools/citrus-test-distribution-coordinator,tools/citrus-test-distribution-worker,tools/citrus-test-distribution,tools/maven/citrus-test-distribution-maven-extension \
  -Dcitrus.test.distribution.enabled=true \
  -Dcitrus.test.distribution.brokerUrl=http://127.0.0.1:18080 \
  -Dcitrus.test.distribution.fallbackLocal=false \
  -Dcitrus.test.distribution.minRemoteDurationMs=0 \
  -Dcitrus.test.distribution.minRemoteColdUnits=0 \
  -Dcitrus.test.distribution.projectKey=org.citrusframework:citrus:distribution-self-tests
```

The runner saves separate Maven logs, elapsed time, and coordinator session data for this phase. For the local baseline, use the same six-module selection with `-Dcitrus.test.distribution.enabled=false` and restore the original Maven extension configuration.

### Test Distribution worker probe before the fixes

A separate Maven 3.9.8 probe ran all six modules with local fallback disabled. All 468 test records matched the worker ID and attempt of their completed partition. One worker, `distribution-self-probe`, completed all seven partitions in a Maven run of 1 minute 27 seconds.

| Module | Passed | Failed | Skipped |
|---|---:|---:|---:|
| Protocol | 148 | 0 | 0 |
| UI | 1 | 0 | 0 |
| Coordinator | 86 | 0 | 0 |
| Worker | 141 | 1 | 2 |
| JUnit launcher | 59 | 6 | 0 |
| Maven extension | 23 | 1 | 0 |
| **Total** | **458** | **8** | **2** |

The first attempt stopped at a failing worker assertion. The retry used `-Dmaven.test.failure.ignore=true` to reach all six modules, so Maven returned zero even though the coordinator marked the session failed. [Worker probe session](http://127.0.0.1:18080/ui/index.html#session/sess-27d4bb8c-dab/waterfall)

### Test Distribution worker parity after the fixes

The launcher tests now enter a scoped launcher role so they can exercise their own coordinator and workers inside the outer worker JVM. The workspace isolation test verifies that an inner partition restores the enclosing JVM's `basedir`. The Maven extension test verifies Git execution in a temporary repository it initializes itself.

The strict Maven 3.9.8 rerun completed seven partitions across six workers with local fallback disabled. Every test record matched the worker ID and attempt of its completed partition. The fresh local run matched all 468 test cases and outcomes. Both runs passed 466 tests, skipped the same two recovery cases disabled on macOS, and failed zero tests. All eight failures from the original probe passed. Maven reported 1 minute 15 seconds on workers and 1 minute 22 seconds locally for this standalone six-module check. [Six-worker verification session](http://127.0.0.1:18080/ui/index.html#session/sess-ff2db45a-a79/waterfall)

## 95-module warm rerun with six workers

Measured on 2026-11-24 (Europe/Chisinau) at `d53a0b0dfc` on `codex/distribute-local-only-tests` using Java 21 and the same 95-module selection as the saved baselines. The run reused the prior coordinator database, blob cache, and six worker caches. The six worker processes ran in `ALWAYS` fork mode and advertised Docker and Quarkus workspace capabilities. Local fallback was disabled. Maven was configured to continue through test failures. Its test summaries recorded zero failures or errors.

The run completed all 95 selected modules. The coordinator session passed and remains available in the [dashboard](http://127.0.0.1:18080/ui/index.html#session/sess-bd0866e2-c29).

### Worker placement

Every event below matched the worker ID on a completed partition with the same partition ID and attempt. The three suites recorded zero local events and zero retries.

| Suite | Explicitly opted in | Passed on workers | Local baseline | Cold baseline | Worker and partition |
|---|---:|---:|---:|---|---|
| JBang CLI `RunTest` | 8 | 8/8 | 8/8 passed | Module skipped | `worker-5` · `part-8c114815-9f9`, `worker-6` · `part-5da2a3b6-b4e` |
| JBang connector `JBangTestEngineTest` | 4 | 5/5 | 5/5 passed | 5/5 passed, four on the Maven host | `worker-3` · `part-888d5f3e-461` |
| Quarkus `DemoApplicationTest` | 1 | 1/1 | 1/1 passed | 1/1 passed, with no worker assignment recorded | `worker-1` · `part-fa58aedb-924` |

The connector's unannotated `shouldResolveJBangEngine()` method also passed on the worker. The JBang CLI suite generated zero standalone `JUnitCitrusTest` partitions. The older `testRunDirectory()` failure shown under `local-host` comes from a revision where the method was marked `@LocalOnly`. In that run, JBang changed `.claude` in the checkout's absolute path to `/claude` and found no tests. In this session the method passed on `worker-3`.

### Timing, coverage, and parity

| Run | Maven wall | Process wall | Modules with tests | Tests | Failures + errors | Skipped tests |
|---|---:|---:|---:|---:|---:|---:|
| Saved local | 517.00s | 517.97s | 58 | 16,723 | 1 | 0 |
| Saved cold | 615.00s | 616.44s | 54 | 16,495 | 3 | 0 |
| Previous worker-only warm | 800.00s | 801.40s | 59 | 16,858 | 1 | 0 |
| Latest six-worker warm | 1,088.00s | 1,090.11s | 59 | 16,858 | 0 | 0 |

The latest warm run took 571.00s longer than local, 473.00s longer than cold, and 288.00s longer than the previous warm run. The 48 modules with identical outcomes and successful Maven statuses in all three local, cold, and latest-warm runs totaled 407.21s, 421.58s, and 862.47s respectively.

| Baseline compared with latest warm | Test-bearing modules in both | Same test count | Same tests, failures, errors, and skips | Test-bearing modules only in warm |
|---|---:|---:|---:|---:|
| Local | 58 | 50 | 50 | 1 |
| Cold | 54 | 49 | 48 | 5 |
| Previous worker-only warm | 59 | 59 | 58 | 0 |

- Compared with local, counts changed in `citrus-api`, `citrus-jbang`, `citrus-mcp-server`, `citrus-playwright`, `citrus-schema-generator`, `citrus-validation-text`, `citrus-xml`, and `citrus-zookeeper`. `citrus-agent` ran only in the warm rerun. The local `ListTest.shouldListTests` failure passed in the latest warm run.
- Compared with cold, counts changed in `citrus-api`, `citrus-playwright`, `citrus-validation-text`, `citrus-xml`, and `citrus-zookeeper`. All 28 `citrus-testcontainers` tests passed after three cold failures. Five additional modules ran tests in warm: `citrus-agent`, `citrus-cucumber-testcontainers`, `citrus-jbang`, `citrus-mcp-server`, and `citrus-schema-generator`.
- The previous warm run matched the latest test counts in every module. The only outcome difference was `ListTest.shouldListTests`, which passed in the latest run.

The coordinator recorded 16,853 passing worker events, 143 completed partition attempts across 58 modules, and zero retries. All six workers completed 23 to 25 attempts each. Maven's extra five tests came from `citrus-junit`, which had no coordinator assignments. The cache supplied 6,793 of 6,808 requested blobs (99.8%).

The saved cold and local baselines used `61a39ad8f4`. The previous warm run used `0cebc699ed`. The latest run used the project-key history scope `key:org.citrusframework:citrus`, separate from the earlier `name:citrus` records. The run reused blob caches and dispatched more work than the previous warm run, which recorded 92 attempts and 506 local events. Apache Camel took 286.00s in the latest run, up from 112.00s cold and 202.00s local. The timings compare different revisions and scheduling histories.

The initial readiness check counted stale worker records from the saved database. The retry verified current heartbeats from exactly six workers before starting Maven. The Maven extension file and Docker VM async I/O limit were restored, and the six benchmark workers were stopped after completion.

[Raw Maven log](/private/var/folders/h5/j6sjlnjn40d1vh48jv0pk5kw0000gn/T/citrus-partition-bench.m4dOSuIZ/measured/warm-six-workers-165701/maven.log) · [Per-module comparison and worker assignments](/private/var/folders/h5/j6sjlnjn40d1vh48jv0pk5kw0000gn/T/citrus-partition-bench.m4dOSuIZ/measured/warm-six-workers-165701/comparison.json)

### Follow-up on scheduling and Camel time

The 99.8% hit rate counts digests already in the coordinator's blob store when the launcher checked which blobs to upload. It does not measure launcher packaging, worker-local cache hits, JVM startup, or test execution. The elapsed run was warm for blob uploads and used a different scope for duration estimates and local bypass.

All 1,250 submitted work units had the 1,000 ms default estimate in the latest warm run. The prior `name:citrus` scope still returned duration estimates for all 1,250 keys. An offline replay held the latest work units, capabilities, and partition settings fixed. It reproduced the observed 143 partitions in every module with the recorded estimates. Using the prior-scope estimates reduced the prediction to 108 partitions before applying any local bypass. Of the 16 newly dispatched modules, 15 had complete prior-scope estimates totaling under the 2,000 ms bypass threshold. `citrus-mail` totaled 2,051 ms. This is evidence for a scope effect on both partition sizing and local bypass. It does not establish the exact wall time another run would take.

Camel had 10 partitions in cold, previous warm, and latest warm. It took 286s in the latest run, compared with 112s cold, 202s local, and 249s in the previous warm. Camel accounted for 174s of the 473s gap versus cold (37%) and 84s of the 571s gap versus local (15%). The remaining gaps were 299s and 487s respectively. The final Camel partition in the latest run was assigned about 207s before its first recorded test event. Its reported test durations summed to 1.27s. The saved telemetry does not break that interval down into worker setup, class or JVM initialization, execution waits, and result reporting.

A targeted local reproduction of `JBangTestEngineTest#testRunDirectory` failed when the checkout path contained `.codex`. Maven passed the same test through a dot-free path to the same checkout. The method passes an absolute directory in the package list, and `JBangTestEngine` replaces every dot with a slash as it converts packages to paths. The failing log shows `/.codex/` turning into `//codex/` before JBang exits with “No tests to run in current directory.”

[Per-module scheduler replay](/private/var/folders/h5/j6sjlnjn40d1vh48jv0pk5kw0000gn/T/citrus-partition-bench.m4dOSuIZ/measured/warm-six-workers-165701/history-scope-scheduler-replay.json) · [Dotted-path test log](/tmp/citrus-jbang-dotted-investigation.log) · [Dot-free control log](/tmp/citrus-jbang-dotless-investigation.log)

## Previous worker-only warm comparison

The worker-only warm rerun was measured on 2026-11-24 (Europe/Chisinau) at `0cebc699ed` (PR #9). The saved cold and local baselines were measured at `61a39ad8f4` (PR #6). The revisions differ, so changes in test inventories and code can affect both parity and timings.

## Setup

- macOS 27.2.1, Mac16,5, 14 logical CPUs, 36 GiB RAM. Eclipse Temurin Java 21.0.12.1 and Maven Wrapper 3.9.8.
- Rancher Desktop Docker 29.9.2, aarch64 Alpine VM with four virtual CPUs and approximately 7.75 GiB RAM. Set `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock` on the Maven host and workers. Rancher Desktop already had 36 running containers during diagnostics. The Linux VM async I/O limit was temporarily raised from 65,536 to 262,144 for each compared pass.
- The baseline setup installed all 105 reactor snapshots, including the UI bundle. The retry refreshed all 105 snapshots offline from the warm revision.
- Six workers ran in `ALWAYS` fork mode with `docker=true` and separate caches. They advertised both version 1 workspace support and Quarkus bootstrap remapping. The worker launch classpath included the JUnit artifacts resolved by the JBang module.
- The cold pass started with an empty file-backed HSQLDB database and empty broker and worker caches. The warm retry reused the running broker, its persisted history, and the six worker caches.
- Broker default partition size 10, adaptive target 5,000 ms, wave limit 2 per eligible worker, and warm local threshold 2,000 ms. Cold local bypass disabled. The distributed passes disabled local fallback.
- Each pass used Java 21 and the same `./mvnw -o test -fae` reactor selection. The ten exclusions were Kubernetes, Knative, Selenium, TestNG, the five distribution artifacts (including UI), and the distribution Maven extension.
- The first warm attempt used an older installed Kubernetes connector. The schema-generator assertion caused Maven to skip JBang. The assertion passed after the snapshot refresh. The retry kept the same selection and used `-Dmaven.test.failure.ignore=true` to continue after test failures. Outcome counts come from Surefire summaries.
- The JBang CLI `RunTest`, four external-process methods in `JBangTestEngineTest`, and Quarkus `DemoApplicationTest` use `@RemoteOnly` at the warm revision. The reactor ran with distribution enabled and no class or method opt-in properties.
- Raw logs, session API snapshots, and broker database are in `/private/var/folders/h5/j6sjlnjn40d1vh48jv0pk5kw0000gn/T/citrus-partition-bench.m4dOSuIZ/measured`. The rerun's Maven log is `warm-worker-only-retry.log` and its worker logs are under `warm-worker-only-retry/`.

## Wall time and coverage

| Run | Maven wall | Process wall | Modules with tests | Tests | Failures + errors | Skipped tests | Maven exit | Delta vs today's local |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Local Surefire | 517.00s | 517.97s | 58 | 16,723 | 1 | 0 | 1 | +0.00s |
| Distributed cold | 615.00s | 616.44s | 54 | 16,495 | 3 | 0 | 1 | +98.00s |
| Distributed warm, worker-only suites | 800.00s | 801.40s | 59 | 16,858 | 1 | 0 | 0 | +283.00s |
| First worker-only attempt (stale dependency) | 717.00s | 719.22s | 56 | 16,610 | 1 | 0 | 1 | +200.00s |
| Previous warm rerun (nested-launcher fix) | 511.00s | 513.03s | 58 | 16,723 | 1 | 0 | 1 | -6.00s |
| First warm rerun (before nested-launcher fix) | 479.00s | 480.74s | 58 | 16,723 | 8 | 0 | 1 | -38.00s |
| Original distributed warm (before fix) | 49.74s | 51.29s | 7 | 11,916 | 6 | 0 | 1 | -467.26s |
| Prior local | 239s | — | 47 | — | 1 | — | — | — |
| Prior distributed | 316s | — | 47 | — | 1 | — | — | — |

Cold distribution took +98.00s versus today's local run. The warm rerun took +283.00s versus local and +185.00s versus cold. Different outcomes and coverage qualify the total times. The prior distribution premium was 77s. [Prior benchmark](/Users/asuruceanu/development/projects/test-distribution/citrus/.claude/worktrees/core-test-distribution-run-ece2bb/docs/superpowers/metrics/2026-11-23-full-repo-test-distribution-comparison.md)

For the 47 test-bearing modules present in the prior report and all three current passes, the sum of module times was 234.90s local and 309.70s distributed previously. Today's sum was 251.43s local, 469.17s cold, and 450.57s warm. Module sums exclude Maven time outside those modules. The prior module times were rounded.

The 48 modules that succeeded and matched outcomes in all three passes totaled 407.21s locally, 421.58s cold, and 623.90s warm.
Warm timing on that matched cohort changed by +202.33s versus cold.

## Session and partition breakdown

| Metric | Distributed cold | Distributed warm rerun |
|---|---:|---:|
| Broker session | sess-1f908cb1-901 | sess-559dba0c-806 |
| Status | FAILED | FAILED |
| Broker session wall | 614.38s | 799.43s |
| Tests in broker session summary | 16,489 | 16,853 |
| Remote test event records | 16,485 | 16,347 |
| Locally reported test event records | 4 | 506 |
| Remote passed / failed / skipped events | 16,482 / 3 / 0 | 16,346 / 1 / 0 |
| Local passed / failed / skipped events | 4 / 0 / 0 | 506 / 0 / 0 |
| Partition IDs | 136 | 92 |
| Dispatched partition attempts | 136 | 92 |
| Persisted partition attempts | 136 | 92 |
| Retries | 0 | 0 |
| Modules with dispatched partitions | 52 | 42 |
| Cached of requested broker blobs | 4,237 / 5,102 | 5,368 / 5,392 |
| citrus-base dispatched attempts | 12 | 12 |

**Warm-only local bypasses inferred from dispatch and Maven results: 15.** `citrus-sql`, `citrus-validation-binary`, `citrus-docker`, `citrus-agent-connector`, `citrus-jmx`, `citrus-mail`, `citrus-rmi`, `citrus-spring-boot-starter-test`, `citrus-test-api-core`, `citrus-maven-plugin`, `citrus-cucumber-jms`, `citrus-cucumber-playwright`, `citrus-cucumber-selenium`, `citrus-validation-yaml`, `citrus-metadata-processor`.

The inference uses modules that executed in every pass with matching cold and warm outcomes. Test events from the local reporter have the worker ID `local-host` and lack a Maven module field. The Maven totals also include tests run by module opt-outs and other launcher paths outside broker telemetry. Event counts count recorded attempts.

| Worker | Cold attempts | Cold tests | Cold busy | Cold summed test time | Warm attempts | Warm tests | Warm busy | Warm summed test time |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| worker-1 | 24 | 1,123 | 272.45s | 248.85s | 17 | 1,369 | 381.74s | 86.92s |
| worker-2 | 23 | 1,078 | 104.47s | 63.06s | 16 | 1,184 | 320.65s | 92.42s |
| worker-3 | 21 | 866 | 119.09s | 99.80s | 15 | 1,365 | 279.19s | 110.71s |
| worker-4 | 22 | 11,131 | 191.57s | 163.44s | 14 | 813 | 392.65s | 168.84s |
| worker-5 | 24 | 1,277 | 144.15s | 116.67s | 15 | 10,983 | 348.96s | 90.63s |
| worker-6 | 22 | 1,010 | 157.31s | 138.99s | 15 | 633 | 399.24s | 113.20s |

Cold aggregate worker busy time was 989.05s and summed test duration was 830.82s. Partition states were COMPLETED=136.

Warm aggregate worker busy time was 2,122.43s and summed test duration was 662.72s. Partition states were COMPLETED=92.

Busy time sums each assigned partition's elapsed wall time from the broker timestamps. Summed test time can overlap when tests run in parallel within a partition. Their difference provides an aggregate indicator of per-partition work.

## Per-module parity

54 modules executed tests in every pass. 48 of those matched on test count, failures, errors, and skips. 5 test-bearing modules ran in only a subset of the passes.

Outcomes differed in `citrus-playwright`, `citrus-testcontainers`, `citrus-api`, `citrus-zookeeper`, `citrus-xml`, `citrus-validation-text`.

Test coverage differed in `citrus-agent`, `citrus-cucumber-testcontainers`, `citrus-jbang`, `citrus-mcp-server`, `citrus-schema-generator`.

Maven status differed in `citrus-testcontainers`.

Local Maven skipped one module. `ListTest.shouldListTests` failed in `citrus-jbang`.

Cold Maven skipped eight modules. The three Testcontainers failures were `ComposeTest.shouldLoadTestcontainersActions`, `StartContainerTest.shouldLoadTestcontainersActions`, and `StartMongoDBTest.shouldLoadTestcontainersActions`. All 28 tests in that module passed in the local and warm runs.

Warm Maven finished all 95 selected modules. `ListTest.shouldListTests` was the only test failure.

**JUnit Jupiter regression.** The original warm session `sess-d7149987-76d` bypassed the outer Jupiter suite and dispatched six nested fixture partitions. It recorded six outer failures and four intentionally failing fixture events. All 21 JUnit Jupiter tests passed in one worker partition on the retry.

| Run pair | Modules with tests in both | Matching test counts | Matching outcomes | Only in first | Only in second |
|---|---:|---:|---:|---:|---:|
| Local / Cold | 54 | 54 | 53 | 4 | 0 |
| Local / Warm | 58 | 50 | 50 | 0 | 1 |
| Cold / Warm | 54 | 49 | 48 | 0 | 5 |

Local and cold differed in `citrus-testcontainers`.

Local and warm differed in `citrus-playwright`, `citrus-api`, `citrus-zookeeper`, `citrus-xml`, `citrus-jbang`, `citrus-mcp-server`, `citrus-schema-generator`, `citrus-validation-text`.

Cold and warm differed in `citrus-playwright`, `citrus-testcontainers`, `citrus-api`, `citrus-zookeeper`, `citrus-xml`, `citrus-validation-text`.

Matching outcomes compare the number of tests, failures, errors, and skips per module.

| Test-count difference vs local | Local | Warm | Net change | Test sources differ between revisions |
|---|---:|---:|---:|---|
| `citrus-api` | 460 | 468 | +8 | Yes |
| `citrus-xml` | 81 | 83 | +2 | Yes |
| `citrus-validation-text` | 25 | 31 | +6 | Yes |
| `citrus-playwright` | 515 | 536 | +21 | Yes |
| `citrus-zookeeper` | 5 | 39 | +34 | Yes |
| `citrus-schema-generator` | 20 | 23 | +3 | Yes |
| `citrus-jbang` | 124 | 126 | +2 | Yes |
| `citrus-mcp-server` | 63 | 67 | +4 | Yes |

## Worker-only suites

| Suite | Expected methods | Passed on workers | Failed / skipped on workers | Local events | Worker IDs | Forked partitions |
|---|---:|---:|---:|---:|---|---|
| JBang CLI `RunTest` | 8 | 8 | 0 / 0 | 0 | worker-3, worker-6 | part-404959d3-dc4, part-4cb86f82-3a0 |
| JBang connector `JBangTestEngineTest` | 4 | 4 | 0 / 0 | 0 | worker-4 | part-1ed72cd0-908 |
| Quarkus `DemoApplicationTest` | 1 | 1 | 0 / 0 | 0 | worker-6 | part-f25247b9-b62 |

The broker recorded zero standalone `JUnitCitrusTest` partitions for `citrus-jbang`. Placement was verified by joining each test event to its partition ID and attempt.

## Full reactor results

Each test cell displays tests / failures+errors / skipped tests. A dash means Maven recorded no test summary. Partition counts are dispatched attempts.

| Module | Local tests / F+E / S | Cold tests / F+E / S | Warm tests / F+E / S | Local wall | Cold wall | Warm wall | Cold partitions | Warm partitions | Maven status L/C/W |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Citrus | — | — | — | 0.03s | 0.03s | 0.02s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Core | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Core :: API | 460 / 0 / 0 | 460 / 0 / 0 | 468 / 0 / 0 | 8.87s | 9.31s | 10.86s | 7 | 4 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Core :: Base | 11,331 / 0 / 0 | 11,331 / 0 / 0 | 11,331 / 0 / 0 | 12.44s | 21.84s | 69.00s | 12 | 12 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Utils | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Utils :: Test Support | — | — | — | 0.03s | 0.04s | 0.10s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Core :: Spring | 98 / 0 / 0 | 98 / 0 / 0 | 98 / 0 / 0 | 3.43s | 6.46s | 6.83s | 7 | 2 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: JUnit Jupiter | 21 / 0 / 0 | 21 / 0 / 0 | 21 / 0 / 0 | 1.75s | 3.50s | 4.04s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: XML | 81 / 0 / 0 | 81 / 0 / 0 | 83 / 0 / 0 | 10.19s | 8.79s | 15.07s | 4 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: YAML | 40 / 0 / 0 | 40 / 0 / 0 | 40 / 0 / 0 | 9.50s | 7.51s | 12.75s | 4 | 2 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: Hamcrest | 177 / 0 / 0 | 177 / 0 / 0 | 177 / 0 / 0 | 1.54s | 3.75s | 3.73s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: JSON | 268 / 0 / 0 | 268 / 0 / 0 | 268 / 0 / 0 | 1.84s | 4.74s | 4.16s | 4 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: Plaintext | 25 / 0 / 0 | 25 / 0 / 0 | 31 / 0 / 0 | 1.00s | 2.73s | 1.66s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Groovy | 74 / 0 / 0 | 74 / 0 / 0 | 74 / 0 / 0 | 9.33s | 8.04s | 17.18s | 5 | 4 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: SQL | 103 / 0 / 0 | 103 / 0 / 0 | 103 / 0 / 0 | 1.99s | 5.18s | 3.08s | 2 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: Groovy | 28 / 0 / 0 | 28 / 0 / 0 | 28 / 0 / 0 | 1.64s | 3.67s | 3.82s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: XML | 425 / 0 / 0 | 425 / 0 / 0 | 425 / 0 / 0 | 3.16s | 6.82s | 6.21s | 6 | 2 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Spring Integration | 197 / 0 / 0 | 197 / 0 / 0 | 197 / 0 / 0 | 6.60s | 8.82s | 8.95s | 3 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: JMS | 92 / 0 / 0 | 92 / 0 / 0 | 92 / 0 / 0 | 19.17s | 21.56s | 21.00s | 3 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: Binary | 2 / 0 / 0 | 2 / 0 / 0 | 2 / 0 / 0 | 0.42s | 2.13s | 1.04s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: HTTP | 280 / 0 / 0 | 280 / 0 / 0 | 280 / 0 / 0 | 8.02s | 10.81s | 17.06s | 5 | 5 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: OpenAPI | 352 / 0 / 0 | 352 / 0 / 0 | 352 / 0 / 0 | 5.07s | 7.12s | 12.20s | 4 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: Docker | 62 / 0 / 0 | 62 / 0 / 0 | 62 / 0 / 0 | 2.48s | 4.72s | 3.31s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: JBang | 32 / 0 / 0 | 32 / 0 / 0 | 32 / 0 / 0 | 11.72s | 8.95s | 14.57s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: Playwright | 515 / 0 / 0 | 515 / 0 / 0 | 536 / 0 / 0 | 2.12s | 9.91s | 8.82s | 6 | 4 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: Agent | 11 / 0 / 0 | 11 / 0 / 0 | 11 / 0 / 0 | 1.96s | 4.38s | 2.78s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: Testcontainers | 28 / 0 / 0 | 28 / 3 / 0 | 28 / 0 / 0 | 29.98s | 147.00s | 41.35s | 1 | 1 | SUCCESS / FAILURE / SUCCESS |
| Citrus :: Endpoints :: Apache Camel | 308 / 0 / 0 | 308 / 0 / 0 | 308 / 0 / 0 | 202.00s | 112.00s | 249.00s | 10 | 10 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: SSH | 62 / 0 / 0 | 62 / 0 / 0 | 62 / 0 / 0 | 1.59s | 4.36s | 4.48s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: FTP | 65 / 0 / 0 | 65 / 0 / 0 | 65 / 0 / 0 | 5.12s | 8.22s | 7.33s | 3 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: GraphQL | 186 / 0 / 0 | 186 / 0 / 0 | 186 / 0 / 0 | 2.77s | 5.92s | 5.19s | 3 | 2 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: gRPC | 165 / 0 / 0 | 165 / 0 / 0 | 165 / 0 / 0 | 7.55s | 8.54s | 13.71s | 3 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: JMX | 18 / 0 / 0 | 18 / 0 / 0 | 18 / 0 / 0 | 1.09s | 3.49s | 1.92s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Kafka | 124 / 0 / 0 | 124 / 0 / 0 | 124 / 0 / 0 | 2.54s | 5.58s | 5.27s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Mail | 45 / 0 / 0 | 45 / 0 / 0 | 45 / 0 / 0 | 1.25s | 3.90s | 2.29s | 2 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: RMI | 18 / 0 / 0 | 18 / 0 / 0 | 18 / 0 / 0 | 1.46s | 3.80s | 1.83s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Websocket | 28 / 0 / 0 | 28 / 0 / 0 | 28 / 0 / 0 | 2.22s | 5.43s | 4.47s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: SOAP WebServices | 206 / 0 / 0 | 206 / 0 / 0 | 206 / 0 / 0 | 4.76s | 7.96s | 13.26s | 5 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Vert.x | 28 / 0 / 0 | 28 / 0 / 0 | 28 / 0 / 0 | 31.32s | 33.63s | 33.97s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Zookeeper | 5 / 0 / 0 | 5 / 0 / 0 | 39 / 0 / 0 | 6.08s | 8.53s | 18.94s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: JUnit | 5 / 0 / 0 | 5 / 0 / 0 | 5 / 0 / 0 | 0.38s | 0.59s | 0.55s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Spring Boot Starter Test | 8 / 0 / 0 | 8 / 0 / 0 | 8 / 0 / 0 | 0.61s | 2.64s | 0.92s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Quarkus Extension | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Quarkus Runtime | — | — | — | 0.32s | 0.42s | 0.41s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Quarkus Deployment | — | — | — | 0.04s | 0.11s | 0.11s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Quarkus Integration Tests | 1 / 0 / 0 | 1 / 0 / 0 | 1 / 0 / 0 | 3.25s | 2.88s | 6.51s | 0 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Cucumber | 35 / 0 / 0 | 35 / 0 / 0 | 35 / 0 / 0 | 2.38s | 5.05s | 4.82s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Main | 21 / 0 / 0 | 21 / 0 / 0 | 21 / 0 / 0 | 3.30s | 5.36s | 5.19s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Test API Generator | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Test API Generator :: API Core | 49 / 0 / 0 | 49 / 0 / 0 | 49 / 0 / 0 | 1.14s | 3.77s | 2.02s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Test API Generator :: Codegen | 27 / 0 / 0 | 27 / 0 / 0 | 27 / 0 / 0 | 12.92s | 15.98s | 16.07s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Test API Generator :: Maven Plugin | 22 / 0 / 0 | 22 / 0 / 0 | 22 / 0 / 0 | 3.43s | 5.99s | 6.14s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Restdocs | 20 / 0 / 0 | 20 / 0 / 0 | 20 / 0 / 0 | 0.93s | 3.24s | 3.50s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Test Generator | 53 / 0 / 0 | 53 / 0 / 0 | 53 / 0 / 0 | 1.79s | 4.23s | 4.05s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Docs Generator | 6 / 0 / 0 | 6 / 0 / 0 | 6 / 0 / 0 | 0.84s | 2.98s | 2.95s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: AssertJ | 129 / 0 / 0 | 129 / 0 / 0 | 129 / 0 / 0 | 1.30s | 3.79s | 3.56s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Schema Generator | 20 / 0 / 0 | — | 23 / 0 / 0 | 5.59s | — | 10.20s | 0 | 1 | SUCCESS / SKIPPED / SUCCESS |
| Citrus :: Tools :: Maven | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Maven :: Plugin | 17 / 0 / 0 | 17 / 0 / 0 | 17 / 0 / 0 | 4.74s | 7.08s | 5.28s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Maven :: Plugin Integration | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Maven :: Agent Plugin | — | — | — | 1.90s | 2.22s | 2.06s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Core | — | — | — | 0.06s | 0.15s | 0.14s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Http | — | — | — | 0.06s | 0.19s | 0.16s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Groovy | — | — | — | 0.06s | 0.15s | 0.13s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Camel | — | — | — | 0.06s | 0.21s | 0.17s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: JDBC | — | — | — | 0.21s | — | 0.17s | 0 | 0 | SUCCESS / SKIPPED / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: JMS | 12 / 0 / 0 | 12 / 0 / 0 | 12 / 0 / 0 | 0.63s | 2.84s | 1.29s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Kafka | — | — | — | 0.06s | 0.17s | 0.16s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Kubernetes | — | — | — | 0.06s | 0.26s | 0.22s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Knative | — | — | — | 0.06s | 0.46s | 0.24s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: OpenAPI | — | — | — | 0.07s | 0.31s | 0.23s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Playwright | 31 / 0 / 0 | 31 / 0 / 0 | 31 / 0 / 0 | 1.22s | 3.92s | 1.86s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Selenium | 9 / 0 / 0 | 9 / 0 / 0 | 9 / 0 / 0 | 1.04s | 3.53s | 1.72s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Testcontainers | 21 / 0 / 0 | — | 21 / 0 / 0 | 27.45s | — | 40.14s | 0 | 1 | SUCCESS / SKIPPED / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: All | — | — | — | 0.09s | — | 0.09s | 0 | 0 | SUCCESS / SKIPPED / SUCCESS |
| Citrus :: Validation :: YAML | 87 / 0 / 0 | 87 / 0 / 0 | 87 / 0 / 0 | 1.02s | 2.81s | 1.82s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: JBang | 124 / 1 / 0 | — | 126 / 1 / 0 | 10.40s | — | 11.79s | 0 | 2 | FAILURE / SKIPPED / SUCCESS |
| Citrus :: Tools :: Agent | — | — | 55 / 0 / 0 | — | — | 3.84s | 0 | 1 | SKIPPED / SKIPPED / SUCCESS |
| Citrus :: Tools :: Archetypes | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Archetypes :: Quickstart | — | — | — | 0.02s | 0.01s | 0.01s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Archetypes :: Quickstart JMS | — | — | — | 0.01s | 0.01s | 0.01s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Archetypes :: Quickstart SOAP | — | — | — | 0.01s | 0.01s | 0.01s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: MCP Server | 63 / 0 / 0 | — | 67 / 0 / 0 | 2.80s | — | 6.15s | 0 | 1 | SUCCESS / SKIPPED / SUCCESS |
| Citrus :: Utils :: Metadata Processor | 3 / 0 / 0 | 3 / 0 / 0 | 3 / 0 / 0 | 0.54s | 2.06s | 1.24s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Catalog | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Catalog :: Core | — | — | — | 0.01s | 0.03s | 0.02s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Catalog :: Endpoints | — | — | — | 0.09s | 0.42s | 0.23s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Catalog :: BOM | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Catalog :: Schema | — | — | — | 1.06s | — | 0.98s | 0 | 0 | SUCCESS / SKIPPED / SUCCESS |

## Bottlenecks and interpretation

- The longest local modules were `citrus-camel` (202.00s), `citrus-vertx` (31.32s), `citrus-testcontainers` (29.98s), `citrus-cucumber-testcontainers` (27.45s), `citrus-jms` (19.17s).
- The longest cold modules were `citrus-testcontainers` (147.00s), `citrus-camel` (112.00s), `citrus-vertx` (33.63s), `citrus-base` (21.84s), `citrus-jms` (21.56s).
- The longest warm modules were `citrus-camel` (249.00s), `citrus-base` (69.00s), `citrus-testcontainers` (41.35s), `citrus-cucumber-testcontainers` (40.14s), `citrus-vertx` (33.97s).
- The largest warm time premiums on modules executed in all passes were `citrus-base` (+56.56s), `citrus-camel` (+47.00s), `citrus-zookeeper` (+12.86s), `citrus-testcontainers` (+11.36s), `citrus-http` (+9.04s).
- The diagnostic launch initially dropped workers before recording tests because the Maven runtime classpath omitted JUnit Platform. A separate `citrus-api` probe passed all 460 tests after adding the matching JUnit jars. The measured database and worker caches were created after that probe.
- The preliminary local attempt reached Redpanda with the Docker VM's async I/O quota exhausted. That attempt was stopped and excluded from timings. The temporary limit applied equally to the compared passes.
- During the cold pass the Docker VM reported 537 MiB available and used all 2 GiB of swap. Kafka containers showed delayed heartbeats. Testcontainers startup and readiness contributed substantial timing variance.
- Aggregate worker busy time grew while summed test duration fell. Camel and Base had the largest warm time premiums against the local baseline. The run measures scheduling, setup, and caches together, so it provides no separate wall-time estimate for JAR inspection or macOS reference probes.

The Maven extension file and Docker VM async I/O limit were restored. The six workers were stopped. The broker remains active (PID `54610`). [Worker-only warm dashboard](http://127.0.0.1:18080/ui/index.html#session/sess-559dba0c-806)
