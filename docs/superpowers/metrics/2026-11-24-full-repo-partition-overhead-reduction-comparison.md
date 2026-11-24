# Full-repository test distribution benchmark after partition overhead reduction

Measured on 2026-11-24 (Europe/Chisinau) at Citrus merge commit `61a39ad8f4` (PR #6). The warm rerun includes the module's JUnit Jupiter worker configuration and the distribution launcher fix that keeps nested JUnit launchers with their outer tests.

## Setup

- macOS 27.2.1, Mac16,5, 14 logical CPUs, 36 GiB RAM. Eclipse Temurin Java 21.0.12.1 and Maven Wrapper 3.9.8.
- Rancher Desktop Docker 29.9.2, aarch64 Alpine VM with four virtual CPUs and approximately 7.75 GiB RAM. Set `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock` on the Maven host and workers. Rancher Desktop already had 36 running containers during diagnostics. The Linux VM async I/O limit was raised from 65,536 to 262,144 for all measured passes.
- Installed all 105 reactor SNAPSHOT artifacts including the UI bundle. The online install reached Quarkus successfully, then resumed offline at Citrus Cucumber after the configured private artifact server timed out.
- Installed `CitrusJBangMain` launched the broker and six `ALWAYS` workers with `docker=true` and separate caches. The worker launch classpath includes the JUnit artifacts resolved by the JBang module that its runtime classpath omits. No tracked code was needed for the workaround.
- The cold pass used a fresh file-backed HSQLDB database, an empty broker blob cache, and six empty worker caches. The warm rerun restarted the same six worker IDs with those caches and the same broker DB, which retained the cold and earlier warm histories. Each warm pass reused the caches. Launcher staging and existing Maven artifacts were available to all passes.
- Broker default partition size 10, adaptive target 5,000 ms, wave limit 2 per eligible worker, and warm local threshold 2,000 ms. Cold local bypass disabled. The distributed passes disabled local fallback.
- Each pass used Java 21 and the same `./mvnw -o test -fae` reactor selection. The ten exclusions were Kubernetes, Knative, Selenium, TestNG, the five distribution artifacts (including UI), and the distribution Maven extension.
- Raw logs, session API snapshots, and broker database are in `/private/var/folders/h5/j6sjlnjn40d1vh48jv0pk5kw0000gn/T/citrus-partition-bench.m4dOSuIZ/measured`. The rerun's Maven log is `warm-nested-fix.log` and its worker logs are under `warm-nested-fix/`.

## Wall time and coverage

| Run | Maven wall | Process wall | Modules with tests | Tests | Failures + errors | Skipped tests | Maven exit | Delta vs today's local |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Local Surefire | 517.00s | 517.97s | 58 | 16,723 | 1 | 0 | 1 | +0.00s |
| Distributed cold | 615.00s | 616.44s | 54 | 16,495 | 3 | 0 | 1 | +98.00s |
| Distributed warm rerun | 511.00s | 513.03s | 58 | 16,723 | 1 | 0 | 1 | -6.00s |
| First warm rerun (before nested-launcher fix) | 479.00s | 480.74s | 58 | 16,723 | 8 | 0 | 1 | -38.00s |
| Original distributed warm (before fix) | 49.74s | 51.29s | 7 | 11,916 | 6 | 0 | 1 | -467.26s |
| Prior local | 239s | — | 47 | — | 1 | — | — | — |
| Prior distributed | 316s | — | 47 | — | 1 | — | — | — |

Cold distribution took +98.00s versus today's local run. The warm rerun took -6.00s versus local and -104.00s versus cold. Different outcomes and coverage qualify the total times. The prior distribution premium was 77s. [Prior benchmark](/Users/asuruceanu/development/projects/test-distribution/citrus/.claude/worktrees/core-test-distribution-run-ece2bb/docs/superpowers/metrics/2026-11-23-full-repo-test-distribution-comparison.md)

For the 47 test-bearing modules present in the prior report and all three current passes, the sum of module times was 234.90s local and 309.70s distributed previously. Today's sum was 251.43s local, 469.17s cold, and 366.88s warm. Module sums exclude Maven time outside those modules. The prior module times were rounded.

The 53 modules that succeeded and matched outcomes in all three passes totaled 435.46s locally, 460.86s cold, and 363.79s warm.
Warm timing on that matched cohort changed by -97.07s versus cold.

## Session and partition breakdown

| Metric | Distributed cold | Distributed warm rerun |
|---|---:|---:|
| Broker session | sess-1f908cb1-901 | sess-3feba5a5-c31 |
| Status | FAILED | FAILED |
| Broker session wall | 614.38s | 510.88s |
| Tests in broker session summary | 16,489 | 16,717 |
| Remote test event records | 16,485 | 16,010 |
| Locally reported test event records | 4 | 707 |
| Remote passed / failed / skipped events | 16,482 / 3 / 0 | 16,009 / 1 / 0 |
| Local passed / failed / skipped events | 4 / 0 / 0 | 707 / 0 / 0 |
| Partition IDs | 136 | 93 |
| Dispatched partition attempts | 136 | 93 |
| Persisted partition attempts | 136 | 93 |
| Retries | 0 | 0 |
| Modules with dispatched partitions | 52 | 39 |
| Cached of requested broker blobs | 4,237 / 5,102 | 4,770 / 4,773 |
| citrus-base dispatched attempts | 12 | 12 |

**Sixteen modules bypassed remote dispatch in the latest warm run**, based on Maven results and broker assignments. `citrus-validation-hamcrest`, `citrus-validation-text`, `citrus-sql`, `citrus-validation-binary`, `citrus-docker`, `citrus-agent-connector`, `citrus-jmx`, `citrus-rmi`, `citrus-spring-boot-starter-test`, `citrus-test-api-core`, `citrus-maven-plugin`, `citrus-cucumber-jms`, `citrus-cucumber-playwright`, `citrus-cucumber-selenium`, `citrus-validation-yaml`, `citrus-metadata-processor`.

The inference uses modules that executed in every pass with matching cold and warm outcomes. Test events from the local reporter have the worker ID `local-host` and lack a Maven module field. The Maven totals also include tests run by module opt-outs and other launcher paths outside broker telemetry. Event counts count recorded attempts.

| Worker | Cold attempts | Cold tests | Cold busy | Cold summed test time | Warm attempts | Warm tests | Warm busy | Warm summed test time |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| worker-1 | 24 | 1,123 | 272.45s | 248.85s | 16 | 1,074 | 212.66s | 190.13s |
| worker-2 | 23 | 1,078 | 104.47s | 63.06s | 17 | 1,107 | 154.31s | 147.35s |
| worker-3 | 21 | 866 | 119.09s | 99.80s | 16 | 1,128 | 120.37s | 104.41s |
| worker-4 | 22 | 11,131 | 191.57s | 163.44s | 16 | 950 | 115.43s | 96.58s |
| worker-5 | 24 | 1,277 | 144.15s | 116.67s | 14 | 10,916 | 140.00s | 121.25s |
| worker-6 | 22 | 1,010 | 157.31s | 138.99s | 14 | 835 | 137.15s | 115.10s |

Cold aggregate worker busy time was 989.05s and summed test duration was 830.82s. Partition states were COMPLETED=136.

Warm aggregate worker busy time was 879.92s and summed test duration was 774.83s. Partition states were COMPLETED=93.

Busy time sums each assigned partition's elapsed wall time from the broker timestamps. Summed test time can overlap when tests run in parallel within a partition. Their difference provides an aggregate indicator of per-partition work.

## Per-module parity

54 modules executed tests in every pass. 53 of those matched on test count, failures, errors, and skips. 4 test-bearing modules ran in only a subset of the passes.

Outcomes differed in `citrus-testcontainers`.

Test coverage differed in `citrus-cucumber-testcontainers`, `citrus-jbang`, `citrus-mcp-server`, `citrus-schema-generator`.

Maven status differed in `citrus-testcontainers`.

The local reactor skipped one module and failed one. Failures occurred in Citrus :: Tools :: JBang (ListTest.shouldListTests).

Cold Maven reactor skipped 8 modules and failed 1. Failures occurred in Citrus :: Connectors :: Testcontainers (ComposeTest.shouldLoadTestcontainersActions, StartContainerTest.shouldLoadTestcontainersActions, StartMongoDBTest.shouldLoadTestcontainersActions).

The latest warm reactor skipped one module and failed one. Failures occurred in Citrus :: Tools :: JBang (ListTest.shouldListTests).

**JUnit Jupiter regression.** The original warm session `sess-d7149987-76d` bypassed the outer Jupiter suite and dispatched six nested fixture partitions. It recorded six outer failures and four intentionally failing fixture events. All 21 JUnit Jupiter tests passed in one forked worker partition on the latest rerun.

**JBang nested launchers.** The first warm rerun reported seven `RunTest` errors because nested `JUnitCitrusTest` submissions matched zero tests on workers. `RunTest` is pinned to the Maven host by `@LocalOnly`. The distribution fix keeps the nested launcher in that JVM for the duration of the local execution. A targeted broker run verified that all eight `RunTest` cases passed on the Maven host and eight `InitTest` cases passed on a worker. The broker recorded only the `InitTest` partition.

| Run pair | Modules with tests in both | Matching test counts | Matching outcomes | Only in first | Only in second |
|---|---:|---:|---:|---:|---:|
| Local / Cold | 54 | 54 | 53 | 4 | 0 |
| Local / Warm | 58 | 58 | 58 | 0 | 0 |
| Cold / Warm | 54 | 54 | 53 | 0 | 4 |

Local and cold differed in `citrus-testcontainers`.

Cold and warm differed in `citrus-testcontainers`.

Matching outcomes compare the number of tests, failures, errors, and skips per module.

The latest warm run matched local outcome counts in all 58 test-bearing modules. Both had the same `ListTest.shouldListTests` assertion failure. The latest warm run cleared all seven `RunTest` errors, and the broker recorded no standalone `JUnitCitrusTest` partitions for `citrus-jbang`. Testcontainers passed all 28 tests in the latest warm and local runs after three failures in cold.

Each test cell displays tests / failures+errors / skipped tests. A dash means Maven recorded no test summary. Partition counts are dispatched attempts.

| Module | Local tests / F+E / S | Cold tests / F+E / S | Warm tests / F+E / S | Local wall | Cold wall | Warm wall | Cold partitions | Warm partitions | Maven status L/C/W |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Citrus | — | — | — | 0.03s | 0.03s | 0.03s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Core | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Core :: API | 460 / 0 / 0 | 460 / 0 / 0 | 460 / 0 / 0 | 8.87s | 9.31s | 8.19s | 7 | 4 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Core :: Base | 11,331 / 0 / 0 | 11,331 / 0 / 0 | 11,331 / 0 / 0 | 12.44s | 21.84s | 19.34s | 12 | 12 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Utils | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Utils :: Test Support | — | — | — | 0.03s | 0.04s | 0.05s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Core :: Spring | 98 / 0 / 0 | 98 / 0 / 0 | 98 / 0 / 0 | 3.43s | 6.46s | 6.21s | 7 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: JUnit Jupiter | 21 / 0 / 0 | 21 / 0 / 0 | 21 / 0 / 0 | 1.75s | 3.50s | 3.80s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: XML | 81 / 0 / 0 | 81 / 0 / 0 | 81 / 0 / 0 | 10.19s | 8.79s | 7.47s | 4 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: YAML | 40 / 0 / 0 | 40 / 0 / 0 | 40 / 0 / 0 | 9.50s | 7.51s | 7.91s | 4 | 2 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: Hamcrest | 177 / 0 / 0 | 177 / 0 / 0 | 177 / 0 / 0 | 1.54s | 3.75s | 2.27s | 2 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: JSON | 268 / 0 / 0 | 268 / 0 / 0 | 268 / 0 / 0 | 1.84s | 4.74s | 3.80s | 4 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: Plaintext | 25 / 0 / 0 | 25 / 0 / 0 | 25 / 0 / 0 | 1.00s | 2.73s | 1.56s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Groovy | 74 / 0 / 0 | 74 / 0 / 0 | 74 / 0 / 0 | 9.33s | 8.04s | 9.00s | 5 | 4 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: SQL | 103 / 0 / 0 | 103 / 0 / 0 | 103 / 0 / 0 | 1.99s | 5.18s | 2.80s | 2 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: Groovy | 28 / 0 / 0 | 28 / 0 / 0 | 28 / 0 / 0 | 1.64s | 3.67s | 4.06s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: XML | 425 / 0 / 0 | 425 / 0 / 0 | 425 / 0 / 0 | 3.16s | 6.82s | 6.13s | 6 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Spring Integration | 197 / 0 / 0 | 197 / 0 / 0 | 197 / 0 / 0 | 6.60s | 8.82s | 8.69s | 3 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: JMS | 92 / 0 / 0 | 92 / 0 / 0 | 92 / 0 / 0 | 19.17s | 21.56s | 20.38s | 3 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: Binary | 2 / 0 / 0 | 2 / 0 / 0 | 2 / 0 / 0 | 0.42s | 2.13s | 1.03s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: HTTP | 280 / 0 / 0 | 280 / 0 / 0 | 280 / 0 / 0 | 8.02s | 10.81s | 7.60s | 5 | 5 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: OpenAPI | 352 / 0 / 0 | 352 / 0 / 0 | 352 / 0 / 0 | 5.07s | 7.12s | 6.96s | 4 | 4 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: Docker | 62 / 0 / 0 | 62 / 0 / 0 | 62 / 0 / 0 | 2.48s | 4.72s | 3.25s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: JBang | 32 / 0 / 0 | 32 / 0 / 0 | 32 / 0 / 0 | 11.72s | 8.95s | 8.70s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: Playwright | 515 / 0 / 0 | 515 / 0 / 0 | 515 / 0 / 0 | 2.12s | 9.91s | 5.95s | 6 | 5 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: Agent | 11 / 0 / 0 | 11 / 0 / 0 | 11 / 0 / 0 | 1.96s | 4.38s | 2.55s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Connectors :: Testcontainers | 28 / 0 / 0 | 28 / 3 / 0 | 28 / 0 / 0 | 29.98s | 147.00s | 86.00s | 1 | 1 | SUCCESS / FAILURE / SUCCESS |
| Citrus :: Endpoints :: Apache Camel | 308 / 0 / 0 | 308 / 0 / 0 | 308 / 0 / 0 | 202.00s | 112.00s | 63.00s | 10 | 10 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: SSH | 62 / 0 / 0 | 62 / 0 / 0 | 62 / 0 / 0 | 1.59s | 4.36s | 4.11s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: FTP | 65 / 0 / 0 | 65 / 0 / 0 | 65 / 0 / 0 | 5.12s | 8.22s | 7.33s | 3 | 2 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: GraphQL | 186 / 0 / 0 | 186 / 0 / 0 | 186 / 0 / 0 | 2.77s | 5.92s | 4.90s | 3 | 2 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: gRPC | 165 / 0 / 0 | 165 / 0 / 0 | 165 / 0 / 0 | 7.55s | 8.54s | 9.32s | 3 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: JMX | 18 / 0 / 0 | 18 / 0 / 0 | 18 / 0 / 0 | 1.09s | 3.49s | 1.72s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Kafka | 124 / 0 / 0 | 124 / 0 / 0 | 124 / 0 / 0 | 2.54s | 5.58s | 4.62s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Mail | 45 / 0 / 0 | 45 / 0 / 0 | 45 / 0 / 0 | 1.25s | 3.90s | 3.20s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: RMI | 18 / 0 / 0 | 18 / 0 / 0 | 18 / 0 / 0 | 1.46s | 3.80s | 1.71s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Websocket | 28 / 0 / 0 | 28 / 0 / 0 | 28 / 0 / 0 | 2.22s | 5.43s | 3.93s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: SOAP WebServices | 206 / 0 / 0 | 206 / 0 / 0 | 206 / 0 / 0 | 4.76s | 7.96s | 6.83s | 5 | 3 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Vert.x | 28 / 0 / 0 | 28 / 0 / 0 | 28 / 0 / 0 | 31.32s | 33.63s | 33.53s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Endpoints :: Zookeeper | 5 / 0 / 0 | 5 / 0 / 0 | 5 / 0 / 0 | 6.08s | 8.53s | 8.15s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: JUnit | 5 / 0 / 0 | 5 / 0 / 0 | 5 / 0 / 0 | 0.38s | 0.59s | 0.39s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Spring Boot Starter Test | 8 / 0 / 0 | 8 / 0 / 0 | 8 / 0 / 0 | 0.61s | 2.64s | 0.83s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Quarkus Extension | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Quarkus Runtime | — | — | — | 0.32s | 0.42s | 0.32s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Quarkus Deployment | — | — | — | 0.04s | 0.11s | 0.04s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Quarkus Integration Tests | 1 / 0 / 0 | 1 / 0 / 0 | 1 / 0 / 0 | 3.25s | 2.88s | 3.24s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Cucumber | 35 / 0 / 0 | 35 / 0 / 0 | 35 / 0 / 0 | 2.38s | 5.05s | 4.56s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Runtime :: Main | 21 / 0 / 0 | 21 / 0 / 0 | 21 / 0 / 0 | 3.30s | 5.36s | 5.12s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Test API Generator | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Test API Generator :: API Core | 49 / 0 / 0 | 49 / 0 / 0 | 49 / 0 / 0 | 1.14s | 3.77s | 1.69s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Test API Generator :: Codegen | 27 / 0 / 0 | 27 / 0 / 0 | 27 / 0 / 0 | 12.92s | 15.98s | 15.83s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Test API Generator :: Maven Plugin | 22 / 0 / 0 | 22 / 0 / 0 | 22 / 0 / 0 | 3.43s | 5.99s | 5.66s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Restdocs | 20 / 0 / 0 | 20 / 0 / 0 | 20 / 0 / 0 | 0.93s | 3.24s | 3.12s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Test Generator | 53 / 0 / 0 | 53 / 0 / 0 | 53 / 0 / 0 | 1.79s | 4.23s | 4.12s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Docs Generator | 6 / 0 / 0 | 6 / 0 / 0 | 6 / 0 / 0 | 0.84s | 2.98s | 3.04s | 1 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Validation :: AssertJ | 129 / 0 / 0 | 129 / 0 / 0 | 129 / 0 / 0 | 1.30s | 3.79s | 3.50s | 2 | 1 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Schema Generator | 20 / 0 / 0 | — | 20 / 0 / 0 | 5.59s | — | 10.21s | 0 | 1 | SUCCESS / SKIPPED / SUCCESS |
| Citrus :: Tools :: Maven | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Maven :: Plugin | 17 / 0 / 0 | 17 / 0 / 0 | 17 / 0 / 0 | 4.74s | 7.08s | 5.30s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Maven :: Plugin Integration | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Maven :: Agent Plugin | — | — | — | 1.90s | 2.22s | 1.94s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Core | — | — | — | 0.06s | 0.15s | 0.05s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Http | — | — | — | 0.06s | 0.19s | 0.06s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Groovy | — | — | — | 0.06s | 0.15s | 0.05s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Camel | — | — | — | 0.06s | 0.21s | 0.06s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: JDBC | — | — | — | 0.21s | — | 0.06s | 0 | 0 | SUCCESS / SKIPPED / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: JMS | 12 / 0 / 0 | 12 / 0 / 0 | 12 / 0 / 0 | 0.63s | 2.84s | 1.17s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Kafka | — | — | — | 0.06s | 0.17s | 0.07s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Kubernetes | — | — | — | 0.06s | 0.26s | 0.06s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Knative | — | — | — | 0.06s | 0.46s | 0.06s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: OpenAPI | — | — | — | 0.07s | 0.31s | 0.06s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Playwright | 31 / 0 / 0 | 31 / 0 / 0 | 31 / 0 / 0 | 1.22s | 3.92s | 1.73s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Selenium | 9 / 0 / 0 | 9 / 0 / 0 | 9 / 0 / 0 | 1.04s | 3.53s | 1.67s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: Testcontainers | 21 / 0 / 0 | — | 21 / 0 / 0 | 27.45s | — | 30.77s | 0 | 1 | SUCCESS / SKIPPED / SUCCESS |
| Citrus :: Tools :: Cucumber Steps :: All | — | — | — | 0.09s | — | 0.10s | 0 | 0 | SUCCESS / SKIPPED / SUCCESS |
| Citrus :: Validation :: YAML | 87 / 0 / 0 | 87 / 0 / 0 | 87 / 0 / 0 | 1.02s | 2.81s | 1.71s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: JBang | 124 / 1 / 0 | — | 124 / 1 / 0 | 10.40s | — | 12.49s | 0 | 1 | FAILURE / SKIPPED / FAILURE |
| Citrus :: Tools :: Agent | — | — | — | — | — | — | 0 | 0 | SKIPPED / SKIPPED / SKIPPED |
| Citrus :: Tools :: Archetypes | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Archetypes :: Quickstart | — | — | — | 0.02s | 0.01s | 0.01s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Archetypes :: Quickstart JMS | — | — | — | 0.01s | 0.01s | 0.01s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: Archetypes :: Quickstart SOAP | — | — | — | 0.01s | 0.01s | 0.01s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Tools :: MCP Server | 63 / 0 / 0 | — | 63 / 0 / 0 | 2.80s | — | 3.19s | 0 | 0 | SUCCESS / SKIPPED / SUCCESS |
| Citrus :: Utils :: Metadata Processor | 3 / 0 / 0 | 3 / 0 / 0 | 3 / 0 / 0 | 0.54s | 2.06s | 1.13s | 1 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Catalog | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Catalog :: Core | — | — | — | 0.01s | 0.03s | 0.03s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Catalog :: Endpoints | — | — | — | 0.09s | 0.42s | 0.09s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Catalog :: BOM | — | — | — | 0.00s | 0.00s | 0.00s | 0 | 0 | SUCCESS / SUCCESS / SUCCESS |
| Citrus :: Catalog :: Schema | — | — | — | 1.06s | — | 1.02s | 0 | 0 | SUCCESS / SKIPPED / SUCCESS |

## Bottlenecks and interpretation

- The longest local modules were `citrus-camel` (202.00s), `citrus-vertx` (31.32s), `citrus-testcontainers` (29.98s), `citrus-cucumber-testcontainers` (27.45s), `citrus-jms` (19.17s).
- The longest cold modules were `citrus-testcontainers` (147.00s), `citrus-camel` (112.00s), `citrus-vertx` (33.63s), `citrus-base` (21.84s), `citrus-jms` (21.56s).
- The longest warm modules were `citrus-testcontainers` (86.00s), `citrus-camel` (63.00s), `citrus-vertx` (33.53s), `citrus-cucumber-testcontainers` (30.77s), `citrus-jms` (20.38s).
- The largest warm time premiums on modules executed in all passes were `citrus-testcontainers` (+56.02s), `citrus-base` (+6.90s), `citrus-playwright` (+3.84s), `citrus-validation-xml` (+2.97s), `citrus-openapi-codegen` (+2.91s).
- The diagnostic launch initially dropped workers before recording tests because the Maven runtime classpath omitted JUnit Platform. A separate `citrus-api` probe passed all 460 tests after adding the matching JUnit jars. The measured database and worker caches were created after that probe.
- The preliminary local attempt reached Redpanda with the Docker VM's async I/O quota exhausted. That attempt was stopped and excluded from timings. The new limit applied equally to the three measured passes.
- During the cold pass the Docker VM reported 537 MiB available and used all 2 GiB of swap. Kafka containers showed delayed heartbeats. Testcontainers startup and readiness contributed substantial timing variance.
- This run measures scheduling and cache effects together. It does not isolate the individual wall-time contribution of JAR inspection or macOS reference probes.

The Maven extension file and Docker VM async I/O limit were restored. The six workers were stopped. The broker remains active (PID `54610`). [Warm rerun dashboard](http://127.0.0.1:18080/ui/index.html#session/sess-3feba5a5-c31)
