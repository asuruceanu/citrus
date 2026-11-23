# Citrus Core Test Distribution Metrics

Integrity checks: all passed

## 1. Session overview

| Field | Value |
|---|---|
| sessionId | sess-41d483f2-c95 |
| projectName | citrus-api |
| gitBranch | claude/core-test-distribution-run-ece2bb |
| gitCommit | f325b94e5d08 |
| status | PASSED |
| totalTests | 11889 |
| passedTests | 11889 |
| failedTests | 0 |
| skippedTests | 0 |
| wallDuration | 66.87s |
| serialDuration | 98.86s |
| speedup (serial / wall) | 1.48x |

## 2. Per-worker load balance

| Worker | Partitions | Tests | Classes | Busy (partition wall) | Sum test time | Min / avg / max partition | Utilization |
|---|---|---|---|---|---|---|---|
| worker-1 | 5 | 178 | 31 | 45.28s | 21.63s | 2.84s / 9.06s / 19.97s | 68% |
| worker-2 | 6 | 320 | 61 | 44.52s | 16.61s | 2.74s / 7.42s / 12.63s | 67% |
| worker-3 | 6 | 314 | 60 | 46.98s | 15.35s | 4.47s / 7.83s / 10.80s | 70% |
| worker-4 | 6 | 10527 | 60 | 44.52s | 15.71s | 3.71s / 7.42s / 15.07s | 67% |
| worker-5 | 5 | 198 | 50 | 36.71s | 11.94s | 2.82s / 7.34s / 13.45s | 55% |
| worker-6 | 5 | 352 | 51 | 42.30s | 17.62s | 6.77s / 8.46s / 10.02s | 63% |

LPT skew (max / avg worker busy time): **1.08**

## 3. Per-module and execution mode

| Module | forkWorkspace | Partitions | Remote tests | Min / avg / max partition | Total partition time |
|---|---|---|---|---|---|
| citrus-api | [True] | 7 | 460 | 3.71s / 6.86s / 9.57s | 48.05s |
| citrus-base | [True] | 19 | 11331 | 2.74s / 8.09s / 19.97s | 153.70s |
| citrus-spring | [False] | 7 | 98 | 4.47s / 8.37s / 10.02s | 58.57s |

Tests executed locally (@LocalOnly, worker `local-host`): 0

## 4. Blob cache efficiency

- Launcher: 134 of 136 classpath/workspace blobs already cached on the broker (hit rate **98.5%**)
- Broker cache: 65 files, 36.3 MB

| Worker | Cached files | MB |
|---|---|---|
| worker-1 | 65 | 36.3 |
| worker-2 | 65 | 36.3 |
| worker-3 | 65 | 36.3 |
| worker-4 | 65 | 36.3 |
| worker-5 | 65 | 36.3 |
| worker-6 | 65 | 36.3 |

## 5. Retries, flakiness, slowest work

- Partition attempts > 1: 0
- Flaky tests (passed on retry): 0
- Failed tests: 0

**Top 10 slowest partitions**

| Partition | Module | Worker | Units | Duration |
|---|---|---|---|---|
| part-40598c8d-9ce | citrus-base | worker-1 | 3 | 19.97s |
| part-feb773d5-302 | citrus-base | worker-4 | 10 | 15.07s |
| part-1c2e5fb4-5aa | citrus-base | worker-5 | 10 | 13.45s |
| part-f168410f-d01 | citrus-base | worker-2 | 10 | 12.63s |
| part-bc200545-5b8 | citrus-base | worker-3 | 10 | 10.80s |
| part-4eab4231-57c | citrus-spring | worker-6 | 10 | 10.02s |
| part-a15a0cab-ad3 | citrus-api | worker-1 | 6 | 9.57s |
| part-dbbc4526-e8b | citrus-base | worker-6 | 10 | 9.30s |
| part-8259beac-fc3 | citrus-base | worker-3 | 10 | 9.25s |
| part-99836a0b-bad | citrus-spring | worker-1 | 2 | 8.82s |

**Top 10 slowest test classes**

| Class | Sum of test time |
|---|---|
| `org.citrusframework.endpoint.direct.DirectEndpointAdapterTest` | 10.64s |
| `org.citrusframework.actions.WaitTest` | 5.85s |
| `org.citrusframework.container.AbstractIteratingActionContainerTest` | 5.82s |
| `org.citrusframework.base.DefaultTestCaseTest` | 5.54s |
| `org.citrusframework.util.TestUtilsTest` | 5.52s |
| `org.citrusframework.endpoint.direct.DirectEndpointSyncProducerTest` | 5.10s |
| `org.citrusframework.endpoint.direct.DirectEndpointSyncConsumerTest` | 4.71s |
| `org.citrusframework.base.message.correlation.PollingCorrelationManagerTest` | 4.63s |
| `org.citrusframework.spring.config.xml.SequenceAfterSuiteParserTest` | 3.36s |
| `org.citrusframework.validation.HeaderValidatorTest` | 3.29s |

## 6. Baseline: same modules without distribution

| Run | Maven wall time | citrus-api | citrus-base | citrus-spring | Result |
|---|---|---|---|---|---|
| Local (`-Dcitrus.test.distribution.enabled=false`) | 29s | 460 tests, 10.0s | 11331 tests, 12.8s | 98 tests, 3.6s | BUILD SUCCESS |
| Distributed, before balanced partitioning | 81s | 460 tests, 16.5s | 11331 tests, 51.6s | 98 tests, 9.7s | BUILD SUCCESS |
| Distributed, this session (balanced LPT) | 69s | 460 tests, 12.1s | 11331 tests, 32.6s | 98 tests, 18.1s | BUILD SUCCESS |

Surefire counts match the local run exactly, and the coordinator now counts the same 11889 tests (JUnit unique id
per invocation). Balanced partitioning cut `citrus-base` from 51.6s to 32.6s. Distribution is still about 2.4x
slower than plain Surefire on one machine: the reactor runs modules one after another and each module pays
classpath packaging plus partition JVM start-up; real gains need workers on separate machines.

Surefire test-set times are replay times on the host (median 8 ms), not test durations; the real per-test
durations are in the coordinator (sections 2, 3 and 5).

## 7. Experiments: one parallel JUnit run per partition

After the worker started running all classes of a partition in one JUnit execution (so each module's
`junit.jupiter.execution.parallel.*` settings apply, as in a local Surefire run), up to 12 tests overlapped inside
one partition. All runs below passed with 11889 tests and Surefire counts identical to the local run.

| Setup | Maven wall | citrus-api | citrus-base | citrus-spring | Sum of test time |
|---|---|---|---|---|---|
| Local, no distribution | 29s | 10.0s | 12.8s | 3.6s | n/a |
| 6 workers, 10-unit partitions, classes one at a time (before) | 69s | 12.1s | 32.6s | 18.1s | 98.9s |
| 6 workers, 10-unit partitions, parallel inside partitions | 87s | 14.1s | 39.3s | 24.4s | 125.9s |
| 2 workers, 10-unit partitions, parallel inside partitions | 139s | 21.8s | 81s | 29.0s | 101.0s |
| 2 workers, 100-unit partitions, parallel inside partitions | 74s | 18.1s | 23.3s | 21.8s | 81.8s |

- 6 workers each running up to 14 test threads oversubscribe the 14-core machine: summed test time rises by a
  quarter and timing-sensitive tests stretch.
- Few large partitions are what in-partition parallelism needs: `citrus-base` drops to 23.3s, its best distributed time.
- Each module still pays a fixed ~5-8s (classpath packaging and upload, partition JVM start, JUnit and Spring
  bootstrap), three times over because the reactor runs modules one after another. On one machine that fixed cost
  alone exceeds what parallelism saves; distribution pays off with workers on other machines and with modules
  distributed concurrently.

Follow-up runs with all 6 workers connected:

| Setup | Maven wall | citrus-api | citrus-base | citrus-spring | Workers that ran partitions |
|---|---|---|---|---|---|
| 6 workers, 100-unit partitions | 74s | 16.6s | 29.7s | 16.9s | 2 (api 1, base 2, spring 1 partitions) |
| 6 workers, 31-unit partitions | 84s | 22.9s | 26.2s | 14.4s | base 6, api 3, spring 2 |

One `maxPartitionSize` cannot spread modules of 66, 183, and 62 units across all workers; 31 puts `citrus-base` on
all 6 workers, but on one 14-core machine the extra concurrent JVMs cost as much as they save.
