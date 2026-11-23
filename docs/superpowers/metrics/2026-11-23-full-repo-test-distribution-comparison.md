# Full-repo unit tests: local vs distributed

Total Maven time: local **239s**, distributed **316s**

- Modules with tests: 47; identical test counts in both runs: 47
- Failed modules: local 1, distributed 1
- Failing only when distributed: none

| Module | Local tests | Dist tests | Local fail+err | Dist fail+err | Local time | Dist time | Local | Dist |
|---|---|---|---|---|---|---|---|---|
| Citrus :: Core :: API | 460 | 460 | 0 | 0 | 8.9s | 8.5s | SUCCESS | SUCCESS |
| Citrus :: Core :: Base | 11331 | 11331 | 0 | 0 | 12.2s | 17.8s | SUCCESS | SUCCESS |
| Citrus :: Core :: Spring | 98 | 98 | 0 | 0 | 3.1s | 5.6s | SUCCESS | SUCCESS |
| Citrus :: Runtime :: JUnit Jupiter | 21 | 21 | 0 | 0 | 1.5s | 3.4s | SUCCESS | SUCCESS |
| Citrus :: Runtime :: XML | 81 | 81 | 0 | 0 | 9.7s | 7.7s | SUCCESS | SUCCESS |
| Citrus :: Runtime :: YAML | 40 | 40 | 0 | 0 | 9.1s | 7.0s | SUCCESS | SUCCESS |
| Citrus :: Validation :: Hamcrest | 177 | 177 | 0 | 0 | 1.4s | 3.3s | SUCCESS | SUCCESS |
| Citrus :: Validation :: JSON | 268 | 268 | 0 | 0 | 1.5s | 4.1s | SUCCESS | SUCCESS |
| Citrus :: Validation :: Plaintext | 25 | 25 | 0 | 0 | 0.9s | 2.8s | SUCCESS | SUCCESS |
| Citrus :: Runtime :: Groovy | 74 | 74 | 0 | 0 | 8.9s | 7.9s | SUCCESS | SUCCESS |
| Citrus :: Connectors :: SQL | 103 | 103 | 0 | 0 | 1.7s | 3.8s | SUCCESS | SUCCESS |
| Citrus :: Validation :: Groovy | 28 | 28 | 0 | 0 | 1.4s | 3.4s | SUCCESS | SUCCESS |
| Citrus :: Validation :: XML | 425 | 425 | 0 | 0 | 2.9s | 6.7s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: Spring Integration | 197 | 197 | 0 | 0 | 6.5s | 8.0s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: JMS | 92 | 92 | 0 | 0 | 19.0s | 19.6s | SUCCESS | SUCCESS |
| Citrus :: Validation :: Binary | 2 | 2 | 0 | 0 | 0.4s | 2.2s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: HTTP | 280 | 280 | 0 | 0 | 7.7s | 6.3s | SUCCESS | SUCCESS |
| Citrus :: Connectors :: OpenAPI | 352 | 352 | 0 | 0 | 4.0s | 6.2s | SUCCESS | SUCCESS |
| Citrus :: Connectors :: Docker | 62 | 62 | 0 | 0 | 2.0s | 4.7s | SUCCESS | SUCCESS |
| Citrus :: Connectors :: JBang | 32 | 32 | 1 | 1 | 10.2s | 8.0s | FAILURE | FAILURE |
| Citrus :: Connectors :: Playwright | 515 | 515 | 0 | 0 | 1.7s | 4.8s | SUCCESS | SUCCESS |
| Citrus :: Connectors :: Agent | 11 | 11 | 0 | 0 | 1.7s | 4.2s | SUCCESS | SUCCESS |
| Citrus :: Connectors :: Testcontainers | 28 | 28 | 0 | 0 | 25.5s | 29.1s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: SSH | 62 | 62 | 0 | 0 | 1.4s | 3.3s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: FTP | 65 | 65 | 0 | 0 | 5.4s | 7.6s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: GraphQL | 186 | 186 | 0 | 0 | 2.5s | 5.2s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: gRPC | 165 | 165 | 0 | 0 | 6.9s | 9.0s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: JMX | 18 | 18 | 0 | 0 | 1.0s | 2.6s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: Kafka | 124 | 124 | 0 | 0 | 2.4s | 4.2s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: Mail | 45 | 45 | 0 | 0 | 1.1s | 3.1s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: RMI | 18 | 18 | 0 | 0 | 1.3s | 3.0s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: Websocket | 28 | 28 | 0 | 0 | 2.2s | 3.9s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: SOAP WebServices | 206 | 206 | 0 | 0 | 4.3s | 6.1s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: Vert.x | 28 | 28 | 0 | 0 | 31.2s | 33.0s | SUCCESS | SUCCESS |
| Citrus :: Endpoints :: Zookeeper | 5 | 5 | 0 | 0 | 6.0s | 7.6s | SUCCESS | SUCCESS |
| Citrus :: Runtime :: JUnit | 5 | 5 | 0 | 0 | 0.3s | 0.4s | SUCCESS | SUCCESS |
| Citrus :: Runtime :: Spring Boot Starter Test | 8 | 8 | 0 | 0 | 0.6s | 2.1s | SUCCESS | SUCCESS |
| Citrus :: Runtime :: Quarkus Integration Tests | 1 | 1 | 0 | 0 | 2.8s | 2.9s | SUCCESS | SUCCESS |
| Citrus :: Tools :: Test API Generator :: API Core | 49 | 49 | 0 | 0 | 1.0s | 2.8s | SUCCESS | SUCCESS |
| Citrus :: Tools :: Test API Generator :: Codegen | 27 | 27 | 0 | 0 | 12.3s | 14.8s | SUCCESS | SUCCESS |
| Citrus :: Tools :: Restdocs | 20 | 20 | 0 | 0 | 0.8s | 3.0s | SUCCESS | SUCCESS |
| Citrus :: Tools :: Test Generator | 53 | 53 | 0 | 0 | 1.6s | 3.2s | SUCCESS | SUCCESS |
| Citrus :: Tools :: Docs Generator | 6 | 6 | 0 | 0 | 0.8s | 2.7s | SUCCESS | SUCCESS |
| Citrus :: Validation :: AssertJ | 129 | 129 | 0 | 0 | 1.2s | 3.1s | SUCCESS | SUCCESS |
| Citrus :: Tools :: Maven :: Plugin | 17 | 17 | 0 | 0 | 4.5s | 6.2s | SUCCESS | SUCCESS |
| Citrus :: Validation :: YAML | 87 | 87 | 0 | 0 | 0.9s | 2.8s | SUCCESS | SUCCESS |
| Citrus :: Utils :: Metadata Processor | 3 | 3 | 0 | 0 | 0.5s | 2.0s | SUCCESS | SUCCESS |

## Setup and what it took to reach parity

Unit tests of the whole reactor (`./mvnw -o test -fae`), excluding citrus-kubernetes, citrus-knative,
citrus-selenium, citrus-testng and the Test Distribution modules themselves; 6 local workers in
`CITRUS_DIST_WORKER_FORK_MODE=ALWAYS`, 10-unit partitions, no local executors, no fallback.
Both runs with `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock` (Rancher Desktop's socket cannot be
bind-mounted, so Testcontainers' Ryuk needs the override locally too). The one failure in both runs is
`JBangTestEngineTest`, which needs a JBang installation.

Coverage gap: with `-fae`, Maven skips every module that depends on a failed one, so the JBang failure skipped 25
modules in *both* runs (Apache Camel, Runtime Cucumber, Runtime Main, MCP Server, Agent, Schema Generator, the
Cucumber Steps modules and the Catalogs). Their tests were never executed; parity above covers the 47 modules that ran.
CI (`verify` on all modules) counts about 18,400 tests: roughly 1,360 more unit tests from these skipped and excluded
modules, plus about 880 integration tests (`*IT`), which this unit-test comparison does not run. Rerunning with
`-Dmaven.test.failure.ignore=true` would cover the skipped modules.

Distribution bugs found and fixed on the way (each with a failing test first):
- CitrusDynamicSourceFilter loaded from the worker → IllegalAccessError (citrus-junit-jupiter)
- third-party `javax.*` (wsdl4j) loaded from the worker → "Failed to find target schema xsd file resource"
- jars on the partition classpath named by digest → Testcontainers copied a whole jar as `/entrypoint-tc.sh`
  ("exec format error" for Redpanda); jars now keep their file names
- Surefire selecting a single `@Nested` class → the whole outer class was distributed (156 extra tests)
- in-JVM partitions never exit → Testcontainers containers were never cleaned up (fixed by `ALWAYS` fork mode)
- `@QuarkusTest` needs the Maven project layout → new module opt-out `citrus.test.distribution.skip`
- partitions always dispatched to the same idle worker → least-recently-assigned dispatch (19–23 partitions per worker)
