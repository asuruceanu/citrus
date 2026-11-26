# Citrus Test Distribution for local Docker development

The [Compose stack](compose.yaml) starts PostgreSQL 16, a Citrus coordinator with its dashboard, and eight
workers on your development machine. The [Dockerfile](Dockerfile) builds separate coordinator and worker
images from the same checkout. Both use Java 21 and run as a non-root user.

## Start the stack

Install Docker and Docker Compose with BuildKit enabled. Use Java 21 or later on the host to run Maven.
Run the commands in this guide from the Citrus repository root.

```sh
./tools/citrus-test-distribution/docker/start.sh
```

[start.sh](start.sh) builds the images and starts the stack. Compose starts the coordinator when PostgreSQL
is ready and starts the workers after the coordinator passes its health check. The first build downloads
Maven dependencies. Later builds reuse the Docker build cache.

The script prints the published dashboard URL and opens it on supported macOS and Linux desktops.
The default is [http://127.0.0.1:8765/ui/](http://127.0.0.1:8765/ui/). On a headless machine,
use the printed URL. Open the **Workers** view and check that eight workers have registered.

## Coordinator

The coordinator receives test submissions from host Maven builds, schedules partitions on the workers,
and serves the dashboard. It connects to PostgreSQL at `jdbc:postgresql://postgres:5432/citrus_dist` as
`citrus`. PostgreSQL stores session history and results in the `postgres-data` named volume.
The `coordinator-data` named volume holds the artifact cache at `/var/lib/citrus/blobs`.

The coordinator publishes port `8765` on `127.0.0.1` by default. If you change the host port, use the
URL printed by the start script in your browser and Maven commands. The `/health` endpoint returns
the coordinator's status. The `/api/v1/workers` endpoint lists registered workers.

## Workers

Compose registers the eight replicas in the **Local Docker** worker pool. A one-time service
runs before the workers start and reuses the pool and its active registration key on later
starts. The key is stored in the project’s `pool-registration` volume, mounted read-only by
the workers. The Overview summarizes pool capacity, and Workers lists each replica and its slots.

Each worker connects to `http://coordinator:8765` on the Compose network with one execution slot by default.
Run `CITRUS_DIST_WORKER_MAX_SLOTS=2 ./start.sh` to enable a standby slot on each worker.
The coordinator assigns queued partitions to eligible idle workers first. Once those workers are busy,
a worker with a standby slot can run a second partition in an independent JVM. Both slots use the same worker ID.
The standby JVM uses its own cache and working directory under `/var/lib/citrus/overflow-slot`.
Admission uses queue demand without CPU or memory thresholds. Workers and PostgreSQL use the Compose network. The coordinator is the only
service that publishes a host port.

Each worker stores downloaded test artifacts in its own writable container storage at
`/var/lib/citrus/cache`. Stopping and resuming the same container preserves its identity and cache.
For the local `citrus-base` experiment, both slots share preparation of compiled class and resource directories.
The worker verifies and unpacks one snapshot for a session and input set. Every partition gets private writable
copies and starts its own test JVM. A different input set, such as the Failsafe classpath, needs fresh preparation.
JARs still use the slot's cache. The waterfall shows one **Inputs** lane per worker and measures each partition's
wait for shared inputs. **Other startup** estimates the remaining time before its first test.

Compose uses the replica number as the registered ID (`worker-1` through `worker-8` by default). A replacement worker registers under the same ID and starts with a fresh cache. Use `CITRUS_DIST_WORKERS` to resize the pool and
`CITRUS_DIST_WORKER_CAPABILITIES` to advertise tags required by your tests.

The worker image includes Git, JBang, and the Docker CLI with its Compose and Buildx plugins. On Rancher Desktop, workers use the
engine VM's Docker socket to start sibling containers for Testcontainers. The start script
detects the socket group and adds it to each worker. Tests connect to published ports via
the engine's host gateway. Run trusted tests on this local stack because the socket gives
worker code control over the developer's Docker engine.

## Run tests from the checkout

With the stack running, use [run-tests.sh](run-tests.sh) to run Maven `verify` for a module by its
artifact ID or reactor path.

```sh
./tools/citrus-test-distribution/docker/run-tests.sh -module citrus-api
```

Use `-all` to run `verify` across the reactor.

```sh
./tools/citrus-test-distribution/docker/run-tests.sh -all
```

Use `-name` with either mode to set the display name shown in the Sessions UI. Quote names
that contain spaces. The script passes the name to Maven as `citrus.test.distribution.projectName`.

```sh
./tools/citrus-test-distribution/docker/run-tests.sh -all -name "Full reactor baseline"
./tools/citrus-test-distribution/docker/run-tests.sh -module citrus-api -name "Workers UI responsiveness after"
```

By default Maven builds one module at a time on the host. The workers share the eligible tests
from that module. To let two independent modules build and submit tests concurrently, run:

```sh
MAVEN_CONFIG='-T 2' ./tools/citrus-test-distribution/docker/run-tests.sh -all
```

Verification attempts downstream modules after a test failure and the script exits with a failure if Maven reports a failed build. A missing prerequisite can still prevent a module from building. Individual tests retain their configured skip conditions.

Maven still waits for module dependencies. All eight local worker containers share the CPU and
memory assigned to Rancher Desktop.

The script builds the Maven distribution extension and prerequisites with tests skipped and uses a
checkout-local Maven repository under `target/citrus-distribution/maven-repository`. It loads the
extension from a temporary Maven configuration for the test run. Tests execute in the selected modules.
The script discovers the coordinator port for the selected Compose project. Set
`CITRUS_DIST_COORDINATOR_URL` to supply an address explicitly.

The runner disables local fallback when the coordinator is unreachable or a partition is lost.
It sets the local duration and cold-start thresholds to zero so eligible tests can be sent to workers.
The worker image includes Chromium and its Linux libraries for `citrus-playwright`. The image build
uses the Playwright Java version resolved from the checkout. Both worker slots use the installed browser.
The `citrus-cucumber-playwright` step tests also support worker execution.
Placement rules such as `@LocalOnly` and `@AgentRequirement` still apply. Tests that need external
services or tools require them in the environment where they execute.

## Run another Maven project

Register the distribution Maven extension in that project's `.mvn/extensions.xml` as shown in the
[Maven quickstart](../../../src/manual/tools-test-distribution.adoc#tools-test-distribution-maven).
Then run its Maven wrapper with distribution enabled and the coordinator's host URL.

```sh
./mvnw verify \
  -Dcitrus.test.distribution.enabled=true \
  -Dcitrus.test.distribution.coordinatorUrl=http://127.0.0.1:8765
```

## Local configuration

Export overrides before running the scripts. Use the same `COMPOSE_PROJECT_NAME` for startup, test runs,
stop, and cleanup when you select a project name.

| Environment variable | Default | Effect |
| --- | --- | --- |
| `COMPOSE_PROJECT_NAME` | `citrus-test-distribution` | Selects the stack and its named resources. |
| `CITRUS_DIST_HOST_PORT` | `8765` | Changes the port on the host loopback address. Workers still use the coordinator's internal port. |
| `CITRUS_DIST_WORKER_HANDSHAKE_TIMEOUT_MS` | `90000` | Time allowed for a forked test JVM to connect to its worker during startup. |
| `CITRUS_DIST_WORKER_MAX_SLOTS` | `1` | Set to `2` to allow a second partition when work is queued. |
| `CITRUS_DIST_WORKERS` | `8` | Sets the number of worker replicas. |
| `CITRUS_DIST_JDBC_PASSWORD` | `citrus` | Sets the initial PostgreSQL password and the coordinator's JDBC password. Configure it before creating the database volume. To change it later, update the PostgreSQL role as well. |
| `CITRUS_DIST_AUTH_TOKEN` | Empty | Enables bearer authentication on the coordinator and supplies the token to the workers. Export the same value for Maven. |
| `CITRUS_DIST_DOCKER_GID` | Detected by `start.sh` | Group that can access the Docker engine socket. |
| `CITRUS_DIST_DOCKER_SOCKET` | `/var/run/docker.sock` | Absolute socket path on the engine host. For Rancher Desktop, use the path inside its Linux VM. |
| `CITRUS_DIST_WORKER_CAPABILITIES` | `docker=true` | Sets comma-separated capability tags to each worker, for example `suite=local`. |
| `CITRUS_DIST_COORDINATOR_URL` | Published Compose address | Overrides the coordinator URL used by `run-tests.sh`. |

When bearer authentication is enabled, send the token with API requests. For dashboard access with
authentication, use the trusted reverse proxy described in the
[dashboard authentication section](../../../src/manual/tools-test-distribution.adoc#tools-test-distribution-ui).

## Inspect, stop, and clean up

Use the Compose status and logs to diagnose startup and worker registration. Export your project name
first if you customized it.

```sh
docker compose -f tools/citrus-test-distribution/docker/compose.yaml ps
docker compose -f tools/citrus-test-distribution/docker/compose.yaml logs postgres coordinator worker
```

Use [stop.sh](stop.sh) to pause services and keep containers, images, and data.

```sh
./tools/citrus-test-distribution/docker/stop.sh
```

The worker gives an active partition up to 30 seconds to finish. Compose allows a 65-second container
shutdown grace period, including time to close connections and release resources. Run `start.sh` with
the same source and settings to resume the stack. Named PostgreSQL and coordinator volumes also
preserve their data when those containers are recreated.

Use [clean.sh](clean.sh) to reset the selected stack. This removes its containers, network, source-built
Citrus images, and named volumes. The volumes contain local test history and cached artifacts.

```sh
./tools/citrus-test-distribution/docker/clean.sh --yes
```

## Build the coordinator and worker images directly

Compose builds these targets as part of startup. To build either image separately, use the corresponding
target from the repository root.

```sh
docker build -f tools/citrus-test-distribution/docker/Dockerfile --target coordinator -t citrus-dist-coordinator .
docker build -f tools/citrus-test-distribution/docker/Dockerfile --target worker -t citrus-dist-worker .
```

The coordinator target starts `citrus dist coordinator start`. The worker target starts
`citrus dist worker start`. The [Docker deployment guide](../../../src/manual/tools-test-distribution.adoc#tools-test-distribution-docker-production)
describes using the images with deployment tooling and connecting a worker to an existing coordinator.

## Maven property reference

| Property | Effect |
| --- | --- |
| `-Dcitrus.test.distribution.enabled=true` | Enables distribution for the Maven build. The runner sets it to `false` during its prerequisite build. |
| `-Dcitrus.test.distribution.coordinatorUrl=http://127.0.0.1:8765` | Targets the coordinator. Use the published port from `start.sh` if you changed it. |
| `-Dcitrus.test.distribution.authToken=<token>` | Sets the shared bearer token for Maven. You can supply it with the `CITRUS_DIST_AUTH_TOKEN` environment variable. |
| `-Dcitrus.test.distribution.fallbackLocal=false` | Disables local fallback. `run-tests.sh` sets this automatically. |
| `-Dcitrus.test.distribution.minRemoteDurationMs=0` | Disables duration-based local execution. `run-tests.sh` sets this automatically. |
| `-Dcitrus.test.distribution.minRemoteColdUnits=0` | Disables the local shortcut based on the number of work units when duration history is missing. `run-tests.sh` sets this automatically. |
| `-Dcitrus.test.distribution.partitionPolicy=LPT` | Opts into duration-balanced partitions. The default, `SINGLE`, creates one partition per remote Surefire or Failsafe submission. |
| `-Dcitrus.test.distribution.maxPartitionSize=1` | With LPT, places at most one work unit in a partition. Leave the size unset for adaptive sizing under LPT. |
| `-Djunit.platform.launcher.interceptors.enabled=true` | Enables interceptor discovery when using the Citrus distribution library directly. The Maven extension sets it automatically. |

The Dockerfile uses these flags while building both images.

| Property | Effect |
| --- | --- |
| `-DskipTests` | Skips test execution during packaging. The test runner also uses this for prerequisites. |
| `-Dmaven.test.skip=true` | Skips compiling test sources for the image build. |
| `-Dmaven.source.skip=true` | Skips source JAR generation. |
| `-Dmaven.javadoc.skip=true` | Skips Javadoc JAR generation. |
| `-DincludeScope=runtime` | Copies the CLI's runtime dependency set. |
| `-DoutputDirectory=/opt/citrus/lib` | Places those dependencies on the container classpath. |

The runner uses `-Dmaven.repo.local=<directory>` for its checkout-local repository and
`-Dmaven.repo.local.tail=<directory>` to reuse dependencies from the usual host Maven cache. It uses
`-Dexpression=project.version`, `-DforceStdout`, and `-Dstyle.color=never` when reading the checkout's
Maven version for the temporary extension descriptor.

To verify changes to the Citrus CLI, `-Dtest=DistCommandTest` selects its distribution tests and
`-Dsurefire.failIfNoSpecifiedTests=false` lets prerequisite modules without that class pass. See the
[Maven settings guide](../../../src/manual/tools-test-distribution.adoc#tools-test-distribution-maven)
for the rest of the distribution settings.
