# Citrus Test Distribution Docker Compose for local development

## Intent and scope

Give developers a repeatable way to run Citrus Test Distribution on their own machine from an
`incubator/test-distribution` checkout. One Compose command builds the Citrus images in the
local Docker engine and starts a coordinator with the dashboard, PostgreSQL, and eight workers.
Maven runs on the developer's machine and submits tests to the coordinator.

## Approaches

- **Build from the checkout, selected.** One Dockerfile builds the coordinator and worker
  images from the same source revision. Developers can try changes to either component by
  rebuilding. A cold build downloads Maven dependencies.
- **Pull versioned Citrus images.** This would shorten initial startup and would require
  registry access and an image publishing pipeline.

The build uses one multi-stage Dockerfile with separate `coordinator` and `worker` targets
and a shared build stage. Both images package the existing Citrus CLI with its Maven runtime
dependencies, use Java 21, and run as a non-root user. Each uses its own entry point. The worker
image accepts the existing Citrus environment variables for coordinator URL, bearer token, and
pool credentials. Tests run inside the worker image, so workload-specific tools and system
dependencies belong in a worker image extension.

## Compose layout

`tools/citrus-test-distribution/docker/compose.yaml` defines all three services. Each worker is a
replica of the same service. The coordinator connects to PostgreSQL, and workers connect to
the coordinator over the Compose network. The coordinator is the only service with a published
host port.

| Service | Runtime and storage | Connections |
| --- | --- | --- |
| PostgreSQL | PostgreSQL 16 with a named data volume | Accepts the coordinator's JDBC connection at `postgres:5432` |
| Coordinator | One Java 21 container with a named artifact cache volume | Publishes `127.0.0.1:8765` on the developer's machine by default |
| Worker | Eight Java 21 replicas, each with a cache in its own writable container storage | Connects to `http://coordinator:8765` on the Compose network |

PostgreSQL readiness gates coordinator startup. The coordinator's `/health` response gates
worker startup. Use the dashboard or worker API to confirm registration after startup. The
worker gives an active partition 30 seconds to finish. Compose allows a shutdown grace period
of 65 seconds for the container to drain and close its connections.
[Docker Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/)

## Local development workflow

Start from the Citrus checkout.

```sh
./tools/citrus-test-distribution/docker/start.sh
```

The start script opens the dashboard at `http://127.0.0.1:8765/ui/` by default. Point a
distribution-enabled Maven build at `http://127.0.0.1:8765`. Use `CITRUS_DIST_HOST_PORT` to move the host port and
`CITRUS_DIST_WORKERS` to resize the pool.

Compose supplies a development database password for a fresh PostgreSQL volume. The developer
can override it with `CITRUS_DIST_JDBC_PASSWORD` before the first start. Set
`CITRUS_DIST_AUTH_TOKEN` in Compose and Maven when testing bearer authentication. Compose
passes the same token to the coordinator and all workers. With bearer authentication enabled,
the dashboard uses a trusted reverse proxy that supplies the bearer header for its requests.

### Lifecycle scripts

The three executable shell scripts resolve `compose.yaml` beside themselves. They target
`citrus-test-distribution` by default and honor an exported `COMPOSE_PROJECT_NAME`.

| Script | Compose operation | Effect |
| --- | --- | --- |
| `start.sh` | `up --detach --build --wait` | Build from the checkout and start the stack. Once PostgreSQL and the coordinator are healthy and worker containers are running, open the dashboard. |
| `stop.sh` | `stop` | Gracefully stop services and keep containers, images, named volumes, and worker caches. Starting again with the same source and settings can reuse this state. |
| `clean.sh --yes` | `down --volumes --rmi local --remove-orphans` | Delete the project's containers, worker caches, networks, named data volumes, and the coordinator and worker images built by Compose. Require `--yes` because this permanently deletes local test history and artifacts. |

The start script gets the published address from `docker compose port coordinator 8765` and
prints the dashboard URL. It opens the URL in the default browser on macOS and on Linux
desktops with `xdg-open`. On a headless machine the printed URL is the handoff.
[Docker Compose port](https://docs.docker.com/reference/cli/docker/compose/port/)

Named volumes also retain the database and coordinator cache across service recreation.
Replacing a worker creates a fresh container cache.
[Docker Compose stop](https://docs.docker.com/reference/cli/docker/compose/stop/)
[Docker Compose down](https://docs.docker.com/reference/cli/docker/compose/down/)

## Acceptance checks

1. From a checkout, `start.sh` builds and starts the stack, opens the dashboard on the
   published localhost port, and eight distinct workers register.
2. The coordinator's published port is bound to loopback. With an optional bearer token set,
   authenticated Maven builds and workers connect, and API requests without it get HTTP 401.
3. A Maven smoke suite with one partition per test executes eight remote-only tests with
   local fallback disabled. PostgreSQL contains the passing results.
4. After a coordinator restart and a stack recreation with named volumes retained, completed
   results remain visible, the artifact cache is available, and eight workers register again.
5. `stop.sh` retains stack resources and a subsequent start reuses them with the same source
   and settings. `clean.sh --yes` removes the selected stack's resources and source-built
   Citrus images. A second stack with a different project name keeps its resources.
