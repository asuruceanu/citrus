# Citrus Test Distribution Docker Compose Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and operate a local Citrus Test Distribution stack with PostgreSQL and eight workers using three shell scripts.

**Architecture:** One multi-stage Dockerfile builds separate coordinator and worker images from the checkout. Compose manages one coordinator, PostgreSQL, and the worker replicas. The lifecycle scripts target one Compose project, and startup opens the dashboard on the actual published port.

**Tech Stack:** Java 21, Maven 3.9+, Docker BuildKit, Docker Compose, PostgreSQL 16, POSIX shell.

**Spec:** `docs/superpowers/specs/2026-11-25-test-distribution-docker-design.md`

## Global Constraints

- Build both Citrus images locally from the same checkout. The rollout target is a developer's Docker engine.
- Use Java 21 and non-root users in both images. Preserve the existing Citrus coordinator and dedicated worker entry points.
- Default to PostgreSQL 16, one coordinator, and eight worker replicas. Publish only the coordinator on `127.0.0.1:8765`.
- Keep PostgreSQL history and coordinator artifacts in named volumes. Keep each worker cache in that replica's writable container storage. Allow 65 seconds for worker drain.
- Use `CITRUS_DIST_HOST_PORT`, `CITRUS_DIST_WORKERS`, `CITRUS_DIST_JDBC_PASSWORD`, and optional `CITRUS_DIST_AUTH_TOKEN` as specified. All scripts use the same Compose file and `COMPOSE_PROJECT_NAME` selection.
- The current worktree has an exploratory draft. Use it as a reference, reproduce red checks against the base where necessary, and verify the final changes as a whole.

## Review Focus

1. A packaged worker can register with a missing runtime dependency and fail on its first assignment. Task 1 checks the packaged JUnit Platform and Task 3 executes remote work.
2. A caller can launch a script from another directory or use a port from the Compose project's `.env` file. Task 2 checks script-relative paths and discovers the published address after startup.
3. Failed startup and headless shells should give useful terminal output. Task 2 checks startup failure, browser launch failure, and the URL printed when a browser is unavailable.
4. Pausing the stack should retain worker identities, caches, and history. Task 3 compares container IDs and stored results across a stop and start.
5. A cleanup command can delete test history or affect a second stack. Tasks 2 and 3 check the explicit opt-in, project selection, and resource removal.

## Task 1 — Package the coordinator and worker images

**Files**

- Modify `tools/jbang/pom.xml` to include the JUnit Platform launcher at runtime.
- Modify `tools/jbang/src/main/java/org/citrusframework/jbang/cli/commands/DistCoordinatorStart.java` to read the existing JDBC and bearer settings.
- Add a regression in `tools/jbang/src/test/java/org/citrusframework/jbang/cli/commands/DistCommandTest.java`.
- Create or reconcile `tools/citrus-test-distribution/docker/Dockerfile` and `tools/citrus-test-distribution/docker/Dockerfile.dockerignore`.

**Interfaces**

- Build targets `coordinator` and `worker` package `org.citrusframework.jbang.cli.CitrusJBangMain` with its Maven runtime classpath. Use `dist coordinator start` and `dist worker start` as the respective entry points.
- The coordinator accepts existing `--jdbc-url` and `--cache-dir` options. Read the username and password with `DistributionSettings.getJdbcUser()` and `getJdbcPassword()`. The auth token resolves from explicit `--auth-token` first, then `DistributionSettings.getAuthToken()`.
- The worker accepts `CITRUS_DIST_COORDINATOR_URL` and existing worker settings. Set its default cache to `/var/lib/citrus/cache`. The runtime user can write `/var/lib/citrus`.

- [ ] **Write the red checks.** In `shouldUseConfiguredJdbcCredentialsAndAllowAuthTokenOverride`, assert that the PostgreSQL JDBC URL, configured user, password, and inherited token reach `DistCoordinatorStart.buildCoordinatorConfig()`, then assert explicit `--auth-token` wins. Use the existing system property resource lock. For the packaged worker, assert that the runtime contains `org/junit/platform/engine/CancellationToken.class` in the JUnit Platform engine JAR.
- [ ] **Check the baseline.** Run `./mvnw -B -ntp -pl tools/jbang -am test -Dtest=DistCommandTest#shouldUseConfiguredJdbcCredentialsAndAllowAuthTokenOverride -Dsurefire.failIfNoSpecifiedTests=false`. Confirm the baseline cannot pass the configuration regression. Build the worker in a temporary checkout with the launcher set to test scope and confirm the runtime class check fails.
- [ ] **Package both targets.** Change the direct JUnit Platform launcher dependency to runtime scope. Reuse the existing settings accessors, preserving the CLI's HSQLDB defaults. Use the Maven wrapper with a locked dependency cache in the shared build stage. Install `tools/jbang` and its reactor dependencies with `-DskipTests -Dmaven.test.skip=true -Dmaven.source.skip=true -Dmaven.javadoc.skip=true`. Use `dependency:copy-dependencies -DincludeScope=runtime` in the JBang module. Stage the CLI JAR and runtime libraries under `/opt/citrus` for both targets. Run each as UID `10001`. Keep worker storage in the container layer and exclude Git metadata, generated build outputs, and local environment files from the build context.
- [ ] **Run the green checks.** Run `./mvnw -B -ntp -pl tools/jbang -am test -Dtest=DistCommandTest -Dsurefire.failIfNoSpecifiedTests=false`. Build the worker target with `docker build -f tools/citrus-test-distribution/docker/Dockerfile --target worker -t citrus-dist-worker-package-check .`, then check its user and classpath:

  ```sh
  docker run --rm --entrypoint sh citrus-dist-worker-package-check -c '
    test "$(id -u)" = 10001 &&
      jar tf /opt/citrus/lib/junit-platform-engine-*.jar |
      grep -qx org/junit/platform/engine/CancellationToken.class
  '
  ```

  Both checks exit successfully. Remove the temporary package-check tag after verification.
- [ ] **Commit** the CLI regression, runtime dependency, and image packaging together as `feat(dist): package local coordinator and worker images`.

## Task 2 — Define the Compose stack and lifecycle scripts

**Files**

- Create or reconcile `tools/citrus-test-distribution/docker/compose.yaml`.
- Create executable `tools/citrus-test-distribution/docker/start.sh`, `stop.sh`, and `clean.sh`.

**Interfaces**

- Compose project `citrus-test-distribution` with services `postgres`, `coordinator`, and `worker`. Use `postgres:16-alpine`, database `citrus_dist`, user `citrus`, and named volumes `postgres-data` and `coordinator-data`. Leave service image names to Compose for the two source builds.
- Every script resolves its own directory and changes into it. Pass the absolute path of the adjacent `compose.yaml` with `--file` and `${COMPOSE_PROJECT_NAME:-citrus-test-distribution}` with `--project-name`. Scripts propagate Compose failures.
- `start.sh` takes no arguments. It runs `up --detach --build --wait`, then `port coordinator 8765`. It prints `http://<published-address>/ui/` and opens it with `open` on macOS or `xdg-open` on Linux when `DISPLAY` or `WAYLAND_DISPLAY` is set. Launch the browser without waiting for it to close. The printed URL covers headless machines, missing launchers, and browser launch failures. A Compose failure or empty published address exits before browser launch.
- `stop.sh` takes no arguments and runs `stop`. `clean.sh` requires exactly `--yes` and runs `down --volumes --rmi local --remove-orphans`. Print the chosen project name and say that its volumes contain test history and artifacts. A missing or invalid argument exits with status 2 before running Docker.

- [ ] **Write and run the red checks.** Use a temporary shell harness with `docker`, `uname`, and the desktop launcher stubbed in `PATH`. Invoke the scripts from a different directory, using a checkout path with spaces and `COMPOSE_PROJECT_NAME=citrus-lifecycle-smoke`. Assert the exact Compose project, file, and subcommand each script uses. On startup, stub `port coordinator 8765` to return `127.0.0.1:18765` and assert the printed and opened URL. Use stubs for macOS `open` and Linux `xdg-open`. Assert headless startup prints the URL, a launcher failure keeps the successful Compose exit, and a failed `up` or empty port lookup exits nonzero before the browser opens. Assert `stop` never invokes `down` and invalid clean arguments invoke no Docker command. The missing scripts fail these checks at baseline.
- [ ] **Add Compose and the scripts.** Build the two targets from the repository root and connect workers to `http://coordinator:8765`. Give PostgreSQL a TCP readiness probe and wait for it before starting the coordinator. Wait for the coordinator's `/health` before starting workers. Use `init: true` for the Java services and a 65-second worker stop grace period. Set `scale: ${CITRUS_DIST_WORKERS:-8}`, bind only `127.0.0.1:${CITRUS_DIST_HOST_PORT:-8765}:8765`, and pass the optional bearer token to both Citrus services. Start and stop with the configured grace periods.
- [ ] **Run the green checks.** Run the script harness, `sh -n` on each script, and `docker compose -f tools/citrus-test-distribution/docker/compose.yaml config --quiet`. Inspect the rendered config for the service names, default project, build targets, named volumes, health dependencies, loopback port, and eight replicas. Re-render with `CITRUS_DIST_WORKERS=3 CITRUS_DIST_HOST_PORT=18765` and confirm the requested replica count and published port.
- [ ] **Commit** the Compose file and lifecycle scripts as `feat(dist): manage the local Compose stack`.

## Task 3 — Document and verify the developer workflow

**Files**

- Update `src/manual/tools-test-distribution.adoc`.
- Use a temporary Maven smoke project with eight Java 21 tests annotated `@RemoteOnly`.

**Interfaces**

- The smoke project uses the checkout version of `org.citrusframework.mvn:citrus-test-distribution-maven-extension` in `.mvn/extensions.xml`, the checkout's Citrus distribution client dependency, and its JUnit Jupiter version. Each test asserts execution on Linux inside the worker.
- Use `COMPOSE_PROJECT_NAME=citrus-lifecycle-smoke` and an available loopback port for the smoke stack. Use the same values for all lifecycle commands. Any cleanup applies to this disposable project.

- [ ] **Reproduce the packaging failure and run the real workload.** Retain the red evidence from Task 1. From the final checkout run `start.sh`, load the dashboard, and confirm eight distinct workers through `/api/v1/workers`. Run the fixture with `-Dcitrus.test.distribution.enabled=true`, `-Dcitrus.test.distribution.coordinatorUrl=http://127.0.0.1:<port>`, `-Dcitrus.test.distribution.fallbackLocal=false`, and `-Dcitrus.test.distribution.maxPartitionSize=1`. Expect eight passing partitions and eight test execution rows in PostgreSQL. Exercise a token-enabled start and check that Maven and workers connect with the token and an unauthenticated API request returns HTTP 401.
- [ ] **Verify persistence and stop.** Record a passing session ID and the path of one test artifact in the coordinator cache. Restart the coordinator, verify that session and artifact, then run Compose `down` without volume removal and run `start.sh` again. Recheck the session and artifact and confirm eight workers register. Capture container IDs, image references, and volume names, then run `stop.sh` and `start.sh` with the same source and settings and check they match.
- [ ] **Verify scoped cleanup.** Start a disposable second project with one worker, a different `COMPOSE_PROJECT_NAME`, and a different loopback port. Run `clean.sh` for the first project without `--yes` and verify it still exists. Run it with `--yes` and check the first project's containers, network, named volumes, and two source-built image tags are gone. Check the second project's containers, volumes, and image tags and the PostgreSQL image still exist. Clean the second disposable stack with `--yes`.
- [ ] **Update and build the manual.** Document the three scripts, automatic dashboard opening on the published port, environment overrides, worker cache lifetime, and deletion of test history by cleanup. Explain how to verify eight registered workers in the dashboard. Run `./mvnw process-resources -Pbuild-manual -N` and check that the manual builds successfully.
- [ ] **Commit** the manual as `docs(dist): describe local Docker Compose operations`. Review the complete branch against the design and verify the final diff with `git diff --check`.
