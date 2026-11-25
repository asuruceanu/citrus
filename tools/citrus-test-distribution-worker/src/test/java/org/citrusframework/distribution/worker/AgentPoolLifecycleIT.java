/*
 * Copyright the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.citrusframework.distribution.worker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.coordinator.DistributionCoordinatorConfig;
import org.citrusframework.distribution.coordinator.DistributionCoordinatorServer;
import org.citrusframework.distribution.coordinator.SessionCoordinator.CreatedAgentPool;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.HistoricalUsageResponse;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.PoolUsageSeries;
import org.citrusframework.distribution.model.RegistrationKeyRecord;
import org.citrusframework.distribution.model.SessionDetailResponse;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerState;
import org.citrusframework.distribution.model.WorkerWarning;
import org.citrusframework.distribution.model.WorkerWarningCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class AgentPoolLifecycleIT {

    private static final String POOLS = "/agent-pools";
    private static final Map<String, String> DOCKER = Map.of("docker", "true");
    // Workers always advertise the actual JVM release, including when this suite runs on a newer JDK.
    private static final Map<String, String> POOL_CAPABILITIES = Map.of(
            "jdk", Integer.toString(Runtime.version().feature()), "docker", "true");
    private static final String HIGH_WORKER = "z-high-worker";
    private static final String LOW_WORKER = "a-low-worker";

    @TempDir
    Path tempDir;
    private String baseUrl;
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void routesWorkToAuthenticatedPoolsAndRecordsUsageUntilKeyRevocation() throws Exception {
        var config = DistributionCoordinatorConfig.builder().host("127.0.0.1").port(0)
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("db") + ";shutdown=true")
                .cacheDir(tempDir.resolve("coordinator-cache")).requireAgentPool(true).usageSampleIntervalMs(100).build();
        try (var coordinator = new DistributionCoordinatorServer(config)) {
            coordinator.start();
            baseUrl = coordinator.baseUrl();
            var high = createPool("high-prio");
            var low = createPool("low-prio");
            assertThat(high.pool().priorityOrder()).isZero();
            assertThat(low.pool().priorityOrder()).isOne();

            assertRejected("wrong-key", high, low.generatedKey().secretKey(), DOCKER, "INVALID_REGISTRATION_KEY");
            assertRejected("missing-docker", high, high.generatedKey().secretKey(), Map.of(), "MISSING_POOL_CAPABILITIES");

            try (var lowWorker = worker(LOW_WORKER, low, low.generatedKey().secretKey(), DOCKER);
                 var highWorker = worker(HIGH_WORKER, high, high.generatedKey().secretKey(), DOCKER)) {
                lowWorker.start();
                assertThat(lowWorker.awaitRegistration(10, TimeUnit.SECONDS)).isTrue();
                highWorker.start();
                assertThat(highWorker.awaitRegistration(10, TimeUnit.SECONDS)).isTrue();
                assertWorkerPool(LOW_WORKER, low);
                assertWorkerPool(HIGH_WORKER, high);

                exerciseBothPools(high, low);
                revokeOnNextRenewal(highWorker, high);
            }
        }
    }

    private CreatedAgentPool createPool(String name) throws Exception {
        var body = request("POST", POOLS, Map.of("displayName", name, "capabilities", POOL_CAPABILITIES,
                "generateRegistrationKey", true), 201);
        return DistributionJson.fromJson(body, CreatedAgentPool.class);
    }

    private DistributionWorkerClient worker(String id, CreatedAgentPool pool, String key, Map<String, String> caps) {
        return new DistributionWorkerClient(DistributionWorkerConfig.builder().workerId(id).coordinatorUrl(baseUrl)
                .poolId(pool.pool().poolId()).registrationKey(key).capabilities(caps).version("test-version")
                .cacheDir(tempDir.resolve(id)).cacheFreeSpaceBufferBytes(0).registrationRenewalIntervalMs(3_000)
                .forkMode(DistributionSettings.WorkerForkMode.AUTO).cwdCheckMode(DistributionSettings.WorkerCwdCheckMode.OFF)
                .build());
    }

    private void assertRejected(String id, CreatedAgentPool pool, String key, Map<String, String> caps, String reason)
            throws Exception {
        try (var client = worker(id, pool, key, caps)) {
            client.start();
            assertThat(client.awaitRegistration(10, TimeUnit.SECONDS)).isFalse();
            assertThat(client.rejectionReason()).hasValueSatisfying(rejection -> assertThat(rejection.code()).isEqualTo(reason));
        }
    }

    private WorkerRecord registeredWorker(String id) throws Exception {
        return Arrays.stream(get("/workers", WorkerRecord[].class)).filter(worker -> worker.workerId().equals(id))
                .findFirst().orElseThrow();
    }

    private void assertWorkerPool(String id, CreatedAgentPool pool) throws Exception {
        var worker = registeredWorker(id);
        assertThat(worker.poolId()).isEqualTo(pool.pool().poolId());
        assertThat(worker.poolDisplayName()).isEqualTo(pool.pool().displayName());
        assertThat(worker.version()).isEqualTo("test-version");
        assertThat(worker.state()).isEqualTo(WorkerState.IDLE);
    }

    private void exerciseBothPools(CreatedAgentPool high, CreatedAgentPool low) throws Exception {
        var artifact = ClasspathPackager.packagePath(Path.of(BlockingFixture.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI()));
        var upload = HttpRequest.newBuilder(api("/cache/blobs/" + artifact.ref().sha256()))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(artifact.bytes())).build();
        assertThat(http.send(upload, HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(201);

        Path release = tempDir.resolve("release");
        String first = submit(artifact.ref(), release, "first");
        String second;
        try {
            awaitRunning(first, HIGH_WORKER, "first");
            second = submit(artifact.ref(), release, "second");
            awaitRunning(second, LOW_WORKER, "second");
            await(() -> {
                var usage = get("/usage?window=1h&filter=docker", HistoricalUsageResponse.class);
                assertThat(usage.filter()).isEqualTo(DOCKER);
                assertThat(usage.pools()).extracting(PoolUsageSeries::poolId)
                        .containsExactlyInAnyOrder(high.pool().poolId(), low.pool().poolId());
                assertThat(usage.pools()).allSatisfy(pool -> assertThat(pool.points()).as(pool.displayName())
                        .anyMatch(point -> point.connected() > 0 && point.utilized() > 0 && point.desired() > 0));
            });
        } finally {
            Files.createFile(release);
        }
        for (String session : List.of(first, second)) {
            await(() -> {
                var detail = get(session, SessionDetailResponse.class);
                assertThat(detail.partitions()).singleElement().satisfies(p ->
                        assertThat(p.state()).as(p.errorMessage()).isEqualTo(PartitionState.COMPLETED));
                assertThat(detail.tests()).singleElement().satisfies(test ->
                        assertThat(test.status()).as(test.errorMessage()).isEqualTo(TestOutcomeStatus.PASSED));
            });
        }
    }

    private String submit(ArtifactRef artifact, Path release, String label) throws Exception {
        var created = request("POST", "/sessions", new CreateSessionRequest("pool-it", "main", null, List.of(), 1, 0), 201);
        String session = "/sessions/" + DistributionJson.fromJson(created, BuildSessionResponse.class).sessionId();
        String fixture = BlockingFixture.class.getName();
        var unit = new WorkUnitSpec(fixture + "#hold", fixture, "hold", null, DOCKER, 500L);
        request("POST", session + "/work", new SubmitWorkRequest(label, "pool-it", List.of(artifact),
                Map.of("pool.it.release", release.toString(), "pool.it.started", tempDir.resolve(label).toString()),
                List.of(unit), 0, 1), 200);
        return session;
    }

    public static class BlockingFixture {
        @Test
        void hold() throws Exception {
            Files.createFile(Path.of(System.getProperty("pool.it.started")));
            Path release = Path.of(System.getProperty("pool.it.release"));
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
            while (!Files.exists(release)) {
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("Timed out waiting for the pool usage sample");
                }
                Thread.sleep(25);
            }
        }
    }

    private void awaitRunning(String session, String worker, String label) throws Exception {
        await(() -> assertThat(tempDir.resolve(label)).exists());
        await(() -> assertThat(get(session, SessionDetailResponse.class).partitions()).singleElement().satisfies(p -> {
            assertThat(p.state()).as(p.errorMessage()).isEqualTo(PartitionState.RUNNING);
            assertThat(p.workerId()).isEqualTo(worker);
        }));
    }

    private void revokeOnNextRenewal(DistributionWorkerClient client, CreatedAgentPool high) throws Exception {
        String keys = POOLS + "/" + high.pool().poolId() + "/keys";
        long previous = get(keys, RegistrationKeyRecord[].class)[0].lastUsedAt();
        // Revoke just after a successful renewal so the warning can be observed before the next tick.
        await(() -> assertThat(get(keys, RegistrationKeyRecord[].class)[0].lastUsedAt()).isGreaterThan(previous));
        var rejected = new CountDownLatch(1);
        client.onRejection(rejected::countDown);
        request("POST", keys + "/" + high.generatedKey().metadata().keyId() + "/revoke", null, 200);
        var warned = registeredWorker(HIGH_WORKER);
        assertThat(warned.state()).isEqualTo(WorkerState.IDLE);
        assertThat(warned.warnings()).extracting(WorkerWarning::code).contains(WorkerWarningCode.KEY_REVOKED);
        assertThat(rejected.await(10, TimeUnit.SECONDS)).as("revoked worker renewal").isTrue();
        assertThat(client.rejectionReason()).hasValueSatisfying(r -> assertThat(r.code()).isEqualTo("REVOKED_REGISTRATION_KEY"));
        await(() -> assertThat(registeredWorker(HIGH_WORKER).state()).isEqualTo(WorkerState.OFFLINE));
    }

    private <T> T get(String path, Class<T> type) throws Exception {
        return DistributionJson.fromJson(request("GET", path, null, 200), type);
    }

    private URI api(String path) {
        return URI.create(baseUrl + "/api/v1" + path);
    }

    private String request(String method, String path, Object payload, int status) throws Exception {
        var req = HttpRequest.newBuilder(api(path)).timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
                .method(method, payload == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(DistributionJson.toJson(payload))).build();
        var response = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("%s %s", method, path).isEqualTo(status);
        return response.body();
    }

    private static void await(CheckedAssertion assertion) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (true) {
            try {
                assertion.check();
                return;
            } catch (AssertionError pending) {
                if (System.nanoTime() >= deadline) {
                    throw pending;
                }
                Thread.sleep(50);
            }
        }
    }

    @FunctionalInterface
    private interface CheckedAssertion {
        void check() throws Exception;
    }
}
