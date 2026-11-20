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

package org.citrusframework.jbang.cli.commands;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;

import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.jbang.cli.CitrusJBangMain;
import org.citrusframework.jbang.cli.StringPrinter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("citrus-jbang-run")
class DistCommandTest extends CommandTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldPrintDistHelpTree() {
        CitrusJBangMain main = createCitrusJBangMain();
        assertThat(main.execute("dist")).isZero();
        assertThat(printer.getOutput()).contains("broker").contains("worker").contains("sessions").contains("status");

        printer = new StringPrinter();
        main = createCitrusJBangMain();
        assertThat(main.execute("dist", "broker")).isZero();
        assertThat(printer.getOutput()).contains("start");

        printer = new StringPrinter();
        main = createCitrusJBangMain();
        assertThat(main.execute("dist", "worker")).isZero();
        assertThat(printer.getOutput()).contains("start");

        assertThat(main.execute("dist", "--help")).isZero();
        assertThat(main.execute("dist", "broker", "start", "--help")).isZero();
        assertThat(main.execute("dist", "worker", "start", "--help")).isZero();
        assertThat(main.execute("dist", "sessions", "--help")).isZero();
        assertThat(main.execute("dist", "status", "--help")).isZero();
    }

    @Test
    void shouldQueryLiveBrokerSessionsAndStatus() throws Exception {
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("cache"))
                .jdbcUrl("jdbc:hsqldb:mem:jbang_dist_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(config)) {
            broker.start();

            HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(broker.baseUrl() + "/api/v1/sessions"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(DistributionJson.toJson(
                                    new CreateSessionRequest("citrus-demo", "main", "deadbeef", List.of(), 5, 1))))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());

            CitrusJBangMain main = createCitrusJBangMain();
            main.execute("dist", "sessions", "--broker-url", broker.baseUrl());
            assertThat(printer.getOutput()).contains("citrus-demo").contains("RUNNING");

            printer = new StringPrinter();
            main = createCitrusJBangMain();
            main.execute("dist", "status", "--broker-url", broker.baseUrl(), "--json");
            assertThat(printer.getOutput()).contains("\"status\"").contains("UP");
        }
    }

    @Test
    void shouldStartBrokerAndWorkerWithBoundedDuration() throws Exception {
        DistributionBrokerConfig config = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("worker-broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:jbang_worker_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(config)) {
            broker.start();

            CitrusJBangMain main = createCitrusJBangMain();
            main.execute("dist", "worker", "start",
                    "--broker-url", broker.baseUrl(),
                    "--worker-id", "jbang-worker-1",
                    "--cache-dir", tempDir.resolve("wcache").toString(),
                    "--duration-seconds", "1");
            assertThat(printer.getOutput()).contains("jbang-worker-1");
        }

        printer = new StringPrinter();
        CitrusJBangMain main = createCitrusJBangMain();
        main.execute("dist", "broker", "start",
                "--port", "0",
                "--jdbc-url", "jdbc:hsqldb:mem:jbang_broker_cli_" + System.nanoTime(),
                "--cache-dir", tempDir.resolve("bcache").toString(),
                "--duration-seconds", "1");
        assertThat(printer.getOutput()).contains("Citrus Test Distribution Broker listening on");
    }
}
