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

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Option;
import org.aesh.command.option.ParentCommand;
import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.jbang.cli.CitrusJBangMain;

@CommandDefinition(name = "start", description = "Start Citrus Test Distribution broker and web UI", generateHelp = true)
public class DistBrokerStart extends CitrusCommand {

    @ParentCommand
    DistBroker parent;

    @Option(name = "host", defaultValue = "0.0.0.0", description = "Bind host (default: 0.0.0.0)")
    String host;

    @Option(name = "port", shortName = 'p', defaultValue = "8765", description = "HTTP/WebSocket port (default: 8765)")
    int port;

    @Option(name = "jdbc-url", description = "JDBC URL for session/history persistence (default: embedded HSQLDB)")
    String jdbcUrl;

    @Option(name = "cache-dir", description = "Content-addressed blob cache directory")
    String cacheDir;

    @Option(name = "auth-token", description = "Shared Bearer authentication token")
    String authToken;

    @Option(name = "max-partition-size", defaultValue = "10", description = "Maximum tests per partition")
    int maxPartitionSize;

    @Option(name = "max-retries", defaultValue = "1", description = "Maximum partition requeue retries")
    int maxRetries;

    @Option(name = "duration-seconds", defaultValue = "0", description = "Optional bounded run duration in seconds (0 runs until Ctrl+C)")
    int durationSeconds;

    @Option(name = "demo", hasValue = false, description = "Pre-seed realistic demo sessions, workers, and failure traces")
    boolean demo;

    public DistBrokerStart() {
        super(null);
    }

    public DistBrokerStart(CitrusJBangMain main) {
        super(main);
    }

    @Override
    public CitrusJBangMain getMain() {
        if (super.getMain() == null && parent != null) {
            setMain(parent.getMain());
        }
        return super.getMain();
    }

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        DistributionBrokerConfig.Builder builder = DistributionBrokerConfig.builder()
                .host(host)
                .port(port)
                .defaultMaxPartitionSize(maxPartitionSize)
                .defaultMaxRetries(maxRetries)
                .demoData(demo);
        if (jdbcUrl != null && !jdbcUrl.isBlank()) {
            builder.jdbcUrl(jdbcUrl);
        }
        if (cacheDir != null && !cacheDir.isBlank()) {
            builder.cacheDir(Paths.get(cacheDir));
        }
        if (authToken != null && !authToken.isBlank()) {
            builder.authToken(authToken);
        }

        CountDownLatch stopLatch = new CountDownLatch(1);
        Thread hook = new Thread(stopLatch::countDown, "citrus-dist-broker-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);

        try (DistributionBrokerServer server = new DistributionBrokerServer(builder.build())) {
            server.start();
            printer().println("Citrus Test Distribution Broker listening on " + server.baseUrl() + " (UI: " + server.baseUrl() + "/ui/)");
            if (durationSeconds > 0) {
                stopLatch.await(durationSeconds, TimeUnit.SECONDS);
            } else {
                stopLatch.await();
            }
            return CommandResult.SUCCESS;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CommandResult.SUCCESS;
        } catch (Exception e) {
            printer().println("Failed to start Citrus Test Distribution Broker: " + e.getMessage());
            return CommandResult.FAILURE;
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // JVM already shutting down
            }
        }
    }
}
