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
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.worker.DistributionWorkerClient;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.citrusframework.jbang.cli.CitrusJBangMain;

@CommandDefinition(name = "start", description = "Start a Citrus Test Distribution worker connected to a broker", generateHelp = true)
public class DistWorkerStart extends CitrusCommand {

    @ParentCommand
    DistWorker parent;

    @Option(name = "broker-url", shortName = 'b', defaultValue = "http://localhost:8765", description = "Broker base URL")
    String brokerUrl;

    @Option(name = "worker-id", shortName = 'w', description = "Unique worker identifier")
    String workerId;

    @Option(name = "auth-token", description = "Shared Bearer authentication token")
    String authToken;

    @Option(name = "capabilities", shortName = 'C', description = "Comma-separated capability tags (e.g. docker=true,gpu=true)")
    String capabilities;

    @Option(name = "cache-dir", description = "Local blob cache directory")
    String cacheDir;

    @Option(name = "duration-seconds", defaultValue = "0", description = "Optional bounded run duration in seconds (0 runs until Ctrl+C)")
    int durationSeconds;

    public DistWorkerStart() {
        super(null);
    }

    public DistWorkerStart(CitrusJBangMain main) {
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
        DistributionWorkerConfig.Builder builder = DistributionWorkerConfig.builder()
                .brokerUrl(brokerUrl)
                .capabilities(DistributionSettings.detectHostCapabilities(capabilities));
        if (workerId != null && !workerId.isBlank()) {
            builder.workerId(workerId);
        }
        if (authToken != null && !authToken.isBlank()) {
            builder.authToken(authToken);
        }
        if (cacheDir != null && !cacheDir.isBlank()) {
            builder.cacheDir(Paths.get(cacheDir));
        }

        DistributionWorkerConfig config = builder.build();
        CountDownLatch stopLatch = new CountDownLatch(1);
        Thread hook = new Thread(stopLatch::countDown, "citrus-dist-worker-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);

        try (DistributionWorkerClient client = new DistributionWorkerClient(config)) {
            client.start();
            printer().println("Citrus Test Distribution Worker " + config.workerId() + " connected to " + config.brokerUrl());
            if (durationSeconds > 0) {
                stopLatch.await(durationSeconds, TimeUnit.SECONDS);
            } else {
                stopLatch.await();
            }
            client.drain(30, TimeUnit.SECONDS);
            return CommandResult.SUCCESS;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CommandResult.SUCCESS;
        } catch (Exception e) {
            printer().println("Failed to run Citrus Test Distribution Worker: " + e.getMessage());
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
