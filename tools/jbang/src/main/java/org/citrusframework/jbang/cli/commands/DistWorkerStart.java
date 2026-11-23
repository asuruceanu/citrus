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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    private static final String CACHE_DIR = "--cache-dir";
    private static final String CACHE_CLEANUP = "--cache-cleanup";
    private static final String NO_CACHE_CLEANUP = "--no-cache-cleanup";
    private static final String CACHE_CLEANUP_SOURCE = "--[no-]cache-cleanup";
    private static final String CACHE_FREE_SPACE_BUFFER_SIZE = "--cache-free-space-buffer-size";

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

    @Option(name = "cache-dir", description = "Local file cache directory")
    String cacheDir;

    @Option(name = "cache-cleanup", description = "Enable or disable cleanup of the local file cache (true/false)")
    String cacheCleanup;

    @Option(name = "no-cache-cleanup", hasValue = false, description = "Disable cleanup of the local file cache")
    boolean noCacheCleanup;

    @Option(name = "cache-free-space-buffer-size", description = "Minimum free space to keep on the cache drive (e.g. 2GiB, 512MiB, or bytes)")
    String cacheFreeSpaceBufferSize;

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

    /**
     * Validates and normalizes the worker cache options of a {@code dist worker start} command line before it is
     * handed to the command parser. All {@code --[no-]cache-cleanup[=true|false]} forms collapse into a single
     * {@code --cache-cleanup=true|false} option (last flag wins), {@code --cache-dir} and
     * {@code --cache-free-space-buffer-size} must carry a non-blank value and unknown {@code --cache*} or
     * {@code --no-cache*} options are rejected. Other command lines and help requests are returned unchanged.
     *
     * @throws IllegalArgumentException on malformed, blank or unknown cache options
     */
    public static String[] normalizeWorkerStartArgs(String[] args) {
        if (args == null || args.length < 3
                || !"dist".equals(args[0]) || !"worker".equals(args[1]) || !"start".equals(args[2])) {
            return args;
        }
        for (int i = 3; i < args.length; i++) {
            if ("--help".equals(args[i]) || "-h".equals(args[i])) {
                return args;
            }
        }

        List<String> normalized = new ArrayList<>(Arrays.asList(args).subList(0, 3));
        Boolean cleanup = null;
        for (int i = 3; i < args.length; i++) {
            String arg = args[i];
            String cleanupName = matchingOption(arg, CACHE_CLEANUP, NO_CACHE_CLEANUP);
            String valueName = matchingOption(arg, CACHE_DIR, CACHE_FREE_SPACE_BUFFER_SIZE);
            if (cleanupName != null) {
                boolean value;
                if (arg.equals(cleanupName)) {
                    value = i + 1 < args.length && !args[i + 1].startsWith("-")
                            ? DistributionSettings.parseBooleanStrict(args[++i], CACHE_CLEANUP_SOURCE)
                            : true;
                } else {
                    value = DistributionSettings.parseBooleanStrict(arg.substring(cleanupName.length() + 1),
                            CACHE_CLEANUP_SOURCE);
                }
                cleanup = NO_CACHE_CLEANUP.equals(cleanupName) != value;
            } else if (valueName != null) {
                String value;
                normalized.add(arg);
                if (arg.equals(valueName)) {
                    if (i + 1 >= args.length || args[i + 1].startsWith("-")) {
                        throw new IllegalArgumentException("Missing parameter value for " + valueName + " option");
                    }
                    value = args[++i];
                    normalized.add(value);
                } else {
                    value = arg.substring(valueName.length() + 1);
                }
                if (value.isBlank()) {
                    throw new IllegalArgumentException("Missing parameter value for " + valueName + " option");
                }
                if (CACHE_FREE_SPACE_BUFFER_SIZE.equals(valueName)) {
                    DistributionSettings.parseByteSize(value, CACHE_FREE_SPACE_BUFFER_SIZE);
                }
            } else if (arg.startsWith("--cache") || arg.startsWith("--no-cache")) {
                throw new IllegalArgumentException("Unknown cache option: " + arg);
            } else {
                normalized.add(arg);
            }
        }

        if (cleanup != null) {
            normalized.add(CACHE_CLEANUP + "=" + cleanup);
        }
        return normalized.toArray(String[]::new);
    }

    private static String matchingOption(String arg, String... names) {
        for (String name : names) {
            if (arg.equals(name) || arg.startsWith(name + "=")) {
                return name;
            }
        }
        return null;
    }

    DistributionWorkerConfig buildWorkerConfig() {
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
            builder.cacheDir(Paths.get(cacheDir.trim()));
        }
        if (cacheCleanup != null) {
            builder.cacheCleanup(DistributionSettings.parseBooleanStrict(cacheCleanup, CACHE_CLEANUP_SOURCE));
        } else if (noCacheCleanup) {
            builder.cacheCleanup(false);
        }
        if (cacheFreeSpaceBufferSize != null) {
            builder.cacheFreeSpaceBufferBytes(
                    DistributionSettings.parseByteSize(cacheFreeSpaceBufferSize, CACHE_FREE_SPACE_BUFFER_SIZE));
        }
        return builder.build();
    }

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        DistributionWorkerConfig config;
        try {
            config = buildWorkerConfig();
        } catch (IllegalArgumentException e) {
            printer().println("Invalid Citrus Test Distribution Worker options: " + e.getMessage());
            return CommandResult.FAILURE;
        }

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
