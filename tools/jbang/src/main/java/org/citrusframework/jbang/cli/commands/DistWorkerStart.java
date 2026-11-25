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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Option;
import org.aesh.command.option.ParentCommand;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.config.WorkerCacheCliOptions;
import org.citrusframework.distribution.worker.DistributionWorkerClient;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.citrusframework.jbang.cli.CitrusJBangMain;

@CommandDefinition(name = "start", description = "Start a Citrus Test Distribution worker connected to a coordinator")
public class DistWorkerStart extends CitrusCommand {

    private static final List<String> COMMAND_WORDS = List.of("dist", "worker", "start");

    /** Options of this command that take a separate value token, which is never parsed as a cache option. */
    private static final Set<String> VALUE_OPTIONS = Set.of("--coordinator-url", "-u", "--worker-id", "-w",
            "--auth-token", "--pool", "-p", "--registration-key", "-k", "--capabilities", "-C", "--duration-seconds");

    /** Drain timeout plus slack for closing the worker client after a shutdown request. */
    private static final long SHUTDOWN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(60);

    @ParentCommand
    DistWorker parent;

    // Handled by this command (instead of generated help) so usage goes to the main printer like all other output
    @Option(name = "help", shortName = 'h', hasValue = false, description = "Display this help and exit")
    boolean helpRequested;

    @Option(name = "coordinator-url", shortName = 'u', defaultValue = "http://localhost:8765", description = "Coordinator base URL")
    String coordinatorUrl;

    @Option(name = "worker-id", shortName = 'w', description = "Unique worker identifier")
    String workerId;

    @Option(name = "auth-token", description = "Shared Bearer authentication token")
    String authToken;

    @Option(name = "pool", shortName = 'p', description = "Agent pool ID used for worker registration")
    String pool;

    @Option(name = "registration-key", shortName = 'k', description = "Registration key used to join the worker pool")
    String registrationKey;

    @Option(name = "capabilities", shortName = 'C', description = "Comma-separated capability tags (e.g. docker=true,gpu=true)")
    String capabilities;

    @Option(name = "cache-dir", description = "Local file cache directory")
    String cacheDir;

    @Option(name = "cache-cleanup", description = "Enable or disable cleanup of the local file cache (true/false)")
    String cacheCleanup;

    /*
     * Help only: lists the negated form in the generated usage. normalizeWorkerStartArgs rewrites every
     * --[no-]cache-cleanup form into --cache-cleanup=true|false before the command parser runs, so this field is
     * only set when the command is bound without normalization.
     */
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
     * handed to the command parser. The {@code dist worker start} command words are located after any leading
     * option tokens; the options following them are parsed with the shared {@link WorkerCacheCliOptions} parser
     * (see there for the accepted forms, the help rule and the error messages) and every given cache setting is
     * re-emitted once in canonical {@code --name=value} form after the other options, so all
     * {@code --[no-]cache-cleanup[=true|false]} forms collapse into a single {@code --cache-cleanup=true|false}
     * (last flag wins). Other command lines are returned unchanged.
     *
     * @throws IllegalArgumentException on malformed, blank or unknown cache options
     */
    public static String[] normalizeWorkerStartArgs(String[] args) {
        int optionsStart = workerStartOptionsIndex(args);
        if (optionsStart < 0) {
            return args;
        }

        List<String> all = Arrays.asList(args);
        WorkerCacheCliOptions.Result cache = WorkerCacheCliOptions.parse(all.subList(optionsStart, args.length),
                VALUE_OPTIONS);
        List<String> normalized = new ArrayList<>(all.subList(0, optionsStart));
        normalized.addAll(cache.remainingArgs());
        normalized.addAll(cache.canonicalArgs());
        return normalized.toArray(String[]::new);
    }

    /**
     * Index of the first option after the {@code dist worker start} command words, which may be preceded by option
     * tokens (e.g. global options), or {@code -1} when the command line is not a worker start command.
     */
    private static int workerStartOptionsIndex(String[] args) {
        if (args == null) {
            return -1;
        }
        int matched = 0;
        for (int i = 0; i < args.length; i++) {
            if (COMMAND_WORDS.get(matched).equals(args[i])) {
                if (++matched == COMMAND_WORDS.size()) {
                    return i + 1;
                }
            } else if (!args[i].startsWith("-")) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Shutdown hook that asks the running worker to stop and then holds JVM exit until the worker client has been
     * closed (releasing the cache lock) or the timeout elapses.
     */
    static Thread shutdownHook(CountDownLatch stopLatch, CountDownLatch closedLatch, long timeoutMillis) {
        return new Thread(() -> {
            stopLatch.countDown();
            try {
                closedLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "citrus-dist-worker-shutdown");
    }

    DistributionWorkerConfig buildWorkerConfig() {
        DistributionWorkerConfig.Builder builder = DistributionWorkerConfig.builder()
                .coordinatorUrl(coordinatorUrl)
                .poolId(pool)
                .registrationKey(registrationKey)
                .addCapabilitiesCsv(capabilities);
        if (workerId != null && !workerId.isBlank()) {
            builder.workerId(workerId);
        }
        if (authToken != null && !authToken.isBlank()) {
            builder.authToken(authToken);
        }
        if (cacheDir != null && !cacheDir.isBlank()) {
            builder.cacheDir(Paths.get(cacheDir));
        }
        if (cacheCleanup != null) {
            builder.cacheCleanup(DistributionSettings.parseBooleanStrict(cacheCleanup,
                    WorkerCacheCliOptions.CACHE_CLEANUP_SOURCE));
        } else if (noCacheCleanup) {
            builder.cacheCleanup(false);
        }
        if (cacheFreeSpaceBufferSize != null) {
            builder.cacheFreeSpaceBufferBytes(DistributionSettings.parseByteSize(cacheFreeSpaceBufferSize,
                    WorkerCacheCliOptions.CACHE_FREE_SPACE_BUFFER_SIZE));
        }
        return builder.build();
    }

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        if (helpRequested) {
            printer().println(invocation.getHelpInfo());
            return CommandResult.SUCCESS;
        }

        DistributionWorkerConfig config;
        try {
            config = buildWorkerConfig();
        } catch (IllegalArgumentException e) {
            printer().println("Invalid Citrus Test Distribution Worker options: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        CountDownLatch stopLatch = new CountDownLatch(1);
        CountDownLatch closedLatch = new CountDownLatch(1);
        Thread hook = shutdownHook(stopLatch, closedLatch, SHUTDOWN_TIMEOUT_MILLIS);
        Runtime.getRuntime().addShutdownHook(hook);

        try (DistributionWorkerClient client = new DistributionWorkerClient(config)) {
            client.onRejection(stopLatch::countDown);
            client.start();
            printer().println("Citrus Test Distribution Worker " + config.workerId() + " connected to " + config.coordinatorUrl());
            if (durationSeconds > 0) {
                stopLatch.await(durationSeconds, TimeUnit.SECONDS);
            } else {
                stopLatch.await();
            }
            var rejection = client.rejectionReason();
            if (rejection.isPresent()) {
                var reason = rejection.get();
                String message = "Worker registration rejected (" + reason.code() + "): " + reason.message();
                String key = config.registrationKey();
                printer().println(key == null || key.isEmpty() ? message : message.replace(key, "[redacted]"));
                return CommandResult.FAILURE;
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
            // The client (and with it the cache lock) is closed by now; let a pending shutdown hook finish.
            closedLatch.countDown();
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // JVM already shutting down
            }
        }
    }
}
