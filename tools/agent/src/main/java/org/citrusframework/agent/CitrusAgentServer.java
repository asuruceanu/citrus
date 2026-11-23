/*
 * Copyright 2006-2018 the original author or authors.
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

package org.citrusframework.agent;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.Router;
import org.citrusframework.CitrusSettings;
import org.citrusframework.agent.CitrusAgentOptions.ParsedOptions;
import org.citrusframework.agent.util.ConfigurationHelper;
import org.citrusframework.api.agent.CitrusAgentConfiguration;
import org.citrusframework.exceptions.CitrusRuntimeException;
import org.citrusframework.message.MessageType;
import org.citrusframework.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.currentThread;

public class CitrusAgentServer implements Server {

    /** Logger */
    private static final Logger logger = LoggerFactory.getLogger(CitrusAgentServer.class);

    /** Process exit code for invalid options or failed startup. */
    static final int STARTUP_FAILURE_EXIT_CODE = 1;

    private String name = "citrus-agent-server";

    /** Endpoint configuration */
    private final CitrusAgentConfiguration configuration;

    private final String usage;

    /** Router customizations */
    private final List<Consumer<Router>> routerCustomizations;

    private Vertx vertx;

    private boolean startRequested;

    private volatile boolean stopRequested;

    private DistributionWorkerBridge workerBridge;

    private Thread workerShutdownHook;

    /** Completed future marking completed state */
    protected final CompletableFuture<Boolean> completed = new CompletableFuture<>();

    /**
     * Default constructor uses configuration from envVars only.
     */
    public CitrusAgentServer() {
        this(ConfigurationHelper.fromEnvVars());
    }

    /**
     * Constructor uses given configuration and no router customizations.
     */
    public CitrusAgentServer(CitrusAgentConfiguration configuration) {
        this(configuration, Collections.emptyList());
    }

    /**
     * Constructor uses the configuration and the given router customizations.
     */
    public CitrusAgentServer(CitrusAgentConfiguration configuration, List<Consumer<Router>> routerCustomizations) {
        this(new ParsedOptions(configuration, null), routerCustomizations);
    }

    private CitrusAgentServer(ParsedOptions options, List<Consumer<Router>> routerCustomizations) {
        this.configuration = options.configuration();
        this.usage = options.usage();
        this.routerCustomizations = routerCustomizations;
    }

    /**
     * Constructor uses command line arguments and envVars.
     */
    public CitrusAgentServer(String[] args, List<Consumer<Router>> routerCustomizations) {
        this(new CitrusAgentOptions().parse(ConfigurationHelper.fromEnvVars(), args), routerCustomizations);
    }

    /**
     * Main method
     */
    public static void main(String[] args) {
        entrypoint(args, Collections.emptyList());
    }

    /**
     * Entrypoint method
     */
    public static void entrypoint(String[] args, List<Consumer<Router>> routerCustomizations) {
        entrypoint(args, routerCustomizations, System.out, System.err, System::exit);
    }

    static void entrypoint(String[] args, List<Consumer<Router>> routerCustomizations,
                           PrintStream out, PrintStream err, IntConsumer exit) {
        Optional<ParsedOptions> parsed = parseOptions(args, err);
        if (parsed.isEmpty()) {
            exit.accept(STARTUP_FAILURE_EXIT_CODE);
            return;
        }
        ParsedOptions options = parsed.get();
        if (options.usage() != null) {
            out.println(options.usage());
            return;
        }
        CitrusAgentServer server = new CitrusAgentServer(options, routerCustomizations);

        try {
            server.start(true);
        } catch (RuntimeException e) {
            err.println("Failed to start Citrus agent: " + e.getMessage());
            server.completed.join();
            exit.accept(STARTUP_FAILURE_EXIT_CODE);
            return;
        }

        boolean healthyShutdown = server.waitForCompletion();
        if (server.configuration.isSystemExit()) {
            exit.accept(healthyShutdown ? 0 : -1);
        }
    }

    /**
     * Parses the command line on top of the environment configuration. Invalid options, including worker cache
     * options, are reported as a single {@code Invalid Citrus agent options: <message>} line without stack trace.
     *
     * @return the parsed options, or empty when the options are invalid
     */
    static Optional<ParsedOptions> parseOptions(String[] args, PrintStream err) {
        try {
            return Optional.of(new CitrusAgentOptions().parse(ConfigurationHelper.fromEnvVars(), args));
        } catch (IllegalArgumentException | CitrusRuntimeException e) {
            err.println("Invalid Citrus agent options: " + e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Start server instance and listen for incoming requests.
     */
    @Override
    public void start() {
        start(false);
    }

    private void start(boolean limitRunTime) {
        if (usage != null) {
            System.out.println(usage);
            complete();
            return;
        }
        try {
            Future<String> deployment = startServices();
            if (deployment == null) {
                return;
            }
            awaitHttpListener(deployment);
            if (limitRunTime && configuration.getTimeToLive() > 0) {
                CompletableFuture.delayedExecutor(configuration.getTimeToLive(), TimeUnit.MILLISECONDS)
                        .execute(this::stop);
            }

            if (!configuration.isWorkerMode() && !stopRequested && !completed.isDone()) {
                runStandaloneStartup();
            }

            if (configuration.getTimeToLive() == 0) {
                stop();
            }
        } catch (RuntimeException | Error failure) {
            try {
                stop();
            } catch (RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private synchronized Future<String> startServices() {
        if (startRequested || stopRequested || completed.isDone()) {
            return null;
        }
        startRequested = true;
        ConfigurationHelper.resolveArtifacts(configuration);
        configuration.addDefaultProperty(CitrusSettings.DEFAULT_MESSAGE_TYPE_PROPERTY, MessageType.JSON.name(), false);
        configuration.setDefaultProperties();

        // Validate worker settings and acquire its cache lock before accepting HTTP requests.
        if (configuration.isWorkerMode()) {
            logger.info("Starting Citrus agent in test distribution worker mode connecting to {}", configuration.getBrokerUrl());
            workerBridge = new DistributionWorkerBridge(configuration);
            workerBridge.start();
            Thread hook = new Thread(this::stopWorker, "citrus-agent-worker-drain");
            Runtime.getRuntime().addShutdownHook(hook);
            workerShutdownHook = hook;
        }

        vertx = Vertx.vertx(new VertxOptions()
                .setMaxEventLoopExecuteTime(600)
                .setMaxEventLoopExecuteTimeUnit(TimeUnit.SECONDS));
        return vertx.deployVerticle(new CitrusAgentApplication(configuration, routerCustomizations));
    }

    private void awaitHttpListener(Future<String> deployment) {
        try {
            deployment.toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new CitrusRuntimeException("Interrupted while starting the Citrus agent HTTP listener", e);
        } catch (ExecutionException e) {
            Throwable failure = e.getCause();
            throw new CitrusRuntimeException("Could not start HTTP listener on port " + configuration.getPort()
                    + ": " + failure.getMessage(), failure);
        } catch (TimeoutException e) {
            throw new CitrusRuntimeException("Timed out starting HTTP listener on port " + configuration.getPort(), e);
        }
    }

    private void runStandaloneStartup() {
        if (configuration.isSkipTests()) {
            logger.info("Skip tests on startup - waiting for requests to run tests");
        } else if (configuration.hasTests()) {
            logger.info("Run tests on startup ...");
            new RunService().run(configuration);

            if (configuration.isSystemExit()) {
                stop();
            } else {
                logger.info("Startup done - waiting for requests to run further tests");
            }
        } else {
            logger.info("Startup done - waiting for requests to run tests");
        }
    }

    @Override
    public void run() {
        try {
            start();
            boolean healthyShutdown = waitForCompletion();

            if (!healthyShutdown) {
                logger.error("Citrus agent server has been shutdown");
            }
        } catch (Exception e) {
            throw new CitrusRuntimeException("Failed to start Citrus agent application", e);
        } finally {
            stop();
        }
    }

    /**
     * Initiates shutdown. {@link #waitForCompletion()} reports the result after the HTTP server and Vert.x have closed.
     */
    @Override
    public void stop() {
        synchronized (this) {
            if (stopRequested) {
                return;
            }
            stopRequested = true;
        }
        if (Context.isOnEventLoopThread()) {
            // Worker drain can block. Vert.x must also be able to run its own shutdown callbacks.
            CompletableFuture.runAsync(this::closeServices);
        } else {
            closeServices();
        }
    }

    private void closeServices() {
        boolean workerClosed = closeWorker();
        if (vertx == null) {
            completed.complete(workerClosed);
            return;
        }
        try {
            vertx.close().onComplete(result -> {
                if (result.failed()) {
                    logger.warn("Failed to close the Citrus agent HTTP server", result.cause());
                }
                completed.complete(workerClosed && result.succeeded());
            });
        } catch (RuntimeException | Error e) {
            logger.warn("Failed to close the Citrus agent HTTP server", e);
            completed.complete(false);
        }
    }

    private boolean closeWorker() {
        try {
            stopWorker();
            return true;
        } catch (RuntimeException | Error e) {
            logger.warn("Failed to close the Citrus agent distribution worker", e);
            return false;
        }
    }

    private void stopWorker() {
        Thread hook;
        DistributionWorkerBridge bridge;
        synchronized (this) {
            hook = workerShutdownHook;
            workerShutdownHook = null;
            bridge = workerBridge;
            workerBridge = null;
        }
        try {
            if (hook != null) {
                Runtime.getRuntime().removeShutdownHook(hook);
            }
        } catch (IllegalStateException ignored) {
            // JVM is already shutting down.
        } finally {
            if (bridge != null) {
                bridge.close();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return !completed.isDone();
    }

    /**
     * Completes this application.
     */
    public void complete() {
        completed.complete(true);
    }

    /**
     * Waits for completed state of application.
     */
    public boolean waitForCompletion() {
        try {
            return completed.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Failed to wait for server completion", e);
            currentThread().interrupt();
        }

        return false;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
