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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.ForkedJvmOptions;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.workspace.WorkspaceLease;
import org.junit.platform.engine.CancellationToken;

/**
 * Entry point of a forked partition JVM started by {@link ForkedPartitionRunner}.
 * <p>
 * Usage: {@code ForkedPartitionMain <request.json> <port>}. Connects to the worker on the loopback port,
 * authenticates with {@code HELLO <token>}, applies the partition's system properties, runs the work units with
 * {@link PartitionJUnitLauncher} streaming {@code EVENT <json>} lines, sends {@code RESULT <json>} and exits.
 * The working directory is the partition workspace, set by the parent.
 */
public final class ForkedPartitionMain {

    /** What the parent asks the child to run. */
    public record ForkRequest(String partitionId, String sessionId, int attempt, String authToken,
                              Map<String, String> systemProperties, List<WorkUnitSpec> workUnits) {
    }

    /** Final line sent by the child. */
    public record ForkedExecutionSummary(boolean success, String errorMessage, int eventCount) {
    }

    static final String HELLO = "HELLO ";
    static final String EVENT = "EVENT ";
    static final String RESULT = "RESULT ";

    /** Set before this JVM closes the socket itself, so the parent watch does not mistake that for a lost parent. */
    private static volatile boolean finishing;

    private ForkedPartitionMain() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: ForkedPartitionMain <request.json> <port>");
            System.exit(2);
        }
        // Runs on every exit, including System.exit from a test: processes started by tests must not outlive us.
        Runtime.getRuntime().addShutdownHook(new Thread(ForkedPartitionMain::destroyDescendants, "citrus-dist-child-cleanup"));
        ForkRequest request = DistributionJson.fromJson(Files.readString(Path.of(args[0])), ForkRequest.class);
        int exitCode = 0;
        try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), Integer.parseInt(args[1]));
             Writer out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            try {
                send(out, HELLO + request.authToken());
                haltWhenParentDisappears(socket);
                applySystemProperties(request.systemProperties());

                AtomicInteger events = new AtomicInteger();
                PartitionJUnitLauncher.ExecutionSummary summary = PartitionJUnitLauncher.executeWorkUnits(
                        request.partitionId(), request.sessionId(), request.attempt(), request.workUnits(),
                        ClassLoader.getSystemClassLoader(), CancellationToken.create(), event -> {
                            events.incrementAndGet();
                            sendQuietly(out, EVENT + DistributionJson.toJson(event));
                        });
                // The parent may close its end as soon as it reads the result.
                finishing = true;
                send(out, RESULT + DistributionJson.toJson(
                        new ForkedExecutionSummary(summary.success(), summary.errorMessage(), events.get())));
            } finally {
                // Before the socket closes on either path: from here on, System.exit and its hooks do the cleanup.
                finishing = true;
            }
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 1;
        }
        // Exit explicitly: threads left behind by tests must not keep the partition JVM alive.
        System.exit(exitCode);
    }

    /**
     * The parent never writes to the socket, so end of stream means the worker went away: stop instead of running
     * the rest of the partition for nobody.
     */
    private static void haltWhenParentDisappears(Socket socket) {
        Thread watcher = new Thread(() -> {
            try {
                while (socket.getInputStream().read() != -1) {
                    // Ignore: the protocol is one-way.
                }
            } catch (IOException ignored) {
                // Closed or reset: treat like end of stream.
            }
            if (finishing) {
                return;
            }
            destroyDescendants();
            Runtime.getRuntime().halt(3);
        }, "citrus-dist-child-parent-watch");
        watcher.setDaemon(true);
        watcher.start();
    }

    /**
     * Terminates every process this JVM started, directly or indirectly: gracefully first, then forcibly.
     */
    static void destroyDescendants() {
        List<ProcessHandle> descendants = ProcessHandle.current().descendants().toList();
        descendants.forEach(ProcessHandle::destroy);
        long deadline = System.currentTimeMillis() + 2_000L;
        for (ProcessHandle handle : descendants) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining > 0) {
                try {
                    handle.onExit().get(remaining, TimeUnit.MILLISECONDS);
                } catch (Exception ignored) {
                    // Escalate below.
                }
            }
        }
        descendants.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
    }

    /**
     * Applies the partition's (already expanded) system properties, leaving JVM-owned {@code java.*}, {@code os.*}
     * and {@code user.dir*} properties alone.
     */
    static void applySystemProperties(Map<String, String> properties) {
        System.clearProperty(WorkspaceLease.TICKET_PROPERTY);
        if (properties == null) {
            return;
        }
        properties.forEach((key, value) -> {
            if (key == null || key.startsWith("java.") || key.startsWith("os.") || key.startsWith("user.dir")) {
                return;
            }
            if (ForkedJvmOptions.ASSERTIONS_PROPERTY.equals(key)) {
                ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(Boolean.parseBoolean(value));
            } else if (value == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
        });
    }

    private static synchronized void send(Writer out, String line) throws IOException {
        out.write(line);
        out.write('\n');
        out.flush();
    }

    private static void sendQuietly(Writer out, String line) {
        try {
            send(out, line);
        } catch (IOException e) {
            throw new IllegalStateException("Lost connection to the worker", e);
        }
    }
}
