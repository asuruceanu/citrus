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

package org.citrusframework.mvn.distribution;

import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.maven.AbstractMavenLifecycleParticipant;
import org.apache.maven.MavenExecutionException;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.project.MavenProject;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.junit.BrokerHttpClient;
import org.citrusframework.distribution.junit.DistributionLauncherInterceptor;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maven Core Extension managing a single reactor-wide Citrus Test Distribution session across all
 * module Surefire and Failsafe test executions.
 */
@Named("citrus-test-distribution")
@Singleton
public class TestDistributionLifecycleParticipant extends AbstractMavenLifecycleParticipant {

    private static final Logger LOG = LoggerFactory.getLogger(TestDistributionLifecycleParticipant.class);

    private volatile String activeSessionId;
    private volatile BrokerHttpClient activeBrokerClient;
    private volatile Thread shutdownHook;

    @Override
    public void afterProjectsRead(MavenSession session) throws MavenExecutionException {
        if (!isDistributionEnabled(session)) {
            return;
        }

        String brokerUrl = resolveProperty(session, DistributionSettings.BROKER_URL_PROPERTY,
                DistributionSettings.getBrokerUrl());
        String authToken = resolveProperty(session, DistributionSettings.AUTH_TOKEN_PROPERTY,
                DistributionSettings.getAuthToken());
        boolean fallbackLocal = Boolean.parseBoolean(resolveProperty(
                session, DistributionSettings.FALLBACK_LOCAL_PROPERTY,
                Boolean.toString(DistributionSettings.isFallbackLocal())));
        int maxPartitionSize = Integer.parseInt(resolveProperty(
                session, DistributionSettings.MAX_PARTITION_SIZE_PROPERTY,
                Integer.toString(DistributionSettings.getMaxPartitionSize())));
        int maxRetries = Integer.parseInt(resolveProperty(
                session, DistributionSettings.MAX_RETRIES_PROPERTY,
                Integer.toString(DistributionSettings.getMaxRetries())));

        MavenProject topProject = session.getTopLevelProject();
        String defaultProjectName = defaultProjectName(session);
        String projectName = resolveProperty(session, DistributionSettings.PROJECT_NAME_PROPERTY, defaultProjectName);
        if (projectName == null || projectName.isBlank()) {
            projectName = defaultProjectName;
        }
        Path rootDir = session.getExecutionRootDirectory() != null
                ? Path.of(session.getExecutionRootDirectory())
                : (topProject != null && topProject.getBasedir() != null ? topProject.getBasedir().toPath() : null);

        if (session.getProjects() != null) {
            try {
                for (MavenProject project : session.getProjects()) {
                    SurefirePluginMutator.validateProject(project, session.getUserProperties(), System::getenv);
                }
            } catch (IllegalArgumentException e) {
                throw new MavenExecutionException(e.getMessage(), e);
            }
        }

        GitDiffDetector.GitWorkspaceInfo gitInfo = GitDiffDetector.inspect(rootDir);
        BrokerHttpClient client = new BrokerHttpClient(
                brokerUrl, authToken, DistributionSettings.getConnectTimeoutMs());

        String sessionId = null;
        try {
            BuildSessionResponse created = client.createSession(new CreateSessionRequest(
                    projectName,
                    gitInfo.branch(),
                    gitInfo.commit(),
                    gitInfo.changedFiles(),
                    maxPartitionSize,
                    maxRetries));
            sessionId = created.sessionId();
            this.activeSessionId = sessionId;
            this.activeBrokerClient = client;
            registerAbortShutdownHook(sessionId, client);
            LOG.info("Opened Citrus Test Distribution reactor session {} on broker {}", sessionId, brokerUrl);
        } catch (Exception e) {
            if (!fallbackLocal) {
                throw new MavenExecutionException(
                        "Failed to open Citrus Test Distribution session on broker " + brokerUrl, e);
            }
            LOG.warn("Citrus Test Distribution broker {} unreachable ({}); Surefire/Failsafe will run locally",
                    brokerUrl, e.getMessage());
            return;
        }

        List<Path> runtimeJars = resolveInterceptorClasspathJars();
        Map<String, String> sharedProps = new LinkedHashMap<>();
        sharedProps.put(DistributionSettings.BROKER_URL_PROPERTY, brokerUrl);
        if (authToken != null && !authToken.isBlank()) {
            sharedProps.put(DistributionSettings.AUTH_TOKEN_PROPERTY, authToken);
        }
        sharedProps.put(DistributionSettings.FALLBACK_LOCAL_PROPERTY, Boolean.toString(fallbackLocal));
        sharedProps.put(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY, Integer.toString(maxPartitionSize));
        sharedProps.put(DistributionSettings.MAX_RETRIES_PROPERTY, Integer.toString(maxRetries));
        sharedProps.put(DistributionSettings.PROJECT_NAME_PROPERTY, projectName);

        if (session.getProjects() != null) {
            for (MavenProject project : session.getProjects()) {
                SurefirePluginMutator.configureProject(project, sessionId, runtimeJars, sharedProps,
                        session.getUserProperties(), System::getenv);
            }
        }
    }

    @Override
    public void afterSessionEnd(MavenSession session) {
        unregisterAbortShutdownHook();
        String sessionId = this.activeSessionId;
        BrokerHttpClient client = this.activeBrokerClient;
        this.activeSessionId = null;
        this.activeBrokerClient = null;
        if (sessionId == null || client == null) {
            return;
        }
        try {
            if (isInterruptedBuild(session)) {
                client.abortSession(sessionId, "Maven build interrupted");
                LOG.info("Aborted Citrus Test Distribution reactor session {}", sessionId);
                return;
            }
            boolean buildFailed = session.getResult() != null && session.getResult().hasExceptions();
            String failureReason = buildFailed ? describeBuildFailure(session.getResult().getExceptions()) : null;
            client.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L, buildFailed, failureReason));
            LOG.info("Completed Citrus Test Distribution reactor session {}", sessionId);
        } catch (Exception e) {
            LOG.warn("Failed to complete Citrus Test Distribution session {}: {}", sessionId, e.getMessage());
        }
    }

    private void registerAbortShutdownHook(String sessionId, BrokerHttpClient client) {
        Thread hook = new Thread(() -> {
            if (sessionId.equals(this.activeSessionId)) {
                this.activeSessionId = null;
                this.activeBrokerClient = null;
                try {
                    client.abortSession(sessionId, "Maven build process terminated");
                } catch (Exception ignored) {
                    // Best-effort abort during JVM shutdown
                }
            }
        }, "citrus-dist-abort-hook");
        this.shutdownHook = hook;
        try {
            Runtime.getRuntime().addShutdownHook(hook);
        } catch (IllegalStateException | SecurityException ignored) {
            // Ignore if JVM is already shutting down
        }
    }

    private void unregisterAbortShutdownHook() {
        Thread hook = this.shutdownHook;
        this.shutdownHook = null;
        if (hook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException | SecurityException ignored) {
                // Ignore if JVM is already shutting down
            }
        }
    }

    Thread getShutdownHook() {
        return shutdownHook;
    }

    private static final int MAX_FAILURE_REASON_LENGTH = 4000;

    /** Terminal control sequences: CSI (colors, cursor; including private {@code ESC[?...}) and OSC (titles, links). */
    private static final java.util.regex.Pattern ANSI_ESCAPE = java.util.regex.Pattern.compile(
            "\u001B\\[[0-?]*[ -/]*[@-~]|\u001B\\][^\u0007\u001B]*(?:\u0007|\u001B\\\\)");

    /** A stack frame line such as {@code at org.example.Foo.bar(Foo.java:12)}. */
    private static final java.util.regex.Pattern STACK_FRAME = java.util.regex.Pattern.compile(
            "at [\\w$.<>/]+\\(.*\\)");

    /**
     * Summarizes Maven build exceptions for the broker: each exception's message (or its first cause with a message),
     * without ANSI color codes, blank lines, stack frames, or lines already reported, capped at {@value #MAX_FAILURE_REASON_LENGTH} chars.
     */
    static String describeBuildFailure(List<Throwable> exceptions) {
        if (exceptions == null || exceptions.isEmpty()) {
            return null;
        }
        java.util.LinkedHashSet<String> lines = new java.util.LinkedHashSet<>();
        for (Throwable t : exceptions) {
            String message = firstMessage(t);
            if (message == null) {
                continue;
            }
            for (String line : ANSI_ESCAPE.matcher(message).replaceAll("").split("\\R")) {
                String trimmed = line.strip();
                if (!trimmed.isEmpty() && !STACK_FRAME.matcher(trimmed).matches()
                        && !trimmed.matches("\\.\\.\\. \\d+ more")) {
                    lines.add(trimmed);
                }
            }
        }
        if (lines.isEmpty()) {
            return null;
        }
        String reason = String.join("\n", lines);
        if (reason.length() <= MAX_FAILURE_REASON_LENGTH) {
            return reason;
        }
        int end = MAX_FAILURE_REASON_LENGTH;
        if (Character.isHighSurrogate(reason.charAt(end - 1))) {
            end--;
        }
        return reason.substring(0, end);
    }

    private static String firstMessage(Throwable t) {
        java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (Throwable cur = t; cur != null && seen.add(cur); cur = cur.getCause()) {
            if (cur.getMessage() != null && !cur.getMessage().isBlank()) {
                return cur.getMessage();
            }
        }
        return null;
    }

    /**
     * Session name: the project at the directory Maven was started in (the repository root for
     * {@code ./mvnw -pl a,b,c test}), else Maven's top-level project, which with {@code -pl} is just the first module.
     */
    static String defaultProjectName(MavenSession session) {
        String rootDir = session.getExecutionRootDirectory();
        if (rootDir != null && session.getAllProjects() != null) {
            Path root = Path.of(rootDir).toAbsolutePath().normalize();
            for (MavenProject project : session.getAllProjects()) {
                if (project.getBasedir() != null
                        && project.getBasedir().toPath().toAbsolutePath().normalize().equals(root)) {
                    return project.getArtifactId();
                }
            }
        }
        MavenProject topProject = session.getTopLevelProject();
        return topProject != null ? topProject.getArtifactId() : "maven-reactor";
    }

    private static boolean isInterruptedBuild(MavenSession session) {
        if (session == null || session.getResult() == null || session.getResult().getExceptions() == null) {
            return false;
        }
        for (Throwable t : session.getResult().getExceptions()) {
            Throwable cur = t;
            while (cur != null) {
                if (cur instanceof InterruptedException) {
                    return true;
                }
                cur = cur.getCause() != cur ? cur.getCause() : null;
            }
        }
        return false;
    }

    String getActiveSessionId() {
        return activeSessionId;
    }

    static String runGit(Path dir, String... args) {
        return GitDiffDetector.runGitSingleLine(dir, args);
    }

    /**
     * Jars appended to the Surefire/Failsafe test classpath so the launcher interceptor can run in modules that do not
     * depend on Test Distribution themselves: the launcher, the protocol, and Jackson 3 (the protocol's JSON codec,
     * including the Jackson 2.x annotations it reuses). JUnit Platform and SLF4J come from the module's own classpath.
     */
    static List<Path> resolveInterceptorClasspathJars() {
        List<Path> paths = new ArrayList<>();
        addCodeSourceLocation(DistributionLauncherInterceptor.class, paths);
        addCodeSourceLocation(DistributionSettings.class, paths);
        addCodeSourceLocation(tools.jackson.core.JsonParser.class, paths);
        addCodeSourceLocation(tools.jackson.databind.ObjectMapper.class, paths);
        addCodeSourceLocation(com.fasterxml.jackson.annotation.JsonProperty.class, paths);
        return paths;
    }

    private static void addCodeSourceLocation(Class<?> type, List<Path> target) {
        try {
            if (type.getProtectionDomain() != null && type.getProtectionDomain().getCodeSource() != null) {
                URL url = type.getProtectionDomain().getCodeSource().getLocation();
                if (url != null) {
                    Path p = Path.of(url.toURI());
                    if (!target.contains(p)) {
                        target.add(p);
                    }
                }
            }
        } catch (Exception ignored) {
            // ignore
        }
    }

    private static boolean isDistributionEnabled(MavenSession session) {
        String val = resolveProperty(session, DistributionSettings.ENABLED_PROPERTY, null);
        if (val != null) {
            return Boolean.parseBoolean(val);
        }
        return DistributionSettings.isEnabled();
    }

    private static String resolveProperty(MavenSession session, String key, String defaultValue) {
        if (session != null) {
            Properties userProps = session.getUserProperties();
            if (userProps != null && userProps.getProperty(key) != null) {
                return userProps.getProperty(key);
            }
            Properties sysProps = session.getSystemProperties();
            if (sysProps != null && sysProps.getProperty(key) != null) {
                return sysProps.getProperty(key);
            }
        }
        String jvmProp = System.getProperty(key);
        return jvmProp != null ? jvmProp : defaultValue;
    }
}
