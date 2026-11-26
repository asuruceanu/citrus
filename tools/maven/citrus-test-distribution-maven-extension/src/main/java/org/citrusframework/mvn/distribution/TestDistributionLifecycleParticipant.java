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
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.maven.AbstractMavenLifecycleParticipant;
import org.apache.maven.MavenExecutionException;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.project.MavenProject;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.junit.CoordinatorHttpClient;
import org.citrusframework.distribution.junit.LocalResultDelivery;
import org.citrusframework.distribution.junit.DistributionLauncherInterceptor;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.SessionStatus;
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
    private volatile CoordinatorHttpClient activeCoordinatorClient;
    private volatile Thread shutdownHook;

    @Override
    public void afterProjectsRead(MavenSession session) throws MavenExecutionException {
        if (!isDistributionEnabled(session)) {
            return;
        }

        String coordinatorUrl = resolveProperty(session, DistributionSettings.COORDINATOR_URL_PROPERTY,
                DistributionSettings.getCoordinatorUrl());
        String authToken = resolveProperty(session, DistributionSettings.AUTH_TOKEN_PROPERTY,
                DistributionSettings.getAuthToken());
        boolean fallbackLocal = Boolean.parseBoolean(resolveProperty(
                session, DistributionSettings.FALLBACK_LOCAL_PROPERTY,
                Boolean.toString(DistributionSettings.isFallbackLocal())));
        Properties rootProperties = Optional.ofNullable(executionRootProject(session))
                .map(MavenProject::getProperties).orElseGet(Properties::new);
        Properties mergedProps = reactorProperties(session, rootProperties);
        OptionalInt maxPartitionSize = DistributionSettings.findConfiguredMaxPartitionSize(
                mergedProps::getProperty, System::getenv);
        int maxRetries = Integer.parseInt(resolveProperty(
                session, DistributionSettings.MAX_RETRIES_PROPERTY,
                Integer.toString(DistributionSettings.getMaxRetries())));

        MavenProject topProject = session.getTopLevelProject();
        String defaultProjectName = defaultProjectName(session);
        String projectName = resolveProperty(session, DistributionSettings.PROJECT_NAME_PROPERTY, defaultProjectName);
        if (projectName == null || projectName.isBlank()) {
            projectName = defaultProjectName;
        }
        MavenProject rootProject = executionRootProject(session);
        String projectKey = resolveProperty(session, DistributionSettings.PROJECT_KEY_PROPERTY,
                rootProject == null || rootProject.getGroupId() == null ? null
                        : rootProject.getGroupId() + ":" + rootProject.getArtifactId());
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
        CoordinatorHttpClient client = new CoordinatorHttpClient(
                coordinatorUrl, authToken, DistributionSettings.getConnectTimeoutMs());

        String sessionId = null;
        String resumeId = resolveProperty(session, DistributionSettings.RESUME_SESSION_ID_PROPERTY, null);
        try {
            if (resumeId != null && !resumeId.isBlank()) {
                BuildSessionRecord existing = client.getSession(resumeId);
                if (existing.status() != SessionStatus.RUNNING
                        || !java.util.Objects.equals(existing.projectName(), projectName)
                        || !java.util.Objects.equals(existing.gitCommit(), gitInfo.commit())) {
                    throw new IllegalArgumentException("Resuming requires a running session for this project and revision");
                }
                sessionId = existing.sessionId();
                LOG.info("Resumed Citrus Test Distribution reactor session {} on coordinator {}", sessionId, coordinatorUrl);
            } else {
                BuildSessionResponse created = client.createSession(new CreateSessionRequest(
                        projectName, gitInfo.branch(), gitInfo.commit(), gitInfo.changedFiles(), maxPartitionSize.orElse(0),
                        maxRetries, gitInfo.workspaceChanges(), projectKey,
                        resolveProperty(session, DistributionSettings.RUN_GROUP_PROPERTY, null)));
                sessionId = created.sessionId();
                LOG.info("Opened Citrus Test Distribution reactor session {} on coordinator {}", sessionId, coordinatorUrl);
            }
            this.activeSessionId = sessionId;
            this.activeCoordinatorClient = client;
            registerAbortShutdownHook(sessionId, client);
        } catch (Exception e) {
            if (!fallbackLocal || (resumeId != null && !resumeId.isBlank())) {
                throw new MavenExecutionException(
                        "Failed to open Citrus Test Distribution session on coordinator " + coordinatorUrl, e);
            }
            // Keep the interceptor active so the module launcher can enforce @RemoteOnly during fallback.
            LOG.warn("Citrus Test Distribution coordinator {} unreachable ({}); module launchers will apply fallback policy",
                    coordinatorUrl, e.getMessage());
        }

        List<Path> runtimeJars = resolveInterceptorClasspathJars();
        Map<String, String> sharedProps = new LinkedHashMap<>();
        sharedProps.put(DistributionSettings.COORDINATOR_URL_PROPERTY, coordinatorUrl);
        if (authToken != null && !authToken.isBlank()) {
            sharedProps.put(DistributionSettings.AUTH_TOKEN_PROPERTY, authToken);
        }
        sharedProps.put(DistributionSettings.FALLBACK_LOCAL_PROPERTY, Boolean.toString(fallbackLocal));
        maxPartitionSize.ifPresent(value ->
                sharedProps.put(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY, Integer.toString(value)));
        if (isConfigured(mergedProps, DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY,
                DistributionSettings.MIN_REMOTE_DURATION_MS_ENV)) {
            sharedProps.put(DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY,
                    Long.toString(DistributionSettings.getMinRemoteDurationMs(mergedProps::getProperty, System::getenv)));
        }
        if (isConfigured(mergedProps, DistributionSettings.MIN_REMOTE_COLD_UNITS_PROPERTY,
                DistributionSettings.MIN_REMOTE_COLD_UNITS_ENV)) {
            sharedProps.put(DistributionSettings.MIN_REMOTE_COLD_UNITS_PROPERTY,
                    Integer.toString(DistributionSettings.getMinRemoteColdUnits(mergedProps::getProperty, System::getenv)));
        }
        sharedProps.put(DistributionSettings.MAX_RETRIES_PROPERTY, Integer.toString(maxRetries));
        sharedProps.put(DistributionSettings.PROJECT_NAME_PROPERTY, projectName);
        if (projectKey != null && !projectKey.isBlank()) {
            sharedProps.put(DistributionSettings.PROJECT_KEY_PROPERTY, projectKey);
        }

        if (session.getProjects() != null) {
            Map<String, String> outputDirectories = new LinkedHashMap<>();
            List<MavenProject> reactor = session.getAllProjects() == null ? session.getProjects() : session.getAllProjects();
            for (MavenProject project : reactor) {
                if (project.getBuild() != null && project.getBasedir() != null) {
                    addOutputDirectory(outputDirectories, project, project.getBuild().getOutputDirectory());
                    addOutputDirectory(outputDirectories, project, project.getBuild().getTestOutputDirectory());
                }
            }
            if (!outputDirectories.isEmpty()) {
                sharedProps.put(DistributionSettings.MAVEN_OUTPUT_DIRECTORIES_PROPERTY, DistributionJson.toJson(outputDirectories));
            }
            for (MavenProject project : session.getProjects()) {
                SurefirePluginMutator.configureProject(project, sessionId, runtimeJars, sharedProps,
                        session.getUserProperties(), System::getenv);
            }
        }
    }

    private static void addOutputDirectory(Map<String, String> directories, MavenProject project, String output) {
        if (output != null && !output.isBlank()) {
            Path directory = project.getBasedir().toPath().toAbsolutePath().resolve(output).normalize();
            directories.putIfAbsent(directory.toString(), project.getArtifactId());
        }
    }

    @Override
    public void afterSessionEnd(MavenSession session) {
        unregisterAbortShutdownHook();
        String sessionId = this.activeSessionId;
        CoordinatorHttpClient client = this.activeCoordinatorClient;
        this.activeSessionId = null;
        this.activeCoordinatorClient = null;
        if (sessionId == null || client == null) {
            return;
        }
        try {
            if (isInterruptedBuild(session)) {
                client.abortSession(sessionId, "Maven build interrupted");
                LOG.info("Aborted Citrus Test Distribution reactor session {}", sessionId);
                return;
            }
            LocalResultDelivery.flushSession(client, sessionId);
            boolean buildFailed = session.getResult() != null && session.getResult().hasExceptions();
            String failureReason = buildFailed ? describeBuildFailure(session.getResult().getExceptions()) : null;
            client.completeSession(sessionId, new CompleteSessionRequest(0, 0, 0, 0L, buildFailed, failureReason));
            LOG.info("Completed Citrus Test Distribution reactor session {}", sessionId);
        } catch (Exception e) {
            LOG.warn("Failed to complete Citrus Test Distribution session {}: {}", sessionId, e.getMessage());
        }
    }

    private void registerAbortShutdownHook(String sessionId, CoordinatorHttpClient client) {
        Thread hook = new Thread(() -> {
            if (sessionId.equals(this.activeSessionId)) {
                this.activeSessionId = null;
                this.activeCoordinatorClient = null;
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
     * Summarizes Maven build exceptions for the coordinator: each exception's message (or its first cause with a message),
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
        MavenProject rootProject = executionRootProject(session);
        return rootProject != null ? rootProject.getArtifactId() : "maven-reactor";
    }

    private static MavenProject executionRootProject(MavenSession session) {
        String rootDir = session.getExecutionRootDirectory();
        if (rootDir != null && session.getAllProjects() != null) {
            Path root = Path.of(rootDir).toAbsolutePath().normalize();
            for (MavenProject project : session.getAllProjects()) {
                if (project.getBasedir() != null
                        && project.getBasedir().toPath().toAbsolutePath().normalize().equals(root)) {
                    return project;
                }
            }
        }
        return session.getTopLevelProject();
    }

    private static Properties reactorProperties(MavenSession session, Properties rootProperties) {
        Properties merged = new Properties(rootProperties);
        for (String key : List.of(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY,
                DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY, DistributionSettings.MIN_REMOTE_COLD_UNITS_PROPERTY)) {
            String value = resolveProperty(session, key, null);
            if (value != null) {
                merged.setProperty(key, value);
            }
        }
        return merged;
    }

    private static boolean isConfigured(Properties properties, String key, String envKey) {
        String property = properties.getProperty(key);
        String environment = System.getenv(envKey);
        return (property != null && !property.isBlank()) || (environment != null && !environment.isBlank());
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
     * depend on Test Distribution themselves: the launcher, the protocol and Jackson. The module supplies JUnit Platform.
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
