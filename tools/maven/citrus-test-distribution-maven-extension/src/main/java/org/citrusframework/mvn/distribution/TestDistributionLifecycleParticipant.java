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
        String projectName = topProject != null ? topProject.getArtifactId() : "maven-reactor";
        Path rootDir = session.getExecutionRootDirectory() != null
                ? Path.of(session.getExecutionRootDirectory())
                : (topProject != null && topProject.getBasedir() != null ? topProject.getBasedir().toPath() : null);

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
                SurefirePluginMutator.configureProject(project, sessionId, runtimeJars, sharedProps);
            }
        }
    }

    @Override
    public void afterSessionEnd(MavenSession session) {
        String sessionId = this.activeSessionId;
        BrokerHttpClient client = this.activeBrokerClient;
        this.activeSessionId = null;
        this.activeBrokerClient = null;
        if (sessionId == null || client == null) {
            return;
        }
        try {
            int buildFailures = session.getResult() != null && session.getResult().hasExceptions() ? 1 : 0;
            client.completeSession(sessionId, new CompleteSessionRequest(0, buildFailures, 0, 0L));
            LOG.info("Completed Citrus Test Distribution reactor session {}", sessionId);
        } catch (Exception e) {
            LOG.warn("Failed to complete Citrus Test Distribution session {}: {}", sessionId, e.getMessage());
        }
    }

    String getActiveSessionId() {
        return activeSessionId;
    }

    static String runGit(Path dir, String... args) {
        return GitDiffDetector.runGitSingleLine(dir, args);
    }

    static List<Path> resolveInterceptorClasspathJars() {
        List<Path> paths = new ArrayList<>();
        addCodeSourceLocation(DistributionLauncherInterceptor.class, paths);
        addCodeSourceLocation(DistributionSettings.class, paths);
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
