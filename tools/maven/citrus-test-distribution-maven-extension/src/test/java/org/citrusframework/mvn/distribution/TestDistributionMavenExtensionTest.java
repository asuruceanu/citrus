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

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.apache.maven.execution.MavenExecutionResult;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.model.Build;
import org.apache.maven.model.Plugin;
import org.apache.maven.project.MavenProject;
import org.citrusframework.distribution.broker.DistributionBrokerConfig;
import org.citrusframework.distribution.broker.DistributionBrokerServer;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.SessionStatus;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class TestDistributionMavenExtensionTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldBeNoOpWhenDisabled() throws Exception {
        MavenProject project = createSampleProject("module-a", tempDir.resolve("module-a"));
        MavenSession session = mockSession(List.of(project), new Properties());

        TestDistributionLifecycleParticipant participant = new TestDistributionLifecycleParticipant();
        participant.afterProjectsRead(session);

        Plugin surefire = project.getBuild().getPluginsAsMap().get("org.apache.maven.plugins:maven-surefire-plugin");
        assertThat(surefire.getConfiguration()).isNull();
        assertThat(participant.getActiveSessionId()).isNull();
    }

    @Test
    void shouldOpenReactorSessionMutateSurefireAndFailsafeAndCompleteSession() throws Exception {
        DistributionBrokerConfig brokerConfig = DistributionBrokerConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("broker-cache"))
                .jdbcUrl("jdbc:hsqldb:mem:mvn_ext_" + System.nanoTime())
                .build();

        try (DistributionBrokerServer broker = new DistributionBrokerServer(brokerConfig)) {
            broker.start();

            MavenProject modA = createSampleProject("citrus-mod-a", tempDir.resolve("mod-a"));
            MavenProject modB = createSampleProject("citrus-mod-b", tempDir.resolve("mod-b"));

            Properties userProps = new Properties();
            userProps.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            userProps.setProperty(DistributionSettings.BROKER_URL_PROPERTY, broker.baseUrl());
            MavenSession session = mockSession(List.of(modA, modB), userProps);

            TestDistributionLifecycleParticipant participant = new TestDistributionLifecycleParticipant();
            participant.afterProjectsRead(session);

            String sessionId = participant.getActiveSessionId();
            assertThat(sessionId).isNotBlank();

            for (MavenProject project : List.of(modA, modB)) {
                Plugin surefire = project.getBuild().getPluginsAsMap()
                        .get("org.apache.maven.plugins:maven-surefire-plugin");
                Xpp3Dom dom = (Xpp3Dom) surefire.getConfiguration();
                assertThat(dom).isNotNull();
                Xpp3Dom sysProps = dom.getChild("systemPropertyVariables");
                assertThat(sysProps.getChild("junit.platform.launcher.interceptors.enabled").getValue())
                        .isEqualTo("true");
                assertThat(sysProps.getChild(DistributionSettings.SESSION_ID_PROPERTY).getValue())
                        .isEqualTo(sessionId);
                assertThat(sysProps.getChild(DistributionSettings.BROKER_URL_PROPERTY).getValue())
                        .isEqualTo(broker.baseUrl());
                assertThat(dom.getChild("additionalClasspathElements").getChildCount())
                        .isGreaterThanOrEqualTo(2);
            }

            participant.afterSessionEnd(session);
            BuildSessionRecord record = broker.store().findSession(sessionId).orElseThrow();
            assertThat(record.status()).isEqualTo(SessionStatus.PASSED);
        }
    }

    @Test
    void shouldFallbackLocallyWithoutFailingBuildWhenBrokerUnreachable() throws Exception {
        MavenProject project = createSampleProject("module-fallback", tempDir.resolve("mod-fb"));
        Properties userProps = new Properties();
        userProps.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
        userProps.setProperty(DistributionSettings.BROKER_URL_PROPERTY, "http://127.0.0.1:1");
        userProps.setProperty(DistributionSettings.FALLBACK_LOCAL_PROPERTY, "true");
        MavenSession session = mockSession(List.of(project), userProps);

        TestDistributionLifecycleParticipant participant = new TestDistributionLifecycleParticipant();
        participant.afterProjectsRead(session);

        assertThat(participant.getActiveSessionId()).isNull();
    }

    private MavenProject createSampleProject(String artifactId, Path dir) {
        MavenProject project = new MavenProject();
        project.setGroupId("org.citrusframework");
        project.setArtifactId(artifactId);
        project.setVersion("5.3.0-SNAPSHOT");

        Build build = new Build();
        build.setDirectory(dir.resolve("target").toString());

        Plugin surefire = new Plugin();
        surefire.setGroupId("org.apache.maven.plugins");
        surefire.setArtifactId("maven-surefire-plugin");
        build.addPlugin(surefire);

        Plugin failsafe = new Plugin();
        failsafe.setGroupId("org.apache.maven.plugins");
        failsafe.setArtifactId("maven-failsafe-plugin");
        build.addPlugin(failsafe);

        project.setBuild(build);
        return project;
    }

    private MavenSession mockSession(List<MavenProject> projects, Properties userProps) {
        MavenSession session = Mockito.mock(MavenSession.class);
        MavenExecutionResult result = Mockito.mock(MavenExecutionResult.class);
        when(result.hasExceptions()).thenReturn(false);
        when(session.getProjects()).thenReturn(projects);
        when(session.getTopLevelProject()).thenReturn(projects.get(0));
        when(session.getUserProperties()).thenReturn(userProps);
        when(session.getSystemProperties()).thenReturn(new Properties());
        when(session.getExecutionRootDirectory()).thenReturn(tempDir.toString());
        when(session.getResult()).thenReturn(result);
        return session;
    }
}
