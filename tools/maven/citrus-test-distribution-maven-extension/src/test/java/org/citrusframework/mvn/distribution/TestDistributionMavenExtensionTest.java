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
import java.util.Map;
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

    @Test
    void shouldTimeoutHungGitSubprocessWithinConfiguredBound() {
        long startNs = System.nanoTime();
        List<String> lines = GitDiffDetector.runBoundedCommand(tempDir, 150L, "sh", "-c", "sleep 10");
        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;

        assertThat(lines).isEmpty();
        assertThat(elapsedMs).isLessThan(2_000L);
        assertThat(TestDistributionLifecycleParticipant.runGit(
                Path.of("").toAbsolutePath(), "rev-parse", "--is-inside-work-tree"))
                .isEqualTo("true");
    }

    @Test
    void shouldResolveFiveTierSingleWinnerPrecedenceAndSupportNoneAndEmptyCliClear() {
        MavenProject project = createSampleProject("prec", tempDir.resolve("prec"));
        project.getProperties().setProperty(DistributionSettings.INPUTS_PROPERTY, "src/test/resources");
        project.getProperties().setProperty(DistributionSettings.OUTPUTS_PROPERTY, "target/docs");
        project.getProperties().setProperty(DistributionSettings.FORK_PROPERTY, "true");
        Map<String, String> env = Map.of(
                DistributionSettings.OUTPUTS_ENV, "target/from-env",
                DistributionSettings.PARTITION_POLICY_ENV, "single");

        Properties cli = new Properties();
        cli.setProperty(DistributionSettings.INPUTS_PROPERTY, "pom.xml");
        Xpp3Dom sysProps = resolve(project, cli, env);
        assertThat(value(sysProps, DistributionSettings.INPUTS_PROPERTY)).as("CLI replaces, never merges").isEqualTo("pom.xml");
        assertThat(value(sysProps, DistributionSettings.OUTPUTS_PROPERTY)).as("module beats env").isEqualTo("target/docs");
        assertThat(value(sysProps, DistributionSettings.FORK_PROPERTY)).isEqualTo("true");
        assertThat(value(sysProps, DistributionSettings.PARTITION_POLICY_PROPERTY)).as("env when nothing else").isEqualTo("SINGLE");

        Properties clear = new Properties();
        clear.setProperty(DistributionSettings.INPUTS_PROPERTY, "");
        clear.setProperty(DistributionSettings.OUTPUTS_PROPERTY, "NONE");
        clear.setProperty(DistributionSettings.FORK_PROPERTY, "");
        Xpp3Dom cleared = resolve(createProjectWithProperties(project), clear, env);
        assertThat(value(cleared, DistributionSettings.INPUTS_PROPERTY)).as("empty CLI value clears").isEqualTo("none");
        assertThat(value(cleared, DistributionSettings.OUTPUTS_PROPERTY)).as("none clears env and module").isEqualTo("none");
        assertThat(value(cleared, DistributionSettings.FORK_PROPERTY)).as("cleared fork is the default").isEqualTo("false");
    }

    @Test
    void shouldNotShadowModulePropertiesWithInheritedPlaceholderInSystemPropertyVariables() {
        MavenProject project = createSampleProject("placeholder", tempDir.resolve("placeholder"));
        project.setFile(tempDir.resolve("placeholder/pom.xml").toFile());
        project.getProperties().setProperty(DistributionSettings.INPUTS_PROPERTY, "src/test/resources");
        Xpp3Dom config = new Xpp3Dom("configuration");
        Xpp3Dom inherited = new Xpp3Dom("systemPropertyVariables");
        for (String key : List.of(DistributionSettings.INPUTS_PROPERTY, DistributionSettings.OUTPUTS_PROPERTY)) {
            Xpp3Dom child = new Xpp3Dom(key);
            child.setValue("${" + key + "}");
            inherited.addChild(child);
        }
        Xpp3Dom blank = new Xpp3Dom(DistributionSettings.FORK_PROPERTY);
        blank.setValue("");
        inherited.addChild(blank);
        config.addChild(inherited);
        project.getBuild().getPluginsAsMap().get("org.apache.maven.plugins:maven-surefire-plugin").setConfiguration(config);

        Xpp3Dom sysProps = resolve(project, new Properties(), Map.of());

        assertThat(value(sysProps, DistributionSettings.INPUTS_PROPERTY)).isEqualTo("src/test/resources");
        assertThat(sysProps.getChild(DistributionSettings.OUTPUTS_PROPERTY)).as("unset everywhere: not passed on").isNull();
        assertThat(sysProps.getChild(DistributionSettings.FORK_PROPERTY)).isNull();
        assertThat(value(sysProps, DistributionSettings.BASEDIR_PROPERTY))
                .isEqualTo(tempDir.resolve("placeholder").toAbsolutePath().normalize().toString());
    }

    @Test
    void shouldPreferExplicitSystemPropertyVariablesOverModulePropertiesPerExecution() {
        MavenProject project = createSampleProject("exec", tempDir.resolve("exec"));
        project.getProperties().setProperty(DistributionSettings.OUTPUTS_PROPERTY, "target/module");
        Plugin failsafe = project.getBuild().getPluginsAsMap().get("org.apache.maven.plugins:maven-failsafe-plugin");
        org.apache.maven.model.PluginExecution execution = new org.apache.maven.model.PluginExecution();
        execution.setId("it");
        Xpp3Dom execConfig = new Xpp3Dom("configuration");
        Xpp3Dom execProps = new Xpp3Dom("systemPropertyVariables");
        Xpp3Dom explicit = new Xpp3Dom(DistributionSettings.OUTPUTS_PROPERTY);
        explicit.setValue("target/it-docs");
        execProps.addChild(explicit);
        execConfig.addChild(execProps);
        execution.setConfiguration(execConfig);
        failsafe.addExecution(execution);

        SurefirePluginMutator.configureProject(project, "s1", List.of(), Map.of(), new Properties(), key -> null);

        assertThat(value(((Xpp3Dom) execution.getConfiguration()).getChild("systemPropertyVariables"),
                DistributionSettings.OUTPUTS_PROPERTY)).isEqualTo("target/it-docs");
        assertThat(value(((Xpp3Dom) failsafe.getConfiguration()).getChild("systemPropertyVariables"),
                DistributionSettings.OUTPUTS_PROPERTY)).isEqualTo("target/module");
    }

    @Test
    void shouldFailFastOnInvalidForkOrPartitionPolicy() {
        MavenProject project = createSampleProject("invalid", tempDir.resolve("invalid"));
        project.getProperties().setProperty(DistributionSettings.PARTITION_POLICY_PROPERTY, "round-robin");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> resolve(project, new Properties(), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("round-robin")
                .hasMessageContaining(project.getId())
                .hasMessageContaining("module property");
    }

    @Test
    void shouldValidateSettingsBeforeOpeningTheReactorSession() {
        MavenProject project = createSampleProject("invalid-env", tempDir.resolve("invalid-env"));
        project.getProperties().setProperty(DistributionSettings.FORK_PROPERTY, "maybe");
        Properties userProps = new Properties();
        userProps.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
        userProps.setProperty(DistributionSettings.BROKER_URL_PROPERTY, "http://127.0.0.1:1");
        userProps.setProperty(DistributionSettings.FALLBACK_LOCAL_PROPERTY, "false");
        MavenSession session = mockSession(List.of(project), userProps);

        TestDistributionLifecycleParticipant participant = new TestDistributionLifecycleParticipant();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> participant.afterProjectsRead(session))
                .isInstanceOf(org.apache.maven.MavenExecutionException.class)
                .hasMessageContaining("maybe")
                .hasMessageNotContaining("Failed to open");
        assertThat(participant.getActiveSessionId()).isNull();
    }

    @Test
    void shouldRewriteCliClearsIntoUserPropertiesSoSurefireCannotPromoteTheRawEmptyValue() {
        Properties cli = new Properties();
        cli.setProperty(DistributionSettings.INPUTS_PROPERTY, "");
        cli.setProperty(DistributionSettings.FORK_PROPERTY, "");
        cli.setProperty(DistributionSettings.PARTITION_POLICY_PROPERTY, "single");
        resolve(createSampleProject("cli-a", tempDir.resolve("cli-a")), cli, Map.of(DistributionSettings.INPUTS_ENV, "x"));
        // Surefire copies session user properties into the test JVM after systemPropertyVariables.
        assertThat(cli.getProperty(DistributionSettings.INPUTS_PROPERTY)).isEqualTo("none");
        assertThat(cli.getProperty(DistributionSettings.FORK_PROPERTY)).isEqualTo("false");
        assertThat(cli.getProperty(DistributionSettings.PARTITION_POLICY_PROPERTY)).isEqualTo("SINGLE");
        assertThat(cli).doesNotContainKey(DistributionSettings.OUTPUTS_PROPERTY);

        Xpp3Dom second = resolve(createSampleProject("cli-b", tempDir.resolve("cli-b")), cli, Map.of());
        assertThat(value(second, DistributionSettings.INPUTS_PROPERTY)).as("idempotent across modules").isEqualTo("none");
    }

    @Test
    void shouldLetCliBeatExplicitValuesAndLetNoneAtAnyTierClearLowerTiers() {
        MavenProject explicit = createSampleProject("explicit", tempDir.resolve("explicit"));
        explicit.getBuild().getPluginsAsMap().get("org.apache.maven.plugins:maven-surefire-plugin")
                .setConfiguration(configWith(DistributionSettings.INPUTS_PROPERTY, "src/explicit"));
        Properties cli = new Properties();
        cli.setProperty(DistributionSettings.INPUTS_PROPERTY, "src/cli");
        assertThat(value(resolve(explicit, cli, Map.of()), DistributionSettings.INPUTS_PROPERTY)).isEqualTo("src/cli");

        MavenProject explicitNone = createSampleProject("explicit-none", tempDir.resolve("explicit-none"));
        explicitNone.getProperties().setProperty(DistributionSettings.INPUTS_PROPERTY, "src/module");
        explicitNone.getBuild().getPluginsAsMap().get("org.apache.maven.plugins:maven-surefire-plugin")
                .setConfiguration(configWith(DistributionSettings.INPUTS_PROPERTY, "none"));
        assertThat(value(resolve(explicitNone, new Properties(), Map.of(DistributionSettings.INPUTS_ENV, "src/env")),
                DistributionSettings.INPUTS_PROPERTY)).isEqualTo("none");

        MavenProject moduleNone = createSampleProject("module-none", tempDir.resolve("module-none"));
        moduleNone.getProperties().setProperty(DistributionSettings.OUTPUTS_PROPERTY, "None");
        assertThat(value(resolve(moduleNone, new Properties(), Map.of(DistributionSettings.OUTPUTS_ENV, "target/env")),
                DistributionSettings.OUTPUTS_PROPERTY)).as("module none beats env").isEqualTo("none");
    }

    @Test
    void shouldInjectBasedirUnlessGivenOnTheCommandLine() {
        MavenProject project = createSampleProject("basedir", tempDir.resolve("basedir"));
        project.setFile(tempDir.resolve("basedir/pom.xml").toFile());
        Properties cli = new Properties();
        cli.setProperty(DistributionSettings.BASEDIR_PROPERTY, "/elsewhere");
        assertThat(resolve(project, cli, Map.of()).getChild(DistributionSettings.BASEDIR_PROPERTY)).isNull();

        MavenProject noFile = createSampleProject("no-file", tempDir.resolve("no-file"));
        assertThat(resolve(noFile, new Properties(), Map.of()).getChild(DistributionSettings.BASEDIR_PROPERTY)).isNull();
    }

    private static Xpp3Dom configWith(String key, String value) {
        Xpp3Dom config = new Xpp3Dom("configuration");
        Xpp3Dom sysProps = new Xpp3Dom("systemPropertyVariables");
        Xpp3Dom child = new Xpp3Dom(key);
        child.setValue(value);
        sysProps.addChild(child);
        config.addChild(sysProps);
        return config;
    }

    private Xpp3Dom resolve(MavenProject project, Properties cli, Map<String, String> env) {
        SurefirePluginMutator.configureProject(project, "s1", List.of(), Map.of(), cli, env::get);
        Plugin surefire = project.getBuild().getPluginsAsMap().get("org.apache.maven.plugins:maven-surefire-plugin");
        return ((Xpp3Dom) surefire.getConfiguration()).getChild("systemPropertyVariables");
    }

    private MavenProject createProjectWithProperties(MavenProject source) {
        MavenProject copy = createSampleProject(source.getArtifactId() + "-copy", tempDir.resolve(source.getArtifactId() + "-copy"));
        copy.getProperties().putAll(source.getProperties());
        return copy;
    }

    private static String value(Xpp3Dom sysProps, String key) {
        Xpp3Dom child = sysProps.getChild(key);
        assertThat(child).as(key).isNotNull();
        return child.getValue();
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
