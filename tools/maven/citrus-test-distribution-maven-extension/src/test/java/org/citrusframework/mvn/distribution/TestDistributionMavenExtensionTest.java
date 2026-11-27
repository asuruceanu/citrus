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

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;

import org.apache.maven.execution.MavenExecutionResult;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.model.Build;
import org.apache.maven.model.Plugin;
import org.apache.maven.project.MavenProject;
import org.citrusframework.distribution.coordinator.DistributionCoordinatorConfig;
import org.citrusframework.distribution.coordinator.DistributionCoordinatorServer;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.CreateSessionRequest;
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
        DistributionCoordinatorConfig coordinatorConfig = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("coordinator-cache"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("mvn_ext_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer coordinator = new DistributionCoordinatorServer(coordinatorConfig)) {
            coordinator.start();

            MavenProject modA = createSampleProject("citrus-mod-a", tempDir.resolve("mod-a"));
            MavenProject modB = createSampleProject("citrus-mod-b", tempDir.resolve("mod-b"));

            Properties userProps = new Properties();
            userProps.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            userProps.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, coordinator.baseUrl());
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
                assertThat(sysProps.getChild(DistributionSettings.COORDINATOR_URL_PROPERTY).getValue())
                        .isEqualTo(coordinator.baseUrl());
                assertThat(dom.getChild("additionalClasspathElements").getChildCount())
                        .isGreaterThanOrEqualTo(2);
                assertThat(sysProps.getChild(DistributionSettings.REPORT_DIR_PROPERTY))
                        .as("Surefire writes the reports from the replayed results; a second writer would race it")
                        .isNull();
            }

            participant.afterSessionEnd(session);
            BuildSessionRecord record = coordinator.store().findSession(sessionId).orElseThrow();
            assertThat(record.status()).isEqualTo(SessionStatus.PASSED);
        }
    }

    @Test
    void shouldOmitMaxPartitionSizeWhenUnsetAndPropagateMinRemoteThresholds() throws Exception {
        AtomicReference<CreateSessionRequest> created = new AtomicReference<>();
        HttpServer coordinator = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        coordinator.createContext("/api/v1/sessions", exchange -> {
            try (exchange) {
                if (exchange.getRequestURI().getPath().equals("/api/v1/sessions")) {
                    created.set(DistributionJson.fromJson(
                            new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8),
                            CreateSessionRequest.class));
                    byte[] body = "{\"sessionId\":\"test-session\"}".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                    return;
                }
                exchange.sendResponseHeaders(204, -1);
            }
        });
        coordinator.start();
        MavenProject project = createSampleProject("module-a", tempDir.resolve("module-a"));
        MavenProject root = createSampleProject("root", tempDir);
        root.setFile(tempDir.resolve("pom.xml").toFile());
        root.getProperties().setProperty(DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY, "3000");
        root.getProperties().setProperty(DistributionSettings.MIN_REMOTE_COLD_UNITS_PROPERTY, "2");
        Properties userProps = new Properties();
        userProps.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
        userProps.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, "http://127.0.0.1:" + coordinator.getAddress().getPort());
        userProps.setProperty(DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY, "2000");
        MavenSession session = mockSession(List.of(project), userProps);
        when(session.getAllProjects()).thenReturn(List.of(root, project));
        TestDistributionLifecycleParticipant participant = new TestDistributionLifecycleParticipant();
        try {
            participant.afterProjectsRead(session);

            assertThat(created.get()).isNotNull();
            assertThat(created.get().maxPartitionSize()).isZero();
            for (String plugin : List.of("maven-surefire-plugin", "maven-failsafe-plugin")) {
                Xpp3Dom configuration = (Xpp3Dom) project.getBuild().getPluginsAsMap()
                        .get("org.apache.maven.plugins:" + plugin).getConfiguration();
                Xpp3Dom properties = configuration.getChild("systemPropertyVariables");
                assertThat(properties.getChild(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY)).isNull();
                assertThat(value(properties, DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY)).isEqualTo("2000");
                assertThat(value(properties, DistributionSettings.MIN_REMOTE_COLD_UNITS_PROPERTY)).isEqualTo("2");
            }
        } finally {
            participant.afterSessionEnd(session);
            coordinator.stop(0);
        }
    }

    @Test
    void shouldNotDoubleCountRemoteTestFailureAndShouldAbortOnInterruptedBuild() throws Exception {
        DistributionCoordinatorConfig coordinatorConfig = DistributionCoordinatorConfig.builder()
                .port(0)
                .cacheDir(tempDir.resolve("coordinator-cache-outcomes"))
                .jdbcUrl("jdbc:hsqldb:file:" + tempDir.resolve("mvn_ext_outcomes_" + System.nanoTime()) + ";shutdown=true")
                .build();

        try (DistributionCoordinatorServer coordinator = new DistributionCoordinatorServer(coordinatorConfig)) {
            coordinator.start();

            Properties userProps = new Properties();
            userProps.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
            userProps.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, coordinator.baseUrl());

            // 1. Reactor session with 1 recorded test failure + Surefire build exception -> failedTests stays 1
            MavenProject modFail = createSampleProject("citrus-mod-fail", tempDir.resolve("mod-fail"));
            MavenSession failSession = mockSessionWithExceptions(List.of(modFail), userProps,
                    List.of(new RuntimeException("There are test failures")));
            TestDistributionLifecycleParticipant p1 = new TestDistributionLifecycleParticipant();
            p1.afterProjectsRead(failSession);
            String sid1 = p1.getActiveSessionId();
            java.net.http.HttpClient.newHttpClient().send(
                    java.net.http.HttpRequest.newBuilder(java.net.URI.create(coordinator.baseUrl() + "/api/v1/sessions/" + sid1 + "/local-tests"))
                            .header("Content-Type", "application/json")
                            .POST(java.net.http.HttpRequest.BodyPublishers.ofString(
                                    "{\"workerId\":\"local-host\",\"executions\":[{\"sessionId\":\"" + sid1
                                            + "\",\"partitionId\":\"local\",\"attempt\":1,\"workUnitKey\":\"pkg.FailIT\","
                                            + "\"className\":\"pkg.FailIT\",\"testName\":\"shouldFail\",\"status\":\"FAILED\","
                                            + "\"durationMs\":45,\"errorMessage\":\"boom\",\"stackTrace\":\"trace\",\"endpoints\":[]}]}"))
                            .build(),
                    java.net.http.HttpResponse.BodyHandlers.discarding());
            p1.afterSessionEnd(failSession);
            BuildSessionRecord r1 = coordinator.store().findSession(sid1).orElseThrow();
            assertThat(r1.status()).isEqualTo(SessionStatus.FAILED);
            assertThat(r1.failedTests()).isEqualTo(1);

            // 2. Reactor session with compilation failure (0 test failures) -> FAILED with failedTests == 0
            MavenProject modCompile = createSampleProject("citrus-mod-compile", tempDir.resolve("mod-compile"));
            MavenSession compileSession = mockSessionWithExceptions(List.of(modCompile), userProps,
                    List.of(new RuntimeException("Compilation failure")));
            TestDistributionLifecycleParticipant p2 = new TestDistributionLifecycleParticipant();
            p2.afterProjectsRead(compileSession);
            String sid2 = p2.getActiveSessionId();
            p2.afterSessionEnd(compileSession);
            BuildSessionRecord r2 = coordinator.store().findSession(sid2).orElseThrow();
            assertThat(r2.status()).isEqualTo(SessionStatus.FAILED);
            assertThat(r2.failedTests()).isZero();
            assertThat(r2.failureReason()).isEqualTo("Compilation failure");

            // 3. Reactor session cancelled via InterruptedException -> ABORTED
            MavenProject modCancel = createSampleProject("citrus-mod-cancel", tempDir.resolve("mod-cancel"));
            MavenSession cancelSession = mockSessionWithExceptions(List.of(modCancel), userProps,
                    List.of(new RuntimeException("Build interrupted", new InterruptedException("SIGINT"))));
            TestDistributionLifecycleParticipant p3 = new TestDistributionLifecycleParticipant();
            p3.afterProjectsRead(cancelSession);
            String sid3 = p3.getActiveSessionId();
            p3.afterSessionEnd(cancelSession);
            BuildSessionRecord r3 = coordinator.store().findSession(sid3).orElseThrow();
            assertThat(r3.status()).isEqualTo(SessionStatus.ABORTED);
            assertThat(r3.failureReason()).isEqualTo("Maven build interrupted");

            // 4. Custom project.name property + JVM shutdown hook aborts active session on SIGINT/SIGTERM
            Properties customProps = new Properties();
            customProps.putAll(userProps);
            customProps.setProperty(DistributionSettings.PROJECT_NAME_PROPERTY, "custom-citrus-suite");
            customProps.setProperty(DistributionSettings.RUN_GROUP_PROPERTY, "six-shard-run");
            MavenProject modSignal = createSampleProject("citrus-mod-signal", tempDir.resolve("mod-signal"));
            MavenSession signalSession = mockSession(List.of(modSignal), customProps);
            TestDistributionLifecycleParticipant p4 = new TestDistributionLifecycleParticipant();
            p4.afterProjectsRead(signalSession);
            String sid4 = p4.getActiveSessionId();
            assertThat(coordinator.store().findSession(sid4).orElseThrow().projectName()).isEqualTo("custom-citrus-suite");
            assertThat(p4.getShutdownHook()).isNotNull();
            p4.getShutdownHook().run();
            p4.afterSessionEnd(signalSession);
            assertThat(p4.getShutdownHook()).isNull();
            BuildSessionRecord r4 = coordinator.store().findSession(sid4).orElseThrow();
            assertThat(r4.runGroup()).isEqualTo("six-shard-run");
            assertThat(coordinator.store().listSessions(10)).filteredOn(s -> "six-shard-run".equals(s.runGroup()))
                    .extracting(BuildSessionRecord::sessionId).containsExactly(sid4);
            assertThat(r4.status()).isEqualTo(SessionStatus.ABORTED);
        }
    }

    @Test
    void shouldDescribeSurefireForkFailureWithoutStackFramesOrDuplicateLines() {
        String mojoMessage = "Failed to execute goal org.apache.maven.plugins:maven-surefire-plugin:3.5.6:test "
                + "(default-test) on project citrus-api: \n\n"
                + "There was an error in the forked process\n"
                + "java.lang.NoClassDefFoundError: org/citrusframework/junit/jupiter/CitrusTestFactory\n"
                + "org.apache.maven.surefire.booter.SurefireBooterForkException: There was an error in the forked process\n"
                + "java.lang.NoClassDefFoundError: org/citrusframework/junit/jupiter/CitrusTestFactory\n"
                + "\tat org.apache.maven.plugin.surefire.booterclient.ForkStarter.fork(ForkStarter.java:631)\n"
                + "\tat org.apache.maven.plugin.surefire.booterclient.ForkStarter.run(ForkStarter.java:285)";

        String reason = TestDistributionLifecycleParticipant.describeBuildFailure(
                List.of(new RuntimeException(mojoMessage), new RuntimeException(null, new IllegalStateException("second"))));

        assertThat(reason).isEqualTo(
                "Failed to execute goal org.apache.maven.plugins:maven-surefire-plugin:3.5.6:test (default-test) on project citrus-api:\n"
                        + "There was an error in the forked process\n"
                        + "java.lang.NoClassDefFoundError: org/citrusframework/junit/jupiter/CitrusTestFactory\n"
                        + "org.apache.maven.surefire.booter.SurefireBooterForkException: There was an error in the forked process\n"
                        + "second");
    }

    @Test
    void shouldStopOnCyclicCausesWithoutMessages() {
        RuntimeException first = new RuntimeException((String) null);
        RuntimeException second = new RuntimeException((String) null);
        first.initCause(second);
        second.initCause(first);

        assertThat(org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(2),
                () -> TestDistributionLifecycleParticipant.describeBuildFailure(List.of(first)))).isNull();
    }

    @Test
    void shouldStripAllTerminalControlSequencesButKeepProseStartingWithAt() {
        String message = "\u001b[?25lCompilation failure\u001b]0;mvn\u0007\n"
                + "at least one module failed\n"
                + "\tat org.example.Foo.bar(Foo.java:12)\n"
                + "\tat org.example.Foo$Inner.lambda$run$0(Foo.java)";

        assertThat(TestDistributionLifecycleParticipant.describeBuildFailure(List.of(new RuntimeException(message))))
                .isEqualTo("Compilation failure\nat least one module failed");
    }

    @Test
    void shouldNotSplitACharacterWhenTrimmingTheReason() {
        String message = "a".repeat(3999) + "\uD83D\uDE00";

        String reason = TestDistributionLifecycleParticipant.describeBuildFailure(List.of(new RuntimeException(message)));

        assertThat(reason).hasSize(3999);
        assertThat(Character.isHighSurrogate(reason.charAt(reason.length() - 1))).isFalse();
    }

    @Test
    void shouldNameTheSessionAfterTheExecutionRootRatherThanTheFirstSelectedModule() {
        MavenProject root = new MavenProject();
        root.setArtifactId("citrus");
        root.setFile(tempDir.resolve("repo/pom.xml").toFile());
        MavenProject module = new MavenProject();
        module.setArtifactId("citrus-api");
        module.setFile(tempDir.resolve("repo/core/citrus-api/pom.xml").toFile());
        MavenSession session = Mockito.mock(MavenSession.class);
        // ./mvnw test -pl core/citrus-api,... from the repository root
        when(session.getTopLevelProject()).thenReturn(module);
        when(session.getAllProjects()).thenReturn(List.of(root, module));
        when(session.getExecutionRootDirectory()).thenReturn(tempDir.resolve("repo").toString());

        assertThat(TestDistributionLifecycleParticipant.defaultProjectName(session)).isEqualTo("citrus");

        when(session.getAllProjects()).thenReturn(List.of(module));
        assertThat(TestDistributionLifecycleParticipant.defaultProjectName(session)).isEqualTo("citrus-api");
    }

    @Test
    void shouldLeaveModulesThatOptOutRunningTheirTestsLocally() {
        MavenProject project = createSampleProject("citrus-quarkus-it", tempDir.resolve("quarkus-it"));
        project.getProperties().setProperty(DistributionSettings.SKIP_PROPERTY, "true");
        Properties cli = new Properties();
        cli.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");

        SurefirePluginMutator.configureProject(project, "sess-1", List.of(), Map.of(), cli, key -> null);

        Plugin surefire = project.getBuild().getPluginsAsMap().get("org.apache.maven.plugins:maven-surefire-plugin");
        assertThat(surefire.getConfiguration()).as("Surefire left exactly as the module configured it").isNull();
    }

    @Test
    void shouldTakeTheSkipSettingFromTheCommandLineAndTheEnvironmentLikeTheOtherModuleSettings() {
        String surefireKey = "org.apache.maven.plugins:maven-surefire-plugin";
        Properties cli = new Properties();
        cli.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");

        MavenProject fromEnv = createSampleProject("from-env", tempDir.resolve("from-env"));
        SurefirePluginMutator.configureProject(fromEnv, "sess-1", List.of(), Map.of(), cli,
                key -> DistributionSettings.SKIP_ENV.equals(key) ? "true" : null);
        assertThat(fromEnv.getBuild().getPluginsAsMap().get(surefireKey).getConfiguration())
                .as("CITRUS_DIST_SKIP=true opts the module out").isNull();

        MavenProject moduleWins = createSampleProject("module-wins", tempDir.resolve("module-wins"));
        moduleWins.getProperties().setProperty(DistributionSettings.SKIP_PROPERTY, "false");
        SurefirePluginMutator.configureProject(moduleWins, "sess-1", List.of(), Map.of(), cli,
                key -> DistributionSettings.SKIP_ENV.equals(key) ? "true" : null);
        assertThat(moduleWins.getBuild().getPluginsAsMap().get(surefireKey).getConfiguration())
                .as("the module property outranks the environment").isNotNull();

        MavenProject cliWins = createSampleProject("cli-wins", tempDir.resolve("cli-wins"));
        cliWins.getProperties().setProperty(DistributionSettings.SKIP_PROPERTY, "true");
        Properties cliSkipOff = new Properties();
        cliSkipOff.putAll(cli);
        cliSkipOff.setProperty(DistributionSettings.SKIP_PROPERTY, "false");
        SurefirePluginMutator.configureProject(cliWins, "sess-1", List.of(), Map.of(), cliSkipOff, key -> null);
        assertThat(cliWins.getBuild().getPluginsAsMap().get(surefireKey).getConfiguration())
                .as("-Dcitrus.test.distribution.skip=false outranks the module property").isNotNull();
    }

    @Test
    void shouldInjectJacksonJarsTheLauncherNeedsToTalkToTheCoordinator() {
        List<String> jarNames = TestDistributionLifecycleParticipant.resolveInterceptorClasspathJars().stream()
                .map(path -> path.getFileName().toString())
                .toList();

        assertThat(jarNames)
                .anyMatch(name -> name.startsWith("jackson-core-"))
                .anyMatch(name -> name.startsWith("jackson-databind-"))
                .anyMatch(name -> name.startsWith("jackson-annotations-"));
    }

    @Test
    void shouldStripAnsiColorCodesFromBuildFailureReason() {
        String colored = "Failed to execute goal \u001b[32morg.apache.maven.plugins:maven-surefire-plugin:3.5.6:test\u001b[m "
                + "\u001b[1m(default-test)\u001b[m on project \u001b[36mcitrus-api\u001b[m: \u001b[1;31m\n"
                + "java.lang.NoClassDefFoundError: tools/jackson/databind/ObjectMapper\n\u001b[m";

        assertThat(TestDistributionLifecycleParticipant.describeBuildFailure(List.of(new RuntimeException(colored))))
                .isEqualTo("Failed to execute goal org.apache.maven.plugins:maven-surefire-plugin:3.5.6:test "
                        + "(default-test) on project citrus-api:\n"
                        + "java.lang.NoClassDefFoundError: tools/jackson/databind/ObjectMapper");
    }

    @Test
    void shouldFallbackLocallyWithoutFailingBuildWhenCoordinatorUnreachable() throws Exception {
        MavenProject project = createSampleProject("module-fallback", tempDir.resolve("mod-fb"));
        Properties userProps = new Properties();
        userProps.setProperty(DistributionSettings.ENABLED_PROPERTY, "true");
        userProps.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, "http://127.0.0.1:1");
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
        TestDistributionLifecycleParticipant.runGit(tempDir, "init", "--quiet");
        assertThat(TestDistributionLifecycleParticipant.runGit(
                tempDir, "rev-parse", "--is-inside-work-tree"))
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
        userProps.setProperty(DistributionSettings.COORDINATOR_URL_PROPERTY, "http://127.0.0.1:1");
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
        return mockSessionWithExceptions(projects, userProps, List.of());
    }

    private MavenSession mockSessionWithExceptions(List<MavenProject> projects, Properties userProps, List<Throwable> exceptions) {
        MavenSession session = Mockito.mock(MavenSession.class);
        MavenExecutionResult result = Mockito.mock(MavenExecutionResult.class);
        when(result.hasExceptions()).thenReturn(!exceptions.isEmpty());
        when(result.getExceptions()).thenReturn(exceptions);
        when(session.getProjects()).thenReturn(projects);
        when(session.getTopLevelProject()).thenReturn(projects.get(0));
        when(session.getUserProperties()).thenReturn(userProps);
        when(session.getSystemProperties()).thenReturn(new Properties());
        when(session.getExecutionRootDirectory()).thenReturn(tempDir.toString());
        when(session.getResult()).thenReturn(result);
        return session;
    }
}
