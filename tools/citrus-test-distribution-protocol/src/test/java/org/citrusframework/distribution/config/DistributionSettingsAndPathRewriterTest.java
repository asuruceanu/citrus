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

package org.citrusframework.distribution.config;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.citrusframework.distribution.config.DistributionSettings.DeclaredPathSpec;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionCompletedPayload;
import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DistributionSettingsAndPathRewriterTest {

    private static final String TOKEN = DistributionSettings.WORKSPACE_TOKEN;

    @Test
    void shouldResolveAdaptiveBrokerSettingsFromSystemProperties() {
        String duration = DistributionSettings.BROKER_MIN_PARTITION_DURATION_MS_PROPERTY;
        String waves = DistributionSettings.BROKER_MAX_WAVES_PER_WORKER_PROPERTY;
        String previousDuration = System.getProperty(duration);
        String previousWaves = System.getProperty(waves);
        try {
            System.setProperty(duration, "4500");
            System.setProperty(waves, "3");
            assertThat(DistributionSettings.getBrokerMinPartitionDurationMs()).isEqualTo(4_500L);
            assertThat(DistributionSettings.getBrokerMaxWavesPerWorker()).isEqualTo(3);

            System.setProperty(duration, "-1");
            System.setProperty(waves, "-1");
            assertThat(DistributionSettings.getBrokerMinPartitionDurationMs()).isZero();
            assertThat(DistributionSettings.getBrokerMaxWavesPerWorker()).isZero();

            System.setProperty(duration, "invalid");
            System.setProperty(waves, "invalid");
            assertThat(DistributionSettings.getBrokerMinPartitionDurationMs()).isEqualTo(5_000L);
            assertThat(DistributionSettings.getBrokerMaxWavesPerWorker()).isEqualTo(2);
        } finally {
            restoreProperty(duration, previousDuration);
            restoreProperty(waves, previousWaves);
        }
    }

    private static void restoreProperty(String key, String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }

    @Test
    void shouldReturnOnlyExplicitPositiveMaxPartitionSizes() {
        UnaryOperator<String> unset = key -> null;
        assertThat(DistributionSettings.findConfiguredMaxPartitionSize(unset, unset)).isEmpty();
        for (String invalid : List.of("invalid", "-1", "0", "2147483648")) {
            assertThat(DistributionSettings.findConfiguredMaxPartitionSize(key -> invalid, unset)).isEmpty();
        }

        assertThat(DistributionSettings.findConfiguredMaxPartitionSize(
                Map.of(DistributionSettings.MAX_PARTITION_SIZE_PROPERTY, " 12 ")::get, key -> "24")).hasValue(12);
        assertThat(DistributionSettings.findConfiguredMaxPartitionSize(unset,
                Map.of(DistributionSettings.MAX_PARTITION_SIZE_ENV, "12")::get)).hasValue(12);
    }

    @Test
    void shouldReadAndClampMinRemoteThresholds() {
        UnaryOperator<String> unset = key -> null;
        assertThat(DistributionSettings.getMinRemoteDurationMs(unset, unset)).isEqualTo(2_000L);
        assertThat(DistributionSettings.getMinRemoteColdUnits(unset, unset)).isZero();

        assertThat(DistributionSettings.getMinRemoteDurationMs(
                Map.of(DistributionSettings.MIN_REMOTE_DURATION_MS_PROPERTY, " 3000 ")::get, key -> "9000"))
                .isEqualTo(3_000L);
        assertThat(DistributionSettings.getMinRemoteDurationMs(unset,
                Map.of(DistributionSettings.MIN_REMOTE_DURATION_MS_ENV, "9000")::get)).isEqualTo(9_000L);
        assertThat(DistributionSettings.getMinRemoteColdUnits(
                Map.of(DistributionSettings.MIN_REMOTE_COLD_UNITS_PROPERTY, "4")::get, key -> "3")).isEqualTo(4);
        assertThat(DistributionSettings.getMinRemoteColdUnits(unset,
                Map.of(DistributionSettings.MIN_REMOTE_COLD_UNITS_ENV, "3")::get)).isEqualTo(3);

        assertThat(DistributionSettings.getMinRemoteDurationMs(key -> "-1", unset)).isZero();
        assertThat(DistributionSettings.getMinRemoteColdUnits(unset, key -> "-1")).isZero();
    }

    @Test
    void shouldParseDeclaredPathsWithNoneEscapeOptionalAndAncestorDedup() {
        assertThat(DistributionSettings.parseDeclaredPaths("NONE")).isEmpty();
        assertThat(DistributionSettings.parseDeclaredPaths("./none"))
                .containsExactly(new DeclaredPathSpec("none", false));

        var specs = DistributionSettings.parseDeclaredPaths(
                " ?src/test/resources/sub/a.xml , src\\test\\resources/ , ?pom.xml , pom.xml ");
        assertThat(specs).containsExactly(
                new DeclaredPathSpec("src/test/resources", false),
                new DeclaredPathSpec("pom.xml", false));
    }

    @Test
    void shouldTreatNullBlankAndEmptyTokensAsEmpty() {
        assertThat(DistributionSettings.parseDeclaredPaths(null)).isEmpty();
        assertThat(DistributionSettings.parseDeclaredPaths("")).isEmpty();
        assertThat(DistributionSettings.parseDeclaredPaths("   ")).isEmpty();
        assertThat(DistributionSettings.parseDeclaredPaths(" , ,")).isEmpty();
        assertThat(DistributionSettings.parseDeclaredPaths(" None ")).isEmpty();
    }

    @Test
    void shouldAcceptTrailingOptionalMarkerAndKeepOptionalWhenAlone() {
        assertThat(DistributionSettings.parseDeclaredPaths("target/docs?,?src/main"))
                .containsExactly(
                        new DeclaredPathSpec("target/docs", true),
                        new DeclaredPathSpec("src/main", true));
    }

    @Test
    void shouldCollapseChildrenUnderAncestorRegardlessOfOrderButKeepSiblingPrefixes() {
        assertThat(DistributionSettings.parseDeclaredPaths("src/test/resources,src/test,src/testdata,./src/test/java"))
                .containsExactly(
                        new DeclaredPathSpec("src/test", false),
                        new DeclaredPathSpec("src/testdata", false));
    }

    @Test
    void shouldLetRequiredAncestorWinOverOptionalAncestorButNotTheOtherWay() {
        // Optional ancestor must not swallow a required child: the child must still fail fast if missing.
        assertThat(DistributionSettings.parseDeclaredPaths("?src/test,src/test/resources/a.xml"))
                .containsExactly(
                        new DeclaredPathSpec("src/test", true),
                        new DeclaredPathSpec("src/test/resources/a.xml", false));
    }

    @Test
    void shouldParseForkJvmArgsWithDefaults() {
        assertThat(DistributionSettings.parseForkJvmArgs(null)).containsExactly("-Xmx768m", "-XX:ActiveProcessorCount=2");
        assertThat(DistributionSettings.parseForkJvmArgs("  ")).containsExactly("-Xmx768m", "-XX:ActiveProcessorCount=2");
        assertThat(DistributionSettings.parseForkJvmArgs(" -Xmx2g \t -Dfoo=bar\n-ea "))
                .containsExactly("-Xmx2g", "-Dfoo=bar", "-ea");
    }

    @Test
    void shouldRejectOptionalMarkerOnOutputPaths() {
        assertThat(DistributionSettings.parseDeclaredOutputPaths(" target/docs/ , target/docs/index.html,NONE-file"))
                .containsExactly(
                        new DeclaredPathSpec("target/docs", false),
                        new DeclaredPathSpec("NONE-file", false));
        assertThat(DistributionSettings.parseDeclaredOutputPaths("none")).isEmpty();
        assertThatThrownBy(() -> DistributionSettings.parseDeclaredOutputPaths("target/docs,target/x?"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("target/x?")
                .hasMessageContaining(DistributionSettings.OUTPUTS_PROPERTY);
        assertThatThrownBy(() -> DistributionSettings.parseDeclaredOutputPaths("?target/x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldParseForkFlagStrictly() {
        assertThat(DistributionSettings.parseForkWorkspace(null)).isFalse();
        assertThat(DistributionSettings.parseForkWorkspace(" ")).isFalse();
        assertThat(DistributionSettings.parseForkWorkspace(" TRUE ")).isTrue();
        assertThat(DistributionSettings.parseForkWorkspace("false")).isFalse();
        assertThatThrownBy(() -> DistributionSettings.parseForkWorkspace("yes"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DistributionSettings.FORK_PROPERTY)
                .hasMessageContaining("yes");
    }

    @Test
    void shouldParseEnumSettingsStrictlyWithDefaults() {
        assertThat(DistributionSettings.parsePartitionPolicy(null)).isEqualTo(PartitionPolicy.LPT);
        assertThat(DistributionSettings.parsePartitionPolicy(" single ")).isEqualTo(PartitionPolicy.SINGLE);
        assertThatThrownBy(() -> DistributionSettings.parsePartitionPolicy("ROUND_ROBIN"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DistributionSettings.PARTITION_POLICY_PROPERTY)
                .hasMessageContaining("LPT, SINGLE");

        assertThat(DistributionSettings.parseWorkerForkMode("")).isEqualTo(DistributionSettings.WorkerForkMode.ALWAYS);
        assertThat(DistributionSettings.parseWorkerForkMode(null)).isEqualTo(DistributionSettings.WorkerForkMode.ALWAYS);
        assertThat(DistributionSettings.parseWorkerForkMode("auto")).isEqualTo(DistributionSettings.WorkerForkMode.AUTO);
        assertThatThrownBy(() -> DistributionSettings.parseWorkerForkMode("never"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DistributionSettings.WORKER_FORK_MODE_PROPERTY);

        assertThat(DistributionSettings.parseWorkerCwdCheckMode(null)).isEqualTo(DistributionSettings.WorkerCwdCheckMode.FAIL);
        assertThat(DistributionSettings.parseWorkerCwdCheckMode("Warn")).isEqualTo(DistributionSettings.WorkerCwdCheckMode.WARN);
        assertThatThrownBy(() -> DistributionSettings.parseWorkerCwdCheckMode("ignore"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DistributionSettings.WORKER_CWD_CHECK_PROPERTY);
    }

    @Test
    void shouldBuildWindowsSeparatorFormsOfHostPrefix() {
        assertThat(PathRewriter.separatorForms("C:\\work\\app", true))
                .containsExactlyInAnyOrder("C:\\work\\app", "C:/work/app", "C:\\\\work\\\\app");
        assertThat(PathRewriter.separatorForms("/work/app", false)).containsExactly("/work/app");
    }

    @Test
    void shouldMatchWindowsHostPrefixCaseInsensitivelyAtBoundaries() {
        List<String> candidates = PathRewriter.separatorForms("C:\\Work\\App", true).stream()
                .sorted((a, b) -> b.length() - a.length())
                .toList();

        assertThat(PathRewriter.rewriteValue("c:\\work\\app\\target", candidates, true)).isEqualTo(TOKEN + "\\target");
        assertThat(PathRewriter.rewriteValue("file:///c:/WORK/app/x.xml", candidates, true)).isEqualTo("file:///" + TOKEN + "/x.xml");
        assertThat(PathRewriter.rewriteValue("file:/C:/Work/App", candidates, true)).isEqualTo("file:/" + TOKEN);
        assertThat(PathRewriter.rewriteValue("-Dp=C:\\\\Work\\\\App\\\\db", candidates, true)).isEqualTo("-Dp=" + TOKEN + "\\\\db");
        assertThat(PathRewriter.rewriteValue("C:\\Work\\AppOther", candidates, true)).isEqualTo("C:\\Work\\AppOther");
        assertThat(PathRewriter.rewriteValue("c:\\work\\app", candidates, false)).isEqualTo("c:\\work\\app");
    }

    @Test
    void shouldDeserializeLegacyPayloadsWithSafeDefaults() {
        String legacySubmit = """
                {"submissionId":"s1","moduleName":"m1","classpathArtifacts":[],"systemProperties":{},"workUnits":[],"cachedBlobs":0,"uploadedBlobs":0}
                """;
        SubmitWorkRequest req = DistributionJson.fromJson(legacySubmit, SubmitWorkRequest.class);
        assertThat(req.inputArtifacts()).isEmpty();
        assertThat(req.outputPaths()).isEmpty();
        assertThat(req.forkWorkspace()).isFalse();
        assertThat(req.partitionPolicy()).isEqualTo(PartitionPolicy.LPT);
        assertThat(req.workingDirectory()).as("legacy: workspace root").isEmpty();

        String legacyAssign = """
                {"partitionId":"p1","sessionId":"s1","attempt":2,"classpathArtifacts":[],"systemProperties":{},"workUnits":[]}
                """;
        PartitionAssignPayload assign = DistributionJson.fromJson(legacyAssign, PartitionAssignPayload.class);
        assertThat(assign.attempt()).isEqualTo(2);
        assertThat(assign.workingDirectory()).isEmpty();
        assertThat(assign.inputArtifacts()).isEmpty();
        assertThat(assign.outputPaths()).isEmpty();
        assertThat(assign.forkWorkspace()).isFalse();

        String legacyCompleted = """
                {"partitionId":"p1","sessionId":"s1","success":true,"errorMessage":null}
                """;
        PartitionCompletedPayload done = DistributionJson.fromJson(legacyCompleted, PartitionCompletedPayload.class);
        assertThat(done.attempt()).as("0 marks a legacy sender that did not report an attempt").isZero();
        assertThat(done.success()).isTrue();
        assertThat(done.outputArtifacts()).isEmpty();
    }

    @Test
    void shouldRoundTripWorkspaceFieldsAndKeepLegacyConstructors() {
        ArtifactRef blob = new ArtifactRef("index.html", "abc123", ArtifactKind.REGULAR_FILE, 42L);
        SubmitWorkRequest req = new SubmitWorkRequest("s1", "m1", List.of(), Map.of(), List.of(), 0, 0,
                List.of(new WorkspaceFileSet("src/test/resources", blob)), List.of("target/docs"), true, PartitionPolicy.SINGLE);
        SubmitWorkRequest copy = DistributionJson.fromJson(DistributionJson.toJson(req), SubmitWorkRequest.class);
        assertThat(copy).isEqualTo(req);

        PartitionCompletedPayload done = new PartitionCompletedPayload("p1", "s1", 3, false, "boom",
                List.of(new WorkspaceFileSet("target/docs", blob)));
        assertThat(DistributionJson.fromJson(DistributionJson.toJson(done), PartitionCompletedPayload.class)).isEqualTo(done);

        SubmitWorkRequest legacy = new SubmitWorkRequest("s1", "m1", List.of(), Map.of(), List.of(), 0, 0);
        assertThat(legacy.partitionPolicy()).isEqualTo(PartitionPolicy.LPT);
        assertThat(legacy.inputArtifacts()).isEmpty();

        PartitionAssignPayload legacyAssign = new PartitionAssignPayload("p1", "s1", 1, List.of(), Map.of(), List.of());
        assertThat(legacyAssign.inputArtifacts()).isEmpty();
        assertThat(legacyAssign.forkWorkspace()).isFalse();

        PartitionCompletedPayload legacyDone = new PartitionCompletedPayload("p1", "s1", true, null);
        assertThat(legacyDone.attempt()).isZero();
        assertThat(new PartitionCompletedPayload("p1", "s1", -5, true, null, null).attempt()).isZero();
        assertThat(legacyDone.outputArtifacts()).isEmpty();
    }

    @Test
    void shouldRejectNullElementsInWorkspaceLists() {
        String withNullOutput = """
                {"submissionId":"s1","moduleName":"m1","outputPaths":["target/docs",null]}
                """;
        assertThatThrownBy(() -> DistributionJson.fromJson(withNullOutput, SubmitWorkRequest.class))
                .hasRootCauseInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldCarryTheWorkingDirectoryRelativeToTheWorkspace() {
        SubmitWorkRequest req = new SubmitWorkRequest("s1", "m1", List.of(), Map.of(), List.of(), 0, 0,
                List.of(), List.of(), false, PartitionPolicy.LPT, "target");
        assertThat(DistributionJson.fromJson(DistributionJson.toJson(req), SubmitWorkRequest.class).workingDirectory())
                .isEqualTo("target");
        assertThat(new SubmitWorkRequest("s1", "m1", List.of(), Map.of(), List.of(), 0, 0,
                List.of(), List.of(), false, null, null).workingDirectory()).isEmpty();
        PartitionAssignPayload assign = new PartitionAssignPayload("p1", "s1", 1, List.of(), Map.of(), List.of(),
                List.of(), List.of(), true, "target");
        assertThat(DistributionJson.fromJson(DistributionJson.toJson(assign), PartitionAssignPayload.class).workingDirectory())
                .isEqualTo("target");
    }

    @Test
    void shouldRewritePrefixedUrisWithoutCorruptingSiblingDirectoryNames() {
        Path hostBase = Path.of("/work/app");
        Map<String, String> props = Map.of(
                "dbUrl", "jdbc:h2:file:/work/app/target/db",
                "sibling", "/work/app-other/config.xml",
                "pathList", "/work/app/a:/work/app-2/b;/work/app");
        Map<String, String> rewritten = PathRewriter.rewriteHostProperties(props, hostBase);
        assertThat(rewritten.get("dbUrl")).isEqualTo("jdbc:h2:file:${citrus.dist.workspace}/target/db");
        assertThat(rewritten.get("sibling")).isEqualTo("/work/app-other/config.xml");
        assertThat(rewritten.get("pathList"))
                .isEqualTo("${citrus.dist.workspace}/a:/work/app-2/b;${citrus.dist.workspace}");

        Map<String, String> expanded = PathRewriter.expandWorkspaceProperties(rewritten, Path.of("/tmp/cdist-1/workspace"));
        assertThat(expanded.get("dbUrl")).isEqualTo("jdbc:h2:file:/tmp/cdist-1/workspace/target/db");
    }

    @Test
    void shouldRewriteOnlyAtSegmentBoundaries() {
        Path hostBase = Path.of("/work/citrus/tools/rest");
        Map<String, String> props = new HashMap<>();
        props.put("restdocs", "/work/citrus/tools/restdocs/x");
        props.put("restApi", "/work/citrus/tools/rest-api");
        props.put("superPath", "/other/work/citrus/tools/rest/x");
        props.put("fileUri", "file:///work/citrus/tools/rest/target/x.xml");
        props.put("assignment", "-Dbase=/work/citrus/tools/rest");
        props.put("quoted", "\"/work/citrus/tools/rest/a b\",'/work/citrus/tools/rest'");
        props.put("bracketed", "[/work/citrus/tools/rest](/work/citrus/tools/rest/y)");
        props.put("commaList", "/work/citrus/tools/rest/a,/work/citrus/tools/rest/b");
        props.put("backslash", "/work/citrus/tools/rest\\sub");
        props.put("buildDir", "/work/citrus/tools/rest/target");
        props.put("argFile", "@/work/citrus/tools/rest/target/args.txt");
        props.put("fileUriSingleSlash", "file:/work/citrus/tools/rest/a");
        props.put("unrelated", "hello");
        props.put("nullValue", null);

        Map<String, String> rewritten = PathRewriter.rewriteHostProperties(props, hostBase);

        assertThat(rewritten.get("restdocs")).isEqualTo("/work/citrus/tools/restdocs/x");
        assertThat(rewritten.get("restApi")).isEqualTo("/work/citrus/tools/rest-api");
        assertThat(rewritten.get("superPath")).isEqualTo("/other/work/citrus/tools/rest/x");
        assertThat(rewritten.get("fileUri")).isEqualTo("file://" + TOKEN + "/target/x.xml");
        assertThat(rewritten.get("assignment")).isEqualTo("-Dbase=" + TOKEN);
        assertThat(rewritten.get("quoted")).isEqualTo("\"" + TOKEN + "/a b\",'" + TOKEN + "'");
        assertThat(rewritten.get("bracketed")).isEqualTo("[" + TOKEN + "](" + TOKEN + "/y)");
        assertThat(rewritten.get("commaList")).isEqualTo(TOKEN + "/a," + TOKEN + "/b");
        assertThat(rewritten.get("backslash")).isEqualTo(TOKEN + "\\sub");
        assertThat(rewritten.get("buildDir")).isEqualTo(TOKEN + "/target");
        assertThat(rewritten.get("argFile")).isEqualTo("@" + TOKEN + "/target/args.txt");
        assertThat(rewritten.get("fileUriSingleSlash")).isEqualTo("file:" + TOKEN + "/a");
        assertThat(rewritten.get("unrelated")).isEqualTo("hello");
        assertThat(rewritten).containsKey("nullValue");
        assertThat(rewritten.get("nullValue")).isNull();
    }

    @Test
    void shouldNotRewriteWhenHostBaseDirIsFilesystemRootOrNull() {
        Map<String, String> props = Map.of("path", "/etc/hosts");
        assertThat(PathRewriter.rewriteHostProperties(props, Path.of("/"))).isEqualTo(props);
        assertThat(PathRewriter.rewriteHostProperties(props, null)).isEqualTo(props);
    }

    @Test
    void shouldRewriteBothRealAndSymlinkedHostPaths(@TempDir Path tmp) throws Exception {
        Path real = Files.createDirectories(tmp.resolve("real/app"));
        Path link = Files.createSymbolicLink(tmp.resolve("link"), tmp.resolve("real"));
        Path linkedBase = link.resolve("app");

        String realPath = real.toRealPath().toString();
        Map<String, String> props = Map.of(
                "viaReal", realPath + "/target",
                "viaLink", linkedBase.toAbsolutePath() + "/target");

        Map<String, String> rewritten = PathRewriter.rewriteHostProperties(props, linkedBase);
        assertThat(rewritten.get("viaReal")).isEqualTo(TOKEN + "/target");
        assertThat(rewritten.get("viaLink")).isEqualTo(TOKEN + "/target");
    }

    @Test
    void shouldExpandTokenAndNormalizeFollowingSeparator() {
        Path workspace = Path.of("/tmp/cdist-1/workspace");
        String ws = workspace.toAbsolutePath().normalize().toString();
        Map<String, String> props = new HashMap<>();
        props.put("plain", TOKEN);
        props.put("forward", TOKEN + "/target/db");
        props.put("backward", TOKEN + "\\target\\db");
        props.put("twice", TOKEN + "/a:" + TOKEN + "/b");
        props.put("untouched", "/somewhere/else");
        props.put("nullValue", null);

        Map<String, String> expanded = PathRewriter.expandWorkspaceProperties(props, workspace);

        String sep = String.valueOf(File.separatorChar);
        assertThat(expanded.get("plain")).isEqualTo(ws);
        assertThat(expanded.get("forward")).isEqualTo(ws + sep + "target/db");
        assertThat(expanded.get("backward")).isEqualTo(ws + sep + "target\\db");
        assertThat(expanded.get("twice")).isEqualTo(ws + sep + "a:" + ws + sep + "b");
        assertThat(expanded.get("untouched")).isEqualTo("/somewhere/else");
        assertThat(expanded.get("nullValue")).isNull();
    }

    @Test
    void shouldExposeWorkspaceSettingKeys() {
        assertThat(DistributionSettings.WORKSPACE_CAPABILITY_KEY).isEqualTo("citrus.workspace.v1");
        assertThat(DistributionSettings.WORKSPACE_TOKEN).isEqualTo("${citrus.dist.workspace}");
        assertThat(DistributionSettings.BASEDIR_PROPERTY).isEqualTo("citrus.test.distribution.basedir");
        assertThat(DistributionSettings.INPUTS_ENV).isEqualTo("CITRUS_DIST_INPUTS");
        assertThat(DistributionSettings.WORKER_CWD_CHECK_PROPERTY).isEqualTo("citrus.test.distribution.worker.cwdCheck");
        assertThat(DistributionSettings.WorkerForkMode.values())
                .containsExactly(DistributionSettings.WorkerForkMode.AUTO, DistributionSettings.WorkerForkMode.ALWAYS);
        assertThat(DistributionSettings.WorkerCwdCheckMode.values()).containsExactly(
                DistributionSettings.WorkerCwdCheckMode.FAIL,
                DistributionSettings.WorkerCwdCheckMode.WARN,
                DistributionSettings.WorkerCwdCheckMode.OFF);
        assertThat(ArtifactKind.valueOf("REGULAR_FILE")).isEqualTo(ArtifactKind.REGULAR_FILE);
    }

    @Test
    void shouldParseByteSizeUnitsAndRejectInvalidInputs() {
        assertThat(DistributionSettings.parseByteSize("0", "opt")).isZero();
        assertThat(DistributionSettings.parseByteSize("4096", "opt")).isEqualTo(4096L);
        assertThat(DistributionSettings.parseByteSize("512KiB", "opt")).isEqualTo(524_288L);
        assertThat(DistributionSettings.parseByteSize(" 256 MiB ", "opt")).isEqualTo(268_435_456L);
        assertThat(DistributionSettings.parseByteSize("2GiB", "opt")).isEqualTo(2_147_483_648L);
        assertThat(DistributionSettings.parseByteSize("2gib", "opt")).isEqualTo(2_147_483_648L);
        assertThat(DistributionSettings.parseByteSize("512mib", "opt")).isEqualTo(536_870_912L);
        assertThat(DistributionSettings.parseByteSize("1024KIB", "opt")).isEqualTo(1_048_576L);

        for (String invalid : new String[] {null, "", "   ", "-1", "2GB", "MB", "KB", "1TiB", "1.5GiB", "abc",
                "9999999999GiB", "99999999999999999999"}) {
            assertThatThrownBy(() -> DistributionSettings.parseByteSize(invalid, "my.option"))
                    .as("input %s", invalid)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("my.option")
                    .hasMessageContaining("KiB, MiB, GiB");
        }
    }

    @Test
    void shouldParseBooleanStrictAndRejectInvalidTokens() {
        assertThat(DistributionSettings.parseBooleanStrict("true", "opt")).isTrue();
        assertThat(DistributionSettings.parseBooleanStrict("TRUE", "opt")).isTrue();
        assertThat(DistributionSettings.parseBooleanStrict(" false ", "opt")).isFalse();

        for (String invalid : new String[] {null, "", "flase", "1", "yes"}) {
            assertThatThrownBy(() -> DistributionSettings.parseBooleanStrict(invalid, "my.option"))
                    .as("input %s", invalid)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("my.option");
        }
    }

    @Test
    void shouldResolveWorkerCacheSettingsWithPrecedenceBlankFallthroughAndGlobalIsolation() {
        UnaryOperator<String> none = key -> null;
        Path defaultDir = DistributionSettings.defaultWorkerCacheDir();
        assertThat(defaultDir).isEqualTo(Paths.get(System.getProperty("user.home", "."))
                .resolve(".citrus-test-distribution-agent").resolve("file-cache"));
        assertThat(DistributionSettings.resolveWorkerCacheDir(none, none)).isEqualTo(defaultDir);
        assertThat(DistributionSettings.resolveWorkerCacheCleanup(none, none)).isTrue();
        assertThat(DistributionSettings.resolveWorkerCacheFreeSpaceBufferBytes(none, none))
                .isEqualTo(DistributionSettings.DEFAULT_WORKER_CACHE_FREE_SPACE_BUFFER_BYTES);

        Map<String, String> props = new HashMap<>();
        Map<String, String> env = new HashMap<>();
        env.put(DistributionSettings.AGENT_CACHE_DIR_ENV, "/agent");
        env.put(DistributionSettings.AGENT_CACHE_CLEANUP_ENV, "false");
        env.put(DistributionSettings.AGENT_CACHE_FREE_SPACE_BUFFER_SIZE_ENV, "1GiB");
        assertThat(DistributionSettings.resolveWorkerCacheDir(props::get, env::get)).isEqualTo(Paths.get("/agent"));
        assertThat(DistributionSettings.resolveWorkerCacheCleanup(props::get, env::get)).isFalse();
        assertThat(DistributionSettings.resolveWorkerCacheFreeSpaceBufferBytes(props::get, env::get))
                .isEqualTo(1024L * 1024 * 1024);

        env.put(DistributionSettings.WORKER_CACHE_DIR_ENV, "/citrus");
        env.put(DistributionSettings.WORKER_CACHE_CLEANUP_ENV, "true");
        env.put(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_ENV, "2MiB");
        assertThat(DistributionSettings.resolveWorkerCacheDir(props::get, env::get)).isEqualTo(Paths.get("/citrus"));
        assertThat(DistributionSettings.resolveWorkerCacheCleanup(props::get, env::get)).isTrue();
        assertThat(DistributionSettings.resolveWorkerCacheFreeSpaceBufferBytes(props::get, env::get))
                .isEqualTo(2L * 1024 * 1024);

        props.put(DistributionSettings.WORKER_CACHE_DIR_PROPERTY, "/prop");
        props.put(DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY, "false");
        props.put(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY, "3KiB");
        assertThat(DistributionSettings.resolveWorkerCacheDir(props::get, env::get)).isEqualTo(Paths.get("/prop"));
        assertThat(DistributionSettings.resolveWorkerCacheCleanup(props::get, env::get)).isFalse();
        assertThat(DistributionSettings.resolveWorkerCacheFreeSpaceBufferBytes(props::get, env::get))
                .isEqualTo(3072L);

        // blank values fall through to the next tier
        props.put(DistributionSettings.WORKER_CACHE_DIR_PROPERTY, "");
        props.put(DistributionSettings.WORKER_CACHE_CLEANUP_PROPERTY, "  ");
        props.put(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY, "");
        env.put(DistributionSettings.WORKER_CACHE_DIR_ENV, "  ");
        assertThat(DistributionSettings.resolveWorkerCacheDir(props::get, env::get)).isEqualTo(Paths.get("/agent"));
        assertThat(DistributionSettings.resolveWorkerCacheCleanup(props::get, env::get)).isTrue();
        assertThat(DistributionSettings.resolveWorkerCacheFreeSpaceBufferBytes(props::get, env::get))
                .isEqualTo(2L * 1024 * 1024);

        // an invalid non-blank higher tier is an error, not a fall-through
        env.put(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_ENV, "bad");
        assertThatThrownBy(() -> DistributionSettings.resolveWorkerCacheFreeSpaceBufferBytes(props::get, env::get))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DistributionSettings.WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_ENV);
        env.put(DistributionSettings.WORKER_CACHE_CLEANUP_ENV, "maybe");
        assertThatThrownBy(() -> DistributionSettings.resolveWorkerCacheCleanup(props::get, env::get))
                .isInstanceOf(IllegalArgumentException.class);

        // the global coordinator cache dir does not affect the worker cache dir
        String previous = System.getProperty(DistributionSettings.CACHE_DIR_PROPERTY);
        System.setProperty(DistributionSettings.CACHE_DIR_PROPERTY, "/tmp/global-cache");
        try {
            assertThat(DistributionSettings.getWorkerCacheDir().toString()).doesNotContain("global-cache");
        } finally {
            if (previous == null) {
                System.clearProperty(DistributionSettings.CACHE_DIR_PROPERTY);
            } else {
                System.setProperty(DistributionSettings.CACHE_DIR_PROPERTY, previous);
            }
        }
    }
}
