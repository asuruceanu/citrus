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

package org.citrusframework.distribution.junit;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.config.PathRewriter;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BrokerEvent;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.AbstractTestDescriptor;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryListener;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.LauncherExecutionRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorating JUnit Platform {@link Launcher} that partitions discovered tests across Citrus Test Distribution
 * workers while replaying results into local IDE/Surefire {@link TestExecutionListener}s.
 */
public class DistributedLauncher implements Launcher {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedLauncher.class);

    private final Launcher delegate;
    private final List<TestExecutionListener> registeredListeners = new CopyOnWriteArrayList<>();

    public DistributedLauncher(Launcher delegate) {
        this.delegate = delegate;
    }

    @Override
    public void registerLauncherDiscoveryListeners(LauncherDiscoveryListener... listeners) {
        delegate.registerLauncherDiscoveryListeners(listeners);
    }

    @Override
    public void registerTestExecutionListeners(TestExecutionListener... listeners) {
        delegate.registerTestExecutionListeners(listeners);
        if (listeners != null) {
            registeredListeners.addAll(List.of(listeners));
        }
    }

    @Override
    public TestPlan discover(LauncherDiscoveryRequest discoveryRequest) {
        return delegate.discover(discoveryRequest);
    }

    @Override
    public void execute(LauncherDiscoveryRequest discoveryRequest, TestExecutionListener... listeners) {
        TestPlan plan = delegate.discover(discoveryRequest);
        executeDistributed(plan, listeners);
    }

    @Override
    public void execute(TestPlan testPlan, TestExecutionListener... listeners) {
        executeDistributed(testPlan, listeners);
    }

    @Override
    public void execute(LauncherExecutionRequest executionRequest) {
        TestPlan plan = executionRequest.getTestPlan()
                .orElseGet(() -> delegate.discover(executionRequest.getDiscoveryRequest()
                        .orElseThrow(() -> new IllegalArgumentException("Missing TestPlan and DiscoveryRequest"))));
        TestExecutionListener[] extra = executionRequest.getAdditionalTestExecutionListeners()
                .toArray(new TestExecutionListener[0]);
        executeDistributed(plan, extra);
    }

    private void executeDistributed(TestPlan testPlan, TestExecutionListener[] callListeners) {
        DiscoveredWorkExtractor.ExtractedWork extracted = DiscoveredWorkExtractor.extract(
                testPlan, DistributionSettings.getDefaultTestDurationMs());

        if (extracted.remoteUnits().isEmpty() && extracted.localUnits().isEmpty()) {
            delegate.execute(testPlan, callListeners);
            return;
        }

        if (extracted.remoteUnits().isEmpty()) {
            LocalFallbackExecutor.executeLocally(delegate, extracted.localUnits(), callListeners);
            return;
        }

        // Inputs are packaged before the coordinator is contacted, so a missing required input fails fast.
        WorkspaceSettings workspace = WorkspaceSettings.resolve();

        String brokerUrl = DistributionSettings.getBrokerUrl();
        String authToken = DistributionSettings.getAuthToken();
        boolean fallbackLocal = DistributionSettings.isFallbackLocal();
        String externalSessionId = System.getProperty(DistributionSettings.SESSION_ID_PROPERTY);
        boolean ownsSession = externalSessionId == null || externalSessionId.isBlank();

        BrokerHttpClient client = new BrokerHttpClient(
                brokerUrl, authToken, DistributionSettings.getConnectTimeoutMs());

        String sessionId = externalSessionId;
        try {
            if (ownsSession) {
                String projectName = System.getProperty(DistributionSettings.PROJECT_NAME_PROPERTY, "citrus");
                BuildSessionResponse created = client.createSession(new CreateSessionRequest(
                        projectName,
                        "local",
                        "HEAD",
                        List.of(),
                        DistributionSettings.getMaxPartitionSize(),
                        DistributionSettings.getMaxRetries()));
                sessionId = created.sessionId();
            }
        } catch (Exception e) {
            if (extracted.hasRemoteOnlyUnits() || !fallbackLocal) {
                throw new IllegalStateException(
                        "Citrus Test Distribution broker unreachable at " + brokerUrl
                                + " and remote execution is required", e);
            }
            LOG.warn("Broker {} unreachable ({}); falling back to local execution", brokerUrl, e.getMessage());
            List<WorkUnitSpec> all = new ArrayList<>(extracted.localUnits());
            all.addAll(extracted.remoteUnits());
            LocalFallbackExecutor.executeLocally(delegate, all, callListeners);
            return;
        }

        List<TestExecutionListener> allListeners = combineListeners(callListeners);
        SurefireReportWriter reportWriter = new SurefireReportWriter();

        LocalFallbackExecutor.LocalExecutionStats localStats =
                LocalFallbackExecutor.executeLocally(delegate, extracted.localUnits(), callListeners);

        try {
            PackagedClasspath packaged = packageCurrentClasspath();
            Map<String, ClasspathPackager.PackagedArtifact> uploadable = new LinkedHashMap<>(packaged.byDigest());
            for (WorkspaceFileSet input : workspace.inputArtifacts()) {
                uploadable.putIfAbsent(input.artifact().sha256(), new ClasspathPackager.PackagedArtifact(
                        input.artifact(), workspace.cache().resolveBlobPath(input.artifact().sha256()), null));
            }
            List<String> allDigests = List.copyOf(uploadable.keySet());
            List<String> missing = client.findMissingDigests(allDigests);
            Set<String> missingSet = new HashSet<>(missing);

            for (String sha : missing) {
                ClasspathPackager.PackagedArtifact artifact = uploadable.get(sha);
                if (artifact != null) {
                    client.uploadArtifact(artifact);
                }
            }

            int uploadedCount = missingSet.size();
            int cachedCount = Math.max(0, allDigests.size() - uploadedCount);
            String moduleName = System.getProperty(DistributionSettings.MODULE_NAME_PROPERTY, "default");

            SubmitWorkResponse submitted = client.submitWork(sessionId, new SubmitWorkRequest(
                    UUID.randomUUID().toString(),
                    moduleName,
                    packaged.artifacts(),
                    PathRewriter.rewriteHostProperties(collectPropagatedProperties(), workspace.hostBaseDir()),
                    extracted.remoteUnits(),
                    cachedCount,
                    uploadedCount,
                    workspace.inputArtifacts(),
                    workspace.outputPaths(),
                    workspace.forkWorkspace(),
                    workspace.partitionPolicy()));

            Set<String> submittedPartitions = Set.copyOf(submitted.partitionIds());
            Set<String> pendingPartitions = new HashSet<>(submitted.partitionIds());
            List<WorkUnitSpec> lostUnits = new ArrayList<>();
            PartitionOutcomes outcomes = new PartitionOutcomes(new HostOutputCommitter(workspace.hostBaseDir(),
                    workspace.outputPaths(), workspace.cacheExclusion()), workspace.cache());
            Map<String, WorkUnitSpec> unitsByKey = new LinkedHashMap<>();
            extracted.remoteUnits().forEach(unit -> unitsByKey.putIfAbsent(unit.key(), unit));
            try {
                notifyTestPlanExecutionStarted(allListeners, testPlan);
                try {
                    if (!pendingPartitions.isEmpty()) {
                        client.streamSessionEvents(
                                sessionId,
                                0L,
                                DistributionSettings.getSessionTimeoutMs(),
                                event -> handleBrokerEvent(event, testPlan, allListeners, reportWriter, submittedPartitions,
                                        pendingPartitions, lostUnits, unitsByKey, outcomes, client));
                    }
                } finally {
                    notifyTestPlanExecutionFinished(allListeners, testPlan);
                }

                LocalFallbackExecutor.LocalExecutionStats lostStats = new LocalFallbackExecutor.LocalExecutionStats(0, 0, 0, 0L);
                if (!lostUnits.isEmpty()) {
                    if (!fallbackLocal || extracted.hasRemoteOnlyUnits()) {
                        throw new IllegalStateException("Remote partitions were lost: " + lostUnits.size());
                    }
                    LOG.warn("Executing {} lost remote work unit(s) locally", lostUnits.size());
                    lostStats = LocalFallbackExecutor.executeLocally(delegate, lostUnits, callListeners);
                }

                String reportDirProp = System.getProperty(DistributionSettings.REPORT_DIR_PROPERTY);
                if (reportDirProp != null && !reportDirProp.isBlank()) {
                    reportWriter.writeReports(Path.of(reportDirProp));
                }

                RuntimeException outputFailure = outcomes.finish();

                if (ownsSession) {
                    client.completeSession(sessionId, new CompleteSessionRequest(
                            localStats.passed() + lostStats.passed(),
                            localStats.failed() + lostStats.failed(),
                            localStats.skipped() + lostStats.skipped(),
                            localStats.serialDurationMs() + lostStats.serialDurationMs()));
                }
                if (outputFailure != null) {
                    throw outputFailure;
                }
            } finally {
                // Every abort path (lost partitions, stream errors, failures in the event handler) drops staged outputs.
                outcomes.committer().discardStaging();
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("Distributed test execution failed for session " + sessionId, e);
        }
    }

    /**
     * Handles one session event. Events of partitions this launcher did not submit (sibling modules sharing a reactor
     * session) are ignored, and each partition's completion is processed once even if replayed.
     */
    private boolean handleBrokerEvent(BrokerEvent event,
                                      TestPlan testPlan,
                                      List<TestExecutionListener> listeners,
                                      SurefireReportWriter reportWriter,
                                      Set<String> submittedPartitions,
                                      Set<String> pendingPartitions,
                                      List<WorkUnitSpec> lostUnits,
                                      Map<String, WorkUnitSpec> unitsByKey,
                                      PartitionOutcomes outcomes,
                                      BrokerHttpClient client) {
        boolean ownEvent = event.partitionId() != null && submittedPartitions.contains(event.partitionId());
        switch (event.type()) {
            case "test.event" -> {
                if (ownEvent) {
                    WorkerTestEventPayload payload = DistributionJson.convert(event.data(), WorkerTestEventPayload.class);
                    reportWriter.record(payload);
                    replayToJUnitListeners(testPlan, listeners, payload);
                }
            }
            case "partition.completed" -> {
                if (ownEvent && pendingPartitions.remove(event.partitionId())) {
                    outcomes.completed(event, client);
                }
            }
            case "partition.lost" -> {
                if (!ownEvent || !pendingPartitions.remove(event.partitionId())) {
                    return pendingPartitions.isEmpty();
                }
                List<?> rawUnits = event.data() != null && event.data().get("workUnits") instanceof List<?> list ? list : List.of();
                int mapped = 0;
                for (Object item : rawUnits) {
                    // The coordinator reports lost work units by key.
                    WorkUnitSpec unit = item instanceof String key ? unitsByKey.get(key)
                            : DistributionJson.convert(item, WorkUnitSpec.class);
                    if (unit != null) {
                        lostUnits.add(unit);
                        mapped++;
                    }
                }
                if (rawUnits.isEmpty() || mapped < rawUnits.size()) {
                    // Fail closed: tests of this partition could not be identified for a local re-run.
                    outcomes.failures().add("Partition " + event.partitionId() + " was lost and not all of its work units "
                            + "could be identified for a local re-run");
                }
            }
            default -> {
                // ignore other informational events
            }
        }
        return pendingPartitions.isEmpty();
    }

    private void replayToJUnitListeners(TestPlan testPlan,
                                        List<TestExecutionListener> listeners,
                                        WorkerTestEventPayload payload) {
        TestIdentifier id = synthesizeIdentifier(payload);
        if (payload.status() == TestOutcomeStatus.STARTED) {
            for (TestExecutionListener l : listeners) {
                l.executionStarted(id);
            }
            return;
        }
        if (payload.status() == TestOutcomeStatus.SKIPPED) {
            for (TestExecutionListener l : listeners) {
                l.executionSkipped(id, payload.errorMessage() != null ? payload.errorMessage() : "skipped");
            }
            return;
        }
        TestExecutionResult result = payload.status() == TestOutcomeStatus.PASSED
                ? TestExecutionResult.successful()
                : TestExecutionResult.failed(new AssertionError(
                        payload.errorMessage() != null ? payload.errorMessage() : "Remote test failed"));
        for (TestExecutionListener l : listeners) {
            l.executionStarted(id);
            l.executionFinished(id, result);
        }
    }

    private static TestIdentifier synthesizeIdentifier(WorkerTestEventPayload payload) {
        String className = payload.className() != null ? payload.className() : "DistributedTest";
        String testName = payload.testName() != null ? payload.testName() : payload.workUnitKey();
        UniqueId uid = UniqueId.forEngine("junit-jupiter")
                .append("class", className)
                .append("method", testName);
        SyntheticRemoteDescriptor descriptor = new SyntheticRemoteDescriptor(uid, testName, className);
        return TestIdentifier.from(descriptor);
    }

    private void notifyTestPlanExecutionStarted(List<TestExecutionListener> listeners, TestPlan plan) {
        for (TestExecutionListener l : listeners) {
            l.testPlanExecutionStarted(plan);
        }
    }

    private void notifyTestPlanExecutionFinished(List<TestExecutionListener> listeners, TestPlan plan) {
        for (TestExecutionListener l : listeners) {
            l.testPlanExecutionFinished(plan);
        }
    }

    private List<TestExecutionListener> combineListeners(TestExecutionListener[] callListeners) {
        List<TestExecutionListener> all = new ArrayList<>(registeredListeners);
        if (callListeners != null) {
            for (TestExecutionListener l : callListeners) {
                if (!all.contains(l)) {
                    all.add(l);
                }
            }
        }
        return all;
    }

    private record PackagedClasspath(
            List<ArtifactRef> artifacts,
            Map<String, ClasspathPackager.PackagedArtifact> byDigest) {
    }

    private static PackagedClasspath packageCurrentClasspath() throws IOException {
        String cp = System.getProperty("java.class.path", "");
        List<ArtifactRef> artifacts = new ArrayList<>();
        Map<String, ClasspathPackager.PackagedArtifact> byDigest = new LinkedHashMap<>();

        for (String entry : cp.split(File.pathSeparator)) {
            if (entry == null || entry.isBlank()) {
                continue;
            }
            Path p = Path.of(entry);
            if (!Files.exists(p)) {
                continue;
            }
            ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(p);
            artifacts.add(packaged.ref());
            byDigest.putIfAbsent(packaged.ref().sha256(), packaged);
        }
        return new PackagedClasspath(artifacts, byDigest);
    }

    private static final java.util.Set<String> HOST_SYSTEM_PROPERTY_PREFIXES = java.util.Set.of(
            "java.", "javax.", "jdk.", "sun.", "os.", "user.", "file.", "path.", "line.",
            "awt.", "native.", "stdout.", "stderr.", "ftp.", "http.", "https.", "socksProxy",
            "gopherProxy", "maven.", "surefire.", "failsafe.", "plexus.", "classworlds.",
            "guice.", "org.slf4j.", "jboss.", "idea.", "eclipse.", "junit.platform.launcher.interceptors",
            "citrus.test.distribution."
    );

    private static final java.util.Set<String> HOST_SYSTEM_PROPERTY_KEYS = java.util.Set.of(
            "basedir", "localRepository", "env", "PID"
    );

    private static Map<String, String> collectPropagatedProperties() {
        return collectPropagatedProperties(System.getProperties());
    }

    static Map<String, String> collectPropagatedProperties(java.util.Properties source) {
        Map<String, String> props = new LinkedHashMap<>();
        source.forEach((k, v) -> {
            String key = String.valueOf(k);
            if (!isHostOrInternalProperty(key)) {
                props.put(key, String.valueOf(v));
            }
        });
        return props;
    }

    private static boolean isHostOrInternalProperty(String key) {
        if (HOST_SYSTEM_PROPERTY_KEYS.contains(key)) {
            return true;
        }
        for (String prefix : HOST_SYSTEM_PROPERTY_PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static void deleteRecursivelyQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            stream.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
    }

    /**
     * Workspace settings of this module run: the module base directory, declared inputs (already packaged into the
     * host cache), declared outputs, fork flag and partition policy.
     */
    record WorkspaceSettings(Path hostBaseDir, BlobCacheDirectory cache, List<WorkspaceFileSet> inputArtifacts,
                             List<String> outputPaths, boolean forkWorkspace, PartitionPolicy partitionPolicy) {

        /**
         * The host cache directory relative to the module, when it lies inside it, so remote outputs cannot target it.
         */
        List<String> cacheExclusion() {
            Path cacheRoot = cache.getRootDir();
            return cacheRoot.startsWith(hostBaseDir) && !cacheRoot.equals(hostBaseDir)
                    ? List.of(hostBaseDir.relativize(cacheRoot).toString().replace('\\', '/'))
                    : List.of();
        }

        static WorkspaceSettings resolve() {
            Path hostBaseDir = resolveHostBaseDir();
            String cacheDir = setting(DistributionSettings.CACHE_DIR_PROPERTY, DistributionSettings.CACHE_DIR_ENV);
            BlobCacheDirectory cache = new BlobCacheDirectory(cacheDir != null && !cacheDir.isBlank()
                    ? hostBaseDir.resolve(cacheDir)
                    : hostBaseDir.resolve("target").resolve(".citrus-dist").resolve("cache"));

            List<WorkspaceFileSet> inputs = new ArrayList<>();
            for (DistributionSettings.DeclaredPathSpec input : DistributionSettings.parseDeclaredPaths(
                    setting(DistributionSettings.INPUTS_PROPERTY, DistributionSettings.INPUTS_ENV))) {
                try {
                    WorkspacePackager.packageInputToCache(cache, hostBaseDir, input.relativePath(), input.optional())
                            .ifPresent(inputs::add);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException("Failed to package declared input '" + input.relativePath() + "'", e);
                }
            }
            List<String> outputs = DistributionSettings.parseDeclaredOutputPaths(
                            setting(DistributionSettings.OUTPUTS_PROPERTY, DistributionSettings.OUTPUTS_ENV)).stream()
                    .map(spec -> WorkspacePackager.validateRelativePath(spec.relativePath()))
                    .toList();
            for (String output : outputs) {
                // Outputs are never written through links; fail now rather than after the remote run.
                Path current = hostBaseDir;
                for (Path name : Path.of(output)) {
                    current = current.resolve(name);
                    if (BlobCacheDirectory.isLinkLike(current)) {
                        throw new IllegalArgumentException("Declared output '" + output
                                + "' passes through a symbolic link or junction: " + current);
                    }
                }
            }
            boolean fork = DistributionSettings.parseForkWorkspace(
                    setting(DistributionSettings.FORK_PROPERTY, DistributionSettings.FORK_ENV));
            PartitionPolicy policy = DistributionSettings.parsePartitionPolicy(
                    setting(DistributionSettings.PARTITION_POLICY_PROPERTY, DistributionSettings.PARTITION_POLICY_ENV));
            return new WorkspaceSettings(hostBaseDir, cache, List.copyOf(inputs), outputs,
                    fork || !inputs.isEmpty() || !outputs.isEmpty(), policy);
        }

        /**
         * {@code citrus.test.distribution.basedir}, else {@code basedir}, else {@code user.dir}.
         */
        static Path resolveHostBaseDir() {
            for (String key : List.of(DistributionSettings.BASEDIR_PROPERTY, "basedir", "user.dir")) {
                String value = System.getProperty(key);
                if (value != null && !value.isBlank() && !isPlaceholder(value)) {
                    return Path.of(value).toAbsolutePath().normalize();
                }
            }
            return Path.of("").toAbsolutePath();
        }

        /**
         * The value of a setting: a present system property wins even when empty (an empty value clears the setting);
         * otherwise the environment variable. An unresolved {@code ${...}} placeholder counts as unset.
         */
        static String setting(String property, String env) {
            if (System.getProperties().containsKey(property)) {
                String value = System.getProperty(property);
                if (value != null && !isPlaceholder(value.trim())) {
                    return value;
                }
            }
            String fromEnv = env != null ? System.getenv(env) : null;
            return fromEnv == null || fromEnv.isBlank() ? null : fromEnv;
        }

        private static boolean isPlaceholder(String value) {
            return value.startsWith("${") && value.endsWith("}");
        }
    }

    /**
     * Results of this launcher's partitions: runner failures and staged outputs, finished once all partitions ended.
     */
    private record PartitionOutcomes(HostOutputCommitter committer, BlobCacheDirectory cache, List<String> failures) {

        PartitionOutcomes(HostOutputCommitter committer, BlobCacheDirectory cache) {
            this(committer, cache, new ArrayList<>());
        }

        void completed(BrokerEvent event, BrokerHttpClient client) {
            Map<String, Object> data = event.data() != null ? event.data() : Map.of();
            if (!"COMPLETED".equals(String.valueOf(data.get("state")))) {
                failures.add("Partition " + event.partitionId() + " failed: " + data.getOrDefault("errorMessage", "no details"));
                return;
            }
            Object rawOutputs = data.get("outputArtifacts");
            if (rawOutputs == null) {
                return;
            }
            try {
                List<WorkspaceFileSet> outputs = DistributionJson.mapper().convertValue(rawOutputs,
                        new tools.jackson.core.type.TypeReference<List<WorkspaceFileSet>>() { });
                for (WorkspaceFileSet output : outputs) {
                    String sha256 = output.artifact().sha256();
                    if (cache.getVerifiedPath(sha256) == null) {
                        Path download = cache.newTempFile("output-");
                        try {
                            client.downloadBlobToFile(sha256, download);
                            cache.putFromFile(sha256, download);
                        } finally {
                            Files.deleteIfExists(download);
                        }
                    }
                }
                committer.recordPartitionOutputs(event.partitionId(), outputs, cache);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failures.add("Interrupted while collecting outputs of partition " + event.partitionId());
            } catch (IOException | RuntimeException e) {
                failures.add("Failed to collect outputs of partition " + event.partitionId() + ": " + e.getMessage());
            }
        }

        /**
         * Commits the staged outputs when every partition succeeded and no outputs conflict; otherwise writes nothing.
         *
         * @return the failure to report, or {@code null}
         */
        RuntimeException finish() {
            List<String> problems = new ArrayList<>(failures);
            committer.conflictError().ifPresent(problems::add);
            if (!problems.isEmpty()) {
                committer.discardStaging();
                return new IllegalStateException("Distributed test execution failed; no outputs were written to the module:"
                        + System.lineSeparator() + String.join(System.lineSeparator(), problems));
            }
            try {
                committer.commitOrRollback();
                return null;
            } catch (IOException | RuntimeException e) {
                return new IllegalStateException("Failed to write remote outputs into the module: " + e.getMessage(), e);
            }
        }
    }

    private static final class SyntheticRemoteDescriptor extends AbstractTestDescriptor {
        SyntheticRemoteDescriptor(UniqueId uniqueId, String displayName, String className) {
            super(uniqueId, displayName, ClassSource.from(className));
        }

        @Override
        public Type getType() {
            return Type.TEST;
        }
    }
}
