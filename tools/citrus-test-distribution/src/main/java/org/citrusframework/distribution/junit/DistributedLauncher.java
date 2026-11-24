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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.config.PathRewriter;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.config.QuarkusWorkspace;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BrokerEvent;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.PartitionPolicy;
import org.citrusframework.distribution.model.ReportLocalTestsRequest;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.citrusframework.distribution.workspace.WorkspaceLease;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryListener;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.LauncherExecutionRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

/**
 * Decorating JUnit Platform {@link Launcher} that partitions discovered tests across Citrus Test Distribution
 * workers while replaying results into local IDE/Surefire {@link TestExecutionListener}s.
 */
public class DistributedLauncher implements Launcher {

    private static final System.Logger LOG = System.getLogger(DistributedLauncher.class.getName());
    // Exact class names whose class-level local pin the build explicitly overrides.
    private static final String REMOTE_LOCAL_ONLY_CLASSES_PROPERTY = "citrus.test.distribution.remoteLocalOnlyClasses";
    // Fully qualified Class#method selectors for overriding individual method-level pins.
    private static final String REMOTE_LOCAL_ONLY_METHODS_PROPERTY = "citrus.test.distribution.remoteLocalOnlyMethods";

    // Host-resolved runtime jars or directories, transferred in addition to the test JVM's classpath.
    private static final String ADDITIONAL_WORKER_CLASSPATH_PROPERTY = "citrus.test.distribution.additionalWorkerClasspath";

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
        // Kept here, not on the delegate: results reach them through this launcher exactly once, whether a test ran
        // locally through the delegate (passed per call) or remotely (replayed).
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
        // Only requests explicitly marked as embedded can use a local execution on another thread.
        if (DistributionLauncherInterceptor.isInLocalScope()
                || (DistributionLauncherInterceptor.isExecutingLocally() && testPlan.getConfigurationParameters()
                        .getBoolean(DistributionLauncherInterceptor.EMBEDDED_LAUNCHER_PARAMETER).orElse(false))) {
            executeDelegateLocally(testPlan, callListeners);
            return;
        }

        JacksonClasspathCheck.conflicts(Thread.currentThread().getContextClassLoader())
                .forEach(conflict -> LOG.log(System.Logger.Level.WARNING, "Citrus Test Distribution: {0}", conflict));
        DiscoveredWorkExtractor.ExtractedWork extracted = DiscoveredWorkExtractor.extract(
                testPlan, DistributionSettings.getDefaultTestDurationMs(),
                DiscoveredWorkExtractor.partiallySelectedClasses(delegate, testPlan),
                configuredSelectors(REMOTE_LOCAL_ONLY_CLASSES_PROPERTY),
                configuredSelectors(REMOTE_LOCAL_ONLY_METHODS_PROPERTY));

        if (extracted.remoteUnits().isEmpty() && extracted.localUnits().isEmpty()) {
            executeDelegateLocally(testPlan, callListeners);
            return;
        }

        if (extracted.remoteUnits().isEmpty()) {
            LocalFallbackExecutor.LocalExecutionStats onlyLocal =
                    LocalFallbackExecutor.executeLocally(delegate, extracted.localUnits(),
                            combineListeners(callListeners).toArray(TestExecutionListener[]::new));
            String sharedSessionId = System.getProperty(DistributionSettings.SESSION_ID_PROPERTY);
            if (sharedSessionId != null && !sharedSessionId.isBlank() && !onlyLocal.executions().isEmpty()) {
                try {
                    BrokerHttpClient sharedClient = new BrokerHttpClient(
                            DistributionSettings.getBrokerUrl(),
                            DistributionSettings.getAuthToken(),
                            DistributionSettings.getConnectTimeoutMs());
                    sharedClient.reportLocalTests(
                            sharedSessionId,
                            new ReportLocalTestsRequest("local-host", onlyLocal.executions()));
                } catch (Exception e) {
                    LOG.log(System.Logger.Level.WARNING, "Failed to report @LocalOnly test executions to session {0}: {1}",
                            sharedSessionId, e.getMessage());
                }
            }
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

        Path rootName = workspace.hostBaseDir().getFileName();
        String configuredProjectName = System.getProperty(DistributionSettings.PROJECT_NAME_PROPERTY);
        String projectName = configuredProjectName == null || configuredProjectName.isBlank()
                ? (rootName == null ? "junit-project" : rootName.toString()) : configuredProjectName;
        String projectKey = System.getProperty(DistributionSettings.PROJECT_KEY_PROPERTY);
        if ((projectKey == null || projectKey.isBlank()) && configuredProjectName == null) {
            projectKey = "workspace:" + ClasspathPackager.sha256Hex(
                    workspace.hostBaseDir().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        boolean bypassRemote = shouldBypassRemoteDistribution(extracted, workspace, client,
                DistributionSettings.historyScope(projectKey, projectName));
        if (Thread.currentThread().isInterrupted()) {
            throw new IllegalStateException("Citrus Test Distribution interrupted before test execution");
        }
        if (bypassRemote) {
            List<WorkUnitSpec> units = new ArrayList<>(extracted.localUnits());
            units.addAll(extracted.remoteUnits());
            var stats = LocalFallbackExecutor.executeTestPlanLocally(delegate, testPlan, units,
                    combineListeners(callListeners).toArray(TestExecutionListener[]::new));
            if (!ownsSession && !stats.executions().isEmpty()) {
                try {
                    client.reportLocalTestsWithDeadline(
                            externalSessionId, new ReportLocalTestsRequest("local-host", stats.executions()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.log(System.Logger.Level.WARNING, "Interrupted while reporting local test executions to session {0}", externalSessionId);
                } catch (IOException | RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "Failed to report local test executions to session {0}: {1}", externalSessionId, e.getMessage());
                }
            }
            return;
        }

        String sessionId = externalSessionId;
        try {
            if (ownsSession) {
                BuildSessionResponse created = client.createSession(new CreateSessionRequest(
                        projectName,
                        "local",
                        "HEAD",
                        List.of(),
                        DistributionSettings.findConfiguredMaxPartitionSize().orElse(0),
                        DistributionSettings.getMaxRetries(), List.of(), projectKey));
                sessionId = created.sessionId();
            }
        } catch (Exception e) {
            if (extracted.hasRemoteOnlyUnits() || !fallbackLocal) {
                throw new IllegalStateException(
                        "Citrus Test Distribution broker unreachable at " + brokerUrl
                                + " and remote execution is required", e);
            }
            LOG.log(System.Logger.Level.WARNING, "Broker {0} unreachable ({1}); falling back to local execution", brokerUrl, e.getMessage());
            List<WorkUnitSpec> all = new ArrayList<>(extracted.localUnits());
            all.addAll(extracted.remoteUnits());
            LocalFallbackExecutor.executeLocally(delegate, all,
                    combineListeners(callListeners).toArray(TestExecutionListener[]::new));
            return;
        }

        List<TestExecutionListener> allListeners = combineListeners(callListeners);
        SurefireReportWriter reportWriter = new SurefireReportWriter();

        // Pinned units run below, once the remote partitions are submitted: they execute while the workers do, and
        // their events reach the listeners live through the replayer, sharing class test sets with remote results.
        LocalFallbackExecutor.LocalExecutionStats localStats = new LocalFallbackExecutor.LocalExecutionStats(0, 0, 0, 0L);

        boolean sessionCompleted = false;
        LocalFallbackExecutor.LocalExecutionStats lostStats =
                new LocalFallbackExecutor.LocalExecutionStats(0, 0, 0, 0L);

        try {
            PackagedClasspath packaged = packageCurrentClasspath(workspace.additionalClasspath());
            Map<String, ClasspathPackager.PackagedArtifact> uploadable = new LinkedHashMap<>(packaged.byDigest());
            List<WorkspaceFileSet> inputs = new ArrayList<>(workspace.inputArtifacts());
            List<WorkUnitSpec> remoteUnits = extracted.remoteUnits();
            Map<String, String> properties = PathRewriter.rewriteHostProperties(
                    collectPropagatedProperties(), workspace.hostBaseDir());
            if (workspace.quarkusModel() != null) {
                QuarkusBootstrapInputs quarkus = QuarkusBootstrapInputs.prepare(workspace.quarkusModel(),
                        workspace.hostBaseDir(), packaged.paths(), workspace::launcherCache, inputs);
                inputs.addAll(quarkus.inputs());
                properties.put(QuarkusWorkspace.PROPERTY, DistributionJson.toJson(quarkus.layout()));
                remoteUnits = remoteUnits.stream().map(QuarkusBootstrapInputs::requireCompatibleWorker).toList();
            }
            for (WorkspaceFileSet input : inputs) {
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
                    properties,
                    remoteUnits,
                    cachedCount,
                    uploadedCount,
                    inputs,
                    workspace.outputPaths(),
                    workspace.forkWorkspace(),
                    workspace.partitionPolicy(),
                    workspace.workingDirectory()));

            Set<String> submittedPartitions = Set.copyOf(submitted.partitionIds());
            Set<String> pendingPartitions = new HashSet<>(submitted.partitionIds());
            List<WorkUnitSpec> lostUnits = new ArrayList<>();
            PartitionOutcomes outcomes = new PartitionOutcomes(new HostOutputCommitter(workspace.hostBaseDir(), workspace.hostBuildDir(),
                    workspace.outputPaths(), workspace.cacheExclusion()), workspace.cache());
            Map<String, WorkUnitSpec> unitsByKey = new LinkedHashMap<>();
            extracted.remoteUnits().forEach(unit -> unitsByKey.putIfAbsent(unit.key(), unit));
            List<WorkUnitSpec> expectedUnits = new ArrayList<>(extracted.localUnits());
            expectedUnits.addAll(extracted.remoteUnits());
            RemoteResultReplayer replayer = new RemoteResultReplayer(testPlan, allListeners, expectedUnits);
            try {
                notifyTestPlanExecutionStarted(allListeners, testPlan);
                try {
                    localStats = LocalFallbackExecutor.executeLocally(delegate, extracted.localUnits(),
                            new TestExecutionListener[] {replayer.liveListener()});
                    replayer.localUnitsDone(keysOf(extracted.localUnits()));
                    if (!localStats.executions().isEmpty()) {
                        try {
                            client.reportLocalTests(sessionId, new ReportLocalTestsRequest("local-host", localStats.executions()));
                        } catch (Exception e) {
                            LOG.log(System.Logger.Level.WARNING, "Failed to report @LocalOnly test executions to session {0}: {1}", sessionId, e.getMessage());
                        }
                    }
                    if (!pendingPartitions.isEmpty()) {
                        client.streamSessionEvents(
                                sessionId,
                                0L,
                                DistributionSettings.getSessionTimeoutMs(),
                                event -> handleBrokerEvent(event, replayer, reportWriter, submittedPartitions,
                                        pendingPartitions, lostUnits, unitsByKey, outcomes, client));
                    }
                    if (!lostUnits.isEmpty()) {
                        if (!fallbackLocal || extracted.hasRemoteOnlyUnits()) {
                            throw new IllegalStateException("Remote partitions were lost: " + lostUnits.size());
                        }
                        LOG.log(System.Logger.Level.WARNING, "Executing {0} lost remote work unit(s) locally", lostUnits.size());
                        lostStats = LocalFallbackExecutor.executeLocally(delegate, lostUnits,
                                new TestExecutionListener[] {replayer.liveListener()});
                        replayer.localUnitsDone(keysOf(lostUnits));
                        if (!lostStats.executions().isEmpty()) {
                            try {
                                client.reportLocalTests(
                                        sessionId,
                                        new ReportLocalTestsRequest("local-fallback", lostStats.executions()));
                            } catch (Exception e) {
                                LOG.log(System.Logger.Level.WARNING, "Failed to report fallback test executions to session {0}: {1}",
                                        sessionId, e.getMessage());
                            }
                        }
                    }
                } finally {
                    replayer.finishStartedContainers();
                    notifyTestPlanExecutionFinished(allListeners, testPlan);
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
                            localStats.serialDurationMs() + lostStats.serialDurationMs(),
                            outputFailure != null,
                            outputFailure != null ? describe(outputFailure) : null));
                    sessionCompleted = true;
                }
                if (outputFailure != null) {
                    throw outputFailure;
                }
            } finally {
                // Every abort path (lost partitions, stream errors, failures in the event handler) drops staged outputs.
                outcomes.committer().discardStaging();
            }
        } catch (RuntimeException | Error re) {
            if (ownsSession && !sessionCompleted) {
                completeOwnedSessionOnFailure(client, sessionId, localStats, lostStats, re);
            }
            throw re;
        } catch (Exception e) {
            if (ownsSession && !sessionCompleted) {
                completeOwnedSessionOnFailure(client, sessionId, localStats, lostStats, e);
            }
            throw new IllegalStateException("Distributed test execution failed for session " + sessionId, e);
        }
    }

    private void executeDelegateLocally(TestPlan testPlan, TestExecutionListener[] callListeners) {
        var listeners = combineListeners(callListeners).toArray(TestExecutionListener[]::new);
        DistributionLauncherInterceptor.runLocally(listeners, scoped -> delegate.execute(testPlan, scoped));
    }

    private static boolean shouldBypassRemoteDistribution(DiscoveredWorkExtractor.ExtractedWork extracted,
                                                          WorkspaceSettings workspace, BrokerHttpClient client,
                                                          String historyScope) {
        if (extracted.hasRemoteOnlyUnits() || workspace.forkWorkspace()
                || extracted.remoteUnits().stream().anyMatch(unit ->
                        unit.requiredCapabilities() != null && !unit.requiredCapabilities().isEmpty())) {
            return false;
        }

        long minRemoteDurationMs = DistributionSettings.getMinRemoteDurationMs();
        int minRemoteColdUnits = DistributionSettings.getMinRemoteColdUnits();
        if (minRemoteDurationMs == 0 && minRemoteColdUnits == 0) {
            return false;
        }

        List<String> remoteKeys = extracted.remoteUnits().stream().map(WorkUnitSpec::key).toList();
        Optional<Map<String, Long>> history = minRemoteDurationMs > 0
                ? client.findHistoricalDurations(remoteKeys, 10, historyScope) : Optional.empty();
        if (history.isPresent()) {
            Map<String, Long> durations = history.get();
            if (remoteKeys.stream().allMatch(key -> durations.get(key) != null && durations.get(key) >= 0)) {
                long remaining = minRemoteDurationMs;
                for (String key : remoteKeys) {
                    long duration = durations.get(key);
                    if (duration >= remaining) {
                        return false;
                    }
                    remaining -= duration;
                }
                return true;
            }
        }
        return minRemoteColdUnits > 0 && remoteKeys.size() <= minRemoteColdUnits;
    }

    private static void completeOwnedSessionOnFailure(
            BrokerHttpClient client,
            String sessionId,
            LocalFallbackExecutor.LocalExecutionStats localStats,
            LocalFallbackExecutor.LocalExecutionStats lostStats,
            Throwable cause) {
        try {
            client.completeSession(sessionId, new CompleteSessionRequest(
                    localStats.passed() + lostStats.passed(),
                    localStats.failed() + lostStats.failed(),
                    localStats.skipped() + lostStats.skipped(),
                    localStats.serialDurationMs() + lostStats.serialDurationMs(),
                    true,
                    describe(cause)));
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "Failed to mark failed session {0} as completed on broker: {1}", sessionId, e.getMessage());
        }
    }

    /** {@code <exception class>: <message>} of {@code cause}, falling back to the first cause that has a message. */
    static String describe(Throwable cause) {
        java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (Throwable cur = cause; cur != null && seen.add(cur); cur = cur.getCause()) {
            if (cur.getMessage() != null && !cur.getMessage().isBlank()) {
                return cur.getClass().getName() + ": " + cur.getMessage();
            }
        }
        return cause != null ? cause.getClass().getName() : null;
    }

    /**
     * Handles one session event. Events of partitions this launcher did not submit (sibling modules sharing a reactor
     * session) are ignored, and each partition's completion is processed once even if replayed.
     */
    private boolean handleBrokerEvent(BrokerEvent event,
                                      RemoteResultReplayer replayer,
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
                    replayer.buffer(DistributionJson.convert(event.data(), WorkerTestEventPayload.class));
                }
            }
            case "partition.completed" -> {
                if (ownEvent && pendingPartitions.remove(event.partitionId())) {
                    // Only the attempt that completed the partition is reported, once, with its final outcomes.
                    replayer.complete(event.partitionId(), attemptOf(event), unitKeysOf(event)).forEach(reportWriter::record);
                    outcomes.completed(event, client);
                }
            }
            case "partition.requeued" -> {
                if (ownEvent) {
                    replayer.abandon(event.partitionId());
                }
            }
            case "partition.lost" -> {
                if (!ownEvent || !pendingPartitions.remove(event.partitionId())) {
                    return pendingPartitions.isEmpty();
                }
                // Lost units re-run locally and report themselves; the remote attempt's partial results are dropped.
                replayer.abandon(event.partitionId());
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

    private static List<String> keysOf(List<WorkUnitSpec> units) {
        return units.stream().map(WorkUnitSpec::key).toList();
    }

    /** Work unit keys a {@code partition.completed} event lists, or {@code null} for coordinators that do not send them. */
    private static List<String> unitKeysOf(BrokerEvent event) {
        Object units = event.data() != null ? event.data().get("workUnits") : null;
        if (!(units instanceof List<?> list)) {
            return null;
        }
        return list.stream().map(String::valueOf).toList();
    }

    private static int attemptOf(BrokerEvent event) {
        Object attempt = event.data() != null ? event.data().get("attempt") : null;
        return attempt instanceof Number n ? n.intValue() : 1;
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
            List<Path> paths,
            List<ArtifactRef> artifacts,
            Map<String, ClasspathPackager.PackagedArtifact> byDigest) {
    }

    private static Set<String> configuredSelectors(String property) {
        // A method signature can itself contain commas between parameter types.
        return Arrays.stream(System.getProperty(property, "").split(",(?![^()]*\\))"))
                .map(String::trim)
                .filter(selector -> !selector.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static List<Path> additionalWorkerClasspath() {
        List<Path> paths = new ArrayList<>();
        for (String entry : System.getProperty(ADDITIONAL_WORKER_CLASSPATH_PROPERTY, "").split(File.pathSeparator)) {
            if (entry.isBlank()) {
                continue;
            }
            Path path = Path.of(entry).toAbsolutePath().normalize();
            if (!Files.isDirectory(path) && !(Files.isRegularFile(path) && path.toString().endsWith(".jar"))) {
                throw new IllegalArgumentException("Invalid classpath entry in "
                        + ADDITIONAL_WORKER_CLASSPATH_PROPERTY + ": " + path);
            }
            paths.add(path);
        }
        return List.copyOf(paths);
    }

    private static PackagedClasspath packageCurrentClasspath(List<Path> additionalWorkerClasspath) throws IOException {
        String cp = System.getProperty("java.class.path", "");
        Set<Path> classpath = new LinkedHashSet<>();
        for (String entry : cp.split(File.pathSeparator)) {
            if (!entry.isBlank()) {
                Path path = Path.of(entry).toAbsolutePath().normalize();
                if (Files.exists(path)) {
                    classpath.add(path);
                }
            }
        }
        classpath.addAll(additionalWorkerClasspath);

        List<ArtifactRef> artifacts = new ArrayList<>();
        Map<String, ClasspathPackager.PackagedArtifact> byDigest = new LinkedHashMap<>();
        for (Path path : classpath) {
            ClasspathPackager.PackagedArtifact packaged = ClasspathPackager.packagePath(path);
            artifacts.add(packaged.ref());
            byDigest.putIfAbsent(packaged.ref().sha256(), packaged);
        }
        return new PackagedClasspath(List.copyOf(classpath), artifacts, byDigest);
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
        if (HOST_SYSTEM_PROPERTY_KEYS.contains(key) || WorkspaceLease.TICKET_PROPERTY.equals(key)) {
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
     * host cache), additional worker classpath, declared outputs, fork flag and partition policy.
     */
    record WorkspaceSettings(Path hostBaseDir, Path hostBuildDir, BlobCacheDirectory cache, List<WorkspaceFileSet> inputArtifacts,
                             List<String> outputPaths, boolean forkWorkspace, PartitionPolicy partitionPolicy,
                             List<Path> additionalClasspath, Path quarkusModel, String workingDirectory) {

        /**
         * {@code userDir} relative to {@code hostBaseDir} ({@code /}-separated), or empty when it is the module directory
         * or lies outside it. Forked workers reproduce the working directory inside the workspace.
         */
        static String relativeWorkingDirectory(Path hostBaseDir, Path userDir) {
            Path base = hostBaseDir.toAbsolutePath().normalize();
            Path cwd = userDir.toAbsolutePath().normalize();
            if (!cwd.startsWith(base)) {
                // The two may differ only by a symbolic link (e.g. /var vs /private/var on macOS).
                try {
                    base = base.toRealPath();
                    cwd = cwd.toRealPath();
                } catch (IOException | SecurityException e) {
                    return "";
                }
            }
            if (!cwd.startsWith(base) || cwd.equals(base)) {
                return "";
            }
            return base.relativize(cwd).toString().replace('\\', '/');
        }

        /**
         * The host cache directory relative to the module, when it lies inside it, so remote outputs cannot target it.
         */
        List<String> cacheExclusion() {
            return cacheExclusion(hostBaseDir, cache);
        }

        private static List<String> cacheExclusion(Path hostBaseDir, BlobCacheDirectory cache) {
            Path cacheRoot = cache.getRootDir();
            return cacheRoot.startsWith(hostBaseDir) && !cacheRoot.equals(hostBaseDir)
                    ? List.of(hostBaseDir.relativize(cacheRoot).toString().replace('\\', '/'))
                    : List.of();
        }

        BlobCacheDirectory launcherCache() {
            markLauncherOwned(cache);
            return cache;
        }

        static WorkspaceSettings resolve() {
            List<Path> additionalClasspath = additionalWorkerClasspath();
            Path hostBaseDir = resolveHostBaseDir();
            String buildSetting = setting(DistributionSettings.BUILD_DIRECTORY_PROPERTY, null);
            Path hostBuildDir = hostBaseDir.resolve(buildSetting == null ? "target" : buildSetting).normalize();
            String cacheDir = setting(DistributionSettings.CACHE_DIR_PROPERTY, DistributionSettings.CACHE_DIR_ENV);
            BlobCacheDirectory cache = new BlobCacheDirectory(hostBuildDir.resolve(
                    cacheDir == null ? ".citrus-dist/cache" : cacheDir));
            List<String> internal = new ArrayList<>(List.of("target/.citrus-dist-outputs", "target/.citrus-dist"));
            internal.add(hostBaseDir.relativize(HostOutputCommitter.outputsDirectory(hostBaseDir, hostBuildDir))
                    .toString().replace('\\', '/'));
            if (hostBuildDir.startsWith(hostBaseDir)) {
                internal.add(hostBaseDir.relativize(hostBuildDir.resolve(".citrus-dist")).toString().replace('\\', '/'));
            }
            internal.addAll(cacheExclusion(hostBaseDir, cache));

            List<DistributionSettings.DeclaredPathSpec> declaredInputs = DistributionSettings.parseDeclaredPaths(
                    setting(DistributionSettings.INPUTS_PROPERTY, DistributionSettings.INPUTS_ENV));
            List<WorkspaceFileSet> inputs = new ArrayList<>();
            for (DistributionSettings.DeclaredPathSpec input : declaredInputs) {
                try {
                    WorkspacePackager.packageInputToCacheLazily(() -> {
                        markLauncherOwned(cache);
                        return cache;
                    }, hostBaseDir, input.relativePath(), input.optional(), internal).ifPresent(inputs::add);
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
                Path link = BlobCacheDirectory.findSymlinkComponent(hostBaseDir, hostBaseDir.resolve(output));
                if (link != null) {
                    throw new IllegalArgumentException("Declared output '" + output
                            + "' passes through a symbolic link or junction: " + link);
                }
            }
            if (!outputs.isEmpty()) {
                markLauncherOwned(cache);
            }
            boolean fork = DistributionSettings.parseForkWorkspace(
                    setting(DistributionSettings.FORK_PROPERTY, DistributionSettings.FORK_ENV));
            PartitionPolicy policy = DistributionSettings.parsePartitionPolicy(
                    setting(DistributionSettings.PARTITION_POLICY_PROPERTY, DistributionSettings.PARTITION_POLICY_ENV));
            Path quarkusModel = QuarkusBootstrapInputs.findModel(hostBaseDir);
            // Inputs and the Quarkus model need a private workspace. Extra classpath entries need a JVM classpath.
            return new WorkspaceSettings(hostBaseDir, hostBuildDir, cache, List.copyOf(inputs), outputs,
                    fork || quarkusModel != null || !additionalClasspath.isEmpty() || !declaredInputs.isEmpty()
                            || !outputs.isEmpty(), policy, additionalClasspath, quarkusModel,
                    relativeWorkingDirectory(hostBaseDir, Path.of(System.getProperty("user.dir", ""))));
        }

        /**
         * Claims the host cache before using it, so it cannot be shared with a worker.
         */
        private static void markLauncherOwned(BlobCacheDirectory cache) {
            try {
                String owner = BlobCacheDirectory.recordOwnerIfAbsent(cache.getRootDir(), BlobCacheDirectory.OWNER_LAUNCHER);
                if (!BlobCacheDirectory.OWNER_LAUNCHER.equals(owner) && !BlobCacheDirectory.OWNER_COORDINATOR.equals(owner)) {
                    throw new IllegalStateException("Host cache directory " + cache.getRootDir()
                            + " is marked as owned by " + owner + ". Choose a separate launcher cache directory.");
                }
            } catch (IOException e) {
                throw new java.io.UncheckedIOException("Could not mark host cache " + cache.getRootDir()
                        + " as owned by the launcher", e);
            }
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
         * The value of a setting: the system property when set to a non-blank value, otherwise the environment
         * variable. Blank values and unresolved {@code ${...}} placeholders count as unset (the root POM passes the
         * properties through to Surefire even when undefined); {@code none} clears a path list explicitly, and the
         * Maven extension writes the resolved winner, including {@code none} for a cleared list.
         */
        static String setting(String property, String env) {
            String value = System.getProperty(property);
            if (value != null && !value.isBlank() && !isPlaceholder(value.trim())) {
                return value;
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
}
