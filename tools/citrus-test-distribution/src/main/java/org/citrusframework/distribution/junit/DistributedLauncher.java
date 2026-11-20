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

import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.BrokerEvent;
import org.citrusframework.distribution.model.BuildSessionResponse;
import org.citrusframework.distribution.model.CompleteSessionRequest;
import org.citrusframework.distribution.model.CreateSessionRequest;
import org.citrusframework.distribution.model.SubmitWorkRequest;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
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
            List<String> allDigests = packaged.artifacts().stream().map(ArtifactRef::sha256).distinct().toList();
            List<String> missing = client.findMissingDigests(allDigests);
            Set<String> missingSet = new HashSet<>(missing);

            for (String sha : missing) {
                ClasspathPackager.PackagedArtifact artifact = packaged.byDigest().get(sha);
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
                    collectPropagatedProperties(),
                    extracted.remoteUnits(),
                    cachedCount,
                    uploadedCount));

            Set<String> pendingPartitions = new HashSet<>(submitted.partitionIds());
            List<WorkUnitSpec> lostUnits = new ArrayList<>();

            notifyTestPlanExecutionStarted(allListeners, testPlan);
            try {
                if (!pendingPartitions.isEmpty()) {
                    client.streamSessionEvents(
                            sessionId,
                            0L,
                            DistributionSettings.getSessionTimeoutMs(),
                            event -> handleBrokerEvent(
                                    event, testPlan, allListeners, reportWriter, pendingPartitions, lostUnits));
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

            if (ownsSession) {
                client.completeSession(sessionId, new CompleteSessionRequest(
                        localStats.passed() + lostStats.passed(),
                        localStats.failed() + lostStats.failed(),
                        localStats.skipped() + lostStats.skipped(),
                        localStats.serialDurationMs() + lostStats.serialDurationMs()));
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("Distributed test execution failed for session " + sessionId, e);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean handleBrokerEvent(BrokerEvent event,
                                      TestPlan testPlan,
                                      List<TestExecutionListener> listeners,
                                      SurefireReportWriter reportWriter,
                                      Set<String> pendingPartitions,
                                      List<WorkUnitSpec> lostUnits) {
        switch (event.type()) {
            case "test.event" -> {
                WorkerTestEventPayload payload = DistributionJson.convert(event.data(), WorkerTestEventPayload.class);
                reportWriter.record(payload);
                replayToJUnitListeners(testPlan, listeners, payload);
            }
            case "partition.completed" -> {
                if (event.partitionId() != null) {
                    pendingPartitions.remove(event.partitionId());
                }
            }
            case "partition.lost" -> {
                if (event.partitionId() != null) {
                    pendingPartitions.remove(event.partitionId());
                }
                if (event.data() != null && event.data().get("workUnits") instanceof List<?> rawUnits) {
                    for (Object item : rawUnits) {
                        lostUnits.add(DistributionJson.convert(item, WorkUnitSpec.class));
                    }
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
