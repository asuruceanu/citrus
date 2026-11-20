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

package org.citrusframework.distribution.worker;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.PartitionCompletedPayload;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.junit.platform.engine.CancellationToken;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.LauncherExecutionRequest;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherExecutionRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads missing content-addressed blobs, materializes an isolated partition workspace and
 * child-first {@link URLClassLoader}, and executes assigned JUnit Platform work units.
 */
public final class IsolatedPartitionRunner {

    private static final Logger LOG = LoggerFactory.getLogger(IsolatedPartitionRunner.class);

    private final DistributionWorkerConfig config;
    private final BlobCacheDirectory blobCache;
    private final HttpClient httpClient;

    public IsolatedPartitionRunner(DistributionWorkerConfig config) {
        this.config = config;
        this.blobCache = new BlobCacheDirectory(config.cacheDir());
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public PartitionCompletedPayload execute(
            PartitionAssignPayload assignment,
            CancellationToken cancellationToken,
            Consumer<WorkerTestEventPayload> eventSink) {
        Path workspaceDir = null;
        ClassLoader previousCl = Thread.currentThread().getContextClassLoader();
        Map<String, String> previousProps = new HashMap<>();

        try {
            workspaceDir = Files.createTempDirectory("citrus-dist-partition-" + assignment.partitionId() + "-");
            List<URL> urls = new ArrayList<>();
            if (assignment.classpathArtifacts() != null) {
                for (ArtifactRef artifact : assignment.classpathArtifacts()) {
                    ensureBlobCached(artifact.sha256());
                    Path materialized = blobCache.materializeForClasspath(artifact, workspaceDir);
                    urls.add(materialized.toUri().toURL());
                }
            }

            applySystemProperties(assignment.systemProperties(), previousProps);
            setWithBackup(DistributionSettings.ROLE_PROPERTY, DistributionSettings.ROLE_WORKER, previousProps);

            ClassLoader parentCl = IsolatedPartitionRunner.class.getClassLoader();
            try (URLClassLoader partitionCl = urls.isEmpty()
                    ? new URLClassLoader(new URL[0], parentCl)
                    : new PartitionClassLoader(urls.toArray(new URL[0]), parentCl)) {
                Thread.currentThread().setContextClassLoader(partitionCl);
                resetCitrusExtensionIfPresent(partitionCl);

                for (WorkUnitSpec unit : assignment.workUnits()) {
                    if (cancellationToken != null && cancellationToken.isCancellationRequested()) {
                        break;
                    }
                    executeWorkUnit(assignment, unit, partitionCl, cancellationToken, eventSink);
                }
            }

            return new PartitionCompletedPayload(assignment.partitionId(), assignment.sessionId(), true, null);
        } catch (Throwable t) {
            LOG.warn("Partition {} failed during execution", assignment.partitionId(), t);
            return new PartitionCompletedPayload(
                    assignment.partitionId(),
                    assignment.sessionId(),
                    false,
                    t.getMessage() != null ? t.getMessage() : t.getClass().getName());
        } finally {
            Thread.currentThread().setContextClassLoader(previousCl);
            restoreSystemProperties(previousProps);
            deleteRecursivelyQuietly(workspaceDir);
        }
    }

    private void executeWorkUnit(
            PartitionAssignPayload assignment,
            WorkUnitSpec unit,
            ClassLoader partitionCl,
            CancellationToken cancellationToken,
            Consumer<WorkerTestEventPayload> eventSink) throws Exception {
        Class<?> testClass = Class.forName(unit.className(), false, partitionCl);
        List<DiscoverySelector> selectors = new ArrayList<>();
        if (unit.methodName() != null && !unit.methodName().isBlank() && unit.dynamicSourceName() == null) {
            selectors.add(selectMethodCompatible(testClass, unit.methodName()));
        } else {
            selectors.add(DiscoverySelectors.selectClass(testClass));
        }

        Set<String> allowedDynamicSources = null;
        if (unit.dynamicSourceName() != null && !unit.dynamicSourceName().isBlank()) {
            allowedDynamicSources = Set.of(unit.dynamicSourceName());
        }
        setDynamicSourceFilterIfPresent(partitionCl, allowedDynamicSources);

        try (LauncherSession session = LauncherFactory.openSession()) {
            LauncherDiscoveryRequest discoveryRequest = LauncherDiscoveryRequestBuilder.request()
                    .selectors(selectors)
                    .configurationParameter("junit.platform.launcher.interceptors.enabled", "false")
                    .build();

            Map<String, Long> startTimes = new ConcurrentHashMap<>();
            TestExecutionListener listener = new TestExecutionListener() {
                @Override
                public void executionStarted(TestIdentifier testIdentifier) {
                    if (testIdentifier.isTest()) {
                        startTimes.put(testIdentifier.getUniqueId(), System.currentTimeMillis());
                        eventSink.accept(new WorkerTestEventPayload(
                                assignment.partitionId(),
                                assignment.sessionId(),
                                assignment.attempt(),
                                unit.key(),
                                resolveClassName(testIdentifier, unit.className()),
                                testIdentifier.getDisplayName(),
                                TestOutcomeStatus.STARTED,
                                0L,
                                null,
                                null,
                                List.of()));
                    }
                }

                @Override
                public void executionSkipped(TestIdentifier testIdentifier, String reason) {
                    if (testIdentifier.isTest()) {
                        eventSink.accept(new WorkerTestEventPayload(
                                assignment.partitionId(),
                                assignment.sessionId(),
                                assignment.attempt(),
                                unit.key(),
                                resolveClassName(testIdentifier, unit.className()),
                                testIdentifier.getDisplayName(),
                                TestOutcomeStatus.SKIPPED,
                                0L,
                                reason,
                                null,
                                List.of()));
                    }
                }

                @Override
                public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult result) {
                    if (!testIdentifier.isTest()) {
                        return;
                    }
                    long started = startTimes.getOrDefault(testIdentifier.getUniqueId(), System.currentTimeMillis());
                    long durationMs = Math.max(0L, System.currentTimeMillis() - started);
                    TestOutcomeStatus status = switch (result.getStatus()) {
                        case SUCCESSFUL -> TestOutcomeStatus.PASSED;
                        case ABORTED -> TestOutcomeStatus.SKIPPED;
                        case FAILED -> TestOutcomeStatus.FAILED;
                    };
                    String errorMsg = result.getThrowable().map(Throwable::getMessage).orElse(null);
                    String stackTrace = result.getThrowable().map(IsolatedPartitionRunner::formatStackTrace).orElse(null);

                    eventSink.accept(new WorkerTestEventPayload(
                            assignment.partitionId(),
                            assignment.sessionId(),
                            assignment.attempt(),
                            unit.key(),
                            resolveClassName(testIdentifier, unit.className()),
                            testIdentifier.getDisplayName(),
                            status,
                            durationMs,
                            errorMsg,
                            stackTrace,
                            List.of()));
                }
            };

            LauncherExecutionRequestBuilder execBuilder = LauncherExecutionRequestBuilder.request(discoveryRequest)
                    .listeners(listener);
            if (cancellationToken != null) {
                execBuilder.cancellationToken(cancellationToken);
            }
            LauncherExecutionRequest executionRequest = execBuilder.build();
            session.getLauncher().execute(executionRequest);
        } finally {
            setDynamicSourceFilterIfPresent(partitionCl, null);
        }
    }

    private DiscoverySelector selectMethodCompatible(Class<?> testClass, String methodName) {
        for (Method method : testClass.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                return DiscoverySelectors.selectMethod(testClass, method);
            }
        }
        return DiscoverySelectors.selectMethod(testClass, methodName);
    }

    private void ensureBlobCached(String sha256) throws IOException, InterruptedException {
        if (blobCache.contains(sha256)) {
            return;
        }
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder(
                        URI.create(config.brokerUrl() + "/api/v1/cache/blobs/" + sha256))
                .timeout(Duration.ofSeconds(60))
                .GET();
        if (config.authToken() != null && !config.authToken().isBlank()) {
            reqBuilder.header("Authorization", "Bearer " + config.authToken());
        }
        HttpResponse<byte[]> response = httpClient.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() != 200) {
            throw new IOException("Failed to download blob " + sha256 + " from broker: HTTP " + response.statusCode());
        }
        blobCache.put(sha256, response.body());
    }

    private static String resolveClassName(TestIdentifier identifier, String fallback) {
        TestSource source = identifier.getSource().orElse(null);
        if (source instanceof MethodSource ms) {
            return ms.getClassName();
        }
        if (source instanceof ClassSource cs) {
            return cs.getClassName();
        }
        return fallback;
    }

    private static String formatStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private static void resetCitrusExtensionIfPresent(ClassLoader classLoader) {
        try {
            Class<?> citrusExt = Class.forName("org.citrusframework.junit.jupiter.CitrusExtension", false, classLoader);
            Method resetMethod = citrusExt.getDeclaredMethod("reset");
            resetMethod.setAccessible(true);
            resetMethod.invoke(null);
        } catch (Throwable ignored) {
            // Optional if CitrusExtension is not on partition classpath
        }
    }

    private static void setDynamicSourceFilterIfPresent(ClassLoader classLoader, Set<String> allowed) {
        try {
            Class<?> filterClass = Class.forName(
                    "org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter", false, classLoader);
            if (allowed == null) {
                Method clear = filterClass.getMethod("clear");
                clear.invoke(null);
            } else {
                Method setAllowed = filterClass.getMethod("setAllowedSources", Set.class);
                setAllowed.invoke(null, new LinkedHashSet<>(allowed));
            }
        } catch (Throwable ignored) {
            // Optional if CitrusDynamicSourceFilter is not on partition classpath
        }
    }

    private static void applySystemProperties(Map<String, String> props, Map<String, String> backup) {
        if (props == null) {
            return;
        }
        for (Map.Entry<String, String> entry : props.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.startsWith("java.") || key.startsWith("os.") || key.startsWith("user.dir")) {
                continue;
            }
            setWithBackup(key, entry.getValue(), backup);
        }
    }

    private static void setWithBackup(String key, String value, Map<String, String> backup) {
        if (!backup.containsKey(key)) {
            backup.put(key, System.getProperty(key));
        }
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    private static void restoreSystemProperties(Map<String, String> backup) {
        for (Map.Entry<String, String> entry : backup.entrySet()) {
            if (entry.getValue() == null) {
                System.clearProperty(entry.getKey());
            } else {
                System.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }

    private static void deleteRecursivelyQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
    }

    /**
     * Child-first classloader that delegates JDK, JUnit Platform/Jupiter, OpenTest4J, and SLF4J
     * SPI types to the worker's parent classloader while isolating test and application classes.
     */
    private static final class PartitionClassLoader extends URLClassLoader {

        PartitionClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    if (shouldDelegateToParent(name)) {
                        loaded = super.loadClass(name, false);
                    } else {
                        try {
                            loaded = findClass(name);
                        } catch (ClassNotFoundException ignored) {
                            loaded = super.loadClass(name, false);
                        }
                    }
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }

        @Override
        public URL getResource(String name) {
            if (name.startsWith("META-INF/services/org.junit.")) {
                return super.getResource(name);
            }
            URL url = findResource(name);
            return url != null ? url : super.getResource(name);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            if (name.startsWith("META-INF/services/org.junit.")) {
                return super.getResources(name);
            }
            return super.getResources(name);
        }

        private static boolean shouldDelegateToParent(String name) {
            return name.startsWith("java.")
                    || name.startsWith("jdk.")
                    || name.startsWith("javax.")
                    || name.startsWith("sun.")
                    || name.startsWith("org.junit.")
                    || name.startsWith("org.opentest4j.")
                    || name.startsWith("org.apiguardian.")
                    || name.startsWith("org.slf4j.")
                    || name.startsWith("org.apache.logging.log4j.")
                    || name.startsWith("org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter");
        }
    }
}
