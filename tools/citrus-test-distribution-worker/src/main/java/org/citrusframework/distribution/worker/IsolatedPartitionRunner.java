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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
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
    private static final Object WORKER_JVM_LOCK = new Object();
    private static final java.util.concurrent.atomic.AtomicInteger ACTIVE_WORKER_SCOPES = new java.util.concurrent.atomic.AtomicInteger();

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
        ThreadScopedProperties systemProps = null;
        Properties previousScope = null;
        boolean workerScopeEntered = false;

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

            synchronized (WORKER_JVM_LOCK) {
                systemProps = installThreadScopedProperties();
                ACTIVE_WORKER_SCOPES.incrementAndGet();
                workerScopeEntered = true;
                systemProps.base().setProperty(DistributionSettings.ROLE_PROPERTY, DistributionSettings.ROLE_WORKER);
                previousScope = systemProps.enterScope(assignment.systemProperties());
            }

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
            if (systemProps != null) {
                systemProps.exitScope(previousScope);
            }
            if (workerScopeEntered) {
                synchronized (WORKER_JVM_LOCK) {
                    if (ACTIVE_WORKER_SCOPES.decrementAndGet() <= 0) {
                        ACTIVE_WORKER_SCOPES.set(0);
                        systemProps.base().remove(DistributionSettings.ROLE_PROPERTY);
                    }
                }
            }
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

    private static ThreadScopedProperties installThreadScopedProperties() {
        Properties current = System.getProperties();
        if (current instanceof ThreadScopedProperties existing) {
            return existing;
        }
        ThreadScopedProperties installed = new ThreadScopedProperties(current);
        System.setProperties(installed);
        return installed;
    }

    /**
     * Thread-scoped {@link Properties} wrapper delegating lookups and mutations to an
     * {@link InheritableThreadLocal} overlay while a partition executes on the current thread
     * (or its child threads), falling back to the base JVM {@link Properties} otherwise.
     */
    static final class ThreadScopedProperties extends Properties {

        private final Properties base;
        private final InheritableThreadLocal<Properties> overlay = new InheritableThreadLocal<>();

        ThreadScopedProperties(Properties base) {
            this.base = Objects.requireNonNull(base, "base properties");
        }

        Properties base() {
            return base;
        }

        Properties enterScope(Map<String, String> partitionOverrides) {
            Properties previous = overlay.get();
            Properties snapshot = new Properties();
            for (String name : base.stringPropertyNames()) {
                String val = base.getProperty(name);
                if (val != null) {
                    snapshot.setProperty(name, val);
                }
            }
            snapshot.putAll(base);
            if (partitionOverrides != null) {
                for (Map.Entry<String, String> entry : partitionOverrides.entrySet()) {
                    String key = entry.getKey();
                    if (key == null || key.startsWith("java.") || key.startsWith("os.") || key.startsWith("user.dir")) {
                        continue;
                    }
                    if (entry.getValue() == null) {
                        snapshot.remove(key);
                    } else {
                        snapshot.setProperty(key, entry.getValue());
                    }
                }
            }
            snapshot.setProperty(DistributionSettings.ROLE_PROPERTY, DistributionSettings.ROLE_WORKER);
            overlay.set(snapshot);
            return previous;
        }

        void exitScope(Properties previous) {
            if (previous == null) {
                overlay.remove();
            } else {
                overlay.set(previous);
            }
        }

        private Properties current() {
            Properties scoped = overlay.get();
            return scoped != null ? scoped : base;
        }

        @Override
        public Object setProperty(String key, String value) {
            return current().setProperty(key, value);
        }

        @Override
        public void load(Reader reader) throws IOException {
            current().load(reader);
        }

        @Override
        public void load(InputStream inStream) throws IOException {
            current().load(inStream);
        }

        @Override
        @Deprecated
        public void save(OutputStream out, String comments) {
            current().save(out, comments);
        }

        @Override
        public void store(Writer writer, String comments) throws IOException {
            current().store(writer, comments);
        }

        @Override
        public void store(OutputStream out, String comments) throws IOException {
            current().store(out, comments);
        }

        @Override
        public void loadFromXML(InputStream in) throws IOException {
            current().loadFromXML(in);
        }

        @Override
        public void storeToXML(OutputStream os, String comment) throws IOException {
            current().storeToXML(os, comment);
        }

        @Override
        public void storeToXML(OutputStream os, String comment, String encoding) throws IOException {
            current().storeToXML(os, comment, encoding);
        }

        @Override
        public void storeToXML(OutputStream os, String comment, Charset charset) throws IOException {
            current().storeToXML(os, comment, charset);
        }

        @Override
        public String getProperty(String key) {
            return current().getProperty(key);
        }

        @Override
        public String getProperty(String key, String defaultValue) {
            return current().getProperty(key, defaultValue);
        }

        @Override
        public Enumeration<?> propertyNames() {
            return current().propertyNames();
        }

        @Override
        public Set<String> stringPropertyNames() {
            return current().stringPropertyNames();
        }

        @Override
        public void list(PrintStream out) {
            current().list(out);
        }

        @Override
        public void list(PrintWriter out) {
            current().list(out);
        }

        @Override
        public int size() {
            return current().size();
        }

        @Override
        public boolean isEmpty() {
            return current().isEmpty();
        }

        @Override
        public Enumeration<Object> keys() {
            return current().keys();
        }

        @Override
        public Enumeration<Object> elements() {
            return current().elements();
        }

        @Override
        public boolean contains(Object value) {
            return current().contains(value);
        }

        @Override
        public boolean containsValue(Object value) {
            return current().containsValue(value);
        }

        @Override
        public boolean containsKey(Object key) {
            return current().containsKey(key);
        }

        @Override
        public Object get(Object key) {
            return current().get(key);
        }

        @Override
        public Object put(Object key, Object value) {
            return current().put(key, value);
        }

        @Override
        public Object remove(Object key) {
            return current().remove(key);
        }

        @Override
        public void putAll(Map<?, ?> t) {
            current().putAll(t);
        }

        @Override
        public void clear() {
            current().clear();
        }

        @Override
        public String toString() {
            return current().toString();
        }

        @Override
        public Set<Object> keySet() {
            return current().keySet();
        }

        @Override
        public Collection<Object> values() {
            return current().values();
        }

        @Override
        public Set<Map.Entry<Object, Object>> entrySet() {
            return current().entrySet();
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            return current().equals(o);
        }

        @Override
        public int hashCode() {
            return current().hashCode();
        }

        @Override
        public Object getOrDefault(Object key, Object defaultValue) {
            return current().getOrDefault(key, defaultValue);
        }

        @Override
        public void forEach(BiConsumer<? super Object, ? super Object> action) {
            current().forEach(action);
        }

        @Override
        public void replaceAll(BiFunction<? super Object, ? super Object, ?> function) {
            current().replaceAll(function);
        }

        @Override
        public Object putIfAbsent(Object key, Object value) {
            return current().putIfAbsent(key, value);
        }

        @Override
        public boolean remove(Object key, Object value) {
            return current().remove(key, value);
        }

        @Override
        public boolean replace(Object key, Object oldValue, Object newValue) {
            return current().replace(key, oldValue, newValue);
        }

        @Override
        public Object replace(Object key, Object value) {
            return current().replace(key, value);
        }

        @Override
        public Object computeIfAbsent(Object key, Function<? super Object, ?> mappingFunction) {
            return current().computeIfAbsent(key, mappingFunction);
        }

        @Override
        public Object computeIfPresent(Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
            return current().computeIfPresent(key, remappingFunction);
        }

        @Override
        public Object compute(Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
            return current().compute(key, remappingFunction);
        }

        @Override
        public Object merge(Object key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
            return current().merge(key, value, remappingFunction);
        }

        @Override
        public Object clone() {
            return current().clone();
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
