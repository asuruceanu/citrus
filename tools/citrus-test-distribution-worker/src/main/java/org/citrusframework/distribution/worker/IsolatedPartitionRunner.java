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
import java.io.UncheckedIOException;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.config.DistributionSettings.WorkerCwdCheckMode;
import org.citrusframework.distribution.config.PathRewriter;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.junit.platform.engine.CancellationToken;
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

    static final String REPORT_DIRECTORY_PROPERTY = "citrus.report.directory";
    static final String MESSAGE_TRACE_DIRECTORY_PROPERTY = "citrus.message.trace.directory";

    private final DistributionWorkerConfig config;
    private final BlobCacheDirectory blobCache;
    private final HttpClient httpClient;
    private final Set<String> verifiedBlobs = ConcurrentHashMap.newKeySet();

    public IsolatedPartitionRunner(DistributionWorkerConfig config) {
        this.config = config;
        this.blobCache = new BlobCacheDirectory(config.cacheDir());
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Outcome of a partition run. {@code success} is {@code false} when the partition could not be executed or broke
     * a worker rule (such as writing into the worker's working directory); failing tests are reported as test events.
     */
    public record PartitionExecutionResult(boolean success, String errorMessage, List<WorkspaceFileSet> outputArtifacts) {

        public PartitionExecutionResult {
            outputArtifacts = outputArtifacts == null ? List.of() : List.copyOf(outputArtifacts);
        }

        public PartitionExecutionResult(boolean success, String errorMessage) {
            this(success, errorMessage, List.of());
        }
    }

    /**
     * Runs {@code assignment} in this JVM with thread-scoped system properties and a child-first class loader.
     * <p>
     * The partition gets a private root ({@code <tmp>/cdist-*}) with {@code extracted/} for classpath archives and
     * {@code workspace/} (with {@code target/}) as its {@code basedir}. {@code ${citrus.dist.workspace}} in system
     * properties expands to that workspace, and Citrus report and message-trace directories default to (or resolve
     * relative to) it. Unless {@link DistributionWorkerConfig#cwdCheckMode()} is {@code OFF}, the worker's working
     * directory is snapshotted before and after the run; new or modified entries fail ({@code FAIL}) or warn
     * ({@code WARN}). Nothing in the working directory is ever deleted or modified.
     */
    public PartitionExecutionResult execute(
            PartitionAssignPayload assignment,
            CancellationToken cancellationToken,
            Consumer<WorkerTestEventPayload> eventSink) {
        Path partitionRoot = null;
        ThreadScopedProperties systemProps = null;
        Properties previousScope = null;
        boolean workerScopeEntered = false;

        try {
            partitionRoot = Files.createTempDirectory("cdist-");
            Path workspaceDir = Files.createDirectories(partitionRoot.resolve("workspace"));
            Files.createDirectories(workspaceDir.resolve("target"));

            List<URL> urls = new ArrayList<>();
            for (ArtifactRef artifact : assignment.classpathArtifacts() != null ? assignment.classpathArtifacts() : List.<ArtifactRef>of()) {
                ensureBlobCached(artifact.sha256());
                urls.add(blobCache.materializeForClasspath(artifact, partitionRoot).toUri().toURL());
            }

            Map<String, String> overrides = partitionOverrides(assignment.systemProperties(), workspaceDir);
            WorkingDirectorySnapshot before = null;
            synchronized (WORKER_JVM_LOCK) {
                if (config.cwdCheckMode() != WorkerCwdCheckMode.OFF) {
                    before = WorkingDirectorySnapshot.take(workingDirectory(), config.cacheDir());
                }
                systemProps = installThreadScopedProperties();
                ACTIVE_WORKER_SCOPES.incrementAndGet();
                workerScopeEntered = true;
                systemProps.base().setProperty(DistributionSettings.ROLE_PROPERTY, DistributionSettings.ROLE_WORKER);
                previousScope = systemProps.enterScope(overrides);
            }

            PartitionJUnitLauncher.ExecutionSummary summary;
            ClassLoader parentCl = IsolatedPartitionRunner.class.getClassLoader();
            try (URLClassLoader partitionCl = urls.isEmpty()
                    ? new URLClassLoader(new URL[0], parentCl)
                    : new PartitionClassLoader(urls.toArray(new URL[0]), parentCl)) {
                summary = PartitionJUnitLauncher.executeWorkUnits(assignment.partitionId(), assignment.sessionId(),
                        assignment.attempt(), assignment.workUnits(), partitionCl, cancellationToken, eventSink);
            }
            if (!summary.success()) {
                LOG.debug("Partition {} failed during execution: {}", assignment.partitionId(), summary.errorMessage());
                return new PartitionExecutionResult(false, summary.errorMessage());
            }

            if (before != null && before.available()) {
                List<String> changed;
                synchronized (WORKER_JVM_LOCK) {
                    WorkingDirectorySnapshot after = WorkingDirectorySnapshot.take(workingDirectory(), config.cacheDir());
                    changed = after.available() ? before.changedEntries(after) : List.of();
                }
                if (!changed.isEmpty()) {
                    String message = "Undeclared CWD write detected in worker user.dir " + workingDirectory() + ": "
                            + String.join(", ", changed) + ". Tests of this module write relative to the working "
                            + "directory; set citrus.test.distribution.fork=true or declare the paths in "
                            + "citrus.test.distribution.outputs (or run each worker daemon in its own empty directory). "
                            + "The files were left in place. Other processes writing to this directory during the run "
                            + "also show up here; use citrus.test.distribution.worker.cwdCheck=WARN if the directory "
                            + "is shared.";
                    if (config.cwdCheckMode() == WorkerCwdCheckMode.FAIL) {
                        LOG.warn("Partition {}: {}", assignment.partitionId(), message);
                        return new PartitionExecutionResult(false, message);
                    }
                    LOG.warn("Partition {}: {}", assignment.partitionId(), message);
                }
            }
            return new PartitionExecutionResult(true, null);
        } catch (Throwable t) {
            LOG.warn("Partition {} failed during execution", assignment.partitionId(), t);
            return new PartitionExecutionResult(false, t.getMessage() != null ? t.getMessage() : t.getClass().getName());
        } finally {
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
            deleteRecursivelyQuietly(partitionRoot);
        }
    }

    /**
     * System properties for the partition: the assignment's properties with {@code ${citrus.dist.workspace}} expanded,
     * {@code basedir} set to the workspace, and Citrus report and message-trace directories defaulted to (or resolved
     * relative to) the workspace.
     */
    static Map<String, String> partitionOverrides(Map<String, String> assigned, Path workspaceDir) {
        Map<String, String> overrides = PathRewriter.expandWorkspaceProperties(
                assigned != null ? assigned : Map.of(), workspaceDir);
        Path workspace = workspaceDir.toAbsolutePath().normalize();
        overrides.put("basedir", workspace.toString());
        overrides.put(REPORT_DIRECTORY_PROPERTY,
                workspaceRelative(workspace, overrides.get(REPORT_DIRECTORY_PROPERTY), "target/citrus-reports"));
        overrides.put(MESSAGE_TRACE_DIRECTORY_PROPERTY,
                workspaceRelative(workspace, overrides.get(MESSAGE_TRACE_DIRECTORY_PROPERTY), "target/citrus-logs/trace/messages"));
        return overrides;
    }

    /**
     * {@code configured} if absolute (or rooted, on Windows); otherwise resolved against the workspace, falling back
     * to {@code defaultRelative} when unset or when the relative path would leave the workspace.
     */
    private static String workspaceRelative(Path workspace, String configured, String defaultRelative) {
        Path fallback = workspace.resolve(defaultRelative);
        if (configured == null || configured.isBlank()) {
            return fallback.toString();
        }
        Path path = Path.of(configured);
        if (path.isAbsolute() || path.getRoot() != null) {
            return configured;
        }
        Path resolved = workspace.resolve(path).normalize();
        if (!resolved.startsWith(workspace)) {
            LOG.warn("Ignoring '{}' for a partition report directory: it leaves the partition workspace", configured);
            return fallback.toString();
        }
        return resolved.toString();
    }

    private static Path workingDirectory() {
        return Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
    }

    /**
     * Makes sure {@code sha256} is in the local cache with verified content, downloading it (once more if a cached or
     * downloaded copy fails verification). Digests verified in this JVM are not hashed again.
     */
    private void ensureBlobCached(String sha256) throws IOException, InterruptedException {
        if (verifiedBlobs.contains(sha256) && blobCache.contains(sha256)) {
            return;
        }
        if (blobCache.getVerifiedPath(sha256) == null) {
            byte[] content = download(sha256);
            if (!sha256.equals(ClasspathPackager.sha256Hex(content))) {
                LOG.warn("Blob {} failed verification after download, retrying once", sha256);
                content = download(sha256);
            }
            blobCache.put(sha256, content);
        }
        verifiedBlobs.add(sha256);
    }

    private byte[] download(String sha256) throws IOException, InterruptedException {
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
        return response.body();
    }

    /**
     * Read-only snapshot of {@code (size, lastModified)} for the top-level entries of a directory and up to two levels
     * below its {@code target/} and {@code src/} directories, excluding the worker cache directory.
     */
    record WorkingDirectorySnapshot(Map<String, String> entries, boolean available) {

        /**
         * Takes a snapshot; when the directory itself cannot be listed the snapshot is marked unavailable and no
         * comparison is made. Unreadable sub-directories are skipped.
         */
        static WorkingDirectorySnapshot take(Path root, Path excluded) {
            Map<String, String> entries = new TreeMap<>();
            Path excludedDir = excluded != null ? excluded.toAbsolutePath().normalize() : null;
            List<Path> topLevel;
            try (Stream<Path> top = Files.list(root)) {
                topLevel = top.toList();
            } catch (IOException | UncheckedIOException e) {
                LOG.warn("Could not snapshot worker working directory {}; skipping the CWD check", root, e);
                return new WorkingDirectorySnapshot(Map.of(), false);
            }
            for (Path entry : topLevel) {
                record(root, entry, entries, excludedDir);
                String name = entry.getFileName().toString();
                if ((name.equals("target") || name.equals("src")) && Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
                    try {
                        Files.walkFileTree(entry, Set.of(), 2, new SimpleFileVisitor<>() {
                            @Override
                            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                                if (!dir.equals(entry)) {
                                    record(root, dir, entries, excludedDir);
                                }
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                                record(root, file, entries, excludedDir);
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                                return FileVisitResult.CONTINUE;
                            }
                        });
                    } catch (IOException e) {
                        LOG.debug("Could not snapshot {}", entry, e);
                    }
                }
            }
            return new WorkingDirectorySnapshot(entries, true);
        }

        private static void record(Path root, Path entry, Map<String, String> entries, Path excludedDir) {
            if (excludedDir != null && entry.toAbsolutePath().normalize().startsWith(excludedDir)) {
                return;
            }
            try {
                BasicFileAttributes attrs = Files.readAttributes(entry, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                entries.put(root.relativize(entry).toString(), attrs.size() + "@" + attrs.lastModifiedTime().toMillis());
            } catch (IOException e) {
                // Vanished between listing and reading: not part of this snapshot.
            }
        }

        /**
         * Entries of {@code after} that are new or whose size or modification time changed, sorted.
         */
        List<String> changedEntries(WorkingDirectorySnapshot after) {
            return after.entries().entrySet().stream()
                    .filter(e -> !e.getValue().equals(entries.get(e.getKey())))
                    .map(Map.Entry::getKey)
                    .toList();
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
