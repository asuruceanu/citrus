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

import java.io.Closeable;
import java.io.FilePermission;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
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
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.config.ForkedJvmOptions;
import org.citrusframework.distribution.config.DistributionSettings.WorkerCwdCheckMode;
import org.citrusframework.distribution.config.PathRewriter;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.citrusframework.distribution.workspace.WorkspaceLease;
import org.junit.platform.engine.CancellationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads missing content-addressed blobs, materializes an isolated partition workspace and
 * child-first {@link URLClassLoader}, and executes assigned JUnit Platform work units.
 */
public final class IsolatedPartitionRunner implements PartitionRunner {

    private static final Logger LOG = LoggerFactory.getLogger(IsolatedPartitionRunner.class);
    private static final Object WORKER_JVM_LOCK = new Object();
    private static final java.util.concurrent.atomic.AtomicInteger ACTIVE_WORKER_SCOPES = new java.util.concurrent.atomic.AtomicInteger();

    static final String REPORT_DIRECTORY_PROPERTY = "citrus.report.directory";
    static final String MESSAGE_TRACE_DIRECTORY_PROPERTY = "citrus.message.trace.directory";

    private final DistributionWorkerConfig config;
    private final WorkerFileCache sharedCache;
    private final CoordinatorBlobClient sharedBlobs;

    /**
     * Runs partitions against the worker's shared cache and blob client, both owned by the caller.
     */
    IsolatedPartitionRunner(DistributionWorkerConfig config, WorkerFileCache cache, CoordinatorBlobClient blobs) {
        this.config = Objects.requireNonNull(config, "config");
        this.sharedCache = Objects.requireNonNull(cache, "cache");
        this.sharedBlobs = Objects.requireNonNull(blobs, "blobs");
    }

    /**
     * Test convenience: every {@link #execute} locks the configured cache directory and creates its own blob client,
     * releasing both when the partition ends.
     */
    IsolatedPartitionRunner(DistributionWorkerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.sharedCache = null;
        this.sharedBlobs = null;
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
    @Override
    public PartitionExecutionResult execute(
            PartitionAssignPayload assignment,
            CancellationToken cancellationToken,
            Consumer<WorkerTestEventPayload> eventSink) {
        if (sharedCache != null) {
            return execute(assignment, cancellationToken, eventSink, sharedCache, sharedBlobs);
        }
        WorkerFileCache cache;
        try {
            cache = WorkerFileCache.open(config.cacheDir(), config.cacheCleanup(), config.cacheFreeSpaceBufferBytes());
        } catch (RuntimeException e) {
            LOG.warn("Partition {} failed: cannot open the worker cache", assignment.partitionId(), e);
            return new PartitionExecutionResult(false, e.getMessage() != null ? e.getMessage() : e.getClass().getName());
        }
        try (cache; CoordinatorBlobClient blobs = new CoordinatorBlobClient(config, cache)) {
            return execute(assignment, cancellationToken, eventSink, cache, blobs);
        }
    }

    /**
     * Runs the partition. All classpath digests are pinned before the first download and stay pinned until the
     * partition class loader is closed.
     */
    private PartitionExecutionResult execute(
            PartitionAssignPayload assignment,
            CancellationToken cancellationToken,
            Consumer<WorkerTestEventPayload> eventSink,
            WorkerFileCache cache,
            CoordinatorBlobClient blobs) {
        WorkerWorkspaceRegistry.Workspace workspace = null;
        ThreadScopedProperties systemProps = null;
        Properties previousScope = null;
        boolean workerScopeEntered = false;
        WorkerFileCache.PinLease pins = null;

        try {
            List<ArtifactRef> classpathArtifacts = assignment.classpathArtifacts() != null
                    ? assignment.classpathArtifacts() : List.of();
            pins = cache.pinAll(classpathArtifacts.stream().map(ArtifactRef::sha256).toList());

            workspace = cache.createWorkspace(false, workspaceLeaseRequested(assignment.systemProperties()));
            Path partitionRoot = workspace.root();
            Path workspaceDir = Files.createDirectories(partitionRoot.resolve("workspace"));
            Files.createDirectories(workspaceDir.resolve("target"));

            List<URL> urls = new ArrayList<>();
            for (ArtifactRef artifact : classpathArtifacts) {
                // ensureCached returns a verified blob: materializing it hashes nothing again
                Path verified = blobs.ensureCached(artifact);
                urls.add(cache.materializeForClasspath(artifact, verified, partitionRoot).toUri().toURL());
            }

            Map<String, String> overrides = partitionOverrides(assignment.systemProperties(), workspaceDir);
            String assertions = overrides.remove(ForkedJvmOptions.ASSERTIONS_PROPERTY);
            addWorkspaceLeaseTicket(overrides, workspace);
            WorkingDirectorySnapshot before = null;
            synchronized (WORKER_JVM_LOCK) {
                if (config.cwdCheckMode() != WorkerCwdCheckMode.OFF) {
                    before = WorkingDirectorySnapshot.take(workingDirectory(), config.cacheDir(), cache.rootDir());
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
                if (assertions != null) {
                    partitionCl.setDefaultAssertionStatus(Boolean.parseBoolean(assertions));
                }
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
                    WorkingDirectorySnapshot after = WorkingDirectorySnapshot.take(workingDirectory(), config.cacheDir(),
                            cache.rootDir());
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
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.warn("Partition {} failed during execution", assignment.partitionId(), t);
            return new PartitionExecutionResult(false, t.getMessage() != null ? t.getMessage() : t.getClass().getName());
        } finally {
            try {
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
                if (workspace != null) {
                    try {
                        workspace.close();
                    } catch (IOException e) {
                        LOG.debug("Could not clean up partition workspace {}", workspace.root(), e);
                    }
                }
            } finally {
                if (pins != null) {
                    // The partition class loader is closed by now: its named jars may be evicted again.
                    pins.close();
                }
            }
        }
    }

    /**
     * System properties for the partition: the assignment's properties with {@code ${citrus.dist.workspace}} expanded,
     * {@code basedir} set to the workspace, and Citrus report and message-trace directories defaulted to (or resolved
     * relative to) the workspace.
     */
    static Map<String, String> partitionOverrides(Map<String, String> assigned, Path workspaceDir) {
        Map<String, String> overrides = workspaceOverrides(assigned, workspaceDir);
        Path workspace = workspaceDir.toAbsolutePath().normalize();
        overrides.put(REPORT_DIRECTORY_PROPERTY,
                workspaceRelative(workspace, overrides.get(REPORT_DIRECTORY_PROPERTY), "target/citrus-reports"));
        overrides.put(MESSAGE_TRACE_DIRECTORY_PROPERTY,
                workspaceRelative(workspace, overrides.get(MESSAGE_TRACE_DIRECTORY_PROPERTY), "target/citrus-logs/trace/messages"));
        return overrides;
    }

    /**
     * The assignment's properties with {@code ${citrus.dist.workspace}} expanded and {@code basedir} set to the
     * workspace. Forked partitions use exactly this: they run in the local working directory, so Citrus' relative
     * report defaults already resolve as they do locally.
     */
    static Map<String, String> workspaceOverrides(Map<String, String> assigned, Path workspaceDir) {
        Map<String, String> overrides = PathRewriter.expandWorkspaceProperties(
                assigned != null ? assigned : Map.of(), workspaceDir);
        overrides.put("basedir", workspaceDir.toAbsolutePath().normalize().toString());
        return overrides;
    }

    static boolean workspaceLeaseRequested(Map<String, String> assigned) {
        return assigned != null && Boolean.parseBoolean(assigned.get(WorkspaceLease.ENABLED_PROPERTY));
    }

    static void addWorkspaceLeaseTicket(Map<String, String> properties, WorkerWorkspaceRegistry.Workspace workspace) {
        Path ticket = workspace.leaseTicket();
        if (ticket == null) {
            properties.remove(WorkspaceLease.TICKET_PROPERTY);
        } else {
            properties.put(WorkspaceLease.TICKET_PROPERTY, ticket.toString());
        }
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
     * Read-only snapshot of {@code (size, lastModified)} for the top-level entries of a directory and up to two levels
     * below its {@code target/} and {@code src/} directories, excluding the worker cache directory.
     */
    record WorkingDirectorySnapshot(Map<String, String> entries, boolean available) {

        /**
         * Takes a snapshot; when the directory itself cannot be listed the snapshot is marked unavailable and no
         * comparison is made. Unreadable sub-directories are skipped. Entries at or below any of {@code excluded}
         * (the worker cache, as configured and canonical) are left out, also when an excluded directory is reached
         * through a link or {@code root} itself is a link.
         */
        static WorkingDirectorySnapshot take(Path root, Path... excluded) {
            Map<String, String> entries = new TreeMap<>();
            List<Path> excludedDir = excludedDirectories(root, excluded);
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

        /**
         * The excluded directories as absolute paths in the namespace of {@code root}: each as given, and, when its
         * real location lies inside the real location of {@code root}, also as the corresponding path below
         * {@code root}.
         */
        static List<Path> excludedDirectories(Path root, Path... excluded) {
            List<Path> result = new ArrayList<>();
            if (excluded == null) {
                return result;
            }
            Path absoluteRoot = root.toAbsolutePath().normalize();
            Path realRoot = realPathOrNull(absoluteRoot);
            for (Path path : excluded) {
                if (path == null) {
                    continue;
                }
                Path absolute = path.toAbsolutePath().normalize();
                result.add(absolute);
                Path real = realPathOrNull(absolute);
                if (realRoot != null && real != null && real.startsWith(realRoot)) {
                    result.add(absoluteRoot.resolve(realRoot.relativize(real)).normalize());
                }
            }
            return result;
        }

        private static Path realPathOrNull(Path path) {
            try {
                return path.toRealPath();
            } catch (IOException e) {
                return null;
            }
        }

        private static void record(Path root, Path entry, Map<String, String> entries, List<Path> excludedDirs) {
            Path absolute = entry.toAbsolutePath().normalize();
            for (Path excludedDir : excludedDirs) {
                if (absolute.startsWith(excludedDir)) {
                    return;
                }
            }
            try {
                BasicFileAttributes attrs = Files.readAttributes(entry, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                entries.put(root.relativize(entry).toString(), (attrs.isDirectory() ? "d:" : "f:")
                        + attrs.size() + "@" + attrs.lastModifiedTime().toMillis());
            } catch (IOException e) {
                // Vanished between listing and reading: not part of this snapshot.
            }
        }

        /**
         * Entries of {@code after} that are new or changed, sorted. Changes to regular files that already existed at the
         * top level are ignored (the worker daemon itself may append to its log or {@code nohup.out} there), and so are
         * the top-level {@code target/} and {@code src/} directories themselves, whose contents are compared directly.
         */
        List<String> changedEntries(WorkingDirectorySnapshot after) {
            return after.entries().entrySet().stream()
                    .filter(e -> {
                        String previous = entries.get(e.getKey());
                        if (previous == null) {
                            return true;
                        }
                        if (e.getValue().equals(previous)) {
                            return false;
                        }
                        String name = e.getKey();
                        boolean topLevel = !name.contains("/") && !name.contains(java.io.File.separator);
                        if (!topLevel) {
                            return true;
                        }
                        return e.getValue().startsWith("d:") && !name.equals("target") && !name.equals("src");
                    })
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
            snapshot.remove(WorkspaceLease.TICKET_PROPERTY);
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

    /**
     * Uses the JDK's {@code jar:} URL parsing and comparison and owns the uncached JarFiles opened by its connections.
     * Closing an entry stream closes its JarFile promptly. Closing the handler releases the remaining JarFiles,
     * including those opened just for metadata.
     */
    static final class UncachedJarHandler extends URLStreamHandler implements Closeable {

        private final Set<JarFile> openJars = new HashSet<>();
        private boolean closed;

        @Override
        protected URLConnection openConnection(URL url) throws IOException {
            checkOpen();
            return new OwnedJarConnection(url, (JarURLConnection) jdkUrl(url).openConnection());
        }

        private synchronized void checkOpen() throws IOException {
            if (closed) {
                throw new IOException("Partition JAR handler is closed");
            }
        }

        private synchronized JarFile openLocalJar(JarURLConnection connection) throws IOException {
            checkOpen();
            return retain(connection.getJarFile());
        }

        private synchronized JarFile retain(JarFile jar) throws IOException {
            if (closed) {
                // A connection can finish opening while the partition is closing.
                try (jar) {
                    checkOpen();
                }
            }
            openJars.add(jar);
            return jar;
        }

        private synchronized void release(JarFile jar) {
            openJars.remove(jar);
        }

        @Override
        public synchronized void close() throws IOException {
            closed = true;
            IOException failure = null;
            for (JarFile jar : openJars) {
                try {
                    jar.close();
                } catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            openJars.clear();
            if (failure != null) {
                throw failure;
            }
        }

        private final class OwnedJarConnection extends JarURLConnection {

            private final JarURLConnection delegate;
            private JarFile jar;

            private OwnedJarConnection(URL url, JarURLConnection delegate) throws MalformedURLException {
                super(url);
                this.delegate = delegate;
                delegate.setUseCaches(false);
                useCaches = false;
            }

            @Override
            public synchronized void connect() throws IOException {
                if (!connected) {
                    checkOpen();
                    // Wait for local file opens during shutdown. Remote downloads can finish independently.
                    jar = isLocalFile() ? openLocalJar(delegate) : retain(delegate.getJarFile());
                    connected = true;
                }
            }

            @Override
            public void setUseCaches(boolean useCaches) {
                super.setUseCaches(false);
            }

            @Override
            public JarFile getJarFile() throws IOException {
                connect();
                return jar;
            }

            @Override
            public JarEntry getJarEntry() throws IOException {
                connect();
                return delegate.getJarEntry();
            }

            @Override
            public InputStream getInputStream() throws IOException {
                connect();
                return new FilterInputStream(delegate.getInputStream()) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        release(jar);
                    }
                };
            }

            @Override
            public Object getContent() throws IOException {
                return getEntryName() == null ? getJarFile() : super.getContent();
            }

            @Override
            public int getContentLength() {
                long length = getContentLengthLong();
                return length > Integer.MAX_VALUE ? -1 : (int) length;
            }

            @Override
            public long getContentLengthLong() {
                try {
                    connect();
                    if (getEntryName() == null && isLocalFile()) {
                        return getHeaderFieldLong("content-length", -1);
                    }
                    return delegate.getContentLengthLong();
                } catch (IOException e) {
                    return -1;
                }
            }

            @Override
            public String getContentType() {
                try {
                    if (getEntryName() != null) {
                        connect();
                    }
                    return delegate.getContentType();
                } catch (IOException e) {
                    return Objects.requireNonNullElse(guessContentTypeFromName(getEntryName()), "content/unknown");
                }
            }

            @Override
            public String getHeaderField(String name) {
                try {
                    checkOpen();
                    if (!isLocalFile()) {
                        return delegate.getHeaderField(name);
                    }
                    synchronized (UncachedJarHandler.this) {
                        checkOpen();
                        // The JDK opens a file stream when reading headers. Scope that stream to this call.
                        URLConnection file = getJarFileURL().openConnection();
                        try (InputStream stream = file.getInputStream()) {
                            return file.getHeaderField(name);
                        }
                    }
                } catch (IOException e) {
                    return null;
                }
            }

            private boolean isLocalFile() throws IOException {
                return "file".equals(getJarFileURL().getProtocol()) && getPermission() instanceof FilePermission;
            }

            @Override
            public Permission getPermission() throws IOException {
                return delegate.getPermission();
            }

            @Override
            public void setAllowUserInteraction(boolean allowUserInteraction) {
                delegate.setAllowUserInteraction(allowUserInteraction);
            }

            @Override
            public boolean getAllowUserInteraction() {
                return delegate.getAllowUserInteraction();
            }

            @Override
            public void setIfModifiedSince(long ifModifiedSince) {
                delegate.setIfModifiedSince(ifModifiedSince);
            }

            @Override
            public void setRequestProperty(String key, String value) {
                delegate.setRequestProperty(key, value);
            }

            @Override
            public void addRequestProperty(String key, String value) {
                delegate.addRequestProperty(key, value);
            }

            @Override
            public String getRequestProperty(String key) {
                return delegate.getRequestProperty(key);
            }

            @Override
            public Map<String, List<String>> getRequestProperties() {
                return delegate.getRequestProperties();
            }
        }

        /**
         * The JDK's jar handler does not implement this variant (it throws {@link UnsupportedOperationException}); a
         * {@code jar:} URL opens its inner URL itself, so the proxy is not applicable and the plain connection is used.
         */
        @Override
        protected URLConnection openConnection(URL url, java.net.Proxy proxy) throws IOException {
            return openConnection(url);
        }

        /** Parses {@code spec} like the JDK's jar handler, relative to {@code url}'s current (context) fields. */
        @Override
        @SuppressWarnings("deprecation") // URL(URL, String): resolves through the JDK's jar handler, accepts spaces
        protected void parseURL(URL url, String spec, int start, int limit) {
            try {
                URL context = url.getFile() == null || url.getFile().isEmpty() ? null : jdkUrl(url);
                URL resolved = new URL(context, spec);
                setURL(url, resolved.getProtocol(), resolved.getHost(), resolved.getPort(), resolved.getAuthority(),
                        resolved.getUserInfo(), resolved.getPath(), resolved.getQuery(), resolved.getRef());
            } catch (MalformedURLException e) {
                // URL's constructor reports this as a MalformedURLException.
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }

        @Override
        protected boolean equals(URL first, URL second) {
            URL jdkFirst = jdkUrlOrNull(first);
            return jdkFirst != null && jdkFirst.equals(jdkUrlOrNull(second));
        }

        @Override
        protected int hashCode(URL url) {
            URL jdk = jdkUrlOrNull(url);
            return jdk != null ? jdk.hashCode() : url.toExternalForm().hashCode();
        }

        @Override
        protected boolean sameFile(URL first, URL second) {
            URL jdkFirst = jdkUrlOrNull(first);
            URL jdkSecond = jdkUrlOrNull(second);
            return jdkFirst != null && jdkSecond != null && jdkFirst.sameFile(jdkSecond);
        }

        /** The same URL with the JDK's default handler for its protocol (no URI round trip, so spaces are fine). */
        @SuppressWarnings("deprecation")
        static URL jdkUrl(URL url) throws MalformedURLException {
            return new URL(url.toExternalForm());
        }

        private static URL jdkUrlOrNull(URL url) {
            try {
                return url != null ? jdkUrl(url) : null;
            } catch (MalformedURLException e) {
                return null;
            }
        }
    }

    /**
     * Child-first classloader that isolates a partition from the worker's own classpath.
     * <p>
     * JDK classes (every package of a boot-layer module, including {@code org.w3c.dom} and {@code org.xml.sax}) and the
     * JUnit Platform, OpenTest4J, and SLF4J SPI types shared with the worker launcher come from the parent. Everything
     * else is loaded from the partition, falling back only to the JVM's own platform and bootstrap loaders: a class or
     * resource the partition does not ship must not leak in from the worker's application classpath (for example the
     * worker's own Citrus jars), or it would link against a second copy of the same classes.
     * <p>
     * {@code jar:} URLs of partition resources open their connections without the JDK's JarFile cache: a cached
     * JarFile stays open until the worker JVM exits, which would keep the disk space of an evicted named jar in use
     * (and prevent deleting it on Windows). The loader's own jar handles are released when it is closed after the
     * partition.
     */
    static final class PartitionClassLoader extends URLClassLoader {

        private static final Set<String> JDK_PACKAGES = ModuleLayer.boot().modules().stream()
                .flatMap(module -> module.getPackages().stream())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());

        /** JUnit SPIs the worker's own jars may register providers for (ServiceLoader runs from the shared JUnit). */
        private static final List<String> JUNIT_SERVICE_FILES = List.of(
                "META-INF/services/org.junit.platform.engine.TestEngine",
                "META-INF/services/org.junit.platform.launcher.LauncherSessionListener",
                "META-INF/services/org.junit.platform.launcher.LauncherDiscoveryListener",
                "META-INF/services/org.junit.platform.launcher.TestExecutionListener",
                "META-INF/services/org.junit.platform.launcher.PostDiscoveryFilter",
                "META-INF/services/org.junit.platform.launcher.LauncherInterceptor",
                "META-INF/services/org.junit.jupiter.api.extension.Extension");

        /** Resource prefixes of the libraries shared with the worker: they must see their own resources. */
        private static final List<String> SHARED_RESOURCE_PREFIXES = List.of(
                "META-INF/services/org.junit.",
                "META-INF/services/org.slf4j.",
                "META-INF/services/org.apache.logging.log4j.",
                "META-INF/org/apache/logging/log4j/",
                "org/junit/platform/",
                "org/junit/jupiter/",
                "org/opentest4j/",
                "org/apiguardian/",
                "org/slf4j/",
                "org/apache/logging/log4j/");

        /** Jars of the worker JVM's own {@code -javaagent} arguments. */
        private static final List<Path> WORKER_JAVA_AGENTS = javaAgentPaths(
                java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments());

        private final UncachedJarHandler jarHandler = new UncachedJarHandler();
        private final ClassLoader jdkLoader;
        private final ClassLoader agentHost;
        private final List<Path> agentLocations;
        private final Set<String> workerServiceProviders;

        PartitionClassLoader(URL[] urls, ClassLoader parent) {
            this(urls, parent, ClassLoader.getPlatformClassLoader());
        }

        /** {@code jdkLoader} resolves JDK resources and classes the JVM itself defines (platform and bootstrap). */
        PartitionClassLoader(URL[] urls, ClassLoader parent, ClassLoader jdkLoader) {
            this(urls, parent, jdkLoader, ClassLoader.getSystemClassLoader(), WORKER_JAVA_AGENTS);
        }

        /**
         * {@code agentHost} is the loader the JVM appends {@code -javaagent} jars to; only classes it loads from one of
         * {@code javaAgents} are visible to the partition, never the rest of the worker's classpath.
         */
        PartitionClassLoader(URL[] urls, ClassLoader parent, ClassLoader jdkLoader, ClassLoader agentHost,
                             List<Path> javaAgents) {
            // Named like Surefire's JDK "app" loader is: frameworks such as Quarkus read the test class loader's name.
            super("citrus-distribution-partition", urls, parent);
            this.jdkLoader = jdkLoader;
            this.agentHost = agentHost;
            this.agentLocations = javaAgents.stream().map(PartitionClassLoader::realPath).toList();
            this.workerServiceProviders = readServiceProviders(parent);
        }

        /** Jar paths of the {@code -javaagent:<jar>[=options]} entries in {@code jvmArguments}. */
        static List<Path> javaAgentPaths(List<String> jvmArguments) {
            List<Path> agents = new ArrayList<>();
            for (String argument : jvmArguments) {
                if (argument.startsWith("-javaagent:")) {
                    String jar = argument.substring("-javaagent:".length());
                    int options = jar.indexOf('=');
                    agents.add(Path.of(options >= 0 ? jar.substring(0, options) : jar).toAbsolutePath());
                }
            }
            return List.copyOf(agents);
        }

        private static Path realPath(Path path) {
            try {
                return path.toRealPath();
            } catch (IOException | RuntimeException e) {
                return path.toAbsolutePath().normalize();
            }
        }

        /** File or directory a class resource was read from: the jar of a {@code jar:} URL, else the file itself. */
        private static Path sourceOf(URL resource) {
            try {
                String url = resource.toString();
                if (url.startsWith("jar:")) {
                    int separator = url.indexOf("!/");
                    return realPath(Path.of(java.net.URI.create(url.substring(4, separator > 0 ? separator : url.length()))));
                }
                return "file".equals(resource.getProtocol()) ? realPath(Path.of(resource.toURI())) : null;
            } catch (RuntimeException | java.net.URISyntaxException e) {
                return null;
            }
        }

        private Class<?> loadFromJavaAgent(String name) throws ClassNotFoundException {
            if (agentHost != null && !agentLocations.isEmpty()) {
                URL resource = agentHost.getResource(name.replace('.', '/') + ".class");
                Path source = resource != null ? sourceOf(resource) : null;
                if (source != null) {
                    for (Path location : agentLocations) {
                        if (source.equals(location) || source.startsWith(location)) {
                            return agentHost.loadClass(name);
                        }
                    }
                }
            }
            throw new ClassNotFoundException(name);
        }

        private static Set<String> readServiceProviders(ClassLoader parent) {
            Set<String> providers = new java.util.HashSet<>();
            if (parent == null) {
                return providers;
            }
            for (String serviceFile : JUNIT_SERVICE_FILES) {
                try {
                    for (URL url : Collections.list(parent.getResources(serviceFile))) {
                        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(
                                url.openStream(), java.nio.charset.StandardCharsets.UTF_8))) {
                            reader.lines()
                                    .map(line -> line.replaceAll("#.*", "").strip())
                                    .filter(line -> !line.isEmpty())
                                    .forEach(providers::add);
                        }
                    }
                } catch (IOException ignored) {
                    // unreadable service file: its providers are simply not shared
                }
            }
            return providers;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    if (shouldDelegateToParent(name) || workerServiceProviders.contains(name)) {
                        loaded = super.loadClass(name, false);
                    } else {
                        try {
                            loaded = findClass(name);
                        } catch (ClassNotFoundException notInPartition) {
                            // Only the JVM's own loaders, never the worker's application classpath: covers classes a
                            // test agent injects at runtime (Mockito's MockMethodDispatcher lives in the bootstrap loader).
                            try {
                                loaded = jdkLoader.loadClass(name);
                            } catch (ClassNotFoundException notInJvm) {
                                // Then classes of the worker's -javaagent jars (JaCoCo runtime, APM agents).
                                loaded = loadFromJavaAgent(name);
                            }
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
        public URL findResource(String name) {
            return uncached(super.findResource(name));
        }

        @Override
        public Enumeration<URL> findResources(String name) throws IOException {
            List<URL> urls = new ArrayList<>();
            for (URL url : Collections.list(super.findResources(name))) {
                urls.add(uncached(url));
            }
            return Collections.enumeration(urls);
        }

        /** Attaches the partition's handler to its {@code jar:} URLs. */
        @SuppressWarnings("deprecation") // URL(URL, String, URLStreamHandler): the only way to attach a handler without a URI
        private URL uncached(URL url) {
            if (url == null || !"jar".equals(url.getProtocol())) {
                return url;
            }
            try {
                return new URL(null, url.toExternalForm(), jarHandler);
            } catch (MalformedURLException e) {
                // keep the JDK's own (caching) handler rather than losing the resource
                return url;
            }
        }

        @Override
        public void close() throws IOException {
            try (jarHandler) {
                super.close();
            }
        }

        @Override
        public URL getResource(String name) {
            if (isSharedResource(name)) {
                return super.getResource(name);
            }
            URL url = findResource(name);
            return url != null ? url : jdkLoader.getResource(name);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            if (isSharedResource(name)) {
                return super.getResources(name);
            }
            List<URL> urls = new ArrayList<>(Collections.list(findResources(name)));
            urls.addAll(Collections.list(jdkLoader.getResources(name)));
            return Collections.enumeration(urls);
        }

        private static boolean isSharedResource(String name) {
            for (String prefix : SHARED_RESOURCE_PREFIXES) {
                if (name.startsWith(prefix)) {
                    return true;
                }
            }
            return false;
        }

        static boolean shouldDelegateToParent(String name) {
            // JDK packages are matched exactly (isJdkPackage), not by javax./jdk./sun. prefixes: third-party javax.*
            // libraries such as wsdl4j's javax.wsdl must load from the partition with their implementation.
            return name.startsWith("java.")
                    || name.startsWith("org.junit.platform.")
                    || name.startsWith("org.junit.jupiter.")
                    || name.startsWith("org.opentest4j.")
                    || name.startsWith("org.apiguardian.")
                    || name.startsWith("org.slf4j.")
                    || name.startsWith("org.apache.logging.log4j.")
                    || isJdkPackage(name);
        }

        private static boolean isJdkPackage(String className) {
            int lastDot = className.lastIndexOf('.');
            return lastDot > 0 && JDK_PACKAGES.contains(className.substring(0, lastDot));
        }
    }
}
