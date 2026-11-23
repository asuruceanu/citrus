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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.PartitionAssignPayload;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.junit.platform.engine.CancellationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a partition in a short-lived child JVM whose working directory is a private workspace.
 * <p>
 * Layout: {@code <tmp>/cdist-*}/{@code extracted/} (classpath archives), {@code ipc/} (request and argfile, owner-only),
 * {@code workspace/} (CWD and {@code basedir}, with {@code target/} and the materialized inputs). The child's
 * {@code java.io.tmpdir} is a short {@code /tmp/cdtmp-*} directory on POSIX so Unix domain socket paths stay short.
 * The child authenticates on a loopback socket and streams test events; when it reports a result, files under the
 * declared outputs that are new or changed compared to after input materialization are packaged and uploaded.
 * Cancellation, a crash or a missing handshake kill the child's whole process tree; nothing is uploaded then.
 */
final class ForkedPartitionRunner implements PartitionRunner {

    private static final Logger LOG = LoggerFactory.getLogger(ForkedPartitionRunner.class);

    private static final int MAX_HELLO_LINE = 1024 * 1024;
    private static final int MAX_EVENT_LINE = 64 * 1024 * 1024;
    private static final int STDERR_TAIL_LINES = 50;
    private static final long POST_RESULT_EXIT_WAIT_MS = 10_000L;
    private static final int MAX_JAR_RESOURCE_CACHE_ENTRIES = 4_096;
    private static final String TEST_ENGINE_RESOURCE = "org/junit/platform/engine/TestEngine.class";
    private static final String CANCELLATION_TOKEN_RESOURCE = "org/junit/platform/engine/CancellationToken.class";

    /**
     * Classes the child needs from the worker runtime, added to its classpath only when the partition classpath does
     * not already provide them (so the module under test keeps its own library versions).
     */
    private static final List<String> WORKER_RUNTIME = List.of(
            "org.citrusframework.distribution.worker.ForkedPartitionMain",
            "org.citrusframework.distribution.config.DistributionJson");

    private static final List<String> RUNTIME_ANCHORS = List.of(
            "tools.jackson.databind.ObjectMapper",
            "tools.jackson.core.JsonParser",
            "com.fasterxml.jackson.annotation.JsonProperty",
            "org.junit.platform.launcher.core.LauncherFactory",
            "org.junit.platform.engine.TestEngine",
            "org.junit.platform.commons.JUnitException",
            "org.opentest4j.TestAbortedException",
            "org.slf4j.LoggerFactory");

    private static final Set<String> RUNTIME_ANCHOR_RESOURCES = RUNTIME_ANCHORS.stream()
            .map(ForkedPartitionRunner::classResourceName)
            .collect(Collectors.toUnmodifiableSet());
    private static final Set<String> WATCHED_RESOURCES = Stream.concat(RUNTIME_ANCHOR_RESOURCES.stream(),
            Stream.of(CANCELLATION_TOKEN_RESOURCE)).collect(Collectors.toUnmodifiableSet());

    private static final Map<JarResourceCacheKey, Set<String>> JAR_RESOURCE_CACHE =
            Collections.synchronizedMap(new LinkedHashMap<>(256, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<JarResourceCacheKey, Set<String>> eldest) {
                    return size() > MAX_JAR_RESOURCE_CACHE_ENTRIES;
                }
            });
    private static final Map<String, Optional<Path>> CODE_SOURCE_CACHE = new ConcurrentHashMap<>();
    private static final LongAdder JAR_OPEN_COUNT = new LongAdder();

    private static final Set<ActiveFork> ACTIVE_FORKS = ConcurrentHashMap.newKeySet();
    private static final ReentrantReadWriteLock JVM_FORK_START_FENCE = new ReentrantReadWriteLock();
    private static volatile boolean jvmStopping;
    private static final Thread SHUTDOWN_HOOK = new Thread(ForkedPartitionRunner::cleanUpOnJvmShutdown,
            "citrus-dist-fork-cleanup");

    static {
        Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);
    }

    private final DistributionWorkerConfig config;
    /** Forks of this runner; {@code ACTIVE_FORKS} holds those of every runner for the shutdown hook. */
    private final Set<ActiveFork> forks = ConcurrentHashMap.newKeySet();
    private final Object forkStartMonitor = new Object();
    private volatile boolean stopping;
    private final WorkerFileCache sharedCache;
    private final BrokerBlobClient sharedBlobs;
    private final long handshakeTimeoutMs;

    /**
     * Runs partitions against the worker's shared cache and blob client, both owned by the caller.
     */
    ForkedPartitionRunner(DistributionWorkerConfig config, WorkerFileCache cache, BrokerBlobClient blobs) {
        this(config, Objects.requireNonNull(cache, "cache"), Objects.requireNonNull(blobs, "blobs"), true);
    }

    /**
     * Test convenience: every {@link #execute} locks the configured cache directory and creates its own blob client,
     * releasing both when the partition ends.
     */
    ForkedPartitionRunner(DistributionWorkerConfig config) {
        this(config, null, null, false);
    }

    private ForkedPartitionRunner(DistributionWorkerConfig config, WorkerFileCache cache, BrokerBlobClient blobs,
                                  boolean shared) {
        this.config = Objects.requireNonNull(config, "config");
        this.sharedCache = shared ? cache : null;
        this.sharedBlobs = shared ? blobs : null;
        this.handshakeTimeoutMs = Math.max(1_000L, DistributionSettings.getLong(
                DistributionSettings.WORKER_HANDSHAKE_TIMEOUT_PROPERTY, null,
                DistributionSettings.DEFAULT_WORKER_HANDSHAKE_TIMEOUT_MS));
    }

    @Override
    public IsolatedPartitionRunner.PartitionExecutionResult execute(PartitionAssignPayload assignment,
                                                                    CancellationToken cancellationToken,
                                                                    Consumer<WorkerTestEventPayload> eventSink) {
        if (jvmStopping || stopping || cancelled(cancellationToken)) {
            return cancelledResult();
        }
        if (sharedCache != null) {
            return execute(assignment, cancellationToken, eventSink, sharedCache, sharedBlobs);
        }
        WorkerFileCache cache;
        try {
            cache = WorkerFileCache.open(config.cacheDir(), config.cacheCleanup(), config.cacheFreeSpaceBufferBytes());
        } catch (RuntimeException e) {
            LOG.warn("Forked partition {} failed: cannot open the worker cache", assignment.partitionId(), e);
            return new IsolatedPartitionRunner.PartitionExecutionResult(false,
                    e.getMessage() != null ? e.getMessage() : e.getClass().getName());
        }
        try (cache; BrokerBlobClient blobs = new BrokerBlobClient(config, cache)) {
            return execute(assignment, cancellationToken, eventSink, cache, blobs);
        }
    }

    /**
     * Runs the partition. Classpath and input digests are pinned before the first download, output digests as they
     * are staged; all pins are held until the child's process tree is gone and the outputs are uploaded.
     */
    private IsolatedPartitionRunner.PartitionExecutionResult execute(PartitionAssignPayload assignment,
                                                                     CancellationToken cancellationToken,
                                                                     Consumer<WorkerTestEventPayload> eventSink,
                                                                     WorkerFileCache cache,
                                                                     BrokerBlobClient blobs) {
        ActiveFork fork = new ActiveFork();
        Lock startLock = JVM_FORK_START_FENCE.readLock();
        startLock.lock();
        try {
            if (jvmStopping) {
                return cancelledResult();
            }
            ACTIVE_FORKS.add(fork);
            forks.add(fork);
        } finally {
            startLock.unlock();
        }
        WorkerFileCache.PinLease pins = null;
        try {
            List<ArtifactRef> classpathArtifacts = assignment.classpathArtifacts() != null
                    ? assignment.classpathArtifacts() : List.of();
            List<String> pinned = new ArrayList<>();
            classpathArtifacts.forEach(artifact -> pinned.add(artifact.sha256()));
            assignment.inputArtifacts().forEach(input -> pinned.add(input.artifact().sha256()));
            pins = cache.pinAll(pinned);

            Path workspace;
            Path ipc;
            synchronized (fork.cleanupMonitor) {
                if (jvmStopping) {
                    return cancelledResult();
                }
                fork.workspace = cache.createWorkspace(true,
                        IsolatedPartitionRunner.workspaceLeaseRequested(assignment.systemProperties()));
                fork.partitionRoot = fork.workspace.root();
                fork.tmpDir = fork.workspace.childTmpDir();
                workspace = Files.createDirectories(fork.partitionRoot.resolve("workspace"));
                Files.createDirectories(workspace.resolve("target"));
                ipc = createOwnerOnlyDirectory(fork.partitionRoot.resolve("ipc"));
            }

            // Verification and downloads may block. Only materialization can recreate paths removed by cleanup.
            List<Path> classpath = new ArrayList<>();
            for (ArtifactRef artifact : classpathArtifacts) {
                Path verified = blobs.ensureCached(artifact);
                synchronized (fork.cleanupMonitor) {
                    if (jvmStopping) {
                        return cancelledResult();
                    }
                    classpath.add(cache.materializeForClasspath(artifact, verified, fork.partitionRoot));
                }
            }
            for (WorkspaceFileSet input : assignment.inputArtifacts()) {
                Path verified = blobs.ensureCached(input.artifact());
                synchronized (fork.cleanupMonitor) {
                    if (jvmStopping) {
                        return cancelledResult();
                    }
                    cache.materializeIntoDirectory(input, verified, workspace);
                }
            }
            Map<String, String> baseline = WorkspacePackager.snapshotBaselineHashes(workspace, assignment.outputPaths());
            verifyJUnitPlatform(classpath);
            String classpathLine = argfileClasspathLine(childClasspath(classpath)) + System.lineSeparator();

            byte[] secret = new byte[32];
            new SecureRandom().nextBytes(secret);
            String token = HexFormat.of().formatHex(secret);
            Map<String, String> properties = new LinkedHashMap<>(
                    IsolatedPartitionRunner.workspaceOverrides(assignment.systemProperties(), workspace));
            IsolatedPartitionRunner.addWorkspaceLeaseTicket(properties, fork.workspace);
            properties.put(DistributionSettings.ROLE_PROPERTY, DistributionSettings.ROLE_WORKER);
            String requestJson = DistributionJson.toJson(new ForkedPartitionMain.ForkRequest(assignment.partitionId(),
                    assignment.sessionId(), assignment.attempt(), token, properties, assignment.workUnits()));
            Path argfile = ipc.resolve("classpath.argfile");
            Path request = ipc.resolve("request.json");
            Path workingDirectory;
            synchronized (fork.cleanupMonitor) {
                if (jvmStopping) {
                    return cancelledResult();
                }
                workingDirectory = childWorkingDirectory(workspace, assignment.workingDirectory());
                writeOwnerOnly(argfile, classpathLine, argfileCharset());
                writeOwnerOnly(request, requestJson, StandardCharsets.UTF_8);
            }

            ForkedPartitionMain.ForkedExecutionSummary summary;
            try (ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
                // Publish the child before either worker close or JVM shutdown can collect it for cleanup.
                synchronized (forkStartMonitor) {
                    startLock.lock();
                    try {
                        if (jvmStopping || stopping || cancelled(cancellationToken)) {
                            return cancelledResult();
                        }
                        fork.workspace.markLaunching();
                        try {
                            fork.process = launch(argfile, request, server.getLocalPort(), workspace, workingDirectory,
                                    fork.tmpDir);
                        } catch (IOException e) {
                            try {
                                fork.workspace.markLaunchFailed();
                            } catch (IOException recordFailure) {
                                e.addSuppressed(recordFailure);
                            }
                            throw e;
                        }
                        fork.workspace.markLaunched(fork.process.toHandle());
                        fork.watcher = processWatcher(fork, cancellationToken);
                    } finally {
                        startLock.unlock();
                    }
                }
                summary = runProtocol(server, fork, token, cancellationToken, eventSink);
            }

            if (cancelled(cancellationToken)) {
                return cancelledResult();
            }
            if (summary == null) {
                return new IsolatedPartitionRunner.PartitionExecutionResult(false, crashDiagnostic(fork));
            }
            awaitExit(fork, POST_RESULT_EXIT_WAIT_MS);
            // Processes left behind by tests must not keep writing while outputs are hashed and packaged.
            fork.killAll();
            if (!summary.success()) {
                return new IsolatedPartitionRunner.PartitionExecutionResult(false, summary.errorMessage());
            }

            List<WorkspaceFileSet> outputs = new ArrayList<>();
            for (String outputPath : assignment.outputPaths()) {
                if (cancelled(cancellationToken)) {
                    return cancelledResult();
                }
                cache.stageAndPinOutput(workspace, outputPath, baseline, pins::pin).ifPresent(outputs::add);
            }
            if (cancelled(cancellationToken)) {
                return cancelledResult();
            }
            blobs.uploadMissing(outputs.stream().map(o -> o.artifact().sha256()).toList());
            return new IsolatedPartitionRunner.PartitionExecutionResult(true, null, outputs);
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.warn("Forked partition {} failed", assignment.partitionId(), t);
            return new IsolatedPartitionRunner.PartitionExecutionResult(false,
                    t.getMessage() != null ? t.getMessage() : t.getClass().getName());
        } finally {
            try {
                fork.cleanUp();
                ACTIVE_FORKS.remove(fork);
                forks.remove(fork);
            } finally {
                if (pins != null) {
                    // The child's process tree is gone and the outputs are uploaded: the blobs may be evicted again.
                    pins.close();
                }
            }
        }
    }

    /**
     * Prevents new launches and kills the process trees of this runner's running forks. Partitions then fail and
     * clean up as after a crash.
     */
    void killActiveForks() {
        synchronized (forkStartMonitor) {
            stopping = true;
        }
        for (ActiveFork fork : forks) {
            fork.killAll();
        }
    }

    private static void cleanUpOnJvmShutdown() {
        jvmStopping = true;
        List<ActiveFork> active;
        Lock shutdownLock = JVM_FORK_START_FENCE.writeLock();
        shutdownLock.lock();
        try {
            // Every registration and launch that passed the shutdown check has now published its resources.
            active = List.copyOf(ACTIVE_FORKS);
        } finally {
            shutdownLock.unlock();
        }
        // Reap launched children before waiting for any in-flight workspace materialization.
        active.stream().sorted(Comparator.comparing(fork -> fork.process == null)).forEach(ActiveFork::cleanUp);
    }

    private static boolean cancelled(CancellationToken cancellationToken) {
        return cancellationToken != null && cancellationToken.isCancellationRequested();
    }

    private static IsolatedPartitionRunner.PartitionExecutionResult cancelledResult() {
        return new IsolatedPartitionRunner.PartitionExecutionResult(false, "Partition cancelled");
    }

    /**
     * The charset the JDK launcher decodes argfiles with: the platform's file-name encoding.
     */
    private static Charset argfileCharset() {
        String name = System.getProperty("sun.jnu.encoding", System.getProperty("native.encoding"));
        try {
            return name != null ? Charset.forName(name) : Charset.defaultCharset();
        } catch (IllegalArgumentException e) {
            return Charset.defaultCharset();
        }
    }

    /**
     * Fails clearly when the partition brings a JUnit Platform without the cancellation API the forked launcher uses
     * (JUnit 6), instead of letting the child JVM die with a linkage error.
     */
    static void verifyJUnitPlatform(List<Path> partitionClasspath) {
        Set<String> found = presentResources(partitionClasspath,
                Set.of(TEST_ENGINE_RESOURCE, CANCELLATION_TOKEN_RESOURCE));
        if (found.contains(TEST_ENGINE_RESOURCE) && !found.contains(CANCELLATION_TOKEN_RESOURCE)) {
            throw new IllegalStateException("The partition's test classpath provides a JUnit Platform without "
                    + "org.junit.platform.engine.CancellationToken; forked partitions need JUnit Platform 6 or newer");
        }
    }

    /**
     * The argfile line for the child classpath: {@code -cp "<entries>"} with {@code \} and {@code "} escaped as the
     * JDK launcher's argfile syntax requires, so spaces and {@code #} in paths are safe.
     *
     * @throws IllegalArgumentException for paths containing line breaks, which argfiles cannot express
     */
    static String argfileClasspathLine(List<Path> classpath) {
        StringBuilder joined = new StringBuilder();
        for (Path entry : classpath) {
            String path = entry.toString();
            if (path.indexOf('\n') >= 0 || path.indexOf('\r') >= 0) {
                throw new IllegalArgumentException("Classpath entry contains a line break: " + path);
            }
            if (!joined.isEmpty()) {
                joined.append(File.pathSeparatorChar);
            }
            joined.append(path);
        }
        return "-cp \"" + joined.toString().replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /**
     * The child classpath: the worker's own runtime (fork entry point and wire protocol) first, so the module's copy
     * of the protocol jar cannot shadow it; then the partition classpath in order; then worker locations for libraries
     * the partition does not provide itself (keeping the module's own versions where it does).
     */
    static List<Path> childClasspath(List<Path> partitionClasspath) {
        List<Path> result = new ArrayList<>();
        WORKER_RUNTIME.forEach(anchor -> codeSource(anchor).filter(location -> !result.contains(location)).ifPresent(result::add));
        partitionClasspath.stream().filter(entry -> !result.contains(entry)).forEach(result::add);
        Set<String> provided = presentResources(partitionClasspath, RUNTIME_ANCHOR_RESOURCES);
        for (String anchor : RUNTIME_ANCHORS) {
            if (!provided.contains(classResourceName(anchor))) {
                codeSource(anchor).filter(location -> !result.contains(location)).ifPresent(result::add);
            }
        }
        return result;
    }

    private static String classResourceName(String className) {
        return className.replace('.', '/') + ".class";
    }

    /**
     * Which of {@code resources} at least one classpath entry contains.
     */
    private static Set<String> presentResources(List<Path> classpath, Set<String> resources) {
        Set<String> found = new HashSet<>();
        List<String> unwatched = resources.stream().filter(resource -> !WATCHED_RESOURCES.contains(resource)).toList();
        for (Path entry : classpath) {
            if (found.size() == resources.size()) {
                break;
            }
            if (Files.isDirectory(entry)) {
                resources.stream().filter(r -> Files.isRegularFile(entry.resolve(r))).forEach(found::add);
                continue;
            }
            collectJarResources(entry, resources, unwatched, found);
        }
        return found;
    }

    private static void collectJarResources(Path entry, Set<String> resources, List<String> unwatched, Set<String> found) {
        try {
            Path real = entry.toRealPath();
            BasicFileAttributes attrs = Files.readAttributes(real, BasicFileAttributes.class);
            if (!attrs.isRegularFile()) {
                return;
            }
            boolean contentAddressed = isContentAddressedCacheJar(entry) || isContentAddressedCacheJar(real);
            // WorkerFileCache updates mtime on access, including on the inode shared with a named cache JAR.
            JarResourceCacheKey key = new JarResourceCacheKey(real, attrs.size(),
                    contentAddressed ? 0L : attrs.lastModifiedTime().toMillis(), attrs.fileKey(),
                    contentAddressed && attrs.fileKey() == null ? attrs.creationTime().toMillis() : 0L);
            Set<String> present = JAR_RESOURCE_CACHE.get(key);
            if (present == null || !unwatched.isEmpty()) {
                present = scanJarResources(real, unwatched);
                Set<String> watched = unwatched.isEmpty() ? present : present.stream()
                        .filter(WATCHED_RESOURCES::contains)
                        .collect(Collectors.toUnmodifiableSet());
                JAR_RESOURCE_CACHE.put(key, watched);
            }
            present.stream().filter(resources::contains).forEach(found::add);
        } catch (NoSuchFileException e) {
            LOG.debug("Could not inspect missing classpath entry {}", entry, e);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not inspect classpath archive " + entry, e);
        }
    }

    private static Set<String> scanJarResources(Path real, List<String> unwatched) throws IOException {
        JAR_OPEN_COUNT.increment();
        try (JarFile jar = new JarFile(real.toFile())) {
            return Stream.concat(WATCHED_RESOURCES.stream(), unwatched.stream())
                    .filter(resource -> jar.getEntry(resource) != null)
                    .collect(Collectors.toUnmodifiableSet());
        }
    }

    private static boolean isContentAddressedCacheJar(Path path) {
        Path digest = path.getParent();
        Path classpath = digest != null ? digest.getParent() : null;
        Path classpathName = classpath != null ? classpath.getFileName() : null;
        return classpathName != null && "classpath".equals(classpathName.toString())
                && BlobCacheDirectory.SHA256_HEX_PATTERN.matcher(digest.getFileName().toString()).matches();
    }

    private record JarResourceCacheKey(Path realPath, long size, long lastModifiedMillis, Object fileKey,
                                       long creationMillis) {
    }

    static long jarOpenCountForTesting() {
        return JAR_OPEN_COUNT.sum();
    }

    static int jarResourceCacheSizeForTesting() {
        return JAR_RESOURCE_CACHE.size();
    }

    private static Optional<Path> codeSource(String className) {
        return CODE_SOURCE_CACHE.computeIfAbsent(className, ForkedPartitionRunner::resolveCodeSource);
    }

    private static Optional<Path> resolveCodeSource(String className) {
        try {
            Class<?> type = Class.forName(className, false, ForkedPartitionRunner.class.getClassLoader());
            if (type.getProtectionDomain().getCodeSource() == null) {
                return Optional.empty();
            }
            return Optional.of(Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()));
        } catch (ClassNotFoundException | URISyntaxException | IllegalArgumentException e) {
            LOG.debug("Runtime class {} not available for forked partitions", className, e);
            return Optional.empty();
        }
    }

    /**
     * The child's working directory: the host test JVM's working directory relative to its module (for Citrus'
     * Surefire setup {@code target}), reproduced inside the workspace; the workspace root when empty.
     *
     * @throws IllegalArgumentException if the relative path is unsafe
     */
    static Path childWorkingDirectory(Path workspace, String relative) throws IOException {
        if (relative == null || relative.isBlank()) {
            return workspace;
        }
        Path directory = workspace.resolve(WorkspacePackager.validateRelativePath(relative));
        Path link = BlobCacheDirectory.findSymlinkComponent(workspace, directory);
        if (link != null) {
            throw new IllegalArgumentException("Working directory '" + relative + "' passes through a link: " + link);
        }
        return Files.createDirectories(directory);
    }

    /**
     * Fixed system properties of every child JVM. The worker role keeps Citrus' own launcher interceptor passive in
     * the child; launcher interceptors are not switched off, since modules enable them for their frameworks (e.g.
     * {@code quarkus-junit5-config}), exactly as Surefire runs them.
     */
    static final List<String> CHILD_SYSTEM_PROPERTIES = List.of(
            "-D" + DistributionSettings.ROLE_PROPERTY + "=" + DistributionSettings.ROLE_WORKER);

    private Process launch(Path argfile, Path request, int port, Path workspace, Path workingDirectory, Path tmpDir)
            throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin",
                File.separatorChar == '\\' ? "java.exe" : "java").toString();
        List<String> command = new ArrayList<>();
        command.add(java);
        command.add("@" + argfile);
        command.addAll(config.forkJvmArgs());
        command.add("-Djava.io.tmpdir=" + tmpDir);
        command.addAll(CHILD_SYSTEM_PROPERTIES);
        command.add("-Dbasedir=" + workspace);
        command.add(ForkedPartitionMain.class.getName());
        command.add(request.toString());
        command.add(String.valueOf(port));
        return new ProcessBuilder(command).directory(workingDirectory.toFile()).start();
    }

    /**
     * Accepts connections until the child authenticates or the handshake times out, then reads events until the
     * result line. Returns {@code null} when the child never reports a result.
     */
    private ForkedPartitionMain.ForkedExecutionSummary runProtocol(ServerSocket server, ActiveFork fork, String token,
                                                                   CancellationToken cancellationToken,
                                                                   Consumer<WorkerTestEventPayload> eventSink) throws IOException {
        fork.drains.add(drain(fork.process.getInputStream(), fork, "stdout"));
        fork.drains.add(drain(fork.process.getErrorStream(), fork, "stderr"));

        AuthenticatedChild authenticated = awaitAuthenticatedChild(server, fork, token, cancellationToken);
        if (authenticated == null) {
            return null;
        }
        try (Socket child = authenticated.socket(); BufferedReader reader = authenticated.reader()) {
            child.setSoTimeout(0);
            String line;
            while ((line = readLine(reader, MAX_EVENT_LINE)) != null) {
                if (line.startsWith(ForkedPartitionMain.EVENT)) {
                    eventSink.accept(DistributionJson.fromJson(line.substring(ForkedPartitionMain.EVENT.length()),
                            WorkerTestEventPayload.class));
                } else if (line.startsWith(ForkedPartitionMain.RESULT)) {
                    return DistributionJson.fromJson(line.substring(ForkedPartitionMain.RESULT.length()),
                            ForkedPartitionMain.ForkedExecutionSummary.class);
                }
            }
        } catch (IOException e) {
            LOG.debug("Lost connection to forked partition JVM", e);
        } catch (RuntimeException e) {
            LOG.warn("Malformed message from forked partition JVM", e);
            fork.protocolError = e.getMessage();
        }
        return null;
    }

    private AuthenticatedChild awaitAuthenticatedChild(ServerSocket server, ActiveFork fork, String token,
                                           CancellationToken cancellationToken) throws IOException {
        long deadline = System.currentTimeMillis() + handshakeTimeoutMs;
        server.setSoTimeout(200);
        boolean lastChance = false;
        while (System.currentTimeMillis() < deadline) {
            if (cancelled(cancellationToken)) {
                return null;
            }
            if (!fork.process.isAlive()) {
                if (lastChance) {
                    return null;
                }
                // The child may have connected and exited quickly: take what is queued before giving up.
                lastChance = true;
            }
            Socket socket;
            try {
                socket = server.accept();
            } catch (SocketTimeoutException e) {
                continue;
            }
            try {
                long remaining = Math.max(1L, deadline - System.currentTimeMillis());
                socket.setSoTimeout((int) Math.min(5_000L, remaining));
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                String hello = readLine(reader, MAX_HELLO_LINE);
                if ((ForkedPartitionMain.HELLO + token).equals(hello)) {
                    return new AuthenticatedChild(socket, reader);
                }
            } catch (IOException e) {
                LOG.debug("Rejected connection on forked partition port", e);
            }
            socket.close();
        }
        fork.timedOut = true;
        return null;
    }

    /**
     * Reads one line of at most {@code maxChars} characters, or {@code null} at end of stream.
     */
    private static String readLine(BufferedReader reader, int maxChars) throws IOException {
        StringBuilder line = new StringBuilder();
        int c;
        while ((c = reader.read()) != -1) {
            if (c == '\n') {
                return line.toString();
            }
            if (line.length() >= maxChars) {
                throw new IOException("Line from forked partition JVM exceeds " + maxChars + " characters");
            }
            line.append((char) c);
        }
        // An unterminated final line is a truncated message: treat it as end of stream.
        return null;
    }

    private static Thread drain(InputStream stream, ActiveFork fork, String name) {
        Thread thread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, Charset.defaultCharset()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    LOG.debug("[fork {}] {}", name, line);
                    fork.remember(line);
                }
            } catch (IOException ignored) {
                // Process ended.
            }
        }, "citrus-dist-fork-" + name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Until the partition is cleaned up: remembers every process the child starts (they are re-parented, and so no
     * longer its descendants, once the child exits) and kills the tree as soon as the partition is cancelled.
     */
    private static Thread processWatcher(ActiveFork fork, CancellationToken cancellationToken) {
        Thread watcher = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    fork.seen.removeIf(handle -> !handle.isAlive());
                    if (fork.process.isAlive()) {
                        fork.process.toHandle().descendants().forEach(fork.seen::add);
                    }
                    if (cancelled(cancellationToken)) {
                        fork.killAll();
                        return;
                    }
                    Thread.sleep(100);
                }
            } catch (InterruptedException ignored) {
                // Partition finished.
            }
        }, "citrus-dist-fork-watch");
        watcher.setDaemon(true);
        watcher.start();
        return watcher;
    }

    private static void awaitExit(ActiveFork fork, long timeoutMs) throws InterruptedException {
        if (!fork.process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            fork.killAll();
        }
    }

    private String crashDiagnostic(ActiveFork fork) throws InterruptedException {
        if (fork.timedOut) {
            fork.killAll();
            fork.joinDrains();
            return "Forked partition JVM did not connect within " + handshakeTimeoutMs + " ms" + fork.tail();
        }
        String protocol = fork.protocolError != null ? " (malformed message: " + fork.protocolError + ")" : "";
        if (!fork.process.waitFor(5, TimeUnit.SECONDS)) {
            fork.killAll();
            fork.joinDrains();
            return "Forked partition JVM closed its connection without reporting a result" + protocol + fork.tail();
        }
        fork.joinDrains();
        return "Forked partition JVM exited with code " + fork.process.exitValue() + " before reporting a result"
                + protocol + fork.tail();
    }

    /**
     * Terminates {@code handles}: graceful destroy in the given order, then forcibly for survivors.
     */
    static void destroyAll(List<ProcessHandle> handles) {
        List<ProcessHandle> alive = handles.stream().filter(ProcessHandle::isAlive).toList();
        if (alive.isEmpty()) {
            return;
        }
        alive.forEach(ProcessHandle::destroy);
        waitForExit(alive, 2_000L);
        alive.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
        waitForExit(alive, 3_000L);
    }

    private static void waitForExit(List<ProcessHandle> handles, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        for (ProcessHandle handle : handles) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return;
            }
            try {
                handle.onExit().get(remaining, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
                // Timed out or interrupted: the caller escalates.
            }
        }
    }

    private static Path createOwnerOnlyDirectory(Path dir) throws IOException {
        Files.createDirectories(dir);
        if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
        }
        return dir;
    }

    private static void writeOwnerOnly(Path file, String content, Charset charset) throws IOException {
        Files.writeString(file, content, charset);
        if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
        }
    }

    /**
     * The authenticated child connection. The reader used for the handshake must keep being used: it may already
     * have buffered the lines that follow.
     */
    private record AuthenticatedChild(Socket socket, BufferedReader reader) {
    }

    /**
     * Resources of one running fork, cleaned up when the partition ends or the worker JVM shuts down.
     */
    private static final class ActiveFork {
        private WorkerWorkspaceRegistry.Workspace workspace;
        private volatile Path partitionRoot;
        private volatile Path tmpDir;
        private volatile Process process;
        private volatile Thread watcher;
        private volatile boolean timedOut;
        private volatile String protocolError;
        private final Set<ProcessHandle> seen = ConcurrentHashMap.newKeySet();
        private final List<Thread> drains = new CopyOnWriteArrayList<>();
        private final Deque<String> tail = new ArrayDeque<>();
        /** Serializes private path creation and writes with cleanup. */
        private final Object cleanupMonitor = new Object();

        synchronized void remember(String line) {
            tail.addLast(line);
            while (tail.size() > STDERR_TAIL_LINES) {
                tail.removeFirst();
            }
        }

        synchronized String tail() {
            return tail.isEmpty() ? "" : System.lineSeparator() + String.join(System.lineSeparator(), tail);
        }

        /**
         * Kills the child, its current descendants and every process seen under it earlier (now possibly orphaned).
         * All are signalled before waiting, so their order does not matter. {@link ProcessHandle} identity includes
         * the start time, so a reused PID is not hit.
         */
        void killAll() {
            List<ProcessHandle> handles = new ArrayList<>();
            if (process != null) {
                process.toHandle().descendants().forEach(seen::add);
                handles.addAll(seen);
                handles.add(process.toHandle());
            }
            destroyAll(handles);
        }

        void joinDrains() throws InterruptedException {
            for (Thread drain : drains) {
                drain.join(1_000L);
            }
        }

        void cleanUp() {
            synchronized (cleanupMonitor) {
                Thread watching = watcher;
                if (watching != null) {
                    watching.interrupt();
                }
                killAll();
                if (workspace != null) {
                    try {
                        workspace.close();
                    } catch (IOException e) {
                        LOG.debug("Could not clean up partition workspace {}", partitionRoot, e);
                    }
                }
            }
        }
    }
}
