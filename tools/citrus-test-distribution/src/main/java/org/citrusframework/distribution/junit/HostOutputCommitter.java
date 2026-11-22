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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects output files produced by remote partitions and writes them into the module directory all-or-nothing.
 * <p>
 * Outputs are extracted into {@code <module>/target/.citrus-dist-outputs/tx-<uuid>/staged/<partition>/} while
 * partitions complete. The same file produced with identical content by several partitions is written once; different
 * content is a conflict, and then nothing is written. {@link #commitOrRollback()} validates every destination first,
 * then backs up files it replaces into {@code tx-<uuid>/backup/} and moves the staged files into place, undoing
 * everything (restoring backups, removing created files and directories) if any step fails. Commits to the same module
 * are serialized within the JVM and, through a file lock, across JVMs. Nothing outside {@code target/} is written
 * except the output files themselves.
 */
final class HostOutputCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(HostOutputCommitter.class);
    private static final Map<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();

    private record Origin(String relativePath, String partitionId, String sha256, Path stagedFile) {
    }

    private sealed interface UndoAction permits DeleteCreatedFile, RestoreBackup, DeleteCreatedDir {
    }

    private record DeleteCreatedFile(Path file) implements UndoAction {
    }

    private record RestoreBackup(Path destination, Path backup) implements UndoAction {
    }

    private record DeleteCreatedDir(Path dir) implements UndoAction {
    }

    /** Module-relative directories that remote outputs may never write into, whatever is declared. */
    private static final List<String> INTERNAL_PATHS = List.of("target/.citrus-dist-outputs", "target/.citrus-dist");

    private final Path hostBaseDir;
    private final Path outputsDir;
    private final Path txDir;
    private final List<String> declaredOutputs;
    private final List<String> excludedPaths;
    /** Staged files keyed by their case-folded relative path, so names differing only in case collide here too. */
    private final Map<String, Origin> files = new TreeMap<>();
    private final List<String> conflicts = new ArrayList<>();
    private Predicate<Path> failBeforeMove = path -> false;
    private int stagingCounter;

    /**
     * @param hostBaseDir     the module directory
     * @param declaredOutputs module-relative output paths; remote files outside them are rejected
     */
    HostOutputCommitter(Path hostBaseDir, List<String> declaredOutputs) {
        this(hostBaseDir, declaredOutputs, List.of());
    }

    /**
     * @param excludedPaths further module-relative paths remote outputs may not write into (e.g. a host cache)
     */
    HostOutputCommitter(Path hostBaseDir, List<String> declaredOutputs, List<String> excludedPaths) {
        this.hostBaseDir = hostBaseDir.toAbsolutePath().normalize();
        this.outputsDir = this.hostBaseDir.resolve("target").resolve(".citrus-dist-outputs");
        this.txDir = outputsDir.resolve("tx-" + UUID.randomUUID());
        this.declaredOutputs = List.copyOf(declaredOutputs);
        List<String> excluded = new ArrayList<>(INTERNAL_PATHS);
        excluded.addAll(excludedPaths);
        this.excludedPaths = List.copyOf(excluded);
    }

    /**
     * Stages the output files of one partition (blobs must already be in {@code cache}) and records conflicts with
     * files staged from other partitions.
     */
    synchronized void recordPartitionOutputs(String partitionId, List<WorkspaceFileSet> outputArtifacts,
                                             BlobCacheDirectory cache) throws IOException {
        if (outputArtifacts == null || outputArtifacts.isEmpty()) {
            return;
        }
        Path partitionDir = txDir.resolve("staged").resolve(stagingCounter++ + "-" + safeName(partitionId));
        for (WorkspaceFileSet output : outputArtifacts) {
            for (WorkspacePackager.MaterializedEntry entry : WorkspacePackager.materializeIntoDirectory(cache, output, partitionDir)) {
                String relative = entry.workspaceRelativePath();
                checkAllowed(relative, partitionId);
                String key = relative.toLowerCase(Locale.ROOT);
                Origin existing = files.get(key);
                if (existing == null) {
                    files.put(key, new Origin(relative, partitionId, entry.sha256(), entry.extractedFile()));
                } else if (!existing.relativePath().equals(relative)) {
                    conflicts.add("Conflicting output files '" + existing.relativePath() + "' and '" + relative
                            + "' (names differ only in case) produced by partitions " + existing.partitionId() + " and " + partitionId);
                } else if (!existing.sha256().equals(entry.sha256())) {
                    conflicts.add("Conflicting output file '" + relative + "' produced with different content by partitions "
                            + existing.partitionId() + " and " + partitionId);
                }
            }
        }
    }

    /**
     * Remote files may only land inside a declared output path and never in the committer's own or the host cache's
     * directories: workers run code from other builds and must not be able to plant, say, a pom.xml.
     */
    private void checkAllowed(String relative, String partitionId) throws IOException {
        String folded = relative.toLowerCase(Locale.ROOT);
        for (String excluded : excludedPaths) {
            // Case-insensitive: on macOS and Windows a differently cased name is the same directory.
            if (isWithin(folded, excluded.toLowerCase(Locale.ROOT))) {
                throw new IOException("Output '" + relative + "' from partition " + partitionId
                        + " targets an internal directory (" + excluded + ")");
            }
        }
        if (declaredOutputs.stream().noneMatch(declared -> isWithin(relative, declared))) {
            throw new IOException("Output '" + relative + "' from partition " + partitionId
                    + " is outside the declared outputs " + declaredOutputs);
        }
    }

    private static boolean isWithin(String relative, String root) {
        return relative.equals(root) || relative.startsWith(root + "/");
    }

    /**
     * All conflicts found so far, one per line, or empty.
     */
    synchronized Optional<String> conflictError() {
        return conflicts.isEmpty() ? Optional.empty() : Optional.of(String.join(System.lineSeparator(), new TreeSet<>(conflicts)));
    }

    /**
     * Writes all staged files into the module directory, or nothing.
     *
     * @throws IllegalStateException if conflicts were recorded (nothing is written)
     * @throws IOException           if validation or writing fails; any partial changes have been undone
     */
    synchronized void commitOrRollback() throws IOException {
        try {
            Optional<String> conflict = conflictError();
            if (conflict.isPresent()) {
                throw new IllegalStateException(conflict.get());
            }
            if (files.isEmpty()) {
                return;
            }
            Files.createDirectories(outputsDir);
            Path lockFile = outputsDir.resolve("commit.lock");
            try (FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                // The in-JVM lock comes first: two FileChannel locks on one file in one JVM would throw.
                ReentrantLock jvmLock = JVM_LOCKS.computeIfAbsent(lockFile.toRealPath(), key -> new ReentrantLock());
                jvmLock.lock();
                try (FileLock ignored = channel.lock()) {
                    validateDestinations();
                    apply();
                } finally {
                    jvmLock.unlock();
                }
            }
        } finally {
            discardStaging();
        }
    }

    /**
     * Deletes the staging area of this commit.
     */
    synchronized void discardStaging() {
        deleteRecursively(txDir);
    }

    /**
     * Test seam: makes the commit fail just before moving a file whose destination matches.
     */
    void failBeforeMove(Predicate<Path> predicate) {
        this.failBeforeMove = predicate;
    }

    /**
     * Phase 1: every destination and its parents must be free of symbolic links, parents must be directories (or
     * absent), and destinations must not be directories, before anything is changed.
     */
    private void validateDestinations() throws IOException {
        for (Origin origin : files.values()) {
            String relative = origin.relativePath();
            Path destination = hostBaseDir.resolve(relative);
            Path current = hostBaseDir;
            for (Path name : hostBaseDir.relativize(destination)) {
                current = current.resolve(name);
                if (BlobCacheDirectory.isLinkLike(current)) {
                    throw new IOException("Symbolic link or junction on output path '" + relative + "': " + current);
                }
                boolean isLast = current.equals(destination);
                if (!isLast && Files.exists(current, LinkOption.NOFOLLOW_LINKS) && !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Cannot write output '" + relative + "': " + current + " is not a directory");
                }
                if (isLast && Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Cannot write output '" + relative + "' over an existing directory");
                }
            }
        }
    }

    /**
     * Phases 2 and 3 with an undo journal replayed in reverse on any failure.
     */
    private void apply() throws IOException {
        Deque<UndoAction> undo = new ArrayDeque<>();
        Path backupDir = Files.createDirectories(txDir.resolve("backup"));
        int backups = 0;
        try {
            for (Origin origin : files.values()) {
                Path destination = hostBaseDir.resolve(origin.relativePath());
                createParents(destination.getParent(), undo);
                Path link = linkOnPath(destination);
                if (link != null) {
                    throw new IOException("Symbolic link or junction appeared on output path: " + link);
                }
                if (failBeforeMove.test(destination)) {
                    throw new IOException("Injected failure before writing " + destination);
                }
                Path backup = null;
                if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                    // Numbered, not mirrored, so backups cannot collide on case-insensitive file systems.
                    backup = backupDir.resolve(Integer.toString(backups++));
                    move(destination, backup);
                    undo.push(new RestoreBackup(destination, backup));
                }
                // Recorded before the move: a non-atomic move may copy and then fail to delete its source.
                undo.push(new DeleteCreatedFile(destination));
                move(origin.stagedFile(), destination);
                if (backup != null) {
                    copyPermissions(backup, destination);
                }
            }
        } catch (IOException | RuntimeException | Error failure) {
            rollback(undo, failure);
            throw failure;
        }
    }

    private void createParents(Path dir, Deque<UndoAction> undo) throws IOException {
        List<Path> missing = new ArrayList<>();
        for (Path current = dir; current != null && !current.equals(hostBaseDir)
                && !Files.exists(current, LinkOption.NOFOLLOW_LINKS); current = current.getParent()) {
            missing.add(0, current);
        }
        for (Path created : missing) {
            Files.createDirectory(created);
            undo.push(new DeleteCreatedDir(created));
        }
    }

    private Path linkOnPath(Path destination) {
        Path current = hostBaseDir;
        for (Path name : hostBaseDir.relativize(destination)) {
            current = current.resolve(name);
            if (BlobCacheDirectory.isLinkLike(current)) {
                return current;
            }
        }
        return null;
    }

    private static void copyPermissions(Path from, Path to) {
        try {
            Files.setPosixFilePermissions(to, Files.getPosixFilePermissions(from, LinkOption.NOFOLLOW_LINKS));
        } catch (UnsupportedOperationException | IOException e) {
            LOG.debug("Could not keep permissions of {}", to, e);
        }
    }

    private static void rollback(Deque<UndoAction> undo, Throwable failure) {
        while (!undo.isEmpty()) {
            UndoAction action = undo.pop();
            try {
                switch (action) {
                    case DeleteCreatedFile delete -> Files.deleteIfExists(delete.file());
                    case RestoreBackup restore -> Files.move(restore.backup(), restore.destination(), StandardCopyOption.REPLACE_EXISTING);
                    case DeleteCreatedDir dir -> Files.deleteIfExists(dir.dir());
                }
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
        }
    }

    private static void move(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static String safeName(String partitionId) {
        return partitionId == null ? "partition" : partitionId.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static void deleteRecursively(Path root) {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    LOG.debug("Could not delete {}", path, e);
                }
            });
        } catch (IOException e) {
            LOG.warn("Could not delete output staging directory {}", root, e);
        }
    }
}
