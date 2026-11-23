/*
 * Copyright 2006-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ownership records for private partition directories, kept under the worker's exclusively locked cache.
 * Recovery uses only entries from this registry and leaves directories of active or uncertain ownership alone.
 */
final class WorkerWorkspaceRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerWorkspaceRegistry.class);
    private static final Path SHORT_TEMP = Path.of("/tmp");
    private static final String OWNER_FILE = ".citrus-workspace-owner";
    private static final int MAX_RECORD_SIZE = 4096;
    private static final Pattern ID = Pattern.compile("[0-9a-f]{13}");
    private static final Pattern TOKEN = Pattern.compile("[0-9a-f]{32}");
    private static final Pattern DIGEST = Pattern.compile("[0-9a-f]{64}");
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final boolean POSIX = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    private final Path journal;
    private final Path tempBase;
    private final Path shortTempBase;
    private final String tempKey;
    private final String shortTempKey;
    private final String processScope;
    private final ProcessIdentity worker;

    WorkerWorkspaceRegistry(Path cacheRoot) throws IOException {
        journal = cacheRoot.toRealPath().resolve(".workspaces");
        try {
            Files.createDirectory(journal, privatePermissions("rwx------"));
        } catch (FileAlreadyExistsException ignored) {
            // The cache's previous owner may have left records for recovery.
        }
        if (!isCanonicalDirectory(journal) || (POSIX && !Files.getPosixFilePermissions(journal, LinkOption.NOFOLLOW_LINKS)
                .equals(PosixFilePermissions.fromString("rwx------")))) {
            throw new IOException("Invalid workspace registry directory: " + journal);
        }
        tempBase = Path.of(System.getProperty("java.io.tmpdir")).toRealPath();
        tempKey = digest(tempBase.toString());
        shortTempBase = shortTempBase();
        shortTempKey = shortTempBase == null ? "" : digest(shortTempBase.toString());
        processScope = processScope();
        worker = ProcessIdentity.of(ProcessHandle.current());
    }

    void recover() throws IOException {
        try (var entries = Files.newDirectoryStream(journal)) {
            for (Path entry : entries) {
                String id = entry.getFileName().toString();
                if (!ID.matcher(id).matches()) {
                    continue;
                }
                try {
                    Record record = readRecord(entry);
                    if (record != null && tempKey.equals(record.tempKey())
                            && (record.shortTempKey().isEmpty() || shortTempKey.equals(record.shortTempKey()))
                            && !processScope.isEmpty() && processScope.equals(record.processScope())
                            && record.worker().isGone() && record.canRecover()) {
                        removeOwnedDirectories(id, record, null, null);
                    }
                } catch (IOException | RuntimeException e) {
                    LOG.debug("Could not recover partition workspace {}", id, e);
                }
            }
        }
    }

    Workspace create(boolean forked) throws IOException {
        Workspace workspace = reserve(forked);
        try {
            Path root = workspace.root();
            Files.createDirectory(root, privatePermissions("rwx------"));
            workspace.rootIdentity = DirectoryIdentity.of(root);
            writeOwnerFile(root, workspace.record.token());
            if (forked && !workspace.record.shortTempKey().isEmpty()) {
                Path shortTemp = shortTempBase.resolve("cdtmp-" + workspace.id);
                try {
                    Files.createDirectory(shortTemp, privatePermissions("rwx------"));
                } catch (IOException e) {
                    LOG.debug("Cannot allocate a short forked partition temp directory", e);
                    workspace.publish(workspace.record.withoutShortTemp());
                }
                if (!workspace.record.shortTempKey().isEmpty()) {
                    workspace.shortTempIdentity = DirectoryIdentity.of(shortTemp);
                    writeOwnerFile(shortTemp, workspace.record.token());
                }
            }
            if (forked && workspace.record.shortTempKey().isEmpty()) {
                Files.createDirectory(workspace.childTmpDir(), privatePermissions("rwx------"));
            }
            return workspace;
        } catch (IOException | RuntimeException e) {
            try {
                workspace.close();
            } catch (IOException | RuntimeException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    private Workspace reserve(boolean forked) throws IOException {
        for (;;) {
            String id = randomHex(7).substring(0, 13);
            Path entry = journal.resolve(id);
            try {
                Files.createFile(entry, privatePermissions("rw-------"));
            } catch (FileAlreadyExistsException ignored) {
                continue;
            }
            Record record = new Record(randomHex(16), tempKey, forked ? shortTempKey : "", processScope,
                    worker, LaunchState.PREPARING, null);
            try {
                writeRecord(entry, record);
            } catch (IOException | RuntimeException e) {
                try {
                    Files.deleteIfExists(entry);
                } catch (IOException cleanup) {
                    e.addSuppressed(cleanup);
                }
                throw e;
            }
            return new Workspace(id, record, forked);
        }
    }

    private void removeOwnedDirectories(String id, Record record, DirectoryIdentity rootIdentity,
                                        DirectoryIdentity shortTempIdentity) throws IOException {
        Path entry = journal.resolve(id);
        boolean tempRemoved = true;
        if (!record.shortTempKey().isEmpty()) {
            tempRemoved = deleteOwnedDirectory(shortTempBase.resolve("cdtmp-" + id), entry, record.token(), shortTempIdentity);
        }
        boolean rootRemoved = deleteOwnedDirectory(tempBase.resolve("cdist-" + id), entry, record.token(), rootIdentity);
        if (tempRemoved && rootRemoved) {
            Files.deleteIfExists(entry);
        }
    }

    /**
     * Keeps the marker until the directory contents are gone, so a partial delete is retryable.
     * The workspace is private, and callers have already ruled out a live owner.
     */
    private static boolean deleteOwnedDirectory(Path root, Path entry, String token, DirectoryIdentity created)
            throws IOException {
        Path marker = root.resolve(OWNER_FILE);
        try {
            if (!isCanonicalDirectory(root)) {
                return false;
            }
        } catch (NoSuchFileException e) {
            return Files.notExists(root, LinkOption.NOFOLLOW_LINKS);
        }
        DirectoryIdentity current = DirectoryIdentity.of(root);
        UserPrincipal owner = current.owner();
        if (!owner.equals(Files.getOwner(entry, LinkOption.NOFOLLOW_LINKS))) {
            return false;
        }
        if (Files.notExists(marker, LinkOption.NOFOLLOW_LINKS)) {
            // A test can clear its scratch directory. Only its allocator can verify the original directory's identity.
            if (created == null || created.fileKey() == null || !created.equals(current)) {
                return false;
            }
            writeOwnerFile(root, token);
        } else if (!Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS)
                || !owner.equals(Files.getOwner(marker, LinkOption.NOFOLLOW_LINKS))
                || !token.equals(readSmallFile(marker, 32))) {
            return false;
        }
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (!isCanonicalDirectory(dir)) {
                    throw new IOException("Invalid directory in partition workspace: " + dir);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!marker.equals(file)) {
                    Files.delete(file);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException error) throws IOException {
                if (error != null) {
                    throw error;
                }
                if (!root.equals(dir)) {
                    Files.delete(dir);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        BasicFileAttributes original = Files.readAttributes(root, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        Files.delete(marker);
        try {
            Files.delete(root);
        } catch (IOException e) {
            // An open handle can prevent removal of an otherwise empty directory, particularly on Windows.
            try {
                BasicFileAttributes remaining = Files.readAttributes(root, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (remaining.isDirectory() && remaining.creationTime().equals(original.creationTime())
                        && Objects.equals(remaining.fileKey(), original.fileKey()) && isCanonicalDirectory(root)
                        && owner.equals(Files.getOwner(root, LinkOption.NOFOLLOW_LINKS))) {
                    writeOwnerFile(root, token);
                }
            } catch (IOException restore) {
                e.addSuppressed(restore);
            }
            throw e;
        }
        return true;
    }

    private static boolean isCanonicalDirectory(Path path) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        return attrs.isDirectory() && path.equals(path.toRealPath());
    }

    private static void writeOwnerFile(Path root, String token) throws IOException {
        Path marker = root.resolve(OWNER_FILE);
        Files.createFile(marker, privatePermissions("rw-------"));
        try {
            Files.writeString(marker, token);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(marker);
            } catch (IOException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    private static String readSmallFile(Path file, int limit) throws IOException {
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Workspace record is not a regular file: " + file);
        }
        try (InputStream in = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            byte[] data = in.readNBytes(limit + 1);
            if (data.length > limit) {
                throw new IOException("Workspace record is too large: " + file);
            }
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    private void writeRecord(Path entry, Record record) throws IOException {
        Path staging = Files.createTempFile(journal, ".record-", ".tmp", privatePermissions("rw-------"));
        try {
            Properties properties = new Properties();
            properties.setProperty("version", "1");
            properties.setProperty("token", record.token());
            properties.setProperty("temp", record.tempKey());
            properties.setProperty("shortTemp", record.shortTempKey());
            properties.setProperty("scope", record.processScope());
            properties.setProperty("state", record.state().name());
            record.worker().write(properties, "worker");
            if (record.child() != null) {
                record.child().write(properties, "child");
            }
            try (var out = Files.newOutputStream(staging)) {
                properties.store(out, null);
            }
            Files.move(staging, entry, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(staging);
        }
    }

    private static Record readRecord(Path entry) throws IOException {
        Properties properties = new Properties();
        properties.load(new StringReader(readSmallFile(entry, MAX_RECORD_SIZE)));
        String version = properties.getProperty("version");
        String token = properties.getProperty("token", "");
        String temp = properties.getProperty("temp", "");
        String shortTemp = properties.getProperty("shortTemp");
        if (!"1".equals(version) || !TOKEN.matcher(token).matches() || !DIGEST.matcher(temp).matches()
                || shortTemp == null || (!shortTemp.isEmpty() && !DIGEST.matcher(shortTemp).matches())) {
            return null;
        }
        LaunchState state = LaunchState.valueOf(properties.getProperty("state", ""));
        String scope = properties.getProperty("scope");
        if (scope == null || properties.size() != (state == LaunchState.LAUNCHED ? 10 : 8)) {
            return null;
        }
        return new Record(token, temp, shortTemp, scope,
                ProcessIdentity.read(properties, "worker"), state,
                state == LaunchState.LAUNCHED ? ProcessIdentity.read(properties, "child") : null);
    }

    private static Path shortTempBase() {
        if (POSIX) {
            try {
                Path path = SHORT_TEMP.toRealPath();
                if (Files.isDirectory(path) && Files.isWritable(path)) {
                    return path;
                }
            } catch (IOException ignored) {
                // The partition's private workspace will supply the fallback.
            }
        }
        return null;
    }

    private static FileAttribute<?>[] privatePermissions(String permissions) {
        if (!POSIX) {
            return new FileAttribute<?>[0];
        }
        return new FileAttribute<?>[] { PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(permissions)) };
    }

    private static String processScope() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (!os.contains("linux")) {
            return os;
        }
        try {
            String boot = Files.readString(Path.of("/proc/sys/kernel/random/boot_id")).strip();
            Path namespace = Files.readSymbolicLink(Path.of("/proc/self/ns/pid"));
            return boot.isEmpty() ? "" : boot + ":" + namespace;
        } catch (IOException | RuntimeException e) {
            // A PID from a separate namespace or host cannot safely be judged against /proc here.
            return "";
        }
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
        }
    }

    private static String randomHex(int length) {
        byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    final class Workspace implements AutoCloseable {
        private final String id;
        private final boolean forked;
        private Record record;
        private ProcessHandle child;
        private DirectoryIdentity rootIdentity;
        private DirectoryIdentity shortTempIdentity;

        private Workspace(String id, Record record, boolean forked) {
            this.id = id;
            this.record = record;
            this.forked = forked;
        }

        Path root() {
            return tempBase.resolve("cdist-" + id);
        }

        Path childTmpDir() {
            if (!forked) {
                throw new IllegalStateException("This workspace has no child JVM");
            }
            return record.shortTempKey().isEmpty() ? root().resolve("t") : SHORT_TEMP.resolve("cdtmp-" + id);
        }

        synchronized void markLaunching() throws IOException {
            publish(record.withLaunch(LaunchState.LAUNCHING, null));
        }

        synchronized void markLaunched(ProcessHandle process) throws IOException {
            child = process;
            record = record.withLaunch(LaunchState.LAUNCHED, ProcessIdentity.of(process));
            writeRecord(journal.resolve(id), record);
        }

        synchronized void markLaunchFailed() throws IOException {
            record = record.withLaunch(LaunchState.PREPARING, null);
            writeRecord(journal.resolve(id), record);
        }

        private void publish(Record updated) throws IOException {
            writeRecord(journal.resolve(id), updated);
            record = updated;
        }

        @Override
        public synchronized void close() throws IOException {
            if (record.state() == LaunchState.LAUNCHING || (child != null && child.isAlive())) {
                return;
            }
            int maxAttempts = forked ? 5 : 1;
            for (int attempt = 1; ; attempt++) {
                try {
                    removeOwnedDirectories(id, record, rootIdentity, shortTempIdentity);
                    return;
                } catch (IOException e) {
                    if (attempt == maxAttempts) {
                        throw e;
                    }
                    try {
                        Thread.sleep(200L);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        e.addSuppressed(interrupted);
                        throw e;
                    }
                }
            }
        }
    }

    private enum LaunchState {
        PREPARING, LAUNCHING, LAUNCHED
    }

    private record Record(String token, String tempKey, String shortTempKey, String processScope,
                          ProcessIdentity worker, LaunchState state, ProcessIdentity child) {

        Record withLaunch(LaunchState updatedState, ProcessIdentity updatedChild) {
            return new Record(token, tempKey, shortTempKey, processScope, worker, updatedState, updatedChild);
        }

        Record withoutShortTemp() {
            return new Record(token, tempKey, "", processScope, worker, state, child);
        }

        boolean canRecover() {
            return state == LaunchState.PREPARING || (state == LaunchState.LAUNCHED && child.isGone());
        }
    }

    private record DirectoryIdentity(Object fileKey, FileTime createdAt, UserPrincipal owner) {

        static DirectoryIdentity of(Path dir) throws IOException {
            BasicFileAttributes attrs = Files.readAttributes(dir, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return new DirectoryIdentity(attrs.fileKey(), attrs.creationTime(), Files.getOwner(dir, LinkOption.NOFOLLOW_LINKS));
        }
    }

    private record ProcessIdentity(long pid, Instant startedAt) {

        static ProcessIdentity of(ProcessHandle process) {
            return new ProcessIdentity(process.pid(), process.info().startInstant().orElse(null));
        }

        static ProcessIdentity read(Properties properties, String prefix) {
            long pid = Long.parseLong(properties.getProperty(prefix + "Pid", ""));
            if (pid <= 0) {
                throw new IllegalArgumentException("Invalid workspace process ID");
            }
            String start = properties.getProperty(prefix + "Start");
            if (start == null) {
                throw new IllegalArgumentException("Missing workspace process start time");
            }
            return new ProcessIdentity(pid, start.isEmpty() ? null : Instant.parse(start));
        }

        void write(Properties properties, String prefix) {
            properties.setProperty(prefix + "Pid", Long.toString(pid));
            properties.setProperty(prefix + "Start", startedAt == null ? "" : startedAt.toString());
        }

        boolean isGone() {
            return ProcessHandle.of(pid).filter(ProcessHandle::isAlive).map(process ->
                    startedAt != null && process.info().startInstant().filter(started -> !startedAt.equals(started)).isPresent())
                    .orElse(true);
        }
    }
}
