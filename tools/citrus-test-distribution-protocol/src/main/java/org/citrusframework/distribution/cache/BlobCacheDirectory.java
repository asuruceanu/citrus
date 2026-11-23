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

package org.citrusframework.distribution.cache;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Content-addressed local directory store indexed by validated 64-char lowercase SHA-256 hex digests.
 * <p>
 * Blobs live at {@code <root>/blobs/<first two hex chars>/<sha256>}. Writes go to a temporary file under
 * {@code <root>/tmp} and are moved into place only after their digest has been verified, so a failed or
 * interrupted write never leaves a partial blob or an orphaned temporary file behind.
 */
public class BlobCacheDirectory {

    private static final Logger LOG = LoggerFactory.getLogger(BlobCacheDirectory.class);

    public static final Pattern SHA256_HEX_PATTERN = Pattern.compile("^[a-f0-9]{64}$");

    /**
     * Prefixes {@link #newTempFile(String)} accepts, which are exactly the temporary files {@link #sweepTempFiles()}
     * removes: the first 8 hex characters of a digest being stored ({@link #put}, {@link #putFromFile}), and
     * {@code ws-} (packaged workspace archives), {@code dl-} (worker downloads) and {@code output-} (host output
     * downloads).
     */
    private static final Pattern CACHE_TEMP_PREFIX_PATTERN = Pattern.compile("^(?:[a-f0-9]{8}|ws|dl|output)-$");

    /** Names of the files {@link #newTempFile(String)} creates: an allowed prefix, random digits, {@code .tmp}. */
    private static final Pattern CACHE_TEMP_FILE_PATTERN = Pattern.compile("^(?:[a-f0-9]{8}|ws|dl|output)-[0-9]+\\.tmp$");

    /**
     * File in a cache root naming the component that owns the directory ({@link #OWNER_WORKER},
     * {@link #OWNER_COORDINATOR} or {@link #OWNER_LAUNCHER}), so a worker refuses to adopt another component's cache.
     */
    public static final String OWNER_MARKER_FILE = ".citrus-cache-owner";
    public static final String OWNER_WORKER = "worker";
    public static final String OWNER_COORDINATOR = "coordinator";
    public static final String OWNER_LAUNCHER = "launcher";

    /** Maximum number of entries (files and directories) in one extracted or packaged ZIP archive. */
    public static final int MAX_ZIP_ENTRIES = 50_000;
    /** Maximum total uncompressed bytes of one ZIP archive (2 GiB), counted while streaming. */
    public static final long MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES = 2L * 1024 * 1024 * 1024;
    /** Maximum uncompressed bytes of a single ZIP entry (256 MiB), counted while streaming. */
    public static final long MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES = 256L * 1024 * 1024;

    /**
     * A regular file written by {@link #extractZipSafely(Path, Path)}.
     *
     * @param relativePath {@code /}-separated path relative to the extraction directory
     * @param sha256       digest of the bytes written
     * @param file         absolute path of the written file
     */
    public record ExtractedFile(String relativePath, String sha256, Path file) {
    }

    /**
     * A blob in the store.
     *
     * @param sha256             digest the blob is stored under
     * @param path               absolute path of the blob file
     * @param sizeBytes          size of the blob file
     * @param lastModifiedMillis last modification time of the blob file
     */
    public record BlobEntry(String sha256, Path path, long sizeBytes, long lastModifiedMillis) {
    }

    /** Moves a file; a test seam for file systems without atomic rename. */
    @FunctionalInterface
    interface FileMover {
        void move(Path source, Path target, CopyOption... options) throws IOException;
    }

    private final Path rootDir;
    private volatile FileMover fileMover = Files::move;

    public BlobCacheDirectory(Path rootDir) {
        this.rootDir = rootDir.toAbsolutePath().normalize();
    }

    void fileMover(FileMover fileMover) {
        this.fileMover = Objects.requireNonNull(fileMover);
    }

    /**
     * The recorded owner, or empty when no marker exists (a cache created before markers existed).
     * An incomplete or invalid marker cannot safely be treated as an unclaimed cache.
     */
    public static Optional<String> readOwner(Path root) throws IOException {
        Path marker = root.resolve(OWNER_MARKER_FILE);
        try {
            if (!Files.readAttributes(marker, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).isRegularFile()) {
                throw new IOException("Invalid cache owner marker " + marker);
            }
            String owner = Files.readString(marker, StandardCharsets.UTF_8).strip();
            if (owner.isEmpty()) {
                throw new IOException("Empty cache owner marker " + marker);
            }
            return Optional.of(owner);
        } catch (NoSuchFileException absent) {
            return Optional.empty();
        }
    }

    /**
     * Claims the cache unless an owner is already recorded, without ever replacing the marker.
     *
     * @return the winning owner
     */
    public static String recordOwnerIfAbsent(Path root, String owner) throws IOException {
        Optional<String> existing = readOwner(root);
        if (existing.isPresent()) {
            return existing.get();
        }
        Path marker = Files.createDirectories(root).resolve(OWNER_MARKER_FILE);
        Path staged = Files.createTempFile(root, OWNER_MARKER_FILE + "-", ".tmp");
        String content = owner + "\n";
        try {
            Files.writeString(staged, content, StandardCharsets.UTF_8);
            try {
                // A hard link publishes the entire marker atomically and fails if another owner won the claim.
                Files.createLink(marker, staged);
            } catch (FileAlreadyExistsException concurrent) {
                throw concurrent;
            } catch (IOException | UnsupportedOperationException noHardLink) {
                // Filesystems without hard links still use an exclusive create. A reader seeing a partial write
                // fails closed, and cannot overwrite it or assume that it owns the directory.
                Files.writeString(marker, content, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE);
            }
            return owner;
        } catch (FileAlreadyExistsException concurrent) {
            return readOwner(root).orElseThrow(() -> new IOException("Cache owner marker disappeared: " + marker, concurrent));
        } finally {
            Files.deleteIfExists(staged);
        }
    }

    public Path getRootDir() {
        return rootDir;
    }

    public static void validateDigest(String sha256) {
        if (sha256 == null || !SHA256_HEX_PATTERN.matcher(sha256).matches()) {
            throw new IllegalArgumentException("Invalid SHA-256 digest: " + sha256);
        }
    }

    public Path resolveBlobPath(String sha256) {
        validateDigest(sha256);
        Path resolved = rootDir.resolve("blobs").resolve(sha256.substring(0, 2)).resolve(sha256).normalize();
        if (!resolved.startsWith(rootDir)) {
            throw new IllegalArgumentException("Blob path escapes cache root: " + sha256);
        }
        return resolved;
    }

    /**
     * Resolves a blob path for file operations, rejecting redirected parent directories. The caller decides how
     * to handle the blob entry itself, which may be a symbolic link that needs to be removed.
     */
    public Path resolveBlobPathForAccess(String sha256) throws IOException {
        Path path = resolveBlobPath(sha256);
        requireCacheDirectories(path.getParent());
        return path;
    }

    private void requireCacheDirectories(Path directory) throws IOException {
        Path current = rootDir;
        for (Path name : rootDir.relativize(directory)) {
            current = current.resolve(name);
            try {
                BasicFileAttributes attrs = Files.readAttributes(current, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (!attrs.isDirectory() || attrs.isOther()) {
                    throw new IOException("Cache path is not a real directory: " + current);
                }
            } catch (NoSuchFileException absent) {
                return;
            }
        }
    }

    public boolean contains(String sha256) {
        try {
            return Files.isRegularFile(resolveBlobPathForAccess(sha256), LinkOption.NOFOLLOW_LINKS);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Creates an empty temporary file in this cache's temporary directory, on the same file system as the
     * blob store so it can be moved into place atomically. The caller must delete it when done.
     *
     * @param prefix one of the prefixes {@link #sweepTempFiles()} removes leftovers of: 8 lowercase hex characters
     *               followed by {@code -}, {@code ws-}, {@code dl-} or {@code output-}
     * @throws IllegalArgumentException on any other prefix
     */
    public Path newTempFile(String prefix) throws IOException {
        if (prefix == null || !CACHE_TEMP_PREFIX_PATTERN.matcher(prefix).matches()) {
            throw new IllegalArgumentException("Unsupported cache temporary file prefix '" + prefix
                    + "': it must be one sweepTempFiles() removes");
        }
        Path tmpDir = rootDir.resolve("tmp");
        requireCacheDirectories(tmpDir);
        Files.createDirectories(tmpDir);
        return Files.createTempFile(tmpDir, prefix, ".tmp");
    }

    /**
     * Stores {@code bytes} under {@code sha256} after verifying the digest.
     *
     * @throws IllegalArgumentException on an invalid or mismatching digest, or more than
     *                                  {@link ClasspathPackager#MAX_BLOB_BYTES} bytes
     */
    public Path put(String sha256, byte[] bytes) throws IOException {
        validateDigest(sha256);
        checkBlobSize(bytes.length, sha256);
        String actual = ClasspathPackager.sha256Hex(bytes);
        if (!sha256.equals(actual)) {
            throw new IllegalArgumentException("SHA-256 mismatch: expected " + sha256 + " but got " + actual);
        }
        Path target = resolveBlobPathForAccess(sha256);
        if (Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            return target;
        }
        Path tmp = newTempFile(sha256.substring(0, 8) + "-");
        try {
            Files.write(tmp, bytes);
            moveIntoPlace(tmp, target);
        } finally {
            Files.deleteIfExists(tmp);
        }
        return target;
    }

    /**
     * Stores the content of {@code sourceFile} under {@code expectedSha256}, verifying the digest while copying.
     * A source inside this cache's temporary directory is moved instead of copied; any other source is left
     * untouched and remains owned by the caller.
     *
     * @throws IllegalArgumentException on an invalid or mismatching digest, a source that is not a regular file,
     *                                  or more than {@link ClasspathPackager#MAX_BLOB_BYTES} bytes
     */
    public Path putFromFile(String expectedSha256, Path sourceFile) throws IOException {
        validateDigest(expectedSha256);
        if (!Files.isRegularFile(sourceFile, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Blob source is not a regular file: " + sourceFile);
        }
        checkBlobSize(Files.size(sourceFile), expectedSha256);
        Path target = resolveBlobPathForAccess(expectedSha256);

        Path source = sourceFile.toAbsolutePath().normalize();
        if (rootDir.resolve("tmp").equals(source.getParent())) {
            requireCacheDirectories(source.getParent());
            if (!storeTempBlob(expectedSha256, source, target)) {
                Files.deleteIfExists(source);
            }
            return target;
        }
        if (Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            verifyDigest(expectedSha256, ClasspathPackager.sha256Hex(source));
            return target;
        }

        Path tmp = newTempFile(expectedSha256.substring(0, 8) + "-");
        try {
            MessageDigest digest = ClasspathPackager.newSha256();
            try (InputStream in = new DigestInputStream(Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS), digest);
                 OutputStream out = Files.newOutputStream(tmp, StandardOpenOption.TRUNCATE_EXISTING)) {
                byte[] buffer = new byte[16 * 1024];
                long copied = 0;
                int read;
                while ((read = in.read(buffer)) != -1) {
                    copied += read;
                    checkBlobSize(copied, expectedSha256);
                    out.write(buffer, 0, read);
                }
            }
            verifyDigest(expectedSha256, HexFormat.of().formatHex(digest.digest()));
            moveIntoPlace(tmp, target);
        } finally {
            Files.deleteIfExists(tmp);
        }
        return target;
    }

    /**
     * Verifies a staged cache download and moves it into place. If a regular blob already occupies the target,
     * returns false and leaves the temporary file for the caller to keep or remove.
     */
    public boolean storeTempBlob(String expectedSha256, Path tempFile) throws IOException {
        Path source = tempFile.toAbsolutePath().normalize();
        if (!rootDir.resolve("tmp").equals(source.getParent()) || !Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Blob source is not a regular file in the cache temporary directory: " + source);
        }
        requireCacheDirectories(source.getParent());
        Path target = resolveBlobPathForAccess(expectedSha256);
        checkBlobSize(Files.size(source), expectedSha256);
        return storeTempBlob(expectedSha256, source, target);
    }

    private boolean storeTempBlob(String expectedSha256, Path source, Path target) throws IOException {
        verifyDigest(expectedSha256, ClasspathPackager.sha256Hex(source));
        if (Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }
        moveIntoPlace(source, target);
        return true;
    }

    /**
     * Returns the blob path for {@code sha256} after re-verifying its content, or {@code null} when the blob is
     * absent or corrupted. A corrupted blob is deleted so the caller can fetch or package it again.
     */
    public Path getVerifiedPath(String sha256) throws IOException {
        Path blob = resolveBlobPathForAccess(sha256);
        if (!Files.isRegularFile(blob, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        try {
            if (isIntact(sha256)) {
                return blob;
            }
        } catch (NoSuchFileException vanished) {
            return null;
        }
        delete(sha256);
        return null;
    }

    /**
     * Re-hashes the blob for {@code sha256} without modifying the store.
     *
     * @return {@code true} if its content matches the digest
     * @throws NoSuchFileException if the blob does not exist
     */
    public boolean isIntact(String sha256) throws IOException {
        Path blob = resolveBlobPathForAccess(sha256);
        MessageDigest digest = ClasspathPackager.newSha256();
        try (InputStream in = new DigestInputStream(Files.newInputStream(blob, LinkOption.NOFOLLOW_LINKS), digest)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        return sha256.equals(HexFormat.of().formatHex(digest.digest()));
    }

    /**
     * Lists the blobs in the store: regular files {@code blobs/<prefix>/<sha256>} whose name is a valid digest
     * starting with the parent directory's two-character prefix. Stray files and symbolic links are ignored.
     */
    public List<BlobEntry> listBlobs() throws IOException {
        Path blobsDir = rootDir.resolve("blobs");
        List<BlobEntry> entries = new ArrayList<>();
        if (!Files.isDirectory(blobsDir, LinkOption.NOFOLLOW_LINKS)) {
            return entries;
        }
        Files.walkFileTree(blobsDir, Set.of(), 2, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                Path parent = file.getParent();
                if (attrs.isRegularFile() && !attrs.isSymbolicLink() && !blobsDir.equals(parent)
                        && SHA256_HEX_PATTERN.matcher(name).matches()
                        && name.startsWith(parent.getFileName().toString())
                        && parent.getFileName().toString().length() == 2) {
                    entries.add(new BlobEntry(name, file, attrs.size(), attrs.lastModifiedTime().toMillis()));
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                LOG.warn("Skipping unreadable cache entry {}: {}", file, exc.toString());
                return FileVisitResult.CONTINUE;
            }
        });
        return entries;
    }

    /**
     * Deletes the blob for {@code sha256} together with its {@code classpath/<sha256>/} directory of named jars, and
     * prunes the {@code blobs/<first two hex chars>/} directory when it is left empty (best effort). The named jars
     * are deleted first, so when that fails the blob is still present and the exception reports that nothing was
     * evicted.
     *
     * @return {@code true} if the blob existed
     * @throws IllegalArgumentException on an invalid digest
     */
    public boolean delete(String sha256) throws IOException {
        Path blob = resolveBlobPathForAccess(sha256);
        deleteNamedJars(sha256);
        boolean existed = Files.deleteIfExists(blob);
        pruneEmptyDirectory(blob.getParent());
        return existed;
    }

    /** Removes the cached classpath names for a digest without touching the canonical blob. */
    public void deleteNamedJars(String sha256) throws IOException {
        validateDigest(sha256);
        Path classpathDir = rootDir.resolve("classpath").resolve(sha256);
        requireCacheDirectories(classpathDir.getParent());
        if (isLinkLike(classpathDir)) {
            Files.deleteIfExists(classpathDir);
        } else {
            deleteRecursively(classpathDir);
        }
    }

    private static void pruneEmptyDirectory(Path dir) {
        try {
            Files.deleteIfExists(dir);
        } catch (DirectoryNotEmptyException | NoSuchFileException ignored) {
            // still shared with other blobs, or already gone
        } catch (IOException e) {
            LOG.debug("Could not prune empty cache directory {}: {}", dir, e.toString());
        }
    }

    /**
     * Removes orphaned {@code classpath/<sha256>/.staging-*.jar} files left by an interrupted publish.
     *
     * @return the number of files removed
     */
    public int sweepClasspathStaging() throws IOException {
        Path classpathRoot = rootDir.resolve("classpath");
        if (!Files.isDirectory(classpathRoot, LinkOption.NOFOLLOW_LINKS)) {
            return 0;
        }
        int removed = 0;
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(classpathRoot)) {
            for (Path dir : dirs) {
                if (!Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
                    continue;
                }
                try (DirectoryStream<Path> staged = Files.newDirectoryStream(dir, ".staging-*.jar")) {
                    for (Path orphan : staged) {
                        if (Files.isRegularFile(orphan, LinkOption.NOFOLLOW_LINKS)
                                && Files.deleteIfExists(orphan)) {
                            removed++;
                        }
                    }
                }
            }
        } catch (DirectoryIteratorException e) {
            throw e.getCause();
        }
        return removed;
    }

    /**
     * Removes temporary files left in {@code tmp/} by interrupted writes, keeping {@code tmp/} itself and never
     * touching blobs, named jars or the lock file. Only regular files whose names match what
     * {@link #newTempFile(String)} creates ({@code <allowed prefix><digits>.tmp}, see {@link #newTempFile(String)})
     * are deleted: the cache directory may be an existing directory chosen by the user, so directories, symbolic links
     * and any other file in {@code tmp/} (such as a {@code backup-2024.tmp}) are left alone. A file that cannot be
     * deleted is logged and skipped.
     *
     * @return the number of files removed
     */
    public int sweepTempFiles() throws IOException {
        Path tmpDir = rootDir.resolve("tmp");
        if (!Files.isDirectory(tmpDir, LinkOption.NOFOLLOW_LINKS)) {
            return 0;
        }
        int removed = 0;
        try (DirectoryStream<Path> children = Files.newDirectoryStream(tmpDir)) {
            for (Path child : children) {
                if (!CACHE_TEMP_FILE_PATTERN.matcher(child.getFileName().toString()).matches()
                        || !Files.isRegularFile(child, LinkOption.NOFOLLOW_LINKS)) {
                    continue;
                }
                try {
                    if (Files.deleteIfExists(child)) {
                        removed++;
                    }
                } catch (IOException e) {
                    LOG.warn("Could not delete temporary cache file {}: {}", child, e.toString());
                }
            }
        } catch (DirectoryIteratorException e) {
            throw e.getCause();
        }
        return removed;
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (attrs.isOther()) {
                    Files.deleteIfExists(dir);
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }
                Files.deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public byte[] readBytes(String sha256) throws IOException {
        Path blob = resolveBlobPathForAccess(sha256);
        if (!Files.isRegularFile(blob, LinkOption.NOFOLLOW_LINKS)) {
            throw new NoSuchFileException(blob.toString());
        }
        return Files.readAllBytes(blob);
    }

    /**
     * Places {@code ref} on the classpath, re-verifying a {@link ArtifactKind#DIRECTORY_ZIP} blob before extracting it
     * (a corrupted blob is deleted).
     *
     * @see #materializeForClasspath(ArtifactRef, Path, Path)
     */
    public Path materializeForClasspath(ArtifactRef ref, Path workspaceDir) throws IOException {
        return materializeForClasspath(ref, null, workspaceDir, true);
    }

    /**
     * Places {@code ref} on the classpath from a blob the caller has already verified, without hashing it again: a
     * {@link ArtifactKind#JAR} becomes its named jar, a {@link ArtifactKind#DIRECTORY_ZIP} is extracted into
     * {@code <workspaceDir>/extracted/<sha256>/}.
     *
     * @param verifiedBlob this cache's blob path for {@code ref}, verified by the caller
     * @throws IllegalArgumentException if {@code verifiedBlob} is not this cache's blob path for {@code ref}
     */
    public Path materializeForClasspath(ArtifactRef ref, Path verifiedBlob, Path workspaceDir) throws IOException {
        return materializeForClasspath(ref, requireBlobPath(ref.sha256(), verifiedBlob), workspaceDir, false);
    }

    /**
     * {@code verifiedBlob} unchanged if it is this cache's blob path for {@code sha256}.
     *
     * @throws IllegalArgumentException otherwise
     */
    Path requireBlobPath(String sha256, Path verifiedBlob) throws IOException {
        Path expected = resolveBlobPathForAccess(sha256);
        if (verifiedBlob == null || !expected.equals(verifiedBlob.toAbsolutePath().normalize())) {
            throw new IllegalArgumentException("Verified blob " + verifiedBlob + " is not the cached blob " + expected);
        }
        return expected;
    }

    private Path materializeForClasspath(ArtifactRef ref, Path verifiedBlob, Path workspaceDir, boolean verify)
            throws IOException {
        if (ref.kind() == ArtifactKind.REGULAR_FILE) {
            throw new IllegalArgumentException("REGULAR_FILE artifact " + ref.name() + " cannot be placed on the classpath");
        }
        Path blobPath = verifiedBlob != null ? verifiedBlob : resolveBlobPathForAccess(ref.sha256());
        if (!Files.isRegularFile(blobPath, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Cached blob does not exist: " + ref.sha256());
        }
        if (ref.kind() == ArtifactKind.JAR) {
            return namedJar(ref, blobPath);
        }

        Path extractDir = workspaceDir.toAbsolutePath().normalize().resolve("extracted").resolve(ref.sha256());
        Path marker = extractDir.resolve(".complete");
        if (Files.isRegularFile(marker)) {
            return extractDir;
        }

        synchronized (this) {
            if (Files.isRegularFile(marker)) {
                return extractDir;
            }
            if (verify && getVerifiedPath(ref.sha256()) == null) {
                throw new IOException("Cached blob " + ref.sha256() + " is corrupted and was evicted");
            }
            Files.createDirectories(extractDir);
            extractZipSafely(blobPath, extractDir);
            Files.writeString(marker, "ok");
            return extractDir;
        }
    }

    /**
     * The cached jar under its original file name ({@code classpath/<sha256>/<name>.jar}, a hard link to the blob, or
     * a copy where links are unsupported), shared by every partition of this worker. Libraries inspect classpath file
     * names: for example a container library may extract a resource from an archive only when its URL contains
     * {@code .jar!}, and would copy the whole archive from a digest-named blob instead.
     */
    private Path namedJar(ArtifactRef ref, Path blobPath) throws IOException {
        String name = ref.name() != null ? Path.of(ref.name().replace('\\', '/')).getFileName().toString() : "";
        name = name.replaceAll("[^A-Za-z0-9._+-]", "_");
        if (name.isBlank() || name.chars().allMatch(c -> c == '.' || c == '_')) {
            name = ref.sha256();
        }
        if (!name.endsWith(".jar")) {
            name = name + ".jar";
        }
        Path dir = rootDir.resolve("classpath").resolve(ref.sha256()).normalize();
        requireCacheDirectories(dir);
        Path named = dir.resolve(name).normalize();
        if (!named.startsWith(rootDir)) {
            throw new IOException("Classpath entry escapes cache root: " + ref.name());
        }
        if (Files.isRegularFile(named, LinkOption.NOFOLLOW_LINKS)) {
            return named;
        }
        synchronized (this) {
            if (Files.isRegularFile(named, LinkOption.NOFOLLOW_LINKS)) {
                return named;
            }
            Files.createDirectories(dir);
            // Stage under a unique name, and treat any failure to publish as fine once a concurrent caller
            // has published the same digest.
            Path staging = Files.createTempFile(dir, ".staging-", ".jar");
            try {
                Files.delete(staging);
                try {
                    Files.createLink(staging, blobPath);
                } catch (UnsupportedOperationException | IOException linkFailed) {
                    Files.copy(blobPath, staging, StandardCopyOption.REPLACE_EXISTING, LinkOption.NOFOLLOW_LINKS);
                }
                if (!Files.isRegularFile(staging, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Cached blob is not a regular file: " + ref.sha256());
                }
                Files.move(staging, named, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException publishFailed) {
                if (!Files.isRegularFile(named, LinkOption.NOFOLLOW_LINKS)) {
                    throw publishFailed;
                }
            } finally {
                Files.deleteIfExists(staging);
            }
            return named;
        }
    }

    /**
     * Extracts {@code zipBlob} into {@code targetDir} with the default limits ({@link #MAX_ZIP_ENTRIES},
     * {@link #MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES}, {@link #MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES}).
     *
     * @see #extractZipSafely(Path, Path, int, long, long)
     */
    public static List<ExtractedFile> extractZipSafely(Path zipBlob, Path targetDir) throws IOException {
        return extractZipSafely(zipBlob, targetDir, MAX_ZIP_ENTRIES, MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES,
                MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES);
    }

    /**
     * Extracts {@code zipBlob} into {@code targetDir} and returns the regular files written, in archive order.
     * <ul>
     *     <li>Every entry name must pass {@link WorkspacePackager#validateRelativePath(String)}, so absolute
     *     paths, drive letters, {@code ..} traversal and reserved device names are rejected.</li>
     *     <li>No existing path component between {@code targetDir} and an entry's destination may be a
     *     symbolic link, and files are opened with {@link LinkOption#NOFOLLOW_LINKS}. Entries are always
     *     written as regular files or directories, so the archive cannot create links itself.</li>
     *     <li>Entry count and uncompressed bytes are counted while streaming; declared sizes are never trusted.</li>
     *     <li>Directory entries must be empty, duplicate names and names colliding on a case-insensitive file
     *     system are rejected, so every returned digest describes the file actually on disk.</li>
     * </ul>
     * The archive's digest is not checked here: callers must pass a verified blob (see
     * {@link #getVerifiedPath(String)}), because a truncated or non-ZIP file may extract as empty or partial.
     * On failure, files extracted so far are left in place; callers extract into a scratch directory.
     *
     * @throws IOException on a malformed or unsafe archive or when a limit is exceeded
     */
    static List<ExtractedFile> extractZipSafely(Path zipBlob, Path targetDir, int maxEntries,
                                                long maxTotalBytes, long maxEntryBytes) throws IOException {
        Path root = Files.createDirectories(targetDir.toAbsolutePath().normalize());
        List<ExtractedFile> extracted = new ArrayList<>();
        Set<String> seenNames = new HashSet<>();
        Set<Object> writtenFiles = new HashSet<>();
        byte[] buffer = new byte[16 * 1024];
        long total = 0;
        int entries = 0;
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipBlob))) {
            ZipEntry entry;
            while ((entry = nextEntry(zis, zipBlob)) != null) {
                if (++entries > maxEntries) {
                    throw new IOException("ZIP archive " + zipBlob + " has more than the maximum of " + maxEntries + " entries");
                }
                String name = entry.getName();
                boolean directory = name.endsWith("/");
                String relative = safeEntryName(directory ? name.substring(0, name.length() - 1) : name, zipBlob);
                if (!seenNames.add(relative)) {
                    throw new IOException("Duplicate ZIP entry " + name + " in " + zipBlob);
                }
                Path destination = resolveEntry(root, relative, zipBlob);
                Path link = findSymlinkComponent(root, destination);
                if (link != null) {
                    throw new IOException("Symbolic link on extraction path of ZIP entry " + name + ": " + link);
                }

                if (directory) {
                    if (zis.read(buffer) != -1) {
                        throw new IOException("ZIP directory entry " + name + " in " + zipBlob + " carries content");
                    }
                    Files.createDirectories(destination);
                    continue;
                }
                Files.createDirectories(destination.getParent());
                if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)
                        && writtenFiles.contains(fileIdentity(destination, relative))) {
                    throw new IOException("ZIP entry " + name + " in " + zipBlob
                            + " collides with an entry already extracted (names differing only in case?)");
                }
                MessageDigest digest = ClasspathPackager.newSha256();
                long entryBytes = 0;
                try (OutputStream out = Files.newOutputStream(destination, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                    int read;
                    while ((read = zis.read(buffer)) != -1) {
                        entryBytes += read;
                        total += read;
                        if (entryBytes > maxEntryBytes) {
                            throw new IOException("ZIP entry " + name + " exceeds the maximum of " + maxEntryBytes
                                    + " uncompressed bytes");
                        }
                        if (total > maxTotalBytes) {
                            throw new IOException("ZIP archive " + zipBlob + " exceeds the maximum total of "
                                    + maxTotalBytes + " uncompressed bytes");
                        }
                        digest.update(buffer, 0, read);
                        out.write(buffer, 0, read);
                    }
                }
                writtenFiles.add(fileIdentity(destination, relative));
                extracted.add(new ExtractedFile(relative, HexFormat.of().formatHex(digest.digest()), destination));
            }
        }
        return extracted;
    }

    /**
     * Returns the first existing path component strictly below {@code root} on the way to {@code target}
     * (including {@code target} itself) that is a symbolic link, or {@code null} if there is none.
     * {@code target} must be located under {@code root}.
     */
    public static Path findSymlinkComponent(Path root, Path target) {
        Path current = root;
        for (Path name : root.relativize(target)) {
            current = current.resolve(name);
            if (isLinkLike(current)) {
                return current;
            }
        }
        return null;
    }

    /**
     * True for symbolic links and for other non-regular, non-directory entries such as Windows junctions,
     * which are not reported as symbolic links but redirect like them.
     */
    public static boolean isLinkLike(Path path) {
        if (Files.isSymbolicLink(path)) {
            return true;
        }
        try {
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return attrs.isOther();
        } catch (IOException e) {
            return false;
        }
    }

    private static ZipEntry nextEntry(ZipInputStream zis, Path zipBlob) throws IOException {
        try {
            return zis.getNextEntry();
        } catch (IllegalArgumentException e) {
            // JDK 21 reports entry names that are not valid UTF-8 this way.
            throw new ZipException("Malformed ZIP entry in " + zipBlob + ": " + e.getMessage());
        }
    }

    private static Path resolveEntry(Path root, String relative, Path zipBlob) throws IOException {
        try {
            return root.resolve(relative);
        } catch (InvalidPathException e) {
            throw new ZipException("ZIP entry name " + relative + " in " + zipBlob + " is not valid on this platform");
        }
    }

    /**
     * Identity of an extracted file that also matches names which differ only in case on case-insensitive file
     * systems: the file key where available, otherwise the lower-cased relative path.
     */
    private static Object fileIdentity(Path file, String relative) throws IOException {
        Object key = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey();
        return key != null ? key : relative.toLowerCase(Locale.ROOT);
    }

    private static String safeEntryName(String name, Path zipBlob) throws IOException {
        try {
            return WorkspacePackager.validateRelativePath(name);
        } catch (IllegalArgumentException e) {
            throw new IOException("Unsafe ZIP entry name in " + zipBlob + ": " + e.getMessage(), e);
        }
    }

    private static void checkBlobSize(long size, String sha256) {
        if (size > ClasspathPackager.MAX_BLOB_BYTES) {
            throw new IllegalArgumentException("Blob " + sha256 + " has " + size + " bytes, more than the maximum of "
                    + ClasspathPackager.MAX_BLOB_BYTES);
        }
    }

    private static void verifyDigest(String expected, String actual) {
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException("SHA-256 mismatch: expected " + expected + " but got " + actual);
        }
    }

    private void moveIntoPlace(Path tmp, Path target) throws IOException {
        // delete(String) prunes empty shard directories, so a concurrent delete may remove the parent between
        // createDirectories and the move: recreate it and retry a few times.
        for (int attempt = 1; ; attempt++) {
            requireCacheDirectories(target.getParent());
            Files.createDirectories(target.getParent());
            try {
                moveOnce(tmp, target);
                return;
            } catch (NoSuchFileException e) {
                if (attempt >= 3 || !Files.exists(tmp, LinkOption.NOFOLLOW_LINKS)
                        || Files.isDirectory(target.getParent(), LinkOption.NOFOLLOW_LINKS)) {
                    throw e;
                }
            }
        }
    }

    private void moveOnce(Path tmp, Path target) throws IOException {
        FileMover mover = fileMover;
        boolean targetExisted = Files.exists(target, LinkOption.NOFOLLOW_LINKS);
        try {
            mover.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            try {
                mover.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException moveFailed) {
                if (!targetExisted) {
                    // a non-atomic move can leave a partial target behind
                    Files.deleteIfExists(target);
                }
                throw moveFailed;
            }
        }
    }
}
