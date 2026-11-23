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
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
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
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;

/**
 * Content-addressed local directory store indexed by validated 64-char lowercase SHA-256 hex digests.
 * <p>
 * Blobs live at {@code <root>/blobs/<first two hex chars>/<sha256>}. Writes go to a temporary file under
 * {@code <root>/tmp} and are moved into place only after their digest has been verified, so a failed or
 * interrupted write never leaves a partial blob or an orphaned temporary file behind.
 */
public class BlobCacheDirectory {

    public static final Pattern SHA256_HEX_PATTERN = Pattern.compile("^[a-f0-9]{64}$");

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

    private final Path rootDir;

    public BlobCacheDirectory(Path rootDir) {
        this.rootDir = rootDir.toAbsolutePath().normalize();
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

    public boolean contains(String sha256) {
        return Files.isRegularFile(resolveBlobPath(sha256));
    }

    /**
     * Creates an empty temporary file in this cache's temporary directory, on the same file system as the
     * blob store so it can be moved into place atomically. The caller must delete it when done.
     */
    public Path newTempFile(String prefix) throws IOException {
        Path tmpDir = Files.createDirectories(rootDir.resolve("tmp"));
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
        Path target = resolveBlobPath(sha256);
        if (Files.isRegularFile(target)) {
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
        Path target = resolveBlobPath(expectedSha256);

        Path source = sourceFile.toAbsolutePath().normalize();
        if (rootDir.resolve("tmp").equals(source.getParent())) {
            verifyDigest(expectedSha256, ClasspathPackager.sha256Hex(source));
            if (Files.isRegularFile(target)) {
                Files.deleteIfExists(source);
            } else {
                moveIntoPlace(source, target);
            }
            return target;
        }
        if (Files.isRegularFile(target)) {
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
     * Returns the blob path for {@code sha256} after re-verifying its content, or {@code null} when the blob is
     * absent or corrupted. A corrupted blob is deleted so the caller can fetch or package it again.
     */
    public Path getVerifiedPath(String sha256) throws IOException {
        Path blob = resolveBlobPath(sha256);
        if (!Files.isRegularFile(blob, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        if (sha256.equals(ClasspathPackager.sha256Hex(blob))) {
            return blob;
        }
        Files.deleteIfExists(blob);
        return null;
    }

    public byte[] readBytes(String sha256) throws IOException {
        return Files.readAllBytes(resolveBlobPath(sha256));
    }

    public Path materializeForClasspath(ArtifactRef ref, Path workspaceDir) throws IOException {
        if (ref.kind() == ArtifactKind.REGULAR_FILE) {
            throw new IllegalArgumentException("REGULAR_FILE artifact " + ref.name() + " cannot be placed on the classpath");
        }
        Path blobPath = resolveBlobPath(ref.sha256());
        if (!Files.isRegularFile(blobPath)) {
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
            if (getVerifiedPath(ref.sha256()) == null) {
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
     * a copy where links are unsupported), shared by every partition. Libraries inspect classpath file names: for
     * example Testcontainers' {@code MountableFile} extracts a resource from an archive only when its URL contains
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
        Path named = dir.resolve(name).normalize();
        if (!named.startsWith(rootDir)) {
            throw new IOException("Classpath entry escapes cache root: " + ref.name());
        }
        if (Files.isRegularFile(named)) {
            return named;
        }
        synchronized (this) {
            if (Files.isRegularFile(named)) {
                return named;
            }
            Files.createDirectories(dir);
            // Several worker JVMs may share the cache directory: stage under a unique name, and treat any failure to
            // publish as fine once another process has published the same digest.
            Path staging = Files.createTempFile(dir, ".staging-", ".jar");
            try {
                Files.delete(staging);
                try {
                    Files.createLink(staging, blobPath);
                } catch (UnsupportedOperationException | IOException linkFailed) {
                    Files.copy(blobPath, staging, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
                Files.move(staging, named, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException publishFailed) {
                if (!Files.isRegularFile(named)) {
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

    private static void moveIntoPlace(Path tmp, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
