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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;

/**
 * Deterministically hashes JAR files and zips directories with reproducible SHA-256 digests.
 * <p>
 * Directory archives contain sorted, {@code /}-separated entry names (directories as {@code name/}, so
 * empty directories survive), a fixed DOS timestamp of 1980-02-01T00:00 local time, no extra fields and
 * no comments, so identical trees always produce identical bytes regardless of host, time zone or
 * file-system iteration order. Symbolic links anywhere in the tree are rejected.
 */
public final class ClasspathPackager {

    /** Maximum compressed size of a single blob (256 MiB). */
    public static final long MAX_BLOB_BYTES = 256L * 1024 * 1024;

    static final Pattern MAVEN_OUTPUT_ARCHIVE = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,119}-(?:test-)?classes\\.zip");

    /**
     * Fixed entry time inside the DOS date range, so no extended-timestamp extra field is written (the Unix
     * epoch would need one). Not 1980-01-01T00:00, which the JDK reserves as its "before 1980" marker.
     */
    private static final LocalDateTime ZIP_ENTRY_TIME = LocalDateTime.of(1980, 2, 1, 0, 0);

    private ClasspathPackager() {
    }

    public record PackagedArtifact(ArtifactRef ref, Path sourceFile, byte[] inMemoryBytes) {
        public byte[] bytes() {
            if (inMemoryBytes != null) {
                return inMemoryBytes;
            }
            try {
                return Files.readAllBytes(sourceFile);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read artifact bytes from " + sourceFile, e);
            }
        }
    }

    public static PackagedArtifact packagePath(Path path) throws IOException {
        return packagePath(path, null);
    }

    /** Packages a classpath entry using its build-provided artifact ID when known. */
    public static PackagedArtifact packagePath(Path path, String artifactId) throws IOException {
        Path normalized = path.toAbsolutePath().normalize();
        if (Files.isDirectory(normalized)) {
            byte[] zipBytes = createDeterministicZip(normalized);
            String sha256 = sha256Hex(zipBytes);
            String name = normalized.getFileName() != null ? normalized.getFileName().toString() + ".zip" : "classes.zip";
            Path parent = normalized.getParent();
            if (artifactId != null) {
                String moduleArchive = artifactId + "-classes.zip";
                if (MAVEN_OUTPUT_ARCHIVE.matcher(moduleArchive).matches()) {
                    name = moduleArchive;
                }
            } else if (("classes.zip".equals(name) || "test-classes.zip".equals(name))
                    && parent != null && parent.endsWith("target") && parent.getParent() != null
                    && parent.getParent().getFileName() != null) {
                String moduleArchive = parent.getParent().getFileName() + "-" + name;
                if (MAVEN_OUTPUT_ARCHIVE.matcher(moduleArchive).matches()) {
                    name = moduleArchive;
                }
            }
            ArtifactRef ref = new ArtifactRef(name, sha256, ArtifactKind.DIRECTORY_ZIP, zipBytes.length);
            return new PackagedArtifact(ref, null, zipBytes);
        }
        long size = Files.size(normalized);
        String sha256 = sha256Hex(normalized);
        String name = normalized.getFileName() != null ? normalized.getFileName().toString() : "artifact.jar";
        ArtifactRef ref = new ArtifactRef(name, sha256, ArtifactKind.JAR, size);
        return new PackagedArtifact(ref, normalized, null);
    }

    /**
     * Builds a deterministic in-memory ZIP of {@code directory}.
     *
     * @throws IllegalArgumentException on symbolic links, unsupported file types, or when the archive
     *                                  exceeds {@link #MAX_BLOB_BYTES} or the {@link BlobCacheDirectory} ZIP limits
     */
    public static byte[] createDeterministicZip(Path directory) throws IOException {
        List<String> entries = listEntries(directory);
        ByteArrayOutputStream out = new ByteArrayOutputStream(4096);
        try (ZipOutputStream zos = newZipOutputStream(new LimitedOutputStream(out, MAX_BLOB_BYTES))) {
            writeEntries(directory, entries, zos);
        }
        return out.toByteArray();
    }

    /**
     * Streams a deterministic ZIP of {@code sourceDir} to {@code targetZip} (created or truncated) and returns
     * its SHA-256. The target is deleted if packaging fails.
     *
     * @throws IllegalArgumentException on symbolic links, unsupported file types, or when the archive
     *                                  exceeds {@link #MAX_BLOB_BYTES} or the {@link BlobCacheDirectory} ZIP limits
     */
    public static String createDeterministicZipToFile(Path sourceDir, Path targetZip) throws IOException {
        return createDeterministicZipToFile(sourceDir, targetZip, MAX_BLOB_BYTES);
    }

    static String createDeterministicZipToFile(Path sourceDir, Path targetZip, long maxCompressedBytes) throws IOException {
        return writeZipToFile(sourceDir, listEntries(sourceDir), targetZip, maxCompressedBytes);
    }

    /**
     * Streams a deterministic ZIP holding exactly {@code entryNames} (relative to {@code baseDir}, sorted,
     * directories ending in {@code /}) to {@code targetZip} and returns its SHA-256. The target is deleted if
     * packaging fails.
     */
    static String writeZipToFile(Path baseDir, List<String> entryNames, Path targetZip, long maxCompressedBytes)
            throws IOException {
        MessageDigest digest = newSha256();
        boolean complete = false;
        try {
            try (OutputStream file = Files.newOutputStream(targetZip,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                 ZipOutputStream zos = newZipOutputStream(
                         new DigestOutputStream(new LimitedOutputStream(file, maxCompressedBytes), digest))) {
                writeEntries(baseDir, entryNames, zos);
            }
            complete = true;
        } finally {
            if (!complete) {
                Files.deleteIfExists(targetZip);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /**
     * Lists all directories (as {@code name/}) and regular files below {@code root} as sorted,
     * {@code /}-separated relative names, without following links.
     *
     * @throws IllegalArgumentException if {@code root} is not a directory, if it or anything below it is a
     *                                  symbolic link (or Windows junction) or not a regular file or directory, or
     *                                  if a name would be rejected on extraction
     *                                  (see {@link WorkspacePackager#validateRelativePath(String)})
     */
    static List<String> listEntries(Path root) throws IOException {
        return listEntries(root, false);
    }

    /**
     * Like {@link #listEntries(Path)}, but with {@code skipSpecialFiles} set, sockets, FIFOs and devices are
     * left out instead of failing the listing. Symbolic links are rejected either way.
     */
    static List<String> listEntries(Path root, boolean skipSpecialFiles) throws IOException {
        if (Files.isSymbolicLink(root) || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Packaged path is a symbolic link or not a directory: " + root);
        }
        List<String> entries = new ArrayList<>();
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (attrs.isOther()) {
                    throw new IllegalArgumentException("Symbolic link or junction not allowed in packaged directory: " + dir);
                }
                if (!dir.equals(root)) {
                    entries.add(entryName(root, dir) + "/");
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (attrs.isSymbolicLink()) {
                    throw new IllegalArgumentException("Symbolic link not allowed in packaged directory: " + file);
                }
                if (!attrs.isRegularFile()) {
                    if (skipSpecialFiles) {
                        return FileVisitResult.CONTINUE;
                    }
                    throw new IllegalArgumentException("Unsupported file type in packaged directory: " + file);
                }
                entries.add(entryName(root, file));
                return FileVisitResult.CONTINUE;
            }
        });
        entries.sort(null);
        return entries;
    }

    static String relativeName(Path root, Path path) {
        return root.relativize(path).toString().replace('\\', '/');
    }

    /**
     * Archive entry name of {@code path}, validated so that every packaged name can also be extracted.
     */
    private static String entryName(Path root, Path path) {
        if (File.separatorChar == '/') {
            for (Path segment : root.relativize(path)) {
                if (segment.toString().indexOf('\\') >= 0) {
                    throw new IllegalArgumentException("File name contains a backslash and cannot be packaged portably: " + path);
                }
            }
        }
        String name = relativeName(root, path);
        try {
            return WorkspacePackager.validateRelativePath(name);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot package " + path + ": " + e.getMessage(), e);
        }
    }

    private static ZipOutputStream newZipOutputStream(OutputStream out) {
        ZipOutputStream zos = new ZipOutputStream(out);
        zos.setLevel(Deflater.DEFAULT_COMPRESSION);
        return zos;
    }

    private static void writeEntries(Path baseDir, List<String> entryNames, ZipOutputStream zos) throws IOException {
        if (entryNames.size() > BlobCacheDirectory.MAX_ZIP_ENTRIES) {
            throw new IllegalArgumentException("Archive of " + baseDir + " has " + entryNames.size()
                    + " entries, more than the maximum of " + BlobCacheDirectory.MAX_ZIP_ENTRIES);
        }
        byte[] buffer = new byte[16 * 1024];
        long total = 0;
        for (String name : entryNames) {
            ZipEntry entry = new ZipEntry(name);
            entry.setTimeLocal(ZIP_ENTRY_TIME);
            zos.putNextEntry(entry);
            if (!name.endsWith("/")) {
                long entryBytes = 0;
                try (InputStream in = Files.newInputStream(baseDir.resolve(name), LinkOption.NOFOLLOW_LINKS)) {
                    int read;
                    while ((read = in.read(buffer)) != -1) {
                        entryBytes += read;
                        total += read;
                        if (entryBytes > BlobCacheDirectory.MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES) {
                            throw new IllegalArgumentException("File " + name + " in " + baseDir + " exceeds the maximum of "
                                    + BlobCacheDirectory.MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES + " bytes per archive entry");
                        }
                        if (total > BlobCacheDirectory.MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES) {
                            throw new IllegalArgumentException("Archive of " + baseDir + " exceeds the maximum total of "
                                    + BlobCacheDirectory.MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES + " uncompressed bytes");
                        }
                        zos.write(buffer, 0, read);
                    }
                }
            }
            zos.closeEntry();
        }
    }

    public static String sha256Hex(byte[] bytes) {
        MessageDigest digest = newSha256();
        return HexFormat.of().formatHex(digest.digest(bytes));
    }

    public static String sha256Hex(Path file) throws IOException {
        MessageDigest digest = newSha256();
        byte[] buffer = new byte[16 * 1024];
        try (InputStream in = Files.newInputStream(file)) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm unavailable", e);
        }
    }

    /**
     * Fails once more than {@code maxBytes} have been written, bounding compressed archive size while streaming.
     */
    private static final class LimitedOutputStream extends FilterOutputStream {

        private final long maxBytes;
        private long written;

        LimitedOutputStream(OutputStream out, long maxBytes) {
            super(out);
            this.maxBytes = maxBytes;
        }

        @Override
        public void write(int b) throws IOException {
            count(1);
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            count(len);
            out.write(b, off, len);
        }

        private void count(int len) {
            written += len;
            if (written > maxBytes) {
                throw new IllegalArgumentException("Archive exceeds the maximum compressed size of " + maxBytes + " bytes");
            }
        }
    }
}
