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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.WorkspaceFileSet;

/**
 * Packages declared workspace inputs and produced outputs into the {@link BlobCacheDirectory} and materializes
 * them again into a target directory.
 * <p>
 * A single regular file travels as a {@link ArtifactKind#REGULAR_FILE} blob, a directory as a deterministic
 * {@link ArtifactKind#DIRECTORY_ZIP}. All paths are validated with {@link #validateRelativePath(String)} and
 * symbolic links are rejected both when packaging and when materializing.
 */
public final class WorkspacePackager {

    private static final Set<String> RESERVED_DEVICE_NAMES = Set.of(
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9");

    /**
     * A regular file placed by {@link #materializeIntoDirectory(BlobCacheDirectory, WorkspaceFileSet, Path)}.
     *
     * @param workspaceRelativePath {@code /}-separated path relative to the target root
     * @param sha256                digest of the file content
     * @param extractedFile         absolute path of the written file
     */
    public record MaterializedEntry(String workspaceRelativePath, String sha256, Path extractedFile) {
    }

    private WorkspacePackager() {
    }

    /**
     * Validates a workspace-relative path and returns it with {@code \} normalized to {@code /}.
     * Rejects {@code null}/blank values, absolute and UNC paths, {@code :} anywhere (drive letters, URIs),
     * control characters, empty, {@code .} and {@code ..} segments (including a trailing {@code /}), and
     * Windows reserved device names ({@code CON}, {@code NUL}, {@code COM1}, ... with or without extension).
     *
     * @throws IllegalArgumentException if the path is unsafe
     */
    public static String validateRelativePath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            throw new IllegalArgumentException("Workspace path must not be blank");
        }
        String path = rawPath.replace('\\', '/');
        if (path.startsWith("/")) {
            throw new IllegalArgumentException("Workspace path must be relative: '" + rawPath + "'");
        }
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c < 0x20 || c == 0x7f) {
                throw new IllegalArgumentException("Workspace path contains a control character: '" + rawPath + "'");
            }
            if (c == ':') {
                throw new IllegalArgumentException("Workspace path must not contain ':': '" + rawPath + "'");
            }
        }
        for (String segment : path.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("Workspace path contains an empty, '.' or '..' segment: '" + rawPath + "'");
            }
            int dot = segment.indexOf('.');
            String baseName = (dot < 0 ? segment : segment.substring(0, dot)).toUpperCase(Locale.ROOT);
            if (RESERVED_DEVICE_NAMES.contains(baseName)) {
                throw new IllegalArgumentException("Workspace path uses a reserved device name: '" + rawPath + "'");
            }
        }
        return path;
    }

    /**
     * Packages the declared input {@code rawRelativePath} under {@code baseDir} into {@code cache}.
     *
     * @return the packaged file set, or empty when the path does not exist and {@code optional} is set
     * @throws IllegalArgumentException if the path is unsafe, is or passes through a symbolic link, contains a
     *                                  symbolic link, is missing while required, or exceeds the size limits
     */
    public static Optional<WorkspaceFileSet> packageInputToCache(BlobCacheDirectory cache, Path baseDir,
                                                                 String rawRelativePath, boolean optional) throws IOException {
        return packageInputToCache(cache, baseDir, rawRelativePath, optional, List.of());
    }

    /**
     * Like {@link #packageInputToCache(BlobCacheDirectory, Path, String, boolean)}, leaving out everything at or below
     * the module-relative {@code excludedPaths} (for example the host's own cache and staging directories under
     * {@code target/} when an input covers {@code target}).
     */
    public static Optional<WorkspaceFileSet> packageInputToCache(BlobCacheDirectory cache, Path baseDir,
                                                                 String rawRelativePath, boolean optional,
                                                                 List<String> excludedPaths) throws IOException {
        return packageInputToCacheLazily(() -> cache, baseDir, rawRelativePath, optional, excludedPaths);
    }

    /**
     * Like {@link #packageInputToCache(BlobCacheDirectory, Path, String, boolean, List)}, but obtains the cache
     * only when an existing input is about to be stored.
     */
    public static Optional<WorkspaceFileSet> packageInputToCacheLazily(Supplier<BlobCacheDirectory> cache, Path baseDir,
                                                                 String rawRelativePath, boolean optional,
                                                                 List<String> excludedPaths) throws IOException {
        String relative = validateRelativePath(rawRelativePath);
        Path base = baseDir.toAbsolutePath().normalize();
        Path target = resolveWithoutSymlinks(base, relative, "declared input");
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            if (optional) {
                return Optional.empty();
            }
            throw new IllegalArgumentException("Declared input path '" + relative + "' does not exist under " + base);
        }

        BasicFileAttributes attrs = Files.readAttributes(target, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (attrs.isRegularFile()) {
            return Optional.of(packageRegularFile(cache.get(), relative, target));
        }
        if (attrs.isDirectory()) {
            List<String> entries = ClasspathPackager.listEntries(target).stream()
                    .filter(entry -> !isExcluded(relative + "/" + stripSlash(entry), excludedPaths))
                    .toList();
            return Optional.of(packageEntries(cache.get(), relative, target, entries));
        }
        throw new IllegalArgumentException("Declared input path '" + relative + "' is not a regular file or directory");
    }

    /**
     * Records the SHA-256 of every regular file currently present under {@code outputPaths}, keyed by its
     * {@code /}-separated path relative to {@code workspaceDir}. Missing paths and symbolic links are skipped.
     */
    public static Map<String, String> snapshotBaselineHashes(Path workspaceDir, List<String> outputPaths) throws IOException {
        Path base = workspaceDir.toAbsolutePath().normalize();
        Map<String, String> hashes = new TreeMap<>();
        for (String rawPath : outputPaths) {
            String relative = validateRelativePath(rawPath);
            Path target = base.resolve(relative);
            if (BlobCacheDirectory.findSymlinkComponent(base, target) != null) {
                continue;
            }
            if (Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
                hashes.put(relative, ClasspathPackager.sha256Hex(target));
            } else if (Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)) {
                Files.walkFileTree(target, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (attrs.isRegularFile()) {
                            hashes.put(ClasspathPackager.relativeName(base, file), ClasspathPackager.sha256Hex(file));
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        }
        return Collections.unmodifiableMap(hashes);
    }

    /**
     * Packages the regular files under the declared output {@code rawOutputPath} that are new or changed compared
     * to {@code baselineHashes} (see {@link #snapshotBaselineHashes(Path, List)}). A single file output becomes a
     * {@link ArtifactKind#REGULAR_FILE}; a directory output becomes a {@link ArtifactKind#DIRECTORY_ZIP} containing
     * only the new or changed files.
     *
     * @return the packaged file set, or empty when the path is missing or nothing new or changed exists
     * @throws IllegalArgumentException if the path is unsafe, passes through or contains a symbolic link, or
     *                                  exceeds the size limits
     */
    public static Optional<WorkspaceFileSet> packageModifiedOutputToCache(BlobCacheDirectory cache, Path workspaceDir,
                                                                          String rawOutputPath,
                                                                          Map<String, String> baselineHashes) throws IOException {
        String relative = validateRelativePath(rawOutputPath);
        Path base = workspaceDir.toAbsolutePath().normalize();
        Path target = resolveWithoutSymlinks(base, relative, "declared output");
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            return Optional.empty();
        }

        BasicFileAttributes attrs = Files.readAttributes(target, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (attrs.isRegularFile()) {
            if (ClasspathPackager.sha256Hex(target).equals(baselineHashes.get(relative))) {
                return Optional.empty();
            }
            return Optional.of(packageRegularFile(cache, relative, target));
        }
        if (!attrs.isDirectory()) {
            throw new IllegalArgumentException("Declared output path '" + relative + "' is not a regular file or directory");
        }

        List<String> changed = new ArrayList<>();
        for (String entry : ClasspathPackager.listEntries(target, true)) {
            if (!entry.endsWith("/")
                    && !ClasspathPackager.sha256Hex(target.resolve(entry)).equals(baselineHashes.get(relative + "/" + entry))) {
                changed.add(entry);
            }
        }
        if (changed.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(packageEntries(cache, relative, target, changed));
    }

    /**
     * Places {@code fileSet} under {@code targetRoot} at its relative path, verifying the cached blob first.
     *
     * @return the regular files written
     * @throws IllegalArgumentException if the relative path is unsafe or the artifact is not a
     *                                  {@link ArtifactKind#REGULAR_FILE} or {@link ArtifactKind#DIRECTORY_ZIP}
     * @throws IOException              if the blob is missing or corrupted (a corrupted blob is evicted so the caller
     *                                  can fetch it again), a symbolic link lies on the destination path, or the
     *                                  archive is unsafe or exceeds the ZIP limits
     */
    public static List<MaterializedEntry> materializeIntoDirectory(BlobCacheDirectory cache, WorkspaceFileSet fileSet,
                                                                   Path targetRoot) throws IOException {
        return materializeIntoDirectory(cache, fileSet, null, targetRoot, true);
    }

    /**
     * Places {@code fileSet} under {@code targetRoot} at its relative path from a blob the caller has already verified,
     * without hashing it again.
     *
     * @param verifiedBlob the cache's blob path for the file set's artifact, verified by the caller
     * @return the regular files written
     * @throws IllegalArgumentException if the relative path is unsafe, the artifact is not a
     *                                  {@link ArtifactKind#REGULAR_FILE} or {@link ArtifactKind#DIRECTORY_ZIP}, or
     *                                  {@code verifiedBlob} is not the cache's blob path for it
     * @throws IOException              if the blob is missing, a symbolic link lies on the destination path, or the
     *                                  archive is unsafe or exceeds the ZIP limits
     */
    public static List<MaterializedEntry> materializeIntoDirectory(BlobCacheDirectory cache, WorkspaceFileSet fileSet,
                                                                   Path verifiedBlob, Path targetRoot) throws IOException {
        return materializeIntoDirectory(cache, fileSet, verifiedBlob, targetRoot, false);
    }

    private static List<MaterializedEntry> materializeIntoDirectory(BlobCacheDirectory cache, WorkspaceFileSet fileSet,
                                                                    Path verifiedBlob, Path targetRoot, boolean verify)
            throws IOException {
        String relative = validateRelativePath(fileSet.relativePath());
        ArtifactRef artifact = fileSet.artifact();
        if (artifact == null || (artifact.kind() != ArtifactKind.REGULAR_FILE && artifact.kind() != ArtifactKind.DIRECTORY_ZIP)) {
            throw new IllegalArgumentException("Workspace path '" + relative + "' must be a REGULAR_FILE or DIRECTORY_ZIP artifact");
        }
        Path blob = verify ? cache.getVerifiedPath(artifact.sha256()) : cache.requireBlobPath(artifact.sha256(), verifiedBlob);
        if (blob == null || (!verify && !Files.isRegularFile(blob, LinkOption.NOFOLLOW_LINKS))) {
            throw new IOException("Blob " + artifact.sha256() + " for workspace path '" + relative
                    + "' is missing from or corrupted in the cache");
        }

        Path root = Files.createDirectories(targetRoot.toAbsolutePath().normalize());
        Path destination = root.resolve(relative);
        Path link = BlobCacheDirectory.findSymlinkComponent(root, destination);
        if (link != null) {
            throw new IOException("Symbolic link on materialization path of '" + relative + "': " + link);
        }

        if (artifact.kind() == ArtifactKind.REGULAR_FILE) {
            if (Files.isDirectory(destination, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Cannot materialize file '" + relative + "' over an existing directory");
            }
            Files.createDirectories(destination.getParent());
            try (InputStream in = Files.newInputStream(blob)) {
                Files.copy(in, destination, StandardCopyOption.REPLACE_EXISTING);
            }
            return List.of(new MaterializedEntry(relative, artifact.sha256(), destination));
        }

        Files.createDirectories(destination);
        return BlobCacheDirectory.extractZipSafely(blob, destination).stream()
                .map(file -> new MaterializedEntry(relative + "/" + file.relativePath(), file.sha256(), file.file()))
                .toList();
    }

    private static boolean isExcluded(String moduleRelative, List<String> excludedPaths) {
        for (String excluded : excludedPaths) {
            if (moduleRelative.equals(excluded) || moduleRelative.startsWith(excluded + "/")) {
                return true;
            }
        }
        return false;
    }

    private static String stripSlash(String entry) {
        return entry.endsWith("/") ? entry.substring(0, entry.length() - 1) : entry;
    }

    private static Path resolveWithoutSymlinks(Path base, String relative, String description) {
        Path target = base.resolve(relative);
        Path link = BlobCacheDirectory.findSymlinkComponent(base, target);
        if (link != null) {
            throw new IllegalArgumentException("Symbolic link not allowed in " + description + " '" + relative + "': " + link);
        }
        return target;
    }

    private static WorkspaceFileSet packageRegularFile(BlobCacheDirectory cache, String relative, Path file) throws IOException {
        String sha256 = ClasspathPackager.sha256Hex(file);
        long size = Files.size(cache.putFromFile(sha256, file));
        return new WorkspaceFileSet(relative, new ArtifactRef(file.getFileName().toString(), sha256, ArtifactKind.REGULAR_FILE, size));
    }

    private static WorkspaceFileSet packageEntries(BlobCacheDirectory cache, String relative, Path directory,
                                                   List<String> entries) throws IOException {
        Path tmp = cache.newTempFile("ws-");
        try {
            String sha256 = ClasspathPackager.writeZipToFile(directory, entries, tmp, ClasspathPackager.MAX_BLOB_BYTES);
            long size = Files.size(tmp);
            cache.putFromFile(sha256, tmp);
            String name = directory.getFileName() + ".zip";
            return new WorkspaceFileSet(relative, new ArtifactRef(name, sha256, ArtifactKind.DIRECTORY_ZIP, size));
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
}
