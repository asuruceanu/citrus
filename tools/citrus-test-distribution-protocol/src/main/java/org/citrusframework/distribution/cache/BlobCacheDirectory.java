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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;

/**
 * Content-addressed local directory store indexed by validated 64-char lowercase SHA-256 hex digests.
 */
public class BlobCacheDirectory {

    public static final Pattern SHA256_HEX_PATTERN = Pattern.compile("^[a-f0-9]{64}$");

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

    public Path put(String sha256, byte[] bytes) throws IOException {
        validateDigest(sha256);
        String actual = ClasspathPackager.sha256Hex(bytes);
        if (!sha256.equals(actual)) {
            throw new IllegalArgumentException("SHA-256 mismatch: expected " + sha256 + " but got " + actual);
        }
        Path target = resolveBlobPath(sha256);
        if (Files.isRegularFile(target)) {
            return target;
        }
        Files.createDirectories(target.getParent());
        Path tmp = target.getParent().resolve(sha256 + ".tmp-" + UUID.randomUUID());
        Files.write(tmp, bytes);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        return target;
    }

    public byte[] readBytes(String sha256) throws IOException {
        return Files.readAllBytes(resolveBlobPath(sha256));
    }

    public Path materializeForClasspath(ArtifactRef ref, Path workspaceDir) throws IOException {
        Path blobPath = resolveBlobPath(ref.sha256());
        if (!Files.isRegularFile(blobPath)) {
            throw new IOException("Cached blob does not exist: " + ref.sha256());
        }
        if (ref.kind() == ArtifactKind.JAR) {
            return blobPath;
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
            Files.createDirectories(extractDir);
            try (InputStream fis = Files.newInputStream(blobPath);
                 ZipInputStream zis = new ZipInputStream(fis)) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    Path outPath = extractDir.resolve(entry.getName()).normalize();
                    if (!outPath.startsWith(extractDir)) {
                        throw new IOException("Zip entry escapes extraction root: " + entry.getName());
                    }
                    if (entry.isDirectory()) {
                        Files.createDirectories(outPath);
                    } else {
                        Files.createDirectories(outPath.getParent());
                        Files.copy(zis, outPath, StandardCopyOption.REPLACE_EXISTING);
                    }
                    zis.closeEntry();
                }
            }
            Files.writeString(marker, "ok");
            return extractDir;
        }
    }
}
