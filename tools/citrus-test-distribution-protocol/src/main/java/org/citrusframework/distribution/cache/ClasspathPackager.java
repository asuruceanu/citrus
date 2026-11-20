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
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;

/**
 * Deterministically hashes JAR files and zips loose classpath directories with reproducible SHA-256 digests.
 */
public final class ClasspathPackager {

    private static final FileTime EPOCH_TIME = FileTime.fromMillis(0L);

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
        Path normalized = path.toAbsolutePath().normalize();
        if (Files.isDirectory(normalized)) {
            byte[] zipBytes = createDeterministicZip(normalized);
            String sha256 = sha256Hex(zipBytes);
            String name = normalized.getFileName() != null ? normalized.getFileName().toString() + ".zip" : "classes.zip";
            ArtifactRef ref = new ArtifactRef(name, sha256, ArtifactKind.DIRECTORY_ZIP, zipBytes.length);
            return new PackagedArtifact(ref, null, zipBytes);
        }
        long size = Files.size(normalized);
        String sha256 = sha256Hex(normalized);
        String name = normalized.getFileName() != null ? normalized.getFileName().toString() : "artifact.jar";
        ArtifactRef ref = new ArtifactRef(name, sha256, ArtifactKind.JAR, size);
        return new PackagedArtifact(ref, normalized, null);
    }

    public static byte[] createDeterministicZip(Path directory) throws IOException {
        List<Path> files;
        try (Stream<Path> walk = Files.walk(directory)) {
            files = walk.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(p -> directory.relativize(p).toString().replace('\\', '/')))
                    .toList();
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream(4096);
        try (ZipOutputStream zos = new ZipOutputStream(out)) {
            for (Path file : files) {
                String entryName = directory.relativize(file).toString().replace('\\', '/');
                ZipEntry entry = new ZipEntry(entryName);
                entry.setLastModifiedTime(EPOCH_TIME);
                zos.putNextEntry(entry);
                Files.copy(file, zos);
                zos.closeEntry();
            }
        }
        return out.toByteArray();
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

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm unavailable", e);
        }
    }
}
