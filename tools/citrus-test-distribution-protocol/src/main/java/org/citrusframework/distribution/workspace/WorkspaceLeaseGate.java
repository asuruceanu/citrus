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

package org.citrusframework.distribution.workspace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.regex.Pattern;

/**
 * Worker-side lifecycle for a workspace lease ticket on the local filesystem.
 * Callers serialize creation, retirement and removal for a ticket. Tickets are never reused.
 */
public final class WorkspaceLeaseGate {

    private static final String WORKSPACE_FILE = ".workspace";
    private static final String RETIRED_FILE = ".retired";
    private static final int MAX_WORKSPACE_BYTES = 128 * 1024;
    private static final Pattern TICKET_NAME = Pattern.compile("[.]lease-[0-9a-f]{32}");
    private static final Pattern CLAIM_NAME = Pattern.compile("[1-9][0-9]*-[0-9a-f]{32}[.]claim");
    private static final boolean POSIX = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    private WorkspaceLeaseGate() {
    }

    /**
     * Creates a ticket. Publish it to helpers only after this method returns.
     */
    public static void create(Path ticket, Path workspace) throws IOException {
        Path gate = ticketPath(ticket);
        requireDirectory(gate.getParent());
        byte[] descriptor = workspace.toAbsolutePath().normalize().toString().getBytes(StandardCharsets.UTF_8);
        if (descriptor.length > MAX_WORKSPACE_BYTES) {
            throw new IOException("Workspace path is too long for a lease ticket");
        }
        Files.createDirectory(gate, privatePermissions("rwx------"));
        Path path = Files.createFile(gate.resolve(WORKSPACE_FILE), privatePermissions("rw-------"));
        Files.write(path, descriptor, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
    }

    /**
     * Permanently closes a ticket to new acquisitions and checks for existing claims.
     *
     * @return whether any registered lease is clear for cleanup
     */
    public static boolean tryRetire(Path ticket, boolean required) throws IOException {
        Path gate = ticketPath(ticket);
        if (!directoryPresent(gate)) {
            return !required;
        }
        Path descriptor = gate.resolve(WORKSPACE_FILE);
        Path retired = gate.resolve(RETIRED_FILE);
        if (regularFilePresent(descriptor)) {
            if (regularFilePresent(retired)) {
                throw new IOException("Both open and retired workspace lease descriptors exist");
            }
            // Moving only the descriptor leaves claim files in place, including open Windows handles.
            Files.move(descriptor, retired, StandardCopyOption.ATOMIC_MOVE);
        } else if (!regularFilePresent(retired)) {
            if (required) {
                return false;
            }
            // Publication may have stopped before writing the descriptor. Finish retirement without opening admission.
            Files.createFile(retired, privatePermissions("rw-------"));
        }
        try (var entries = Files.newDirectoryStream(gate)) {
            for (Path entry : entries) {
                if (entry.getFileName().toString().equals(RETIRED_FILE)) {
                    requireRegularFile(entry);
                } else if (!claimIsIdle(entry)) {
                    return false;
                }
            }
            return true;
        } catch (DirectoryIteratorException e) {
            throw e.getCause();
        }
    }

    /**
     * Removes a retired ticket once its workspace directories are gone.
     */
    public static void removeRetired(Path ticket) throws IOException {
        Path gate = ticketPath(ticket);
        if (!directoryPresent(gate)) {
            return;
        }
        if (regularFilePresent(gate.resolve(WORKSPACE_FILE))) {
            throw new IOException("Cannot remove a workspace lease ticket that is open");
        }
        Path retired = gate.resolve(RETIRED_FILE);
        requireRegularFile(retired);
        try (var entries = Files.newDirectoryStream(gate)) {
            for (Path entry : entries) {
                String name = entry.getFileName().toString();
                if (name.equals(RETIRED_FILE)) {
                    continue;
                }
                if (!CLAIM_NAME.matcher(name).matches()) {
                    throw new IOException("Unexpected entry in a retired workspace lease ticket");
                }
                if (regularFilePresent(entry)) {
                    Files.deleteIfExists(entry);
                }
            }
        } catch (DirectoryIteratorException e) {
            throw e.getCause();
        }
        Files.deleteIfExists(retired);
        Files.deleteIfExists(gate);
    }

    static Path openTicket(Path ticket) throws IOException {
        Path gate = ticketPath(ticket);
        requireDirectory(gate);
        requireRegularFile(gate.resolve(WORKSPACE_FILE));
        return gate;
    }

    static Path readWorkspace(Path gate) throws IOException {
        // The caller already holds its claim lock. Check admission again after reading the descriptor so a
        // helper that opened it just as cleanup retired it cannot obtain the workspace after retirement.
        requireDirectory(gate);
        Path descriptor = gate.resolve(WORKSPACE_FILE);
        requireRegularFile(descriptor);
        byte[] bytes;
        try (var input = Files.newInputStream(descriptor, LinkOption.NOFOLLOW_LINKS)) {
            bytes = input.readNBytes(MAX_WORKSPACE_BYTES + 1);
        }
        if (bytes.length == 0 || bytes.length > MAX_WORKSPACE_BYTES) {
            throw new IOException("Invalid workspace lease ticket descriptor");
        }
        Path workspace;
        try {
            workspace = Path.of(StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString());
        } catch (InvalidPathException e) {
            throw new IOException("Invalid workspace path in lease ticket", e);
        }
        requireDirectory(workspace);
        requireRegularFile(descriptor);
        return workspace;
    }

    static void removeClaim(Path gate, Path claimName) throws IOException {
        if (directoryPresent(gate)) {
            Files.deleteIfExists(gate.resolve(claimName));
        }
    }

    static FileAttribute<?>[] privatePermissions(String permissions) {
        return POSIX ? new FileAttribute<?>[] { PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString(permissions)) } : new FileAttribute<?>[0];
    }

    private static boolean claimIsIdle(Path claim) throws IOException {
        String name = claim.getFileName().toString();
        if (!CLAIM_NAME.matcher(name).matches()) {
            return false;
        }
        if (!regularFilePresent(claim)) {
            return true;
        }
        // On POSIX, closing another descriptor for an inode can release this process's existing locks.
        // Never open a claim from this JVM, even when a different classloader created the lease.
        if (name.startsWith(ProcessHandle.current().pid() + "-")) {
            return false;
        }
        try (var channel = FileChannel.open(claim, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
             var lock = channel.tryLock()) {
            return lock != null;
        } catch (NoSuchFileException e) {
            return true;
        } catch (OverlappingFileLockException e) {
            return false;
        }
    }

    private static Path ticketPath(Path ticket) throws IOException {
        Path gate = ticket.toAbsolutePath().normalize();
        if (gate.getFileName() == null || !TICKET_NAME.matcher(gate.getFileName().toString()).matches()) {
            throw new IOException("Invalid workspace lease ticket name");
        }
        return gate;
    }

    private static void requireDirectory(Path path) throws IOException {
        if (!directoryPresent(path)) {
            throw new NoSuchFileException(path.toString());
        }
    }

    private static boolean directoryPresent(Path path) throws IOException {
        BasicFileAttributes attributes = attributes(path);
        if (attributes == null) {
            return false;
        }
        try {
            if (!attributes.isDirectory() || !path.equals(path.toRealPath())) {
                throw new IOException("Invalid workspace lease directory: " + path);
            }
            return true;
        } catch (NoSuchFileException e) {
            return false; // An acquisition or release can race with removing a retired gate.
        }
    }

    private static void requireRegularFile(Path path) throws IOException {
        if (!regularFilePresent(path)) {
            throw new NoSuchFileException(path.toString());
        }
    }

    private static boolean regularFilePresent(Path path) throws IOException {
        BasicFileAttributes attributes = attributes(path);
        if (attributes == null) {
            return false;
        }
        if (!attributes.isRegularFile()) {
            throw new IOException("Invalid workspace lease file: " + path);
        }
        return true;
    }

    private static BasicFileAttributes attributes(Path path) throws IOException {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException e) {
            return null;
        }
    }
}
