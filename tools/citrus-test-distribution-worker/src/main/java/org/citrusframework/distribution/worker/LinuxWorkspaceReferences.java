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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

/** Checks workspace references independent of ancestry, including processes reparented after a JVM exits. */
final class LinuxWorkspaceReferences {

    private static final Path PROC = Path.of("/proc");
    private static final Pattern PID = Pattern.compile("[1-9][0-9]*");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");
    private static final List<String> PROCESS_PATHS = List.of("cwd", "root", "exe");
    private static final String DELETED_SUFFIX = " (deleted)";

    private LinuxWorkspaceReferences() {
    }

    static boolean inUse(List<Path> roots) throws IOException {
        Path self = PROC.resolve("self").toRealPath();
        String[] ownUserIds = processStatus(self).userIds();
        if (ownUserIds.length != 4) {
            throw new IOException("Cannot determine the worker's Linux user IDs");
        }
        String workerUid = ownUserIds[3]; // The filesystem UID governs access to the private workspace.
        try (var processes = Files.newDirectoryStream(PROC, path -> PID.matcher(path.getFileName().toString()).matches())) {
            for (Path process : processes) {
                ProcessStatus status = userProcessStatus(process, self, workerUid);
                if (status != null && processUsesWorkspace(process, status, roots)) {
                    return true;
                }
            }
        } catch (DirectoryIteratorException e) {
            throw e.getCause();
        }
        return unixSocketInWorkspace(roots);
    }

    private static ProcessStatus userProcessStatus(Path process, Path self, String workerUid) throws IOException {
        try {
            ProcessStatus status = processStatus(process);
            for (String uid : status.userIds()) {
                if (workerUid.equals(uid)) {
                    return status;
                }
            }
            return null;
        } catch (NoSuchFileException e) {
            return null; // Process exited before inspection.
        } catch (AccessDeniedException e) {
            try {
                if (!Files.getOwner(process).equals(Files.getOwner(self))) {
                    return null; // hidepid may restrict access to other users' status files.
                }
            } catch (NoSuchFileException exited) {
                return null;
            }
            throw e;
        } catch (IOException e) {
            if (Files.notExists(process)) {
                return null;
            }
            throw e;
        }
    }

    private static ProcessStatus processStatus(Path process) throws IOException {
        boolean zombie = false;
        String[] userIds = null;
        try (BufferedReader status = Files.newBufferedReader(process.resolve("status"))) {
            String line;
            while ((line = status.readLine()) != null) {
                if (line.startsWith("State:")) {
                    zombie = line.substring(6).stripLeading().startsWith("Z");
                } else if (line.startsWith("Uid:")) {
                    userIds = WHITESPACE.split(line.substring(4).strip());
                } else if (line.startsWith("Threads:") && userIds != null) {
                    int threads = Integer.parseInt(line.substring(8).strip());
                    return new ProcessStatus(userIds, zombie, threads);
                }
            }
        }
        throw new IOException("Cannot determine the Linux process status");
    }

    private static boolean processUsesWorkspace(Path process, ProcessStatus status, List<Path> roots) throws IOException {
        try {
            if (status.zombie()) {
                return status.threads() > 1 && siblingTaskUsesWorkspace(process, roots, true);
            }
            if (taskUsesWorkspace(process, roots, true)) {
                return true;
            }
            if (status.threads() > 1) {
                // Siblings can have private working directories and FD tables. Their mappings are shared, so
                // read those only if the leader has exited and may have lost its references during the check.
                ProcessStatus current = processStatus(process);
                return current.threads() > 1 && siblingTaskUsesWorkspace(process, roots, current.zombie());
            }
            return false;
        } catch (NoSuchFileException e) {
            return false; // Process exited while being inspected.
        }
    }

    private static boolean siblingTaskUsesWorkspace(Path process, List<Path> roots, boolean checkMappings)
            throws IOException {
        boolean sawSibling = false;
        try (var tasks = Files.newDirectoryStream(process.resolve("task"))) {
            for (Path task : tasks) {
                if (!task.getFileName().equals(process.getFileName())) {
                    sawSibling = true;
                    if (taskUsesWorkspace(task, roots, checkMappings)) {
                        return true;
                    }
                }
            }
        } catch (NoSuchFileException e) {
            // Some kernels hide the task directory after the group leader terminates.
        } catch (DirectoryIteratorException e) {
            if (!Files.notExists(process)) {
                throw e.getCause();
            }
        }
        if (!sawSibling && processStatus(process).threads() > 1) {
            throw new IOException("Cannot inspect the surviving threads of a Linux process");
        }
        return false;
    }

    private static boolean taskUsesWorkspace(Path process, List<Path> roots, boolean checkMappings) throws IOException {
        try {
            for (String name : PROCESS_PATHS) {
                if (linkInWorkspace(process.resolve(name), roots)) {
                    return true;
                }
            }
            try (var descriptors = Files.newDirectoryStream(process.resolve("fd"))) {
                for (Path descriptor : descriptors) {
                    if (linkInWorkspace(descriptor, roots)) {
                        return true;
                    }
                }
            }
            return checkMappings && mappedFileInWorkspace(process.resolve("maps"), roots);
        } catch (NoSuchFileException e) {
            return false; // Process exited while being inspected.
        } catch (DirectoryIteratorException e) {
            if (Files.notExists(process)) {
                return false;
            }
            throw e.getCause();
        } catch (IOException e) {
            if (Files.notExists(process)) {
                return false;
            }
            throw e;
        }
    }

    private static boolean linkInWorkspace(Path link, List<Path> roots) throws IOException {
        try {
            return pathInWorkspace(Files.readSymbolicLink(link).toString(), roots);
        } catch (NoSuchFileException e) {
            return false; // Process exited, or an individual descriptor closed.
        }
    }

    private static boolean mappedFileInWorkspace(Path maps, List<Path> roots) throws IOException {
        try (BufferedReader mappings = Files.newBufferedReader(maps)) {
            String line;
            while ((line = mappings.readLine()) != null) {
                String[] columns = WHITESPACE.split(line, 6);
                if (columns.length == 6 && (pathInWorkspace(columns[5], roots)
                        || (columns[5].contains("\\012") && pathInWorkspace(columns[5].replace("\\012", "\n"), roots)))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean unixSocketInWorkspace(List<Path> roots) throws IOException {
        // Descriptor links identify sockets only by inode. The current network namespace exposes bound pathname sockets.
        try (BufferedReader sockets = Files.newBufferedReader(PROC.resolve("net/unix"))) {
            String line;
            while ((line = sockets.readLine()) != null) {
                String[] columns = WHITESPACE.split(line, 8);
                if (columns.length == 8 && pathInWorkspace(columns[7], roots)) {
                    return true;
                }
            }
            return false;
        } catch (NoSuchFileException e) {
            return false; // This kernel has no UNIX domain socket table.
        }
    }

    private static boolean pathInWorkspace(String name, List<Path> roots) {
        if (!name.startsWith("/")) {
            return false;
        }
        try {
            Path target = Path.of(name).normalize();
            Path deletedTarget = name.endsWith(DELETED_SUFFIX)
                    ? Path.of(name.substring(0, name.length() - DELETED_SUFFIX.length())).normalize() : null;
            for (Path root : roots) {
                if (target.startsWith(root) || (deletedTarget != null && deletedTarget.startsWith(root))) {
                    return true;
                }
            }
            return false;
        } catch (InvalidPathException e) {
            return false;
        }
    }

    private record ProcessStatus(String[] userIds, boolean zombie, int threads) {
    }
}
