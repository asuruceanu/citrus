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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/** Checks filesystem references that can outlive both JVMs and no longer appear in their process trees. */
final class WorkspaceReferences {

    private static final int UNIX_FILE_TYPE_MASK = 0170000;
    private static final int UNIX_SOCKET_TYPE = 0140000;

    private WorkspaceReferences() {
    }

    static boolean inUse(List<Path> roots, boolean recovering) throws IOException {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("linux")) {
            return LinuxWorkspaceReferences.inUse(roots) || containsUnixSocket(roots);
        }
        if (os.startsWith("mac")) {
            return macOsReferences(roots) || containsUnixSocket(roots);
        }
        // An orphan on a platform without an inspection backend has no owner left to vouch for it.
        return recovering;
    }

    private static boolean containsUnixSocket(List<Path> roots) throws IOException {
        // The OS can report a socket by the relative name it originally bound, even after its listener changes
        // directories. A socket node alone is insufficient to prove that no process is still serving it.
        for (Path root : roots) {
            try (var entries = Files.find(root, Integer.MAX_VALUE, (path, attributes) -> attributes.isOther())) {
                var specialFiles = entries.iterator();
                while (specialFiles.hasNext()) {
                    Path path = specialFiles.next();
                    int mode = (int) Files.getAttribute(path, "unix:mode", LinkOption.NOFOLLOW_LINKS);
                    if ((mode & UNIX_FILE_TYPE_MASK) == UNIX_SOCKET_TYPE) {
                        return true;
                    }
                }
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }
        return false;
    }

    private static boolean macOsReferences(List<Path> roots) throws IOException {
        // Terse mode disables warnings by default. Re-enable them to detect partial directory scans.
        List<String> command = new ArrayList<>(List.of("/usr/sbin/lsof", "-n", "-P", "-t", "+w", "-x", "f"));
        for (Path root : roots) {
            command.add("+D");
            command.add(root.toString());
        }
        Process inspection = new ProcessBuilder(command).redirectErrorStream(true).start();
        boolean wasInterrupted = Thread.interrupted();
        try (var output = inspection.getInputStream()) {
            if (!inspection.waitFor(5, TimeUnit.SECONDS)) {
                throw new IOException("Timed out checking workspace process references");
            }
            // macOS lsof can return 1 with matching PIDs. Only 1 with no output confirms no matches.
            // Diagnostics also keep the workspace, since they may indicate a partial scan.
            return output.read() != -1 || inspection.exitValue() != 1;
        } catch (InterruptedException e) {
            wasInterrupted = true;
            throw new IOException("Interrupted checking workspace process references", e);
        } finally {
            if (inspection.isAlive()) {
                inspection.destroyForcibly();
            }
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
