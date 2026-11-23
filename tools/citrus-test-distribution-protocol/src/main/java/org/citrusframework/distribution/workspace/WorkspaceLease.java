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

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/**
 * Keeps a worker workspace available to a cooperating helper on the same host.
 * Pass the ticket to the helper before disclosing the workspace path. The helper acquires and keeps the lease
 * open until it has finished every access to that path, including access after idle periods.
 */
public final class WorkspaceLease implements AutoCloseable {

    /** Partition system property that enables workspace lease tickets. */
    public static final String ENABLED_PROPERTY = "citrus.dist.workspace.lease.enabled";

    /** Partition system property that contains the path of an enabled workspace's ticket. */
    public static final String TICKET_PROPERTY = "citrus.dist.workspace.lease.ticket";

    private final Path ticket;
    private final Path claimName;
    private final Path workspace;
    private final FileChannel channel;
    private final FileLock lock;

    private WorkspaceLease(Path ticket, Path claimName, Path workspace, FileChannel channel, FileLock lock) {
        this.ticket = ticket;
        this.claimName = claimName;
        this.workspace = workspace;
        this.channel = channel;
        this.lock = lock;
    }

    /**
     * Claims a ticket and resolves its workspace while the claim is held.
     *
     * @param ticket the ticket received from the partition
     * @return a lease owned by the calling process
     * @throws IOException if the ticket is unavailable or could not be claimed
     */
    public static WorkspaceLease acquire(Path ticket) throws IOException {
        Path gate = WorkspaceLeaseGate.openTicket(ticket);
        String name = ProcessHandle.current().pid() + "-" + UUID.randomUUID().toString().replace("-", "") + ".claim";
        Path claim = Files.createFile(gate.resolve(name), WorkspaceLeaseGate.privatePermissions("rw-------"));
        FileChannel channel = null;
        FileLock lock = null;
        try {
            channel = FileChannel.open(claim, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            lock = channel.tryLock();
            if (lock == null) {
                throw new IOException("Could not lock workspace lease claim");
            }
            Path workspace = WorkspaceLeaseGate.readWorkspace(gate);
            return new WorkspaceLease(gate, claim.getFileName(), workspace, channel, lock);
        } catch (IOException | RuntimeException e) {
            try {
                release(gate, claim.getFileName(), channel);
            } catch (IOException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
    }

    /**
     * Returns the leased workspace directory. Keep this lease open for the duration of its use.
     */
    public Path workspace() {
        if (!lock.isValid()) {
            throw new IllegalStateException("Workspace lease is closed");
        }
        return workspace;
    }

    @Override
    public void close() throws IOException {
        release(ticket, claimName, channel);
    }

    private static void release(Path ticket, Path claimName, FileChannel channel) throws IOException {
        // Resources close in reverse order. Closing the channel releases its operating system lock.
        try (Closeable removeClaim = () -> WorkspaceLeaseGate.removeClaim(ticket, claimName); channel) {
            // The claim stays registered until its channel has closed.
        }
    }
}
