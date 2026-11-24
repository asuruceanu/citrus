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
import java.lang.annotation.Annotation;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import org.citrusframework.distribution.model.WorkUnitSpec;

/** Coordinates Jupiter isolated suites across test JVMs that share a home directory. */
public final class IsolatedTestLock implements AutoCloseable {

    private static final String ISOLATED = "org.junit.jupiter.api.parallel.Isolated";
    private static final ReentrantLock JVM_LOCK = new ReentrantLock();
    private static final long LOCK_POLL_MS = 100;
    private final FileChannel channel;

    private IsolatedTestLock(FileChannel channel) {
        this.channel = channel;
    }

    /** Returns a lock held until closed, or {@code null} when the selection needs no isolation. */
    public static IsolatedTestLock acquire(List<WorkUnitSpec> units, ClassLoader loader)
            throws IOException, InterruptedException {
        return acquire(units, loader, () -> false);
    }

    /** Interruptible acquisition for worker assignments that can also be cancelled through a token. */
    public static IsolatedTestLock acquire(List<WorkUnitSpec> units, ClassLoader loader,
                                           BooleanSupplier cancelled) throws IOException, InterruptedException {
        checkCancellation(cancelled);
        if (JVM_LOCK.isHeldByCurrentThread()
                || units.stream().map(WorkUnitSpec::className).distinct().noneMatch(name -> isIsolated(name, loader))) {
            return null;
        }

        Path directory = Path.of(System.getProperty("user.home"), ".citrus-dist");
        Files.createDirectories(directory);
        while (!JVM_LOCK.tryLock(LOCK_POLL_MS, TimeUnit.MILLISECONDS)) {
            checkCancellation(cancelled);
        }
        try {
            checkCancellation(cancelled);
            FileChannel channel = FileChannel.open(directory.resolve("isolated-tests.lock"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            try {
                while (channel.tryLock() == null) {
                    checkCancellation(cancelled);
                    Thread.sleep(LOCK_POLL_MS);
                }
                checkCancellation(cancelled);
                return new IsolatedTestLock(channel);
            } catch (IOException | InterruptedException | RuntimeException | Error e) {
                try {
                    channel.close();
                } catch (IOException failure) {
                    e.addSuppressed(failure);
                }
                throw e;
            }
        } catch (IOException | InterruptedException | RuntimeException | Error e) {
            JVM_LOCK.unlock();
            throw e;
        }
    }

    private static void checkCancellation(BooleanSupplier cancelled) {
        if (cancelled.getAsBoolean()) {
            throw new CancellationException("Partition cancelled while waiting for isolated tests");
        }
    }

    private static boolean isIsolated(String className, ClassLoader loader) {
        try {
            return Arrays.stream(Class.forName(className, false, loader).getAnnotations())
                    .map(Annotation::annotationType)
                    .anyMatch(type -> ISOLATED.equals(type.getName()));
        } catch (ClassNotFoundException e) {
            // The launcher reports missing test classes during normal discovery.
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // Closing the channel releases the OS lock. Keep the file so all contenders use the same inode.
            channel.close();
        } finally {
            JVM_LOCK.unlock();
        }
    }
}
