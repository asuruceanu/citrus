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

package org.citrusframework.distribution.junit;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.citrusframework.distribution.config.DistributionSettings;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherInterceptor;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

/**
 * JUnit Platform {@link LauncherInterceptor} wrapping local {@link Launcher} instances with
 * {@link DistributedLauncher} when distribution is enabled and the launcher is outside a local execution or worker.
 */
public class DistributionLauncherInterceptor implements LauncherInterceptor {

    /** JUnit discovery parameter for a launcher embedded in a locally executing test. */
    public static final String EMBEDDED_LAUNCHER_PARAMETER = "citrus.junit.jupiter.embeddedLauncher";

    private static final AtomicInteger LOCAL_EXECUTIONS = new AtomicInteger();
    private static final ThreadLocal<Boolean> LOCAL_SCOPE = new ThreadLocal<>();
    private static final Map<Thread, Integer> LOCAL_TEST_THREADS = new ConcurrentHashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <T> T intercept(Invocation<T> invocation) {
        T target = invocation.proceed();
        if (target instanceof Launcher launcher && isActiveForCurrentProcess()) {
            return (T) new DistributedLauncher(launcher);
        }
        return target;
    }

    static boolean isActiveForCurrentProcess() {
        return DistributionSettings.isEnabled() && !DistributionSettings.isWorkerRole() && !isInLocalScope();
    }

    static boolean isExecutingLocally() {
        return LOCAL_EXECUTIONS.get() > 0;
    }

    static boolean isInLocalScope() {
        return Boolean.TRUE.equals(LOCAL_SCOPE.get()) || LOCAL_TEST_THREADS.containsKey(Thread.currentThread());
    }

    static void runLocally(Runnable run) {
        if (isInLocalScope()) {
            run.run();
            return;
        }

        LOCAL_SCOPE.set(true);
        LOCAL_EXECUTIONS.incrementAndGet();
        try {
            run.run();
        } finally {
            LOCAL_EXECUTIONS.decrementAndGet();
            LOCAL_SCOPE.remove();
        }
    }

    static void runLocally(TestExecutionListener[] listeners, Consumer<TestExecutionListener[]> execute) {
        try (var scope = new LocalTestScope()) {
            TestExecutionListener[] scopedListeners = new TestExecutionListener[listeners.length + 1];
            scopedListeners[0] = scope;
            System.arraycopy(listeners, 0, scopedListeners, 1, listeners.length);
            runLocally(() -> execute.accept(scopedListeners));
        }
    }

    /** Tracks actual local JUnit execution threads, including threads supplied by its parallel executor. */
    private static final class LocalTestScope implements TestExecutionListener, AutoCloseable {

        private final Map<String, Thread> executions = new HashMap<>();
        private boolean closed;

        @Override
        public synchronized void executionStarted(TestIdentifier identifier) {
            if (!closed) {
                Thread thread = Thread.currentThread();
                if (executions.putIfAbsent(identifier.getUniqueId(), thread) == null) {
                    LOCAL_TEST_THREADS.merge(thread, 1, Integer::sum);
                }
            }
        }

        @Override
        public synchronized void executionFinished(TestIdentifier identifier, TestExecutionResult result) {
            Thread thread = executions.remove(identifier.getUniqueId());
            if (thread != null) {
                release(thread);
            }
        }

        @Override
        public synchronized void close() {
            closed = true;
            executions.values().forEach(LocalTestScope::release);
            executions.clear();
        }

        private static void release(Thread thread) {
            LOCAL_TEST_THREADS.computeIfPresent(thread, (key, count) -> count == 1 ? null : count - 1);
        }
    }

    @Override
    public void close() {
        // nothing to close per launcher session
    }
}
