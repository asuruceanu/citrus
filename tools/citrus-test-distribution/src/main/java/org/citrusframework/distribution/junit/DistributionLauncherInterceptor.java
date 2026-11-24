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

import java.util.concurrent.atomic.AtomicInteger;

import org.citrusframework.distribution.config.DistributionSettings;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherInterceptor;

/**
 * JUnit Platform {@link LauncherInterceptor} wrapping coordinator {@link Launcher} instances with
 * {@link DistributedLauncher} when distribution is enabled and the launcher is outside a local execution or worker.
 */
public class DistributionLauncherInterceptor implements LauncherInterceptor {

    private static final AtomicInteger LOCAL_EXECUTIONS = new AtomicInteger();
    private static final ThreadLocal<Boolean> LOCAL_SCOPE = new ThreadLocal<>();

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
        return Boolean.TRUE.equals(LOCAL_SCOPE.get());
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

    @Override
    public void close() {
        // nothing to close per launcher session
    }
}
