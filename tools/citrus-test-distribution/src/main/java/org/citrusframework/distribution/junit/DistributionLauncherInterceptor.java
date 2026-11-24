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

import org.citrusframework.distribution.config.DistributionSettings;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherInterceptor;

/**
 * JUnit Platform {@link LauncherInterceptor} wrapping coordinator {@link Launcher} instances with
 * {@link DistributedLauncher} when distribution is enabled and the launcher is outside a local execution or worker.
 */
public class DistributionLauncherInterceptor implements LauncherInterceptor {

    private static final InheritableThreadLocal<LocalExecution> LOCAL_EXECUTION = new InheritableThreadLocal<>();

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
        return DistributionSettings.isEnabled() && !DistributionSettings.isWorkerRole() && !isExecutingLocally();
    }

    static boolean isExecutingLocally() {
        LocalExecution execution = LOCAL_EXECUTION.get();
        return execution != null && execution.active;
    }

    static void runLocally(Runnable run) {
        if (isExecutingLocally()) {
            run.run();
            return;
        }

        // JUnit can create executor threads for a local plan. They inherit the guard for that plan's lifetime.
        LocalExecution execution = new LocalExecution();
        LOCAL_EXECUTION.set(execution);
        try {
            run.run();
        } finally {
            execution.active = false;
            LOCAL_EXECUTION.remove();
        }
    }

    private static final class LocalExecution {
        private volatile boolean active = true;
    }

    @Override
    public void close() {
        // nothing to close per launcher session
    }
}
