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
 * {@link DistributedLauncher} when {@code citrus.test.distribution.enabled=true} and not on a worker JVM.
 */
public class DistributionLauncherInterceptor implements LauncherInterceptor {

    @Override
    @SuppressWarnings("unchecked")
    public <T> T intercept(Invocation<T> invocation) {
        T target = invocation.proceed();
        if (target instanceof Launcher launcher
                && DistributionSettings.isEnabled()
                && !DistributionSettings.isWorkerRole()) {
            return (T) new DistributedLauncher(launcher);
        }
        return target;
    }

    @Override
    public void close() {
        // nothing to close per launcher session
    }
}
