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

package org.citrusframework.distribution.worker;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.platform.launcher.LauncherInterceptor;

/**
 * Stands in for a test framework's launcher interceptor (Quarkus installs its class loader this way). Registered in
 * this module's test resources; only active when {@code junit.platform.launcher.interceptors.enabled=true}.
 */
public class CountingLauncherInterceptor implements LauncherInterceptor {

    static final AtomicInteger INVOCATIONS = new AtomicInteger();

    @Override
    public <T> T intercept(Invocation<T> invocation) {
        INVOCATIONS.incrementAndGet();
        return invocation.proceed();
    }

    @Override
    public void close() {
    }
}
