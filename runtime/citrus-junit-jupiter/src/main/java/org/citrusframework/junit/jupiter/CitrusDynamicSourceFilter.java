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

package org.citrusframework.junit.jupiter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Thread-scoped filter and discovery probe for {@link CitrusTestFactorySupport} dynamic test sources.
 * <p>
 * Enables distributed test coordinators to probe the declarative test names produced by a
 * {@code @CitrusTestFactory} method without instantiating {@code TestLoader}s, and enables remote
 * workers to execute an exact subset of declarative test sources within that factory class.
 */
public final class CitrusDynamicSourceFilter {

    public static final String DYNAMIC_SOURCES_PROPERTY = "citrus.test.distribution.dynamic.sources";

    private static final InheritableThreadLocal<Set<String>> ALLOWED_SOURCES = new InheritableThreadLocal<>();
    private static final InheritableThreadLocal<List<String>> PROBED_SOURCES = new InheritableThreadLocal<>();

    private CitrusDynamicSourceFilter() {
    }

    /**
     * Restricts subsequent {@link CitrusTestFactorySupport} calls on the current thread (and child threads)
     * to the supplied declarative test names. Passing {@code null} clears thread-local filtering.
     */
    public static void setAllowedSources(Set<String> allowedTestNames) {
        if (allowedTestNames == null) {
            ALLOWED_SOURCES.remove();
        } else {
            ALLOWED_SOURCES.set(Collections.unmodifiableSet(new LinkedHashSet<>(allowedTestNames)));
        }
    }

    /**
     * Clears any thread-local source filter and active discovery probe on the current thread.
     */
    public static void clear() {
        ALLOWED_SOURCES.remove();
        PROBED_SOURCES.remove();
    }

    /**
     * Starts discovery probe mode on the current thread. While active, {@link CitrusTestFactorySupport}
     * records every candidate declarative test name in encounter order without instantiating {@code TestLoader}s
     * or emitting executable {@code DynamicTest}s.
     */
    public static void startProbe() {
        PROBED_SOURCES.set(new ArrayList<>());
    }

    /**
     * Ends discovery probe mode on the current thread and returns the distinct declarative test names discovered.
     */
    public static List<String> finishProbe() {
        List<String> probed = PROBED_SOURCES.get();
        PROBED_SOURCES.remove();
        if (probed == null || probed.isEmpty()) {
            return Collections.emptyList();
        }
        return List.copyOf(new LinkedHashSet<>(probed));
    }

    /**
     * Checks whether the given declarative test name is allowed by the active thread-local filter
     * or the {@link #DYNAMIC_SOURCES_PROPERTY} system property fallback.
     */
    public static boolean isAllowed(String testName) {
        if (testName == null) {
            return false;
        }
        Set<String> allowed = ALLOWED_SOURCES.get();
        if (allowed != null) {
            return allowed.contains(testName);
        }
        String prop = System.getProperty(DYNAMIC_SOURCES_PROPERTY);
        if (prop != null && !prop.isBlank()) {
            Set<String> fromProp = Arrays.stream(prop.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toSet());
            return fromProp.contains(testName);
        }
        return true;
    }

    /**
     * Package-private hook invoked by {@link CitrusTestFactorySupport}. Records the candidate in probe mode
     * (returning {@code false} to skip {@code TestLoader} instantiation), or evaluates {@link #isAllowed(String)}.
     */
    static boolean recordAndAllow(String testName) {
        List<String> probed = PROBED_SOURCES.get();
        if (probed != null) {
            if (testName != null) {
                probed.add(testName);
            }
            return false;
        }
        return isAllowed(testName);
    }
}
