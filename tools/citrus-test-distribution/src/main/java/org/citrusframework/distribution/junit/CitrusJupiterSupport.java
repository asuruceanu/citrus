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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

/**
 * The only entry point from the launcher into {@code citrus-junit-jupiter}.
 * <p>
 * The Maven extension injects just the distribution and protocol jars into Surefire, so modules that do not depend on
 * {@code citrus-junit-jupiter} (for example {@code core/citrus-api}) must still be able to discover and distribute
 * tests. Callers check {@link #isAvailable()} first; the nested {@link Jupiter} holder is the only class that links
 * against {@code org.citrusframework.junit.jupiter}, and it is only loaded once that check has passed.
 */
final class CitrusJupiterSupport {

    /** Every citrus-junit-jupiter type the nested {@link Jupiter} holder links against. */
    private static final List<String> PROBE_CLASSES = List.of(
            "org.citrusframework.junit.jupiter.CitrusTestFactory",
            "org.citrusframework.junit.jupiter.CitrusTestFactorySupport",
            "org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter");

    private static final boolean AVAILABLE = detect();

    private CitrusJupiterSupport() {
    }

    /** Whether {@code citrus-junit-jupiter} is visible to the class loader that loaded the launcher. */
    static boolean isAvailable() {
        return AVAILABLE;
    }

    /** Whether {@code method} is a {@code @CitrusTestFactory} on a {@code CitrusTestFactorySupport} class. */
    static boolean isFactoryMethod(Class<?> testClass, Method method) {
        return AVAILABLE && Jupiter.isFactoryMethod(testClass, method);
    }

    /** Invokes {@code factoryMethod} on {@code instance} in probe mode and returns the declarative source names. */
    static List<String> probeSources(Object instance, Method factoryMethod) throws Exception {
        if (!AVAILABLE) {
            return List.of();
        }
        return Jupiter.probeSources(instance, factoryMethod);
    }

    /** Restricts the next {@code @CitrusTestFactory} execution on this thread to {@code sources}. */
    static void allowSources(Set<String> sources) {
        if (AVAILABLE) {
            Jupiter.allowSources(sources);
        }
    }

    /** Clears any source restriction set by {@link #allowSources(Set)}. */
    static void clearAllowedSources() {
        if (AVAILABLE) {
            Jupiter.clearAllowedSources();
        }
    }

    private static boolean detect() {
        try {
            for (String probe : PROBE_CLASSES) {
                Class.forName(probe, false, CitrusJupiterSupport.class.getClassLoader());
            }
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /** Links against {@code citrus-junit-jupiter}; loaded only when {@link #AVAILABLE} is {@code true}. */
    private static final class Jupiter {

        static boolean isFactoryMethod(Class<?> testClass, Method method) {
            return method.isAnnotationPresent(org.citrusframework.junit.jupiter.CitrusTestFactory.class)
                    && org.citrusframework.junit.jupiter.CitrusTestFactorySupport.class.isAssignableFrom(testClass);
        }

        static List<String> probeSources(Object instance, Method factoryMethod) throws Exception {
            org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter.startProbe();
            List<String> sources;
            try {
                factoryMethod.invoke(instance);
            } finally {
                sources = org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter.finishProbe();
            }
            return sources;
        }

        static void allowSources(Set<String> sources) {
            org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter.setAllowedSources(sources);
        }

        static void clearAllowedSources() {
            org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter.clear();
        }
    }
}
