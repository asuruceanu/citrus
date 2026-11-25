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

package org.citrusframework.distribution.scheduler;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.citrusframework.distribution.annotations.AgentRequirement;
import org.citrusframework.distribution.annotations.LocalOnly;
import org.citrusframework.distribution.annotations.RemoteOnly;

/**
 * Evaluates test placement annotations and matches required capabilities against worker attributes.
 */
public final class CapabilityMatcher {

    private static final Set<String> NUMERIC_THRESHOLD_KEYS = Set.of("memoryMb", "cpuCores");
    private static final Pattern CAPABILITY_TOKEN = Pattern.compile("[A-Za-z0-9._-]+");

    private CapabilityMatcher() {
    }

    public static boolean isLocalOnly(Class<?> testClass, Method testMethod) {
        if (testMethod != null) {
            if (testMethod.isAnnotationPresent(LocalOnly.class)) {
                return true;
            }
            if (testMethod.isAnnotationPresent(RemoteOnly.class)) {
                return false;
            }
        }
        return testClass != null && testClass.isAnnotationPresent(LocalOnly.class);
    }

    public static boolean isRemoteOnly(Class<?> testClass, Method testMethod) {
        if (isLocalOnly(testClass, testMethod)) {
            return false;
        }
        if (testMethod != null && testMethod.isAnnotationPresent(RemoteOnly.class)) {
            return true;
        }
        return testClass != null && testClass.isAnnotationPresent(RemoteOnly.class);
    }

    public static Map<String, String> extractRequiredCapabilities(Class<?> testClass, Method testMethod) {
        Map<String, String> merged = new LinkedHashMap<>();
        if (testClass != null) {
            for (AgentRequirement req : testClass.getAnnotationsByType(AgentRequirement.class)) {
                merged.put(req.key(), req.value());
            }
        }
        if (testMethod != null) {
            for (AgentRequirement req : testMethod.getAnnotationsByType(AgentRequirement.class)) {
                merged.put(req.key(), req.value());
            }
        }
        return merged;
    }

    /**
     * Validates capability keys and values for settings that require strict capability tokens.
     *
     * @throws IllegalArgumentException if a key or value does not match {@code [A-Za-z0-9._-]+}
     */
    public static void validateCapabilities(Map<String, String> capabilities) {
        if (capabilities == null) {
            return;
        }
        for (Map.Entry<String, String> entry : capabilities.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || !CAPABILITY_TOKEN.matcher(key).matches()) {
                throw new IllegalArgumentException("Invalid capability key '" + key + "'");
            }
            if (value == null || !CAPABILITY_TOKEN.matcher(value).matches()) {
                throw new IllegalArgumentException("Invalid value '" + value + "' for capability '" + key + "'");
            }
        }
    }

    public static boolean matches(Map<String, String> requiredCapabilities, Map<String, String> workerCapabilities) {
        if (requiredCapabilities == null || requiredCapabilities.isEmpty()) {
            return true;
        }
        if (workerCapabilities == null || workerCapabilities.isEmpty()) {
            return false;
        }
        for (Map.Entry<String, String> entry : requiredCapabilities.entrySet()) {
            String key = entry.getKey();
            String required = entry.getValue();
            String actual = workerCapabilities.get(key);
            if (actual == null || required == null) {
                return false;
            }
            if (!matchesValue(key, required.trim(), actual.trim())) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesValue(String key, String required, String actual) {
        if (required.startsWith(">=")) {
            return compareNumericAtLeast(required.substring(2).trim(), actual);
        }
        if (NUMERIC_THRESHOLD_KEYS.contains(key)) {
            try {
                double reqVal = Double.parseDouble(required);
                double actVal = Double.parseDouble(actual);
                return actVal >= reqVal;
            } catch (NumberFormatException ignored) {
                // Fall back to exact string comparison
            }
        }
        return required.equalsIgnoreCase(actual);
    }

    private static boolean compareNumericAtLeast(String thresholdText, String actualText) {
        try {
            double threshold = Double.parseDouble(thresholdText);
            double actual = Double.parseDouble(actualText);
            return actual >= threshold;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
