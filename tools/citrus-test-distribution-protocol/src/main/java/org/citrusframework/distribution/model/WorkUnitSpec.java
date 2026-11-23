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

package org.citrusframework.distribution.model;

import java.util.Map;

public record WorkUnitSpec(
        String key,
        String className,
        String methodName,
        String dynamicSourceName,
        Map<String, String> requiredCapabilities,
        long estimatedDurationMs,
        java.util.List<String> enclosingClassNames
) {

    /**
     * {@code enclosingClassNames}: for a {@code @Nested} class (or a method of one), the classes enclosing it in the
     * host's test plan, outermost first. For a nested class inherited from a superclass this is the subclass, so the
     * worker selects it the same way (JUnit {@code selectNestedClass}) and reports the same unique ids.
     */
    public WorkUnitSpec {
        enclosingClassNames = enclosingClassNames != null ? java.util.List.copyOf(enclosingClassNames) : java.util.List.of();
    }

    public WorkUnitSpec(String key, String className, String methodName, String dynamicSourceName,
                        Map<String, String> requiredCapabilities, long estimatedDurationMs) {
        this(key, className, methodName, dynamicSourceName, requiredCapabilities, estimatedDurationMs, null);
    }
}
