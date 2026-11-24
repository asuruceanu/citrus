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

package org.citrusframework.distribution.config;

import java.util.Map;
import java.util.function.UnaryOperator;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.StringNode;

/** Locations used to reconstruct a Maven Quarkus application model inside a forked workspace. */
public record QuarkusWorkspace(String modelPath, Map<Integer, String> classpathDirectories,
                               Map<String, Location> modelLocations) {

    public static final String PROPERTY = "citrus.test.distribution.internal.quarkusWorkspace";
    public static final String CAPABILITY = "citrus.workspace.quarkus.v1";
    public static final String MODEL_PROPERTY = "quarkus-internal-test.serialized-app-model.path";

    /** A location in the transferred classpath, or a path relative to the partition workspace. */
    public record Location(Integer classpathIndex, String workspacePath) {
    }

    /** Rewrites JSON string values without changing field names or JSON escaping. */
    public static JsonNode rewriteStrings(JsonNode node, UnaryOperator<String> rewrite) {
        if (node instanceof ObjectNode object) {
            object.properties().forEach(entry -> entry.setValue(rewriteStrings(entry.getValue(), rewrite)));
        } else if (node instanceof ArrayNode array) {
            for (int i = 0; i < array.size(); i++) {
                array.set(i, rewriteStrings(array.get(i), rewrite));
            }
        } else if (node.isTextual()) {
            String value = node.asString();
            String replacement = rewrite.apply(value);
            if (!value.equals(replacement)) {
                return StringNode.valueOf(replacement);
            }
        }
        return node;
    }
}
