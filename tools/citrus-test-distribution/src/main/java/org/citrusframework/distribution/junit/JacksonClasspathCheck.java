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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Detects Jackson libraries present more than once on the test classpath.
 * <p>
 * The Maven extension appends the Jackson 3 jars the launcher needs after the module's own classpath. A module that
 * already brings a different version of one of them ends up with mixed versions, which surfaces later as
 * {@code NoSuchMethodError} or {@code NoClassDefFoundError}; this check names the duplicate jars up front instead.
 */
final class JacksonClasspathCheck {

    private static final Map<String, String> MARKERS = new LinkedHashMap<>();

    static {
        MARKERS.put("jackson-core", "tools/jackson/core/JsonParser.class");
        MARKERS.put("jackson-databind", "tools/jackson/databind/ObjectMapper.class");
        MARKERS.put("jackson-annotations", "com/fasterxml/jackson/annotation/JsonProperty.class");
    }

    private JacksonClasspathCheck() {
    }

    /** One message per Jackson library found in more than one location of {@code classLoader}. */
    static List<String> conflicts(ClassLoader classLoader) {
        List<String> conflicts = new ArrayList<>();
        if (classLoader == null) {
            return conflicts;
        }
        for (Map.Entry<String, String> marker : MARKERS.entrySet()) {
            Set<String> locations = new LinkedHashSet<>();
            try {
                for (URL url : Collections.list(classLoader.getResources(marker.getValue()))) {
                    String location = url.toString();
                    int inArchive = location.indexOf("!/");
                    locations.add(inArchive > 0 ? location.substring(0, inArchive)
                            : location.substring(0, location.length() - marker.getValue().length()));
                }
            } catch (IOException e) {
                continue;
            }
            if (locations.size() > 1) {
                conflicts.add(marker.getKey() + " is on the test classpath " + locations.size()
                        + " times, versions may be mixed: " + String.join(", ", locations));
            }
        }
        return conflicts;
    }
}
