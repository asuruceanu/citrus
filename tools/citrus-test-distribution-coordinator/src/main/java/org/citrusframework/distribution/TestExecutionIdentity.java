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

package org.citrusframework.distribution;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/** Test identities shared by the session summary and duration history. */
public final class TestExecutionIdentity {

    // Match the persisted prefix so live and rehydrated keys agree.
    private static final int UNIQUE_ID_KEY_LENGTH = 1024;

    private final Map<String, String> legacyKeyOwners = new HashMap<>();

    /**
     * Identity of one test across attempts: work unit plus JUnit unique id when reported, otherwise work unit, class,
     * and display name. The unique id keeps parameterized and dynamic invocations with equal display names apart; the
     * work unit keeps Citrus factory sources apart, since each source runs in its own filtered JUnit run and so always
     * reports the same {@code [dynamic-test:#1]} id.
     */
    public static String key(String workUnitKey, String className, String testName, String uniqueId) {
        if (uniqueId != null && !uniqueId.isBlank()) {
            String uid = uniqueId.length() > UNIQUE_ID_KEY_LENGTH ? uniqueId.substring(0, UNIQUE_ID_KEY_LENGTH) : uniqueId;
            return "uid:" + (workUnitKey != null ? workUnitKey : "") + "|" + uid;
        }
        String keyPart = (workUnitKey != null && !workUnitKey.isBlank()) ? workUnitKey : "";
        String cls = (className != null && !className.isBlank()) ? className : "";
        String tst = (testName != null && !testName.isBlank()) ? testName : "";
        return keyPart + "|" + cls + "#" + tst;
    }

    /**
     * Keeps a legacy identity when a test first reported without a JUnit unique id later provides one.
     * Only the first unique id can claim the legacy identity; invocations sharing a display name stay separate.
     */
    public String resolve(String workUnitKey, String className, String testName, String uniqueId,
                          Predicate<String> hasResult) {
        String key = key(workUnitKey, className, testName, uniqueId);
        if (!hasResult.test(key)) {
            String legacy = key(workUnitKey, className, testName, null);
            if (!legacy.equals(key) && hasResult.test(legacy)) {
                String owner = legacyKeyOwners.putIfAbsent(legacy, key);
                if (owner == null || owner.equals(key)) {
                    return legacy;
                }
            }
        }
        return key;
    }
}
