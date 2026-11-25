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

package org.citrusframework.distribution.store;

import java.util.regex.Pattern;

import org.citrusframework.distribution.model.RegistrationKeyRecord;

/** Internal registration key metadata and the SHA-256 digest used for verification. */
public record StoredRegistrationKey(RegistrationKeyRecord metadata, String keyHash) {

    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-fA-F]{64}");

    public StoredRegistrationKey {
        if (keyHash == null || !SHA256_HEX.matcher(keyHash).matches()) {
            throw new IllegalArgumentException("Registration key hash must be a hexadecimal SHA-256 digest");
        }
    }
}
