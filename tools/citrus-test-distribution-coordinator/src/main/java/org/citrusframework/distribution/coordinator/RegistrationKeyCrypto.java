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

package org.citrusframework.distribution.coordinator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HexFormat;

final class RegistrationKeyCrypto {
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom RANDOM = new SecureRandom();

    record GeneratedSecret(String keyPrefix, String secretKey, String sha256Hex) {
    }

    private RegistrationKeyCrypto() {
    }

    static String generatePoolId() {
        return randomString(8);
    }

    static GeneratedSecret generateSecret() {
        String prefix = "ctrk_" + randomString(8);
        String secret = prefix + "_" + randomString(32);
        return new GeneratedSecret(prefix, secret, HexFormat.of().formatHex(digest(secret)));
    }

    static boolean verifySecret(String candidateSecret, String expectedSha256Hex) {
        return candidateSecret != null && expectedSha256Hex != null
                && MessageDigest.isEqual(digest(candidateSecret), HexFormat.of().parseHex(expectedSha256Hex));
    }

    private static byte[] digest(String secret) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(secret.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String randomString(int length) {
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            result.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
        }
        return result.toString();
    }
}
