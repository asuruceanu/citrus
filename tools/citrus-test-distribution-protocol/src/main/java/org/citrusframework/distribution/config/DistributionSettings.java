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

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Centralized configuration keys, environment variable fallbacks, and host capability detection
 * for Citrus Test Distribution.
 */
public final class DistributionSettings {

    public static final String ENABLED_PROPERTY = "citrus.test.distribution.enabled";
    public static final String ENABLED_ENV = "CITRUS_DIST_ENABLED";

    public static final String ROLE_PROPERTY = "citrus.test.distribution.role";
    public static final String ROLE_WORKER = "worker";
    public static final String ROLE_COORDINATOR = "coordinator";

    public static final String SESSION_ID_PROPERTY = "citrus.test.distribution.sessionId";
    public static final String MODULE_NAME_PROPERTY = "citrus.test.distribution.moduleName";
    public static final String REPORT_DIR_PROPERTY = "citrus.test.distribution.reportDir";
    public static final String PROJECT_NAME_PROPERTY = "citrus.test.distribution.projectName";

    public static final String BROKER_URL_PROPERTY = "citrus.test.distribution.brokerUrl";
    public static final String BROKER_URL_ENV = "CITRUS_DIST_BROKER_URL";
    public static final String DEFAULT_BROKER_URL = "http://localhost:8567";

    public static final String AUTH_TOKEN_PROPERTY = "citrus.test.distribution.authToken";
    public static final String AUTH_TOKEN_ENV = "CITRUS_DIST_AUTH_TOKEN";

    public static final String FALLBACK_LOCAL_PROPERTY = "citrus.test.distribution.fallbackLocal";
    public static final String FALLBACK_LOCAL_ENV = "CITRUS_DIST_FALLBACK_LOCAL";

    public static final String LOCAL_EXECUTORS_PROPERTY = "citrus.test.distribution.localExecutors";
    public static final String LOCAL_EXECUTORS_ENV = "CITRUS_DIST_LOCAL_EXECUTORS";

    public static final String MAX_PARTITION_SIZE_PROPERTY = "citrus.test.distribution.maxPartitionSize";
    public static final String MAX_PARTITION_SIZE_ENV = "CITRUS_DIST_MAX_PARTITION_SIZE";
    public static final int DEFAULT_MAX_PARTITION_SIZE = 5;

    public static final String MAX_RETRIES_PROPERTY = "citrus.test.distribution.maxRetries";
    public static final String MAX_RETRIES_ENV = "CITRUS_DIST_MAX_RETRIES";
    public static final int DEFAULT_MAX_RETRIES = 1;

    public static final String DEFAULT_DURATION_MS_PROPERTY = "citrus.test.distribution.defaultTestDurationMs";
    public static final String DEFAULT_DURATION_MS_ENV = "CITRUS_DIST_DEFAULT_DURATION_MS";
    public static final long DEFAULT_DURATION_MS = 1_000L;

    public static final String CONNECT_TIMEOUT_MS_PROPERTY = "citrus.test.distribution.connectTimeoutMs";
    public static final String CONNECT_TIMEOUT_MS_ENV = "CITRUS_DIST_CONNECT_TIMEOUT_MS";
    public static final long DEFAULT_CONNECT_TIMEOUT_MS = 3_000L;

    public static final String SESSION_TIMEOUT_MS_PROPERTY = "citrus.test.distribution.sessionTimeoutMs";
    public static final String SESSION_TIMEOUT_MS_ENV = "CITRUS_DIST_SESSION_TIMEOUT_MS";
    public static final long DEFAULT_SESSION_TIMEOUT_MS = 600_000L;

    public static final String JDBC_URL_PROPERTY = "citrus.test.distribution.jdbcUrl";
    public static final String JDBC_URL_ENV = "CITRUS_DIST_JDBC_URL";
    public static final String DEFAULT_JDBC_URL = "jdbc:hsqldb:file:.citrus-dist/db/telemetry;shutdown=true";

    public static final String JDBC_USER_PROPERTY = "citrus.test.distribution.jdbcUser";
    public static final String JDBC_USER_ENV = "CITRUS_DIST_JDBC_USER";

    public static final String JDBC_PASSWORD_PROPERTY = "citrus.test.distribution.jdbcPassword";
    public static final String JDBC_PASSWORD_ENV = "CITRUS_DIST_JDBC_PASSWORD";

    public static final String CACHE_DIR_PROPERTY = "citrus.test.distribution.cacheDir";
    public static final String CACHE_DIR_ENV = "CITRUS_DIST_CACHE_DIR";
    public static final String DEFAULT_CACHE_DIR = ".citrus-dist/cache";

    private DistributionSettings() {
    }

    public static boolean isEnabled() {
        return getBoolean(ENABLED_PROPERTY, ENABLED_ENV, false);
    }

    public static boolean isWorkerRole() {
        return ROLE_WORKER.equalsIgnoreCase(System.getProperty(ROLE_PROPERTY, ""));
    }

    public static String getBrokerUrl() {
        return getString(BROKER_URL_PROPERTY, BROKER_URL_ENV, DEFAULT_BROKER_URL);
    }

    public static String getAuthToken() {
        return getString(AUTH_TOKEN_PROPERTY, AUTH_TOKEN_ENV, "");
    }

    public static boolean isFallbackLocal() {
        return getBoolean(FALLBACK_LOCAL_PROPERTY, FALLBACK_LOCAL_ENV, true);
    }

    public static int getLocalExecutors() {
        return getInt(LOCAL_EXECUTORS_PROPERTY, LOCAL_EXECUTORS_ENV, 0);
    }

    public static int getMaxPartitionSize() {
        return Math.max(1, getInt(MAX_PARTITION_SIZE_PROPERTY, MAX_PARTITION_SIZE_ENV, DEFAULT_MAX_PARTITION_SIZE));
    }

    public static int getMaxRetries() {
        return Math.max(0, getInt(MAX_RETRIES_PROPERTY, MAX_RETRIES_ENV, DEFAULT_MAX_RETRIES));
    }

    public static long getDefaultTestDurationMs() {
        return Math.max(1L, getLong(DEFAULT_DURATION_MS_PROPERTY, DEFAULT_DURATION_MS_ENV, DEFAULT_DURATION_MS));
    }

    public static long getConnectTimeoutMs() {
        return Math.max(250L, getLong(CONNECT_TIMEOUT_MS_PROPERTY, CONNECT_TIMEOUT_MS_ENV, DEFAULT_CONNECT_TIMEOUT_MS));
    }

    public static long getSessionTimeoutMs() {
        return Math.max(1_000L, getLong(SESSION_TIMEOUT_MS_PROPERTY, SESSION_TIMEOUT_MS_ENV, DEFAULT_SESSION_TIMEOUT_MS));
    }

    public static String getJdbcUrl() {
        return getString(JDBC_URL_PROPERTY, JDBC_URL_ENV, DEFAULT_JDBC_URL);
    }

    public static String getJdbcUser() {
        return getString(JDBC_USER_PROPERTY, JDBC_USER_ENV, "SA");
    }

    public static String getJdbcPassword() {
        return getString(JDBC_PASSWORD_PROPERTY, JDBC_PASSWORD_ENV, "");
    }

    public static String getCacheDir() {
        return getString(CACHE_DIR_PROPERTY, CACHE_DIR_ENV, DEFAULT_CACHE_DIR);
    }

    public static String getString(String prop, String env, String defaultValue) {
        String value = System.getProperty(prop);
        if (value == null || value.isBlank()) {
            value = env != null ? System.getenv(env) : null;
        }
        return (value == null || value.isBlank()) ? defaultValue : value.trim();
    }

    public static boolean getBoolean(String prop, String env, boolean defaultValue) {
        String raw = getString(prop, env, null);
        return raw == null ? defaultValue : Boolean.parseBoolean(raw);
    }

    public static int getInt(String prop, String env, int defaultValue) {
        String raw = getString(prop, env, null);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    public static long getLong(String prop, String env, long defaultValue) {
        String raw = getString(prop, env, null);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    public static Map<String, String> detectHostCapabilities(String extraCsv) {
        Map<String, String> caps = new LinkedHashMap<>();
        String osName = System.getProperty("os.name", "unknown").toLowerCase(Locale.ROOT);
        if (osName.contains("linux")) {
            caps.put("os", "linux");
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            caps.put("os", "macos");
        } else if (osName.contains("win")) {
            caps.put("os", "windows");
        } else {
            caps.put("os", osName);
        }
        caps.put("arch", System.getProperty("os.arch", "unknown"));
        caps.put("jdk", String.valueOf(Runtime.version().feature()));
        caps.put("cpuCores", String.valueOf(Runtime.getRuntime().availableProcessors()));
        caps.put("memoryMb", String.valueOf(Runtime.getRuntime().maxMemory() / (1024L * 1024L)));
        caps.putAll(parseCapabilitiesCsv(extraCsv));
        return caps;
    }

    public static Map<String, String> parseCapabilitiesCsv(String csv) {
        Map<String, String> parsed = new LinkedHashMap<>();
        if (csv == null || csv.isBlank()) {
            return parsed;
        }
        for (String token : csv.split(",")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq > 0) {
                parsed.put(trimmed.substring(0, eq).trim(), trimmed.substring(eq + 1).trim());
            } else {
                parsed.put(trimmed, "true");
            }
        }
        return parsed;
    }
}
