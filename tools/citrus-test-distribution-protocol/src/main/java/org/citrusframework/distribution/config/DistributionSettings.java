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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.citrusframework.distribution.model.PartitionPolicy;

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

    public static final String BROKER_MIN_PARTITION_DURATION_MS_PROPERTY =
            "citrus.test.distribution.broker.minPartitionDurationMs";
    public static final String BROKER_MIN_PARTITION_DURATION_MS_ENV = "CITRUS_DIST_BROKER_MIN_PARTITION_DURATION_MS";
    public static final long DEFAULT_BROKER_MIN_PARTITION_DURATION_MS = 5_000L;

    public static final String BROKER_MAX_WAVES_PER_WORKER_PROPERTY = "citrus.test.distribution.broker.maxWavesPerWorker";
    public static final String BROKER_MAX_WAVES_PER_WORKER_ENV = "CITRUS_DIST_BROKER_MAX_WAVES_PER_WORKER";
    public static final int DEFAULT_BROKER_MAX_WAVES_PER_WORKER = 2;

    public static final String MIN_REMOTE_DURATION_MS_PROPERTY = "citrus.test.distribution.minRemoteDurationMs";
    public static final String MIN_REMOTE_DURATION_MS_ENV = "CITRUS_DIST_MIN_REMOTE_DURATION_MS";
    public static final long DEFAULT_MIN_REMOTE_DURATION_MS = 2_000L;

    public static final String MIN_REMOTE_COLD_UNITS_PROPERTY = "citrus.test.distribution.minRemoteColdUnits";
    public static final String MIN_REMOTE_COLD_UNITS_ENV = "CITRUS_DIST_MIN_REMOTE_COLD_UNITS";
    public static final int DEFAULT_MIN_REMOTE_COLD_UNITS = 0;

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

    public static final String WORKER_CACHE_DIR_PROPERTY = "citrus.test.distribution.worker.cacheDir";
    public static final String WORKER_CACHE_DIR_ENV = "CITRUS_DIST_WORKER_CACHE_DIR";
    public static final String AGENT_CACHE_DIR_ENV = "TEST_DISTRIBUTION_AGENT_CACHE_DIR";

    public static final String WORKER_CACHE_CLEANUP_PROPERTY = "citrus.test.distribution.worker.cacheCleanup";
    public static final String WORKER_CACHE_CLEANUP_ENV = "CITRUS_DIST_WORKER_CACHE_CLEANUP";
    public static final String AGENT_CACHE_CLEANUP_ENV = "TEST_DISTRIBUTION_AGENT_CACHE_CLEANUP";
    public static final boolean DEFAULT_WORKER_CACHE_CLEANUP = true;

    public static final String WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY =
            "citrus.test.distribution.worker.cacheFreeSpaceBufferSize";
    public static final String WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_ENV = "CITRUS_DIST_WORKER_CACHE_FREE_SPACE_BUFFER_SIZE";
    public static final String AGENT_CACHE_FREE_SPACE_BUFFER_SIZE_ENV = "TEST_DISTRIBUTION_AGENT_CACHE_FREE_SPACE_BUFFER_SIZE";
    public static final long DEFAULT_WORKER_CACHE_FREE_SPACE_BUFFER_BYTES = 2L * 1024 * 1024 * 1024;

    private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("^\\s*(\\d+)\\s*(KiB|MiB|GiB)?\\s*$",
            Pattern.CASE_INSENSITIVE);

    /** Worker capability advertised by workspace-aware workers and required by workspace partitions. */
    public static final String WORKSPACE_CAPABILITY_KEY = "citrus.workspace.v1";
    /** Placeholder substituted for the host module base directory in forwarded system properties. */
    public static final String WORKSPACE_TOKEN = "${citrus.dist.workspace}";
    /** Case-insensitive explicit empty-list value for {@code inputs} and {@code outputs}. */
    public static final String NONE_SENTINEL = "none";

    public static final String BASEDIR_PROPERTY = "citrus.test.distribution.basedir";

    public static final String INPUTS_PROPERTY = "citrus.test.distribution.inputs";
    public static final String INPUTS_ENV = "CITRUS_DIST_INPUTS";

    public static final String OUTPUTS_PROPERTY = "citrus.test.distribution.outputs";
    public static final String OUTPUTS_ENV = "CITRUS_DIST_OUTPUTS";

    /**
     * Module property: {@code true} keeps the module's tests local, for suites that cannot run on a worker (for
     * example {@code @QuarkusTest}, which bootstraps from the Maven project layout). Wins over an enabled build.
     */
    public static final String SKIP_PROPERTY = "citrus.test.distribution.skip";
    public static final String SKIP_ENV = "CITRUS_DIST_SKIP";
    public static final String FORK_PROPERTY = "citrus.test.distribution.fork";
    public static final String FORK_ENV = "CITRUS_DIST_FORK";

    public static final String PARTITION_POLICY_PROPERTY = "citrus.test.distribution.partitionPolicy";
    public static final String PARTITION_POLICY_ENV = "CITRUS_DIST_PARTITION_POLICY";

    public static final String WORKER_FORK_MODE_PROPERTY = "citrus.test.distribution.worker.forkMode";
    public static final String WORKER_FORK_MODE_ENV = "CITRUS_DIST_WORKER_FORK_MODE";

    public static final String WORKER_FORK_JVM_ARGS_PROPERTY = "citrus.test.distribution.worker.forkJvmArgs";
    public static final String WORKER_FORK_JVM_ARGS_ENV = "CITRUS_DIST_WORKER_FORK_JVM_ARGS";
    public static final List<String> DEFAULT_FORK_JVM_ARGS = List.of("-Xmx768m", "-XX:ActiveProcessorCount=2");

    public static final String WORKER_CWD_CHECK_PROPERTY = "citrus.test.distribution.worker.cwdCheck";
    public static final String WORKER_CWD_CHECK_ENV = "CITRUS_DIST_WORKER_CWD_CHECK";

    public static final String WORKER_HANDSHAKE_TIMEOUT_PROPERTY = "citrus.test.distribution.worker.handshakeTimeoutMs";
    public static final long DEFAULT_WORKER_HANDSHAKE_TIMEOUT_MS = 15_000L;

    /** Whether a worker forks a child JVM only when a partition needs a workspace, or for every partition. */
    public enum WorkerForkMode {
        AUTO,
        ALWAYS
    }

    /** How a worker reacts when a test partition writes into the worker daemon's working directory. */
    public enum WorkerCwdCheckMode {
        FAIL,
        WARN,
        OFF
    }

    /**
     * A declared input or output path, relative to the module base directory and {@code /}-separated.
     * Optional paths are skipped when absent instead of failing the build.
     */
    public record DeclaredPathSpec(String relativePath, boolean optional) {
    }

    private DistributionSettings() {
    }

    /**
     * Parses a comma-separated declared path list as configured by a single winning precedence tier.
     * <ul>
     *     <li>{@code null}, blank, or {@code none} (case-insensitive) yields an empty list.</li>
     *     <li>A leading or trailing {@code ?} marks a path optional.</li>
     *     <li>{@code \} becomes {@code /}; a single leading {@code ./} and trailing {@code /} are stripped
     *     ({@code ./none} therefore denotes a literal file named {@code none}).</li>
     *     <li>Exact duplicates collapse with required winning over optional.</li>
     *     <li>A path under a declared ancestor is dropped when the ancestor is required, or when both are
     *     optional; an optional ancestor never hides a required descendant.</li>
     * </ul>
     * Paths are not validated for traversal or absolute forms here; packaging validates them.
     */
    public static List<DeclaredPathSpec> parseDeclaredPaths(String rawValue) {
        return parseDeclaredPaths(rawValue, true, INPUTS_PROPERTY);
    }

    /**
     * Parses a comma-separated declared output path list with the same rules as
     * {@link #parseDeclaredPaths(String)}, except that the optional marker {@code ?} is rejected:
     * outputs are collected only when produced, so marking them optional has no meaning.
     *
     * @throws IllegalArgumentException if any path carries a leading or trailing {@code ?}
     */
    public static List<DeclaredPathSpec> parseDeclaredOutputPaths(String rawValue) {
        return parseDeclaredPaths(rawValue, false, OUTPUTS_PROPERTY);
    }

    private static List<DeclaredPathSpec> parseDeclaredPaths(String rawValue,
                                                             boolean allowOptional,
                                                             String settingKey) {
        if (rawValue == null) {
            return List.of();
        }
        String trimmedValue = rawValue.trim();
        if (trimmedValue.isEmpty() || NONE_SENTINEL.equalsIgnoreCase(trimmedValue)) {
            return List.of();
        }

        Map<String, Boolean> optionalByPath = new LinkedHashMap<>();
        for (String token : trimmedValue.split(",")) {
            String path = token.trim();
            if (!allowOptional && (path.startsWith("?") || path.endsWith("?"))) {
                throw new IllegalArgumentException("Optional marker '?' is not supported for " + settingKey
                        + ": '" + path + "'");
            }
            boolean optional = false;
            if (path.startsWith("?")) {
                optional = true;
                path = path.substring(1).trim();
            }
            if (path.endsWith("?")) {
                optional = true;
                path = path.substring(0, path.length() - 1).trim();
            }
            path = path.replace('\\', '/');
            if (path.startsWith("./")) {
                path = path.substring(2);
            }
            while (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            if (path.isEmpty()) {
                continue;
            }
            optionalByPath.merge(path, optional, (existing, added) -> existing && added);
        }

        List<DeclaredPathSpec> result = new ArrayList<>();
        for (Map.Entry<String, Boolean> candidate : optionalByPath.entrySet()) {
            String path = candidate.getKey();
            boolean optional = candidate.getValue();
            boolean subsumed = optionalByPath.entrySet().stream()
                    .anyMatch(ancestor -> path.startsWith(ancestor.getKey() + "/")
                            && (!ancestor.getValue() || optional));
            if (!subsumed) {
                result.add(new DeclaredPathSpec(path, optional));
            }
        }
        return List.copyOf(result);
    }

    /**
     * Parses the {@code fork} setting strictly: {@code null}/blank yields {@code false}, otherwise only
     * {@code true} or {@code false} (case-insensitive) are accepted.
     *
     * @throws IllegalArgumentException for any other value
     */
    public static boolean parseForkWorkspace(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return false;
        }
        String value = rawValue.trim();
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("Invalid value '" + value + "' for " + FORK_PROPERTY
                + " - expected true or false");
    }

    /**
     * Parses the partition policy; {@code null}/blank yields {@link PartitionPolicy#LPT}.
     *
     * @throws IllegalArgumentException for an unknown policy
     */
    public static PartitionPolicy parsePartitionPolicy(String rawValue) {
        return parseEnum(rawValue, PartitionPolicy.class, PartitionPolicy.LPT, PARTITION_POLICY_PROPERTY);
    }

    /**
     * Parses the worker fork mode; {@code null}/blank yields {@link WorkerForkMode#ALWAYS}: every partition runs in a fresh
     * JVM so state left behind by one partition (containers, system properties, statics, threads) cannot leak into the next.
     *
     * @throws IllegalArgumentException for an unknown mode
     */
    public static WorkerForkMode parseWorkerForkMode(String rawValue) {
        return parseEnum(rawValue, WorkerForkMode.class, WorkerForkMode.ALWAYS, WORKER_FORK_MODE_PROPERTY);
    }

    /**
     * Parses the worker working-directory check mode; {@code null}/blank yields {@link WorkerCwdCheckMode#FAIL}.
     *
     * @throws IllegalArgumentException for an unknown mode
     */
    public static WorkerCwdCheckMode parseWorkerCwdCheckMode(String rawValue) {
        return parseEnum(rawValue, WorkerCwdCheckMode.class, WorkerCwdCheckMode.FAIL, WORKER_CWD_CHECK_PROPERTY);
    }

    private static <E extends Enum<E>> E parseEnum(String rawValue, Class<E> type, E defaultValue, String settingKey) {
        if (rawValue == null || rawValue.isBlank()) {
            return defaultValue;
        }
        String value = rawValue.trim();
        for (E constant : type.getEnumConstants()) {
            if (constant.name().equalsIgnoreCase(value)) {
                return constant;
            }
        }
        String allowed = String.join(", ", Arrays.stream(type.getEnumConstants()).map(Enum::name).toList());
        throw new IllegalArgumentException("Invalid value '" + value + "' for " + settingKey
                + " - expected one of " + allowed);
    }

    /**
     * Splits child JVM arguments on whitespace, falling back to {@link #DEFAULT_FORK_JVM_ARGS} when unset.
     * Quoting is not interpreted, so an argument cannot itself contain whitespace.
     */
    public static List<String> parseForkJvmArgs(String rawArgs) {
        if (rawArgs == null || rawArgs.isBlank()) {
            return DEFAULT_FORK_JVM_ARGS;
        }
        return List.of(rawArgs.trim().split("\\s+"));
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

    /** Returns an explicitly configured positive partition limit, or empty for broker-managed sizing. */
    public static OptionalInt findConfiguredMaxPartitionSize() {
        return findConfiguredMaxPartitionSize(System::getProperty, System::getenv);
    }

    public static OptionalInt findConfiguredMaxPartitionSize(UnaryOperator<String> prop, UnaryOperator<String> env) {
        int configured = parseInt(getString(prop, env, MAX_PARTITION_SIZE_PROPERTY, MAX_PARTITION_SIZE_ENV, null), 0);
        return configured > 0 ? OptionalInt.of(configured) : OptionalInt.empty();
    }

    public static long getMinRemoteDurationMs() {
        return getMinRemoteDurationMs(System::getProperty, System::getenv);
    }

    public static long getMinRemoteDurationMs(UnaryOperator<String> prop, UnaryOperator<String> env) {
        String raw = getString(prop, env, MIN_REMOTE_DURATION_MS_PROPERTY, MIN_REMOTE_DURATION_MS_ENV, null);
        return Math.max(0L, parseLong(raw, DEFAULT_MIN_REMOTE_DURATION_MS));
    }

    public static int getMinRemoteColdUnits() {
        return getMinRemoteColdUnits(System::getProperty, System::getenv);
    }

    public static int getMinRemoteColdUnits(UnaryOperator<String> prop, UnaryOperator<String> env) {
        String raw = getString(prop, env, MIN_REMOTE_COLD_UNITS_PROPERTY, MIN_REMOTE_COLD_UNITS_ENV, null);
        return Math.max(0, parseInt(raw, DEFAULT_MIN_REMOTE_COLD_UNITS));
    }

    public static long getBrokerMinPartitionDurationMs() {
        return Math.max(0L, getLong(BROKER_MIN_PARTITION_DURATION_MS_PROPERTY, BROKER_MIN_PARTITION_DURATION_MS_ENV,
                DEFAULT_BROKER_MIN_PARTITION_DURATION_MS));
    }

    public static int getBrokerMaxWavesPerWorker() {
        return Math.max(0, getInt(BROKER_MAX_WAVES_PER_WORKER_PROPERTY, BROKER_MAX_WAVES_PER_WORKER_ENV,
                DEFAULT_BROKER_MAX_WAVES_PER_WORKER));
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

    /** Default worker file cache directory, private to one worker host account. */
    public static Path defaultWorkerCacheDir() {
        return Paths.get(System.getProperty("user.home", ".")).resolve(".citrus-test-distribution-agent")
                .resolve("file-cache");
    }

    /**
     * Resolves the worker file cache directory: system property, then {@code CITRUS_DIST_WORKER_CACHE_DIR}, then
     * {@code TEST_DISTRIBUTION_AGENT_CACHE_DIR}, then {@link #defaultWorkerCacheDir()}. Deliberately independent of
     * the coordinator's {@link #CACHE_DIR_PROPERTY}.
     */
    public static Path getWorkerCacheDir() {
        return resolveWorkerCacheDir(System::getProperty, System::getenv);
    }

    /** Whether the worker evicts least-recently-used blobs to keep free disk space; defaults to enabled. */
    public static boolean isWorkerCacheCleanupEnabled() {
        return resolveWorkerCacheCleanup(System::getProperty, System::getenv);
    }

    /** Free disk space in bytes the worker keeps available when cache cleanup is enabled. */
    public static long getWorkerCacheFreeSpaceBufferBytes() {
        return resolveWorkerCacheFreeSpaceBufferBytes(System::getProperty, System::getenv);
    }

    static Path resolveWorkerCacheDir(UnaryOperator<String> prop, UnaryOperator<String> env) {
        ResolvedSetting resolved = resolveFirstNonBlank(prop, env, WORKER_CACHE_DIR_PROPERTY, WORKER_CACHE_DIR_ENV,
                AGENT_CACHE_DIR_ENV);
        return resolved == null ? defaultWorkerCacheDir() : Paths.get(resolved.value());
    }

    static boolean resolveWorkerCacheCleanup(UnaryOperator<String> prop, UnaryOperator<String> env) {
        ResolvedSetting resolved = resolveFirstNonBlank(prop, env, WORKER_CACHE_CLEANUP_PROPERTY,
                WORKER_CACHE_CLEANUP_ENV, AGENT_CACHE_CLEANUP_ENV);
        return resolved == null ? DEFAULT_WORKER_CACHE_CLEANUP
                : parseBooleanStrict(resolved.value(), resolved.source());
    }

    static long resolveWorkerCacheFreeSpaceBufferBytes(UnaryOperator<String> prop, UnaryOperator<String> env) {
        ResolvedSetting resolved = resolveFirstNonBlank(prop, env, WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_PROPERTY,
                WORKER_CACHE_FREE_SPACE_BUFFER_SIZE_ENV, AGENT_CACHE_FREE_SPACE_BUFFER_SIZE_ENV);
        return resolved == null ? DEFAULT_WORKER_CACHE_FREE_SPACE_BUFFER_BYTES
                : parseByteSize(resolved.value(), resolved.source());
    }

    /** Trimmed value of a setting together with the property or environment key it was read from. */
    private record ResolvedSetting(String value, String source) {
    }

    /** Returns the first non-blank tier, or {@code null} when none is set. */
    private static ResolvedSetting resolveFirstNonBlank(UnaryOperator<String> prop, UnaryOperator<String> env,
                                                        String propKey, String primaryEnvKey, String aliasEnvKey) {
        String value = prop.apply(propKey);
        if (value != null && !value.isBlank()) {
            return new ResolvedSetting(value.trim(), propKey);
        }
        for (String envKey : new String[] {primaryEnvKey, aliasEnvKey}) {
            value = env.apply(envKey);
            if (value != null && !value.isBlank()) {
                return new ResolvedSetting(value.trim(), envKey);
            }
        }
        return null;
    }

    /**
     * Parses a byte size: a non-negative integer with an optional case-insensitive {@code KiB}, {@code MiB} or
     * {@code GiB} suffix. SI units, decimals and overflow are rejected.
     *
     * @param source the setting the value came from, named in error messages
     * @throws IllegalArgumentException for a missing, malformed or overflowing value
     */
    public static long parseByteSize(String raw, String source) {
        Matcher matcher = raw == null ? null : BYTE_SIZE_PATTERN.matcher(raw);
        if (matcher == null || !matcher.matches()) {
            throw new IllegalArgumentException("Invalid byte size '" + raw + "' for " + source
                    + " - expected an integer number of bytes or a value with unit KiB, MiB, GiB");
        }
        long multiplier = 1L;
        String unit = matcher.group(2);
        if (unit != null) {
            multiplier = switch (unit.toLowerCase(Locale.ROOT)) {
                case "kib" -> 1024L;
                case "mib" -> 1024L * 1024;
                default -> 1024L * 1024 * 1024;
            };
        }
        try {
            return Math.multiplyExact(Long.parseLong(matcher.group(1)), multiplier);
        } catch (NumberFormatException | ArithmeticException e) {
            throw new IllegalArgumentException("Byte size '" + raw.trim() + "' for " + source
                    + " is too large - expected an integer number of bytes or a value with unit KiB, MiB, GiB", e);
        }
    }

    /**
     * Parses a boolean strictly: only {@code true} or {@code false} (case-insensitive, surrounding whitespace
     * ignored) are accepted.
     *
     * @param source the setting the value came from, named in error messages
     * @throws IllegalArgumentException for any other value, including {@code null}
     */
    public static boolean parseBooleanStrict(String raw, String source) {
        String value = raw == null ? null : raw.trim();
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("Invalid value '" + raw + "' for " + source + " - expected true or false");
    }

    public static String getString(String prop, String env, String defaultValue) {
        return getString(System::getProperty, System::getenv, prop, env, defaultValue);
    }

    private static String getString(UnaryOperator<String> prop, UnaryOperator<String> env,
                                    String propKey, String envKey, String defaultValue) {
        String value = prop.apply(propKey);
        if (value == null || value.isBlank()) {
            value = envKey != null ? env.apply(envKey) : null;
        }
        return (value == null || value.isBlank()) ? defaultValue : value.trim();
    }

    public static boolean getBoolean(String prop, String env, boolean defaultValue) {
        String raw = getString(prop, env, null);
        return raw == null ? defaultValue : Boolean.parseBoolean(raw);
    }

    public static int getInt(String prop, String env, int defaultValue) {
        return parseInt(getString(prop, env, null), defaultValue);
    }

    private static int parseInt(String raw, int defaultValue) {
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
        return parseLong(getString(prop, env, null), defaultValue);
    }

    private static long parseLong(String raw, long defaultValue) {
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
