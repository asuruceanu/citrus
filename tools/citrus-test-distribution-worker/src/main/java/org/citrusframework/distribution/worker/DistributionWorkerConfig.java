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

package org.citrusframework.distribution.worker;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.distribution.config.DistributionSettings.WorkerCwdCheckMode;
import org.citrusframework.distribution.config.DistributionSettings.WorkerForkMode;

/**
 * Configuration for a Citrus Test Distribution worker node.
 */
public final class DistributionWorkerConfig {

    private final String workerId;
    private final String hostName;
    private final String brokerUrl;
    private final String authToken;
    private final Path cacheDir;
    private final boolean cacheCleanup;
    private final long cacheFreeSpaceBufferBytes;
    private final Map<String, String> capabilities;
    private final long heartbeatIntervalMs;
    private final long reconnectDelayMs;
    private final WorkerForkMode forkMode;
    private final List<String> forkJvmArgs;
    private final WorkerCwdCheckMode cwdCheckMode;

    private DistributionWorkerConfig(Builder builder) {
        this.workerId = builder.workerId;
        this.hostName = builder.hostName;
        this.brokerUrl = builder.brokerUrl;
        this.authToken = builder.authToken;
        // Cache settings fall back to system property / environment / default only when not set explicitly, so an
        // invalid lower-tier value cannot break a configuration whose explicit option overrides it.
        this.cacheDir = builder.cacheDir != null ? builder.cacheDir : DistributionSettings.getWorkerCacheDir();
        this.cacheCleanup = builder.cacheCleanup != null ? builder.cacheCleanup
                : DistributionSettings.isWorkerCacheCleanupEnabled();
        this.cacheFreeSpaceBufferBytes = builder.cacheFreeSpaceBufferBytes != null
                ? builder.cacheFreeSpaceBufferBytes : DistributionSettings.getWorkerCacheFreeSpaceBufferBytes();
        Map<String, String> caps = new LinkedHashMap<>(builder.capabilities);
        caps.putIfAbsent(DistributionSettings.WORKSPACE_CAPABILITY_KEY, "true");
        this.capabilities = Collections.unmodifiableMap(caps);
        this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
        this.reconnectDelayMs = builder.reconnectDelayMs;
        this.forkMode = builder.forkMode;
        this.forkJvmArgs = List.copyOf(builder.forkJvmArgs);
        this.cwdCheckMode = builder.cwdCheckMode;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DistributionWorkerConfig fromSettings() {
        return builder().build();
    }

    public String workerId() {
        return workerId;
    }

    public String hostName() {
        return hostName;
    }

    public String brokerUrl() {
        return brokerUrl;
    }

    public String authToken() {
        return authToken;
    }

    public Path cacheDir() {
        return cacheDir;
    }

    /**
     * Whether the worker evicts least recently used blobs to keep the free space buffer on the cache volume.
     */
    public boolean cacheCleanup() {
        return cacheCleanup;
    }

    /**
     * Free disk space, in bytes, the worker tries to keep on the cache volume when cache cleanup is enabled.
     */
    public long cacheFreeSpaceBufferBytes() {
        return cacheFreeSpaceBufferBytes;
    }

    public Map<String, String> capabilities() {
        return capabilities;
    }

    public long heartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public long reconnectDelayMs() {
        return reconnectDelayMs;
    }

    /**
     * Whether partitions always run in a forked child JVM ({@link WorkerForkMode#ALWAYS}) or only when they need a
     * workspace ({@link WorkerForkMode#AUTO}, default).
     */
    public WorkerForkMode forkMode() {
        return forkMode;
    }

    /**
     * JVM arguments for forked partition JVMs.
     */
    public List<String> forkJvmArgs() {
        return forkJvmArgs;
    }

    /**
     * How in-process partitions react to writes into the worker's working directory.
     */
    public WorkerCwdCheckMode cwdCheckMode() {
        return cwdCheckMode;
    }

    public static final class Builder {
        private String workerId = "worker-" + UUID.randomUUID().toString().substring(0, 8);
        private String hostName = resolveHostName();
        private String brokerUrl = normalizeBrokerBaseUrl(DistributionSettings.getBrokerUrl());
        private String authToken = DistributionSettings.getAuthToken();
        /** Explicit cache settings; {@code null} means "resolve from settings in {@link #build()}". */
        private Path cacheDir;
        private Boolean cacheCleanup;
        private Long cacheFreeSpaceBufferBytes;
        private Map<String, String> capabilities = new LinkedHashMap<>(DistributionSettings.detectHostCapabilities(null));
        private long heartbeatIntervalMs = 5_000L;
        private long reconnectDelayMs = 1_000L;
        private WorkerForkMode forkMode = DistributionSettings.parseWorkerForkMode(DistributionSettings.getString(
                DistributionSettings.WORKER_FORK_MODE_PROPERTY, DistributionSettings.WORKER_FORK_MODE_ENV, null));
        private List<String> forkJvmArgs = DistributionSettings.parseForkJvmArgs(DistributionSettings.getString(
                DistributionSettings.WORKER_FORK_JVM_ARGS_PROPERTY, DistributionSettings.WORKER_FORK_JVM_ARGS_ENV, null));
        private WorkerCwdCheckMode cwdCheckMode = DistributionSettings.parseWorkerCwdCheckMode(DistributionSettings.getString(
                DistributionSettings.WORKER_CWD_CHECK_PROPERTY, DistributionSettings.WORKER_CWD_CHECK_ENV, null));

        private Builder() {
        }

        public Builder workerId(String workerId) {
            if (workerId != null && !workerId.isBlank()) {
                this.workerId = workerId;
            }
            return this;
        }

        public Builder hostName(String hostName) {
            if (hostName != null && !hostName.isBlank()) {
                this.hostName = hostName;
            }
            return this;
        }

        public Builder brokerUrl(String brokerUrl) {
            if (brokerUrl != null && !brokerUrl.isBlank()) {
                this.brokerUrl = normalizeBrokerBaseUrl(brokerUrl);
            }
            return this;
        }

        private static String normalizeBrokerBaseUrl(String rawUrl) {
            if (rawUrl == null || rawUrl.isBlank()) {
                return DistributionSettings.DEFAULT_BROKER_URL;
            }
            String normalized = rawUrl.trim().replaceAll("/+$", "");
            if (normalized.startsWith("wss://")) {
                normalized = "https://" + normalized.substring("wss://".length());
            } else if (normalized.startsWith("ws://")) {
                normalized = "http://" + normalized.substring("ws://".length());
            }
            if (normalized.endsWith("/ws/v1/worker")) {
                normalized = normalized.substring(0, normalized.length() - "/ws/v1/worker".length());
            }
            return normalized;
        }

        public Builder authToken(String authToken) {
            this.authToken = authToken == null ? "" : authToken;
            return this;
        }

        /**
         * @throws NullPointerException if {@code cacheDir} is {@code null}
         */
        public Builder cacheDir(Path cacheDir) {
            this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir must not be null");
            return this;
        }

        public Builder cacheCleanup(boolean cacheCleanup) {
            this.cacheCleanup = cacheCleanup;
            return this;
        }

        /**
         * @throws IllegalArgumentException if {@code cacheFreeSpaceBufferBytes} is negative
         */
        public Builder cacheFreeSpaceBufferBytes(long cacheFreeSpaceBufferBytes) {
            if (cacheFreeSpaceBufferBytes < 0) {
                throw new IllegalArgumentException("Worker cache free space buffer must not be negative: "
                        + cacheFreeSpaceBufferBytes);
            }
            this.cacheFreeSpaceBufferBytes = cacheFreeSpaceBufferBytes;
            return this;
        }

        public Builder capabilities(Map<String, String> capabilities) {
            if (capabilities != null) {
                this.capabilities = new LinkedHashMap<>(capabilities);
            }
            return this;
        }

        public Builder addCapabilitiesCsv(String csv) {
            this.capabilities.putAll(DistributionSettings.parseCapabilitiesCsv(csv));
            return this;
        }

        public Builder heartbeatIntervalMs(long heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            return this;
        }

        public Builder reconnectDelayMs(long reconnectDelayMs) {
            this.reconnectDelayMs = reconnectDelayMs;
            return this;
        }

        public Builder forkMode(WorkerForkMode forkMode) {
            if (forkMode != null) {
                this.forkMode = forkMode;
            }
            return this;
        }

        public Builder forkJvmArgs(List<String> forkJvmArgs) {
            if (forkJvmArgs != null) {
                this.forkJvmArgs = List.copyOf(forkJvmArgs);
            }
            return this;
        }

        public Builder cwdCheckMode(WorkerCwdCheckMode cwdCheckMode) {
            if (cwdCheckMode != null) {
                this.cwdCheckMode = cwdCheckMode;
            }
            return this;
        }

        /**
         * Builds the configuration. Cache settings not set on this builder are resolved now from system properties,
         * environment variables and defaults; an invalid value in the tier that is used throws
         * {@link IllegalArgumentException}. Workers advertise {@code citrus.workspace.v1=true} unless the capabilities
         * explicitly set it (for example to {@code false}).
         */
        public DistributionWorkerConfig build() {
            return new DistributionWorkerConfig(this);
        }

        private static String resolveHostName() {
            try {
                return java.net.InetAddress.getLocalHost().getHostName();
            } catch (Exception ignored) {
                return "localhost";
            }
        }
    }
}
