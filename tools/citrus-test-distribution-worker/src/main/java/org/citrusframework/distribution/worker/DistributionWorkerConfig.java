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
import java.util.Map;
import java.util.UUID;

import org.citrusframework.distribution.config.DistributionSettings;

/**
 * Configuration for a Citrus Test Distribution worker node.
 */
public final class DistributionWorkerConfig {

    private final String workerId;
    private final String hostName;
    private final String brokerUrl;
    private final String authToken;
    private final Path cacheDir;
    private final Map<String, String> capabilities;
    private final long heartbeatIntervalMs;
    private final long reconnectDelayMs;

    private DistributionWorkerConfig(Builder builder) {
        this.workerId = builder.workerId;
        this.hostName = builder.hostName;
        this.brokerUrl = builder.brokerUrl;
        this.authToken = builder.authToken;
        this.cacheDir = builder.cacheDir;
        this.capabilities = Collections.unmodifiableMap(new LinkedHashMap<>(builder.capabilities));
        this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
        this.reconnectDelayMs = builder.reconnectDelayMs;
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

    public Map<String, String> capabilities() {
        return capabilities;
    }

    public long heartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public long reconnectDelayMs() {
        return reconnectDelayMs;
    }

    public static final class Builder {
        private String workerId = "worker-" + UUID.randomUUID().toString().substring(0, 8);
        private String hostName = resolveHostName();
        private String brokerUrl = DistributionSettings.getBrokerUrl();
        private String authToken = DistributionSettings.getAuthToken();
        private Path cacheDir = java.nio.file.Paths.get(DistributionSettings.getCacheDir()).resolve("worker");
        private Map<String, String> capabilities = new LinkedHashMap<>(DistributionSettings.detectHostCapabilities(null));
        private long heartbeatIntervalMs = 5_000L;
        private long reconnectDelayMs = 1_000L;

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
                this.brokerUrl = brokerUrl.replaceAll("/+$", "");
            }
            return this;
        }

        public Builder authToken(String authToken) {
            this.authToken = authToken == null ? "" : authToken;
            return this;
        }

        public Builder cacheDir(Path cacheDir) {
            if (cacheDir != null) {
                this.cacheDir = cacheDir;
            }
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
