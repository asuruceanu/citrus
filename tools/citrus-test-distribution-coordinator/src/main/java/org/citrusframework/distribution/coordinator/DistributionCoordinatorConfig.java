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

import java.nio.file.Path;

import org.citrusframework.distribution.config.DistributionSettings;

/**
 * Configuration for {@link DistributionCoordinatorServer}.
 */
public final class DistributionCoordinatorConfig {

    public static final String MIN_PARTITION_DURATION_MS_PROPERTY =
            DistributionSettings.COORDINATOR_MIN_PARTITION_DURATION_MS_PROPERTY;
    public static final String MIN_PARTITION_DURATION_MS_ENV = DistributionSettings.COORDINATOR_MIN_PARTITION_DURATION_MS_ENV;
    public static final String MAX_WAVES_PER_WORKER_PROPERTY = DistributionSettings.COORDINATOR_MAX_WAVES_PER_WORKER_PROPERTY;

    public static final String MAX_WAVES_PER_WORKER_ENV = DistributionSettings.COORDINATOR_MAX_WAVES_PER_WORKER_ENV;

    private final String host;
    private final int port;
    private final String authToken;
    private final Path cacheDir;
    private final String jdbcUrl;
    private final String jdbcUsername;
    private final String jdbcPassword;
    private final int defaultMaxPartitionSize;
    private final int defaultMaxRetries;
    private final long defaultDurationMs;
    private final long minPartitionDurationMs;
    private final int maxWavesPerWorker;
    private final long leaseTimeoutMs;

    private DistributionCoordinatorConfig(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.authToken = builder.authToken;
        this.cacheDir = builder.cacheDir;
        this.jdbcUrl = builder.jdbcUrl;
        this.jdbcUsername = builder.jdbcUsername;
        this.jdbcPassword = builder.jdbcPassword;
        this.defaultMaxPartitionSize = builder.defaultMaxPartitionSize;
        this.defaultMaxRetries = builder.defaultMaxRetries;
        this.defaultDurationMs = builder.defaultDurationMs;
        this.minPartitionDurationMs = Math.max(0L, builder.minPartitionDurationMs);
        this.maxWavesPerWorker = Math.max(0, builder.maxWavesPerWorker);
        this.leaseTimeoutMs = builder.leaseTimeoutMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DistributionCoordinatorConfig fromSettings() {
        return builder()
                .authToken(DistributionSettings.getAuthToken())
                .cacheDir(Path.of(DistributionSettings.getCacheDir()))
                .jdbcUrl(DistributionSettings.getJdbcUrl())
                .jdbcUsername(DistributionSettings.getJdbcUser())
                .jdbcPassword(DistributionSettings.getJdbcPassword())
                .defaultMaxPartitionSize(DistributionSettings.getMaxPartitionSize())
                .defaultMaxRetries(DistributionSettings.getMaxRetries())
                .defaultDurationMs(DistributionSettings.getDefaultTestDurationMs())
                .build();
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String authToken() {
        return authToken;
    }

    public Path cacheDir() {
        return cacheDir;
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    public String jdbcUsername() {
        return jdbcUsername;
    }

    public String jdbcPassword() {
        return jdbcPassword;
    }

    public int defaultMaxPartitionSize() {
        return defaultMaxPartitionSize;
    }

    public int defaultMaxRetries() {
        return defaultMaxRetries;
    }

    public long defaultDurationMs() {
        return defaultDurationMs;
    }

    public long minPartitionDurationMs() {
        return minPartitionDurationMs;
    }

    public int maxWavesPerWorker() {
        return maxWavesPerWorker;
    }

    public long leaseTimeoutMs() {
        return leaseTimeoutMs;
    }

    public static final class Builder {
        private String host = "0.0.0.0";
        private int port = 8084;
        private String authToken;
        private Path cacheDir = Path.of(System.getProperty("user.home"), ".citrus", "dist-coordinator-cache");
        private String jdbcUrl = "jdbc:hsqldb:file:" + Path.of(System.getProperty("user.home"), ".citrus", "dist-db", "citrus_dist");
        private String jdbcUsername = "SA";
        private String jdbcPassword = "";
        private int defaultMaxPartitionSize = 10;
        private int defaultMaxRetries = 1;
        private long defaultDurationMs = 5_000L;
        private long minPartitionDurationMs = DistributionSettings.getCoordinatorMinPartitionDurationMs();
        private int maxWavesPerWorker = DistributionSettings.getCoordinatorMaxWavesPerWorker();
        private long leaseTimeoutMs = 60_000L;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder authToken(String authToken) {
            this.authToken = authToken;
            return this;
        }

        public Builder cacheDir(Path cacheDir) {
            this.cacheDir = cacheDir;
            return this;
        }

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder jdbcUsername(String jdbcUsername) {
            this.jdbcUsername = jdbcUsername;
            return this;
        }

        public Builder jdbcPassword(String jdbcPassword) {
            this.jdbcPassword = jdbcPassword;
            return this;
        }

        public Builder defaultMaxPartitionSize(int defaultMaxPartitionSize) {
            this.defaultMaxPartitionSize = defaultMaxPartitionSize;
            return this;
        }

        public Builder defaultMaxRetries(int defaultMaxRetries) {
            this.defaultMaxRetries = defaultMaxRetries;
            return this;
        }

        public Builder defaultDurationMs(long defaultDurationMs) {
            this.defaultDurationMs = defaultDurationMs;
            return this;
        }

        public Builder minPartitionDurationMs(long minPartitionDurationMs) {
            this.minPartitionDurationMs = minPartitionDurationMs;
            return this;
        }

        public Builder maxWavesPerWorker(int maxWavesPerWorker) {
            this.maxWavesPerWorker = maxWavesPerWorker;
            return this;
        }

        public Builder leaseTimeoutMs(long leaseTimeoutMs) {
            this.leaseTimeoutMs = leaseTimeoutMs;
            return this;
        }

        public DistributionCoordinatorConfig build() {
            return new DistributionCoordinatorConfig(this);
        }
    }
}
