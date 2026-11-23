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

package org.citrusframework.agent;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.citrusframework.api.agent.CitrusAgentConfiguration;
import org.citrusframework.distribution.worker.DistributionWorkerClient;
import org.citrusframework.distribution.worker.DistributionWorkerConfig;
import org.citrusframework.util.StringUtils;

/**
 * Lifecycle bridge connecting {@link CitrusAgentServer} worker mode to {@link DistributionWorkerClient}.
 */
final class DistributionWorkerBridge implements AutoCloseable {

    private final DistributionWorkerClient workerClient;

    DistributionWorkerBridge(CitrusAgentConfiguration configuration) {
        this.workerClient = new DistributionWorkerClient(buildWorkerConfig(configuration));
    }

    /**
     * Builds the worker configuration from the agent configuration. Cache settings not explicitly configured on the
     * agent are left to the worker defaults (system property, environment variable, built-in default).
     */
    static DistributionWorkerConfig buildWorkerConfig(CitrusAgentConfiguration configuration) {
        DistributionWorkerConfig.Builder builder = DistributionWorkerConfig.builder()
                .brokerUrl(configuration.getBrokerUrl())
                .authToken(configuration.getAuthToken())
                .addCapabilitiesCsv(configuration.getCapabilities());
        if (StringUtils.hasText(configuration.getCacheDir())) {
            builder.cacheDir(Paths.get(configuration.getCacheDir()));
        }
        if (configuration.getCacheCleanup() != null) {
            builder.cacheCleanup(configuration.getCacheCleanup());
        }
        if (StringUtils.hasText(configuration.getCacheFreeSpaceBufferSize())) {
            builder.cacheFreeSpaceBufferSize(configuration.getCacheFreeSpaceBufferSize());
        }
        return builder.build();
    }

    void start() {
        workerClient.start();
    }

    @Override
    public void close() {
        workerClient.drain(15, TimeUnit.SECONDS);
        workerClient.close();
    }
}
