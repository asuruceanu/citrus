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

package org.citrusframework.distribution.broker;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.store.DistributionStore;
import org.citrusframework.distribution.store.JdbcDistributionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embeddable and standalone lifecycle manager for the Citrus Test Distribution Broker.
 */
public final class DistributionBrokerServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionBrokerServer.class);

    private final DistributionBrokerConfig config;
    private Vertx vertx;
    private DistributionStore store;
    private DistributionBrokerVerticle verticle;

    public DistributionBrokerServer(DistributionBrokerConfig config) {
        this.config = config;
    }

    public synchronized void start() {
        if (vertx != null) {
            return;
        }
        store = new JdbcDistributionStore(config.jdbcUrl(), config.jdbcUsername(), config.jdbcPassword());
        store.initialize();
        if (config.demoData()) {
            DemoDataSeeder.seed(store);
        }

        BlobCacheDirectory blobCache = new BlobCacheDirectory(config.cacheDir());
        try {
            // Lets a worker refuse this directory; an existing owner is kept and never makes the coordinator refuse it.
            BlobCacheDirectory.recordOwnerIfAbsent(blobCache.getRootDir(), BlobCacheDirectory.OWNER_COORDINATOR);
        } catch (IOException e) {
            LOG.warn("Could not mark blob cache {} as owned by the coordinator: {}", blobCache.getRootDir(), e.toString());
        }
        BrokerEventStreamHub eventHub = new BrokerEventStreamHub();
        BrokerSessionCoordinator coordinator = new BrokerSessionCoordinator(config, store, eventHub);

        vertx = Vertx.vertx();
        verticle = new DistributionBrokerVerticle(config, blobCache, eventHub, coordinator);
        try {
            vertx.deployVerticle(verticle).toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            close();
            throw new IllegalStateException("Failed to start DistributionBrokerServer", e);
        }
    }

    public int port() {
        return verticle != null ? verticle.actualPort() : config.port();
    }

    public String baseUrl() {
        String connectHost = "0.0.0.0".equals(config.host()) ? "127.0.0.1" : config.host();
        return "http://" + connectHost + ":" + port();
    }

    public String wsUrl() {
        String connectHost = "0.0.0.0".equals(config.host()) ? "127.0.0.1" : config.host();
        return "ws://" + connectHost + ":" + port() + "/ws/v1/worker";
    }

    public DistributionStore store() {
        return store;
    }

    @Override
    public synchronized void close() {
        if (vertx != null) {
            try {
                vertx.close().toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // Ignored on shutdown
            } finally {
                vertx = null;
            }
        }
        if (store != null) {
            try {
                store.close();
            } catch (Exception ignored) {
                // Ignored on shutdown
            } finally {
                store = null;
            }
        }
    }
}
