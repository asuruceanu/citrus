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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.store.DistributionStore;
import org.citrusframework.distribution.store.JdbcDistributionStore;

/**
 * Embeddable and standalone lifecycle manager for the Citrus Test Distribution Coordinator.
 */
public final class DistributionCoordinatorServer implements AutoCloseable {

    private final DistributionCoordinatorConfig config;
    private Vertx vertx;
    private DistributionStore store;
    private DistributionCoordinatorVerticle verticle;

    public DistributionCoordinatorServer(DistributionCoordinatorConfig config) {
        this.config = config;
    }

    public synchronized void start() {
        if (vertx != null) {
            return;
        }
        BlobCacheDirectory blobCache = new BlobCacheDirectory(config.cacheDir());
        try {
            String owner = BlobCacheDirectory.recordOwnerIfAbsent(blobCache.getRootDir(), BlobCacheDirectory.OWNER_COORDINATOR);
            if (!BlobCacheDirectory.OWNER_COORDINATOR.equals(owner) && !BlobCacheDirectory.OWNER_LAUNCHER.equals(owner)) {
                throw new IllegalStateException("Coordinator cache directory " + blobCache.getRootDir()
                        + " is marked as owned by " + owner + ". Choose a separate --cache-dir.");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Could not mark blob cache " + blobCache.getRootDir() + " as owned by the coordinator", e);
        }

        store = new JdbcDistributionStore(config.jdbcUrl(), config.jdbcUsername(), config.jdbcPassword());
        store.initialize();
        CoordinatorEventStreamHub eventHub = new CoordinatorEventStreamHub();
        SessionCoordinator coordinator = new SessionCoordinator(config, store, eventHub);

        vertx = Vertx.vertx();
        verticle = new DistributionCoordinatorVerticle(config, blobCache, eventHub, coordinator);
        try {
            vertx.deployVerticle(verticle).toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            close();
            throw new IllegalStateException("Failed to start DistributionCoordinatorServer", e);
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
