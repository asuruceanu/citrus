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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.MissingDigestsRequest;
import org.citrusframework.distribution.model.MissingDigestsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves content-addressed blobs between the worker's {@link BlobCacheDirectory} and the coordinator's blob store.
 */
final class BrokerBlobClient {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerBlobClient.class);

    private final DistributionWorkerConfig config;
    private final BlobCacheDirectory blobCache;
    private final HttpClient httpClient;
    private final Set<String> verifiedBlobs = ConcurrentHashMap.newKeySet();

    BrokerBlobClient(DistributionWorkerConfig config, BlobCacheDirectory blobCache) {
        this.config = config;
        this.blobCache = blobCache;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Makes sure {@code sha256} is in the local cache with verified content, downloading it from the coordinator if
     * absent or corrupted (once more if the download fails verification). Digests verified in this JVM are not hashed
     * again.
     */
    void ensureCached(String sha256) throws IOException, InterruptedException {
        if (verifiedBlobs.contains(sha256) && blobCache.contains(sha256)) {
            return;
        }
        if (blobCache.getVerifiedPath(sha256) == null) {
            byte[] content = download(sha256);
            if (!sha256.equals(ClasspathPackager.sha256Hex(content))) {
                LOG.warn("Blob {} failed verification after download, retrying once", sha256);
                content = download(sha256);
            }
            blobCache.put(sha256, content);
        }
        verifiedBlobs.add(sha256);
    }

    /**
     * Uploads the cached blobs among {@code digests} that the coordinator does not have yet.
     */
    void uploadMissing(List<String> digests) throws IOException, InterruptedException {
        if (digests.isEmpty()) {
            return;
        }
        HttpResponse<String> missingResponse = httpClient.send(request("/api/v1/cache/missing")
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(DistributionJson.toJson(
                                new MissingDigestsRequest(List.copyOf(new LinkedHashSet<>(digests))))))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (missingResponse.statusCode() != 200) {
            throw new IOException("Failed to query missing blobs from broker: HTTP " + missingResponse.statusCode());
        }
        MissingDigestsResponse missing = DistributionJson.fromJson(missingResponse.body(), MissingDigestsResponse.class);
        for (String sha256 : missing.missing() != null ? missing.missing() : List.<String>of()) {
            Path blob = blobCache.getVerifiedPath(sha256);
            if (blob == null) {
                throw new IOException("Output blob " + sha256 + " is missing from the worker cache");
            }
            HttpResponse<String> put = httpClient.send(request("/api/v1/cache/blobs/" + sha256)
                            .timeout(Duration.ofMinutes(5))
                            .PUT(HttpRequest.BodyPublishers.ofFile(blob))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            if (put.statusCode() != 200 && put.statusCode() != 201) {
                throw new IOException("Failed to upload blob " + sha256 + " to broker: HTTP " + put.statusCode());
            }
        }
    }

    private byte[] download(String sha256) throws IOException, InterruptedException {
        HttpResponse<byte[]> response = httpClient.send(request("/api/v1/cache/blobs/" + sha256)
                        .timeout(Duration.ofSeconds(60))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() != 200) {
            throw new IOException("Failed to download blob " + sha256 + " from broker: HTTP " + response.statusCode());
        }
        return response.body();
    }

    private HttpRequest.Builder request(String path) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(config.brokerUrl() + path));
        if (config.authToken() != null && !config.authToken().isBlank()) {
            builder.header("Authorization", "Bearer " + config.authToken());
        }
        return builder;
    }
}
