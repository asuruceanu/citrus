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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.MissingDigestsRequest;
import org.citrusframework.distribution.model.MissingDigestsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves content-addressed blobs between the worker's {@link WorkerFileCache} and the coordinator's blob store.
 * <p>
 * Downloads stream in {@value #CHUNK_BYTES}-byte chunks into a temporary file under the cache's {@code tmp/}, never
 * exceed {@link ClasspathPackager#MAX_BLOB_BYTES}, make room on the cache volume first (from the declared artifact
 * size, and again from a larger {@code Content-Length}), and are verified while promoted into the blob store. A
 * download that fails verification is retried once. Temporary files are removed on every outcome.
 */
final class BrokerBlobClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerBlobClient.class);

    static final int CHUNK_BYTES = 64 * 1024;

    /** Opens the output stream for a download's temporary file; a test seam for write failures. */
    @FunctionalInterface
    interface TempFileOpener {
        OutputStream open(Path tempFile) throws IOException;
    }

    private final DistributionWorkerConfig config;
    private final WorkerFileCache cache;
    private final HttpClient httpClient;
    private final AtomicLong downloadRequests = new AtomicLong();
    private volatile TempFileOpener tempFileOpener = Files::newOutputStream;

    BrokerBlobClient(DistributionWorkerConfig config, WorkerFileCache cache) {
        this.config = config;
        this.cache = Objects.requireNonNull(cache, "cache");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Makes sure the blob of {@code artifact} is in the local cache with verified content and returns its path.
     *
     * @throws IOException if the declared size exceeds {@link ClasspathPackager#MAX_BLOB_BYTES}, or the blob cannot be
     *                     downloaded, stored or verified
     */
    Path ensureCached(ArtifactRef artifact) throws IOException, InterruptedException {
        return ensureCached(artifact.sha256(), artifact.sizeBytes());
    }

    /**
     * Makes sure {@code sha256} is in the local cache with verified content and returns its path. The size is taken
     * from the response.
     */
    Path ensureCached(String sha256) throws IOException, InterruptedException {
        return ensureCached(sha256, 0L);
    }

    private Path ensureCached(String sha256, long sizeBytes) throws IOException, InterruptedException {
        BlobCacheDirectory.validateDigest(sha256);
        if (sizeBytes > ClasspathPackager.MAX_BLOB_BYTES) {
            throw new IOException("Blob " + sha256 + " declares " + sizeBytes + " bytes, more than the maximum of "
                    + ClasspathPackager.MAX_BLOB_BYTES);
        }
        Path cached = cache.verifyOrInvalidate(sha256);
        if (cached != null) {
            return cached;
        }
        long expectedBytes = Math.clamp(sizeBytes, 0L, ClasspathPackager.MAX_BLOB_BYTES);
        cache.ensureFreeSpaceForDownload(expectedBytes);
        try {
            return download(sha256, expectedBytes);
        } catch (DigestMismatchException first) {
            LOG.warn("Blob {} failed verification after download, retrying once", sha256);
            try {
                return download(sha256, expectedBytes);
            } catch (DigestMismatchException second) {
                second.addSuppressed(first);
                throw new IOException("Blob " + sha256 + " failed verification twice after download", second);
            }
        }
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
            Path blob = cache.verifyOrInvalidate(sha256);
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

    /**
     * Streams the blob into a temporary file and promotes it into the cache.
     *
     * @throws DigestMismatchException if the downloaded content does not match {@code sha256}
     */
    private Path download(String sha256, long expectedBytes) throws IOException, InterruptedException {
        downloadRequests.incrementAndGet();
        HttpResponse<InputStream> response = httpClient.send(request("/api/v1/cache/blobs/" + sha256)
                        .timeout(Duration.ofSeconds(60))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream body = response.body()) {
            if (response.statusCode() != 200) {
                throw new IOException("Failed to download blob " + sha256 + " from broker: HTTP " + response.statusCode());
            }
            OptionalLong contentLength = response.headers().firstValueAsLong("Content-Length");
            if (contentLength.isPresent()) {
                long declared = contentLength.getAsLong();
                if (declared > ClasspathPackager.MAX_BLOB_BYTES) {
                    throw new IOException("Blob " + sha256 + " has a Content-Length of " + declared
                            + " bytes, more than the maximum of " + ClasspathPackager.MAX_BLOB_BYTES);
                }
                if (declared > expectedBytes) {
                    cache.ensureFreeSpaceForDownload(declared);
                }
            }

            Path tmp = cache.newDownloadTempFile();
            try {
                long total = 0;
                try (OutputStream out = tempFileOpener.open(tmp)) {
                    byte[] buffer = new byte[CHUNK_BYTES];
                    int read;
                    while ((read = body.read(buffer)) != -1) {
                        total += read;
                        if (total > ClasspathPackager.MAX_BLOB_BYTES) {
                            throw new IOException("Blob " + sha256 + " exceeds the maximum of "
                                    + ClasspathPackager.MAX_BLOB_BYTES + " bytes");
                        }
                        out.write(buffer, 0, read);
                    }
                }
                try {
                    return cache.promoteDownload(sha256, tmp);
                } catch (IllegalArgumentException e) {
                    throw new DigestMismatchException(e);
                }
            } finally {
                Files.deleteIfExists(tmp);
            }
        }
    }

    private HttpRequest.Builder request(String path) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(config.brokerUrl() + path));
        if (config.authToken() != null && !config.authToken().isBlank()) {
            builder.header("Authorization", "Bearer " + config.authToken());
        }
        return builder;
    }

    /** Blob download requests sent to the coordinator by this client. */
    long downloadRequests() {
        return downloadRequests.get();
    }

    void tempFileOpener(TempFileOpener opener) {
        this.tempFileOpener = Objects.requireNonNull(opener);
    }

    /** Aborts outstanding exchanges and releases the HTTP client. Idempotent. */
    @Override
    public void close() {
        httpClient.shutdownNow();
    }

    /** A downloaded blob whose content does not match its digest. */
    private static final class DigestMismatchException extends IOException {
        DigestMismatchException(IllegalArgumentException cause) {
            super(cause.getMessage(), cause);
        }
    }
}
