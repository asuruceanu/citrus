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

package org.citrusframework.distribution.model;

/**
 * Point-in-time telemetry of a worker's local file cache, reported on registration and with each heartbeat.
 *
 * @param cleanupEnabled       whether least-recently-used eviction is enabled on the worker
 * @param freeSpaceBufferBytes free disk space the worker keeps available when cleanup is enabled
 * @param blobCount            number of cached blobs
 * @param cachedBytes          total size of the cached blobs
 * @param usableDiskBytes      usable bytes on the file store holding the cache
 * @param evictedBlobsTotal    blobs evicted since the worker started
 * @param lowDiskSpace         whether the free space buffer could not be satisfied
 */
public record WorkerCacheStats(
        boolean cleanupEnabled,
        long freeSpaceBufferBytes,
        int blobCount,
        long cachedBytes,
        long usableDiskBytes,
        long evictedBlobsTotal,
        boolean lowDiskSpace
) {
}
