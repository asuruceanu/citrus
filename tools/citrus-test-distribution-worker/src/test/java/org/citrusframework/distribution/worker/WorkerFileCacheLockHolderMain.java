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

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Child JVM used by {@link WorkerFileCacheTest}: opens (and thereby locks) the cache directory given as the first
 * argument, leaves an orphan download temp file behind, prints {@code LOCKED} and waits until it is killed. It exits on
 * its own after a bounded time so a crashed parent never leaks it.
 */
public final class WorkerFileCacheLockHolderMain {

    static final String LOCKED_MARKER = "LOCKED";

    private WorkerFileCacheLockHolderMain() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        WorkerFileCache cache = WorkerFileCache.open(dir, false, 0L);
        Files.writeString(cache.rootDir().resolve("tmp").resolve("dl-4711.tmp"), "partial");
        System.out.println(LOCKED_MARKER);
        System.out.flush();
        Thread.sleep(120_000L);
        cache.close();
    }
}
