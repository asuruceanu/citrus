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

package org.citrusframework.distribution.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Shared command line parser for the worker cache options of every worker host ({@code citrus dist worker start}
 * and the Citrus Agent in worker mode), so that both hosts accept and reject exactly the same command lines.
 * <p>
 * Recognized options, matched by their exact name as {@code --name} or {@code --name=value}:
 * <ul>
 *     <li>{@code --cache-dir DIR} / {@code --cache-dir=DIR}</li>
 *     <li>{@code --cache-free-space-buffer-size SIZE} / {@code --cache-free-space-buffer-size=SIZE}, validated with
 *     {@link DistributionSettings#parseByteSize(String, String)}</li>
 *     <li>{@code --[no-]cache-cleanup[=true|false]}; without {@code =} a following token that does not start with
 *     {@code -} is consumed as the boolean value and validated with
 *     {@link DistributionSettings#parseBooleanStrict(String, String)}</li>
 * </ul>
 * Rules:
 * <ul>
 *     <li>A value option with a missing or blank value, or followed by another {@code -}/{@code --} token, fails with
 *     {@code Missing parameter value for <flag> option}; the following token is never consumed.</li>
 *     <li>When an option is given several times the last one wins.</li>
 *     <li>Any other token starting with {@code --cache} or {@code --no-cache} fails with
 *     {@code Unknown cache option: <arg>}.</li>
 *     <li>The token following an exact host value option (see {@code hostValueOptions}) is that option's value and is
 *     never interpreted as a cache option, e.g. {@code --auth-token --cache-x} passes through untouched.</li>
 *     <li>Help requests ({@code --help}, {@code -h}) get no special treatment: cache options are validated even when
 *     help is requested, and the help token is returned with the remaining arguments for the host to print its
 *     usage. Both hosts therefore fail identically on invalid cache options regardless of help.</li>
 * </ul>
 * All failures are {@link IllegalArgumentException}s whose message names the offending option.
 */
public final class WorkerCacheCliOptions {

    public static final String CACHE_DIR = "--cache-dir";
    public static final String CACHE_CLEANUP = "--cache-cleanup";
    public static final String NO_CACHE_CLEANUP = "--no-cache-cleanup";
    public static final String CACHE_FREE_SPACE_BUFFER_SIZE = "--cache-free-space-buffer-size";

    /** Option name used in cache cleanup error messages. */
    public static final String CACHE_CLEANUP_SOURCE = "--[no-]cache-cleanup";

    private WorkerCacheCliOptions() {
        // utility class
    }

    /**
     * Extracted worker cache settings plus all non-cache arguments in their original order.
     *
     * @param cacheDir cache directory exactly as given or {@code null} when not given
     * @param cacheCleanup resolved cleanup flag or {@code null} when not given
     * @param cacheFreeSpaceBufferSize trimmed buffer size expression or {@code null} when not given
     * @param cacheFreeSpaceBufferBytes parsed buffer size in bytes or {@code null} when not given
     * @param remainingArgs all arguments that are not worker cache options, in their original order
     */
    public record Result(String cacheDir, Boolean cacheCleanup, String cacheFreeSpaceBufferSize,
                         Long cacheFreeSpaceBufferBytes, List<String> remainingArgs) {

        public Result {
            remainingArgs = List.copyOf(remainingArgs);
        }

        /**
         * The given cache settings as canonical {@code --name=value} options, one per setting, in the order
         * {@code --cache-dir}, {@code --cache-cleanup}, {@code --cache-free-space-buffer-size}.
         */
        public List<String> canonicalArgs() {
            List<String> args = new ArrayList<>(3);
            if (cacheDir != null) {
                args.add(CACHE_DIR + "=" + cacheDir);
            }
            if (cacheCleanup != null) {
                args.add(CACHE_CLEANUP + "=" + cacheCleanup);
            }
            if (cacheFreeSpaceBufferSize != null) {
                args.add(CACHE_FREE_SPACE_BUFFER_SIZE + "=" + cacheFreeSpaceBufferSize);
            }
            return List.copyOf(args);
        }
    }

    /**
     * Parses the worker cache options out of the given host command line.
     *
     * @param args the host command line options, without command names
     * @param hostValueOptions exact names (e.g. {@code --auth-token}, {@code -b}) of the host's other options that
     *                         take a separate value token; may be {@code null} when there are none
     * @throws IllegalArgumentException on malformed, blank or unknown cache options
     */
    public static Result parse(List<String> args, Set<String> hostValueOptions) {
        Objects.requireNonNull(args, "args must not be null");
        Set<String> valueOptions = hostValueOptions == null ? Set.of() : hostValueOptions;

        List<String> remaining = new ArrayList<>(args.size());
        String cacheDir = null;
        Boolean cacheCleanup = null;
        String bufferSize = null;
        Long bufferBytes = null;

        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            if (valueOptions.contains(arg)) {
                remaining.add(arg);
                if (i + 1 < args.size()) {
                    remaining.add(args.get(++i));
                }
            } else if (matches(arg, CACHE_CLEANUP) || matches(arg, NO_CACHE_CLEANUP)) {
                String name = matches(arg, CACHE_CLEANUP) ? CACHE_CLEANUP : NO_CACHE_CLEANUP;
                boolean value;
                if (arg.equals(name)) {
                    value = i + 1 < args.size() && !args.get(i + 1).startsWith("-")
                            ? DistributionSettings.parseBooleanStrict(args.get(++i), CACHE_CLEANUP_SOURCE)
                            : true;
                } else {
                    value = DistributionSettings.parseBooleanStrict(arg.substring(name.length() + 1),
                            CACHE_CLEANUP_SOURCE);
                }
                cacheCleanup = NO_CACHE_CLEANUP.equals(name) != value;
            } else if (matches(arg, CACHE_DIR)) {
                cacheDir = value(args, i, CACHE_DIR);
                if (arg.equals(CACHE_DIR)) {
                    i++;
                }
            } else if (matches(arg, CACHE_FREE_SPACE_BUFFER_SIZE)) {
                bufferSize = value(args, i, CACHE_FREE_SPACE_BUFFER_SIZE).trim();
                bufferBytes = DistributionSettings.parseByteSize(bufferSize, CACHE_FREE_SPACE_BUFFER_SIZE);
                if (arg.equals(CACHE_FREE_SPACE_BUFFER_SIZE)) {
                    i++;
                }
            } else if (arg.startsWith("--cache") || arg.startsWith("--no-cache")) {
                throw new IllegalArgumentException("Unknown cache option: " + arg);
            } else {
                remaining.add(arg);
            }
        }

        return new Result(cacheDir, cacheCleanup, bufferSize, bufferBytes, remaining);
    }

    private static boolean matches(String arg, String name) {
        return arg.equals(name) || arg.startsWith(name + "=");
    }

    /** Returns the raw value of the value option at {@code index}, given as separate token or after {@code =}. */
    private static String value(List<String> args, int index, String name) {
        String arg = args.get(index);
        String value;
        if (arg.equals(name)) {
            value = index + 1 < args.size() ? args.get(index + 1) : null;
            if (value != null && value.startsWith("-")) {
                value = null;
            }
        } else {
            value = arg.substring(name.length() + 1);
        }
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing parameter value for " + name + " option");
        }
        return value;
    }
}
