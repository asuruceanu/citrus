/*
 * Copyright 2006-2018 the original author or authors.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.stream.Collectors;

import org.citrusframework.api.agent.CitrusAgentConfiguration;
import org.citrusframework.distribution.config.WorkerCacheCliOptions;
import org.citrusframework.exceptions.CitrusRuntimeException;
import org.citrusframework.main.CitrusAppOptions;
import org.citrusframework.util.StringUtils;

public class CitrusAgentOptions extends CitrusAppOptions<CitrusAgentConfiguration> {

    private final CliOption<CitrusAgentConfiguration> helpOption;

    protected CitrusAgentOptions() {
        super();
        this.helpOption = options.stream().filter(option -> "--help".equals(option.getFullName()))
                .findFirst().orElseThrow();

        options.add(new CliOption<>("P", "port", "Server port") {
            @Override
            protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
                if (StringUtils.hasText(value)) {
                    configuration.setPort(Integer.parseInt(value));
                } else {
                    throw new CitrusRuntimeException("Missing parameter value for -P/--port option");
                }
            }
        });

        options.add(new CliOption<>("m", "mode", "Agent mode (standalone or worker)") {
            @Override
            protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
                if (StringUtils.hasText(value)) {
                    configuration.setMode(value);
                } else {
                    throw new CitrusRuntimeException("Missing parameter value for -m/--mode option");
                }
            }
        });

        options.add(new CliOption<>("u", "coordinator-url", "Test distribution coordinator URL") {
            @Override
            protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
                if (StringUtils.hasText(value)) {
                    configuration.setCoordinatorUrl(value);
                } else {
                    throw new CitrusRuntimeException("Missing parameter value for -u/--coordinator-url option");
                }
            }
        });

        options.add(new CliOption<>("t", "auth-token", "Test distribution coordinator auth token") {
            @Override
            protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
                configuration.setAuthToken(value != null ? value : "");
            }
        });

        options.add(new CliOption<>("C", "capabilities", "Comma-separated worker capabilities (key=value)") {
            @Override
            protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
                configuration.setCapabilities(value != null ? value : "");
            }
        });

        options.add(new CacheOption(WorkerCacheCliOptions.CACHE_DIR, "Worker local file cache directory"));
        options.add(new CacheOption(WorkerCacheCliOptions.CACHE_CLEANUP,
                "Enable or disable cleanup of the worker local file cache (true/false)"));
        options.add(new CacheOption(WorkerCacheCliOptions.NO_CACHE_CLEANUP,
                "Disable cleanup of the worker local file cache"));
        options.add(new CacheOption(WorkerCacheCliOptions.CACHE_FREE_SPACE_BUFFER_SIZE,
                "Minimum free space to keep on the worker cache drive (e.g. 2GiB, 512MiB, or bytes)"));
    }

    @Override
    public CitrusAgentConfiguration apply(String[] arguments) {
        return apply(new CitrusAgentConfiguration(), arguments);
    }

    /**
     * Applies the given command line. Worker cache options are extracted first with the shared
     * {@link WorkerCacheCliOptions} parser (exact names, identical rules to {@code citrus dist worker start}); the
     * remaining arguments are processed by the regular options.
     *
     * @throws IllegalArgumentException on malformed, blank or unknown cache options
     */
    @Override
    public CitrusAgentConfiguration apply(CitrusAgentConfiguration configuration, String[] arguments) {
        ParsedOptions parsed = parse(configuration, arguments);
        if (parsed.usage() != null) {
            // Keep the inherited help and configuration behavior for direct callers of apply().
            helpOption.processOption(configuration, "--help", new LinkedList<>());
        }
        return parsed.configuration();
    }

    ParsedOptions parse(CitrusAgentConfiguration configuration, String[] arguments) {
        WorkerCacheCliOptions.Result cache = WorkerCacheCliOptions.parse(Arrays.asList(arguments), hostValueOptions());
        LinkedList<String> args = new LinkedList<>(cache.remainingArgs());
        boolean helpRequested = false;

        while (!args.isEmpty()) {
            String arg = args.removeFirst();
            if ("--help".equals(arg) || "-h".equals(arg)) {
                helpRequested = true;
                continue;
            }

            for (CliOption<CitrusAgentConfiguration> option : options) {
                if (option != helpOption && option.processOption(configuration, arg, args)) {
                    break;
                }
            }
        }

        if (cache.cacheDir() != null) {
            configuration.setCacheDir(cache.cacheDir());
        }
        if (cache.cacheCleanup() != null) {
            configuration.setCacheCleanup(cache.cacheCleanup());
        }
        if (cache.cacheFreeSpaceBufferSize() != null) {
            configuration.setCacheFreeSpaceBufferSize(cache.cacheFreeSpaceBufferSize());
        }
        return new ParsedOptions(configuration, helpRequested ? usage() : null);
    }

    private String usage() {
        return options.stream()
                .map(CliOption::getInformation)
                .collect(Collectors.joining(System.lineSeparator(),
                        "Citrus application option usage:" + System.lineSeparator(), ""));
    }

    record ParsedOptions(CitrusAgentConfiguration configuration, String usage) {
    }

    /**
     * Exact names of all non-cache options that consume the following token as their value, i.e. every option except
     * help (which, like the help of {@code citrus dist worker start}, takes no value for cache parsing purposes).
     */
    private Set<String> hostValueOptions() {
        Set<String> names = new HashSet<>();
        for (CliOption<CitrusAgentConfiguration> option : options) {
            if (option instanceof CacheOption || option == helpOption) {
                continue;
            }
            names.add(option.getFullName());
            if (!"-".equals(option.getShortName())) {
                names.add(option.getShortName());
            }
        }
        return names;
    }

    /**
     * Usage entry for a worker cache option. Cache options are parsed by {@link WorkerCacheCliOptions} before the
     * regular options run, so this option never matches an argument itself.
     */
    private static final class CacheOption extends CliOption<CitrusAgentConfiguration> {

        CacheOption(String fullName, String description) {
            super("", fullName.substring(2), description);
        }

        @Override
        public boolean processOption(CitrusAgentConfiguration configuration, String arg, LinkedList<String> remainingArgs) {
            return false;
        }

        @Override
        protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
            // parsed by WorkerCacheCliOptions
        }
    }
}
