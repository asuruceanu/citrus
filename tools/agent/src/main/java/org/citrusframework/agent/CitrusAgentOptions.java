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
import java.util.LinkedList;

import org.citrusframework.api.agent.CitrusAgentConfiguration;
import org.citrusframework.distribution.config.DistributionSettings;
import org.citrusframework.exceptions.CitrusRuntimeException;
import org.citrusframework.main.CitrusAppOptions;
import org.citrusframework.util.StringUtils;

public class CitrusAgentOptions extends CitrusAppOptions<CitrusAgentConfiguration> {

    protected CitrusAgentOptions() {
        super();

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

        options.add(new CliOption<>("b", "broker-url", "Test distribution broker URL") {
            @Override
            protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
                if (StringUtils.hasText(value)) {
                    configuration.setBrokerUrl(value);
                } else {
                    throw new CitrusRuntimeException("Missing parameter value for -b/--broker-url option");
                }
            }
        });

        options.add(new CliOption<>("t", "auth-token", "Test distribution broker auth token") {
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

        options.add(new ValueOption("cache-dir", "Worker local file cache directory") {
            @Override
            protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
                configuration.setCacheDir(value);
            }
        });

        options.add(new CacheCleanupOption("cache-cleanup", "Enable or disable cleanup of the worker local file cache (true/false)", false));
        options.add(new CacheCleanupOption("no-cache-cleanup", "Disable cleanup of the worker local file cache", true));

        options.add(new ValueOption("cache-free-space-buffer-size", "Minimum free space to keep on the worker cache drive (e.g. 2GiB, 512MiB, or bytes)") {
            @Override
            protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
                DistributionSettings.parseByteSize(value, getFullName());
                configuration.setCacheFreeSpaceBufferSize(value);
            }
        });
    }

    @Override
    public CitrusAgentConfiguration apply(String[] arguments) {
        return apply(new CitrusAgentConfiguration(), arguments);
    }

    public CitrusAgentConfiguration apply(CitrusAgentConfiguration configuration, String[] arguments) {
        LinkedList<String> args = new LinkedList<>(Arrays.asList(arguments));

        while (!args.isEmpty()) {
            String arg = args.removeFirst();

            boolean processed = false;
            for (CliOption<CitrusAgentConfiguration> option : options) {
                if (option.processOption(configuration, arg, args)) {
                    processed = true;
                    break;
                }
            }

            if (!processed && (arg.startsWith("--cache") || arg.startsWith("--no-cache"))) {
                throw new IllegalArgumentException("Unknown cache option: " + arg);
            }
        }

        return configuration;
    }

    /**
     * Option matching its exact name only, either as {@code --name value} or {@code --name=value}.
     * The value must be present and non-blank and a following option token is never consumed as value.
     */
    private abstract static class ValueOption extends CliOption<CitrusAgentConfiguration> {

        ValueOption(String name, String description) {
            super("", name, description);
        }

        @Override
        public boolean processOption(CitrusAgentConfiguration configuration, String arg, LinkedList<String> remainingArgs) {
            String value;
            if (arg.equals(getFullName())) {
                String next = remainingArgs.peekFirst();
                if (next == null || next.startsWith("-")) {
                    throw new IllegalArgumentException("Missing parameter value for " + getFullName() + " option");
                }
                value = remainingArgs.removeFirst();
            } else if (arg.startsWith(getFullName() + "=")) {
                value = arg.substring(getFullName().length() + 1);
            } else {
                return false;
            }

            if (!StringUtils.hasText(value)) {
                throw new IllegalArgumentException("Missing parameter value for " + getFullName() + " option");
            }

            doProcess(configuration, arg, value.trim(), remainingArgs);
            return true;
        }
    }

    /**
     * Exact match boolean option {@code --[no-]cache-cleanup[=true|false]}. When given without {@code =} a following
     * token that does not start with {@code -} is consumed as the boolean value and validated strictly.
     */
    private static final class CacheCleanupOption extends CliOption<CitrusAgentConfiguration> {

        private static final String SOURCE = "--[no-]cache-cleanup";

        private final boolean negated;

        CacheCleanupOption(String name, String description, boolean negated) {
            super("", name, description);
            this.negated = negated;
        }

        @Override
        public boolean processOption(CitrusAgentConfiguration configuration, String arg, LinkedList<String> remainingArgs) {
            boolean value;
            if (arg.equals(getFullName())) {
                String next = remainingArgs.peekFirst();
                if (next != null && !next.startsWith("-")) {
                    value = DistributionSettings.parseBooleanStrict(remainingArgs.removeFirst(), SOURCE);
                } else {
                    value = true;
                }
            } else if (arg.startsWith(getFullName() + "=")) {
                value = DistributionSettings.parseBooleanStrict(arg.substring(getFullName().length() + 1), SOURCE);
            } else {
                return false;
            }

            configuration.setCacheCleanup(negated != value);
            return true;
        }

        @Override
        protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
            // processing is done in processOption
        }
    }
}
