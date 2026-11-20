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

        options.add(new CliOption<>(null, "cache-dir", "Worker blob cache directory") {
            @Override
            protected void doProcess(CitrusAgentConfiguration configuration, String arg, String value, LinkedList<String> remainingArgs) {
                configuration.setCacheDir(value != null ? value : "");
            }
        });
    }

    public CitrusAgentConfiguration apply(CitrusAgentConfiguration configuration, String[] arguments) {
        LinkedList<String> args = new LinkedList<>(Arrays.asList(arguments));

        while (!args.isEmpty()) {
            String arg = args.removeFirst();

            for (CliOption<CitrusAgentConfiguration> option : options) {
                if (option.processOption(configuration, arg, args)) {
                    break;
                }
            }
        }

        return configuration;
    }
}
