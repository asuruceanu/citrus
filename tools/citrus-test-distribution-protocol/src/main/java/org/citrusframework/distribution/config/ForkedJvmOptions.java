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

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.regex.Pattern;

/** Build JVM switches that can be reapplied to a worker without referring to host files. */
public final class ForkedJvmOptions {

    public static final String PROPERTY = "citrus.test.distribution.internal.forkedJvmOptions";
    public static final String CAPABILITY = "citrus.workspace.jvm-options.v1";

    private static final Pattern MODULE_OPTION = Pattern.compile(
            "--(?:add-opens|add-exports|add-reads|add-modules|limit-modules|enable-native-access)=[a-zA-Z0-9_.$/,=-]+");

    private ForkedJvmOptions() {
    }

    public static List<String> capture() {
        return ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(ForkedJvmOptions::supported).toList();
    }

    public static boolean supported(String option) {
        return "--enable-preview".equals(option) || MODULE_OPTION.matcher(option).matches();
    }
}
