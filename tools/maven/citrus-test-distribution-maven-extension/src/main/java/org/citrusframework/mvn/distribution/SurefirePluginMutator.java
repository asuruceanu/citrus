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

package org.citrusframework.mvn.distribution;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import org.apache.maven.model.Plugin;
import org.apache.maven.model.PluginExecution;
import org.apache.maven.project.MavenProject;
import org.citrusframework.distribution.config.DistributionSettings;
import org.codehaus.plexus.util.xml.Xpp3Dom;

/**
 * Mutates Surefire and Failsafe plugin XML configurations across reactor projects so forked test JVMs
 * load {@code citrus-test-distribution-junit} and attach to the shared reactor {@code sessionId}.
 */
public final class SurefirePluginMutator {

    private static final String SUREFIRE_KEY = "org.apache.maven.plugins:maven-surefire-plugin";
    private static final String FAILSAFE_KEY = "org.apache.maven.plugins:maven-failsafe-plugin";

    private SurefirePluginMutator() {
    }

    /**
     * The workspace settings resolved per Surefire/Failsafe configuration, with the value written when the winning
     * tier clears the setting.
     */
    private enum WorkspaceSetting {
        INPUTS(DistributionSettings.INPUTS_PROPERTY, DistributionSettings.INPUTS_ENV, DistributionSettings.NONE_SENTINEL),
        OUTPUTS(DistributionSettings.OUTPUTS_PROPERTY, DistributionSettings.OUTPUTS_ENV, DistributionSettings.NONE_SENTINEL),
        FORK(DistributionSettings.FORK_PROPERTY, DistributionSettings.FORK_ENV, "false"),
        PARTITION_POLICY(DistributionSettings.PARTITION_POLICY_PROPERTY, DistributionSettings.PARTITION_POLICY_ENV, "LPT");

        private final String property;
        private final String env;
        private final String clearedValue;

        WorkspaceSetting(String property, String env, String clearedValue) {
            this.property = property;
            this.env = env;
            this.clearedValue = clearedValue;
        }

        /**
         * The value to pass to the test JVM for a winning raw value; fork and policy are validated and normalized.
         */
        String normalize(String raw) {
            String value = raw.trim();
            if (value.isEmpty() || DistributionSettings.NONE_SENTINEL.equalsIgnoreCase(value)) {
                return clearedValue;
            }
            return switch (this) {
                case FORK -> Boolean.toString(DistributionSettings.parseForkWorkspace(value));
                case PARTITION_POLICY -> DistributionSettings.parsePartitionPolicy(value).name();
                default -> value;
            };
        }
    }

    private record Resolution(Properties cliUserProperties, Function<String, String> environment) {
    }

    /** The winning raw value of a setting and the tier it came from. */
    private record Winner(String raw, String tier) {
    }

    public static void configureProject(MavenProject project,
                                        String sessionId,
                                        List<Path> extraClasspathJars,
                                        Map<String, String> sharedProperties) {
        configureProject(project, sessionId, extraClasspathJars, sharedProperties, new Properties(), System::getenv);
    }

    /**
     * Configures Surefire and Failsafe of {@code project}, resolving the workspace settings (inputs, outputs, fork,
     * partition policy) per configuration with single-winner precedence: CLI user property (present wins, even empty)
     * &gt; explicit {@code <systemPropertyVariables>} value (blank or unexpanded {@code ${...}} ignored) &gt; module
     * property (blank ignored) &gt; {@code CITRUS_DIST_*} environment variable &gt; unset. A winning empty or
     * {@code none} value clears the setting. The resolved value is always written explicitly ({@code none} for cleared
     * lists, the default for fork and policy), and nothing is written when no tier defines the setting.
     *
     * @throws IllegalArgumentException for an invalid fork or partition policy value
     */
    public static void configureProject(MavenProject project,
                                        String sessionId,
                                        List<Path> extraClasspathJars,
                                        Map<String, String> sharedProperties,
                                        Properties cliUserProperties,
                                        Function<String, String> environment) {
        if (project == null || project.getBuild() == null || project.getBuild().getPluginsAsMap() == null) {
            return;
        }
        Resolution resolution = new Resolution(cliUserProperties != null ? cliUserProperties : new Properties(),
                environment != null ? environment : key -> null);
        if (isSkipped(project, resolution)) {
            // A skipped module opted out: its tests run locally, exactly as without Test Distribution.
            return;
        }
        normalizeCliUserProperties(project, resolution);
        Map<String, Plugin> plugins = project.getBuild().getPluginsAsMap();
        Plugin surefire = plugins.get(SUREFIRE_KEY);
        if (surefire != null) {
            mutatePlugin(project, surefire, sessionId, extraClasspathJars, sharedProperties, resolution);
        }
        Plugin failsafe = plugins.get(FAILSAFE_KEY);
        if (failsafe != null) {
            mutatePlugin(project, failsafe, sessionId, extraClasspathJars, sharedProperties, resolution);
        }
    }

    /**
     * Whether {@code project} opted out of distribution: the first non-blank value of the command line
     * ({@code -Dcitrus.test.distribution.skip}), the module's {@code <properties>}, then {@code CITRUS_DIST_SKIP}.
     */
    static boolean isSkipped(MavenProject project, Resolution resolution) {
        String cli = resolution.cliUserProperties().getProperty(DistributionSettings.SKIP_PROPERTY);
        if (cli != null && !cli.isBlank()) {
            return Boolean.parseBoolean(cli.trim());
        }
        String module = project.getProperties() != null
                ? project.getProperties().getProperty(DistributionSettings.SKIP_PROPERTY) : null;
        if (module != null && !module.isBlank()) {
            return Boolean.parseBoolean(module.trim());
        }
        String env = resolution.environment().apply(DistributionSettings.SKIP_ENV);
        return env != null && Boolean.parseBoolean(env.trim());
    }

    /**
     * Checks the workspace settings of every Surefire/Failsafe configuration of {@code project} without changing
     * anything, so invalid values fail the build before a reactor session is opened.
     *
     * @throws IllegalArgumentException naming the module, the setting, the value and the tier it came from
     */
    public static void validateProject(MavenProject project, Properties cliUserProperties, Function<String, String> environment) {
        if (project == null || project.getBuild() == null || project.getBuild().getPluginsAsMap() == null) {
            return;
        }
        Resolution resolution = new Resolution(cliUserProperties != null ? cliUserProperties : new Properties(),
                environment != null ? environment : key -> null);
        if (isSkipped(project, resolution)) {
            // A skipped module opted out: its tests run locally, exactly as without Test Distribution.
            return;
        }
        for (String key : List.of(SUREFIRE_KEY, FAILSAFE_KEY)) {
            Plugin plugin = project.getBuild().getPluginsAsMap().get(key);
            if (plugin == null) {
                continue;
            }
            List<Object> configs = new java.util.ArrayList<>();
            configs.add(plugin.getConfiguration());
            if (plugin.getExecutions() != null) {
                plugin.getExecutions().forEach(execution -> configs.add(execution.getConfiguration()));
            }
            for (Object config : configs) {
                Xpp3Dom dom = config instanceof Xpp3Dom existing ? existing : new Xpp3Dom("configuration");
                Xpp3Dom sysProps = dom.getChild("systemPropertyVariables");
                for (WorkspaceSetting setting : WorkspaceSetting.values()) {
                    Winner winner = winningValue(setting, project, sysProps != null ? sysProps : new Xpp3Dom("systemPropertyVariables"), resolution);
                    if (winner != null) {
                        normalized(setting, winner, project);
                    }
                }
            }
        }
    }

    /**
     * Surefire copies the session's user properties into the test JVM after {@code systemPropertyVariables}, so a raw
     * CLI value (say an empty one meaning "clear") would replace the resolved value. Normalize it in place; the result
     * is the same for every module, so this is idempotent across the reactor.
     */
    private static void normalizeCliUserProperties(MavenProject project, Resolution resolution) {
        for (WorkspaceSetting setting : WorkspaceSetting.values()) {
            if (resolution.cliUserProperties().containsKey(setting.property)) {
                String raw = resolution.cliUserProperties().getProperty(setting.property);
                resolution.cliUserProperties().setProperty(setting.property,
                        normalized(setting, new Winner(raw != null ? raw : "", "command line"), project));
            }
        }
    }

    private static String normalized(WorkspaceSetting setting, Winner winner, MavenProject project) {
        try {
            return setting.normalize(winner.raw());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Citrus Test Distribution setting in module " + project.getId()
                    + " (from " + winner.tier() + "): " + e.getMessage(), e);
        }
    }

    private static void mutatePlugin(MavenProject project,
                                     Plugin plugin,
                                     String sessionId,
                                     List<Path> extraClasspathJars,
                                     Map<String, String> sharedProperties,
                                     Resolution resolution) {
        Xpp3Dom config = ensureDom(plugin.getConfiguration());
        applyDomMutations(project, config, sessionId, extraClasspathJars, sharedProperties, resolution);
        plugin.setConfiguration(config);

        if (plugin.getExecutions() != null) {
            for (PluginExecution execution : plugin.getExecutions()) {
                if (execution.getConfiguration() != null) {
                    Xpp3Dom execDom = ensureDom(execution.getConfiguration());
                    applyDomMutations(project, execDom, sessionId, extraClasspathJars, sharedProperties, resolution);
                    execution.setConfiguration(execDom);
                }
            }
        }
    }

    private static void applyDomMutations(MavenProject project,
                                          Xpp3Dom config,
                                          String sessionId,
                                          List<Path> extraClasspathJars,
                                          Map<String, String> sharedProperties,
                                          Resolution resolution) {
        Xpp3Dom additionalCp = getOrCreateChild(config, "additionalClasspathElements");
        for (Path jar : extraClasspathJars) {
            String absPath = jar.toAbsolutePath().toString();
            if (!containsChildValue(additionalCp, absPath)) {
                Xpp3Dom elem = new Xpp3Dom("additionalClasspathElement");
                elem.setValue(absPath);
                additionalCp.addChild(elem);
            }
        }

        Xpp3Dom sysProps = getOrCreateChild(config, "systemPropertyVariables");
        setChildValue(sysProps, "junit.platform.launcher.interceptors.enabled", "true");
        setChildValue(sysProps, DistributionSettings.ENABLED_PROPERTY, "true");
        if (sessionId != null && !sessionId.isBlank()) {
            setChildValue(sysProps, DistributionSettings.SESSION_ID_PROPERTY, sessionId);
        }
        setChildValue(sysProps, DistributionSettings.MODULE_NAME_PROPERTY, project.getArtifactId());
        // No REPORT_DIR_PROPERTY: remote results are replayed into Surefire/Failsafe, which write the reports themselves.
        if (sharedProperties != null) {
            sharedProperties.forEach((k, v) -> {
                if (v != null && !v.isBlank()) {
                    setChildValue(sysProps, k, v);
                }
            });
        }

        if (!resolution.cliUserProperties().containsKey(DistributionSettings.BASEDIR_PROPERTY) && project.getBasedir() != null) {
            setChildValue(sysProps, DistributionSettings.BASEDIR_PROPERTY,
                    project.getBasedir().toPath().toAbsolutePath().normalize().toString());
        }
        String buildDirectory = project.getBuild().getDirectory();
        if (buildDirectory != null && !buildDirectory.isBlank()
                && !resolution.cliUserProperties().containsKey(DistributionSettings.BUILD_DIRECTORY_PROPERTY)) {
            setChildValue(sysProps, DistributionSettings.BUILD_DIRECTORY_PROPERTY, buildDirectory);
        }
        for (WorkspaceSetting setting : WorkspaceSetting.values()) {
            Winner winner = winningValue(setting, project, sysProps, resolution);
            if (winner != null) {
                setChildValue(sysProps, setting.property, normalized(setting, winner, project));
            } else {
                removeChild(sysProps, setting.property);
            }
        }
    }

    /**
     * The raw value of the highest tier that defines {@code setting}, or {@code null}.
     */
    private static Winner winningValue(WorkspaceSetting setting, MavenProject project, Xpp3Dom sysProps, Resolution resolution) {
        if (resolution.cliUserProperties().containsKey(setting.property)) {
            String cli = resolution.cliUserProperties().getProperty(setting.property);
            return new Winner(cli != null ? cli : "", "command line");
        }
        Xpp3Dom explicit = sysProps.getChild(setting.property);
        if (explicit != null && isDefined(explicit.getValue())) {
            return new Winner(explicit.getValue(), "systemPropertyVariables");
        }
        String module = project.getProperties() != null ? project.getProperties().getProperty(setting.property) : null;
        if (isDefined(module)) {
            return new Winner(module, "module property");
        }
        String env = resolution.environment().apply(setting.env);
        return isDefined(env) ? new Winner(env, "environment variable " + setting.env) : null;
    }

    private static boolean isDefined(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        String trimmed = value.trim();
        return !(trimmed.startsWith("${") && trimmed.endsWith("}"));
    }

    private static void removeChild(Xpp3Dom parent, String name) {
        for (int i = parent.getChildCount() - 1; i >= 0; i--) {
            if (name.equals(parent.getChild(i).getName())) {
                parent.removeChild(i);
            }
        }
    }

    private static Xpp3Dom ensureDom(Object existing) {
        if (existing instanceof Xpp3Dom dom) {
            return dom;
        }
        return new Xpp3Dom("configuration");
    }

    private static Xpp3Dom getOrCreateChild(Xpp3Dom parent, String name) {
        Xpp3Dom child = parent.getChild(name);
        if (child == null) {
            child = new Xpp3Dom(name);
            parent.addChild(child);
        }
        return child;
    }

    private static void setChildValue(Xpp3Dom parent, String name, String value) {
        Xpp3Dom child = getOrCreateChild(parent, name);
        child.setValue(value);
    }

    private static boolean containsChildValue(Xpp3Dom parent, String expected) {
        for (Xpp3Dom child : parent.getChildren()) {
            if (expected.equals(child.getValue())) {
                return true;
            }
        }
        return false;
    }
}
