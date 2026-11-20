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

    public static void configureProject(MavenProject project,
                                        String sessionId,
                                        List<Path> extraClasspathJars,
                                        Map<String, String> sharedProperties) {
        if (project == null || project.getBuild() == null || project.getBuild().getPluginsAsMap() == null) {
            return;
        }
        Map<String, Plugin> plugins = project.getBuild().getPluginsAsMap();
        Plugin surefire = plugins.get(SUREFIRE_KEY);
        if (surefire != null) {
            mutatePlugin(project, surefire, sessionId, extraClasspathJars, sharedProperties, "surefire-reports");
        }
        Plugin failsafe = plugins.get(FAILSAFE_KEY);
        if (failsafe != null) {
            mutatePlugin(project, failsafe, sessionId, extraClasspathJars, sharedProperties, "failsafe-reports");
        }
    }

    private static void mutatePlugin(MavenProject project,
                                     Plugin plugin,
                                     String sessionId,
                                     List<Path> extraClasspathJars,
                                     Map<String, String> sharedProperties,
                                     String defaultReportFolder) {
        Xpp3Dom config = ensureDom(plugin.getConfiguration());
        applyDomMutations(project, config, sessionId, extraClasspathJars, sharedProperties, defaultReportFolder);
        plugin.setConfiguration(config);

        if (plugin.getExecutions() != null) {
            for (PluginExecution execution : plugin.getExecutions()) {
                if (execution.getConfiguration() != null) {
                    Xpp3Dom execDom = ensureDom(execution.getConfiguration());
                    applyDomMutations(project, execDom, sessionId, extraClasspathJars, sharedProperties, defaultReportFolder);
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
                                          String defaultReportFolder) {
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
        if (project.getBuild() != null && project.getBuild().getDirectory() != null) {
            Path reportDir = Path.of(project.getBuild().getDirectory()).resolve(defaultReportFolder);
            setChildValue(sysProps, DistributionSettings.REPORT_DIR_PROPERTY, reportDir.toAbsolutePath().toString());
        }
        if (sharedProperties != null) {
            sharedProperties.forEach((k, v) -> {
                if (v != null && !v.isBlank()) {
                    setChildValue(sysProps, k, v);
                }
            });
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
