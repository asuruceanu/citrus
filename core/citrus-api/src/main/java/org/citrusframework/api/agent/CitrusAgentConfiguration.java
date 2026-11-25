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

package org.citrusframework.api.agent;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.citrusframework.TestSourceProvider;
import org.citrusframework.api.main.CitrusAppConfiguration;
import org.citrusframework.spi.Resource;
import org.citrusframework.spi.Resources;
import org.citrusframework.api.yaml.SchemaProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CitrusAgentConfiguration extends CitrusAppConfiguration {

    public static final String DEFAULT_COORDINATOR_URL = "http://localhost:8084";

    /** Logger */
    private static final Logger logger = LoggerFactory.getLogger(CitrusAgentConfiguration.class);

    /** Server port */
    private int port = CitrusAgentSettings.getServerPort();

    private boolean offline;

    private boolean inspectCode = true;

    private String mode = "standalone";

    private String coordinatorUrl;

    private String authToken = "";

    private String pool;

    private String registrationKey;

    private String capabilities = "";

    private String cacheDir = "";

    private Boolean cacheCleanup;

    private String cacheFreeSpaceBufferSize;

    /**
     * Gets the port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port.
     */
    @SchemaProperty(required = true, description = "The Http port the agent service is listening on.", defaultValue = "4567")
    public void setPort(int port) {
        this.port = port;
    }

    public boolean isOffline() {
        return offline;
    }

    @SchemaProperty(description = "When enabled there will be no attempts to resolve Maven artifacts via internet connection.", defaultValue = "true")
    public void setOffline(boolean offline) {
        this.offline = offline;
    }

    public boolean isInspectCode() {
        return inspectCode;
    }

    @SchemaProperty(description = "When enabled the source code gets analyzed for required modules and dependencies that are added to the classpath.")
    public void setInspectCode(boolean inspectCode) {
        this.inspectCode = inspectCode;
    }

    public String getMode() {
        return mode;
    }

    @SchemaProperty(description = "Agent execution mode: standalone or worker.", defaultValue = "standalone")
    public void setMode(String mode) {
        this.mode = mode;
    }

    public boolean isWorkerMode() {
        return "worker".equalsIgnoreCase(mode);
    }

    public String getCoordinatorUrl() {
        return getCoordinatorUrl(DEFAULT_COORDINATOR_URL);
    }

    /**
     * Returns the explicitly configured coordinator URL, or the supplied fallback when none was configured.
     */
    public String getCoordinatorUrl(String fallbackUrl) {
        return coordinatorUrl == null || coordinatorUrl.isBlank() ? fallbackUrl : coordinatorUrl;
    }

    @SchemaProperty(description = "Citrus Test Distribution Coordinator URL when running in worker mode.", defaultValue = DEFAULT_COORDINATOR_URL)
    public void setCoordinatorUrl(String coordinatorUrl) {
        this.coordinatorUrl = coordinatorUrl;
    }

    public String getAuthToken() {
        return authToken;
    }

    @SchemaProperty(description = "Shared authentication token for the Citrus Test Distribution Coordinator.")
    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public String getPool() {
        return pool;
    }

    @SchemaProperty(description = "Agent pool ID used for worker registration.")
    public void setPool(String pool) {
        this.pool = pool;
    }

    public String getRegistrationKey() {
        return registrationKey;
    }

    @SchemaProperty(description = "Registration key used to join the worker pool.")
    public void setRegistrationKey(String registrationKey) {
        this.registrationKey = registrationKey;
    }

    public String getCapabilities() {
        return capabilities;
    }

    @SchemaProperty(description = "Comma-separated capability key=value pairs advertised in worker mode.")
    public void setCapabilities(String capabilities) {
        this.capabilities = capabilities;
    }

    public String getCacheDir() {
        return cacheDir;
    }

    @SchemaProperty(description = "Local content-addressed blob cache directory in worker mode.")
    public void setCacheDir(String cacheDir) {
        this.cacheDir = cacheDir;
    }

    /**
     * Gets the worker cache cleanup flag or {@code null} when not explicitly configured.
     */
    public Boolean getCacheCleanup() {
        return cacheCleanup;
    }

    @SchemaProperty(description = "Enable or disable cleanup of the worker local file cache (worker mode)")
    public void setCacheCleanup(Boolean cacheCleanup) {
        this.cacheCleanup = cacheCleanup;
    }

    /**
     * Gets the worker cache free space buffer size expression or {@code null} when not explicitly configured.
     */
    public String getCacheFreeSpaceBufferSize() {
        return cacheFreeSpaceBufferSize;
    }

    @SchemaProperty(description = "Minimum free space to keep on the worker cache drive, e.g. 2GiB or 512MiB (worker mode)")
    public void setCacheFreeSpaceBufferSize(String cacheFreeSpaceBufferSize) {
        this.cacheFreeSpaceBufferSize = cacheFreeSpaceBufferSize;
    }

    /**
     * Applies configuration with settable properties at runtime.
     */
    public void apply(CitrusAppConfiguration configuration) {
        setEngine(configuration.getEngine());
        setPackages(configuration.getPackages());
        setTestSources(configuration.getTestSources());
        setIncludes(configuration.getIncludes());
        setWorkDir(configuration.getWorkDir());
        setVerbose(configuration.isVerbose());
        setReset(configuration.isReset());
        addDefaultProperties(configuration.getDefaultProperties());
        setConfigClass(configuration.getConfigClass());
    }

    public static CitrusAgentConfiguration fromEnvVars(TestSourceProvider provider) {
        CitrusAgentConfiguration configuration = new CitrusAgentConfiguration();

        configuration.setPort(CitrusAgentSettings.getServerPort());

        configuration.setMode(CitrusAgentSettings.getMode());
        configuration.setEngine(CitrusAgentSettings.getTestEngine());
        configuration.setIncludes(CitrusAgentSettings.getIncludes());
        configuration.setWorkDir(CitrusAgentSettings.getWorkDir());
        configuration.setSystemExit(CitrusAgentSettings.isSystemExit());
        configuration.setSkipTests(CitrusAgentSettings.isSkipTests());
        configuration.setConfigClass(CitrusAgentSettings.getConfigClass());

        configuration.setPackages(Arrays.asList(CitrusAgentSettings.getPackages()));
        configuration.setTestSources(Arrays.stream(CitrusAgentSettings.getTestSources())
                .map(provider::create)
                .collect(Collectors.toList()));

        configuration.setVerbose(CitrusAgentSettings.isVerbose());
        configuration.setReset(CitrusAgentSettings.isReset());
        configuration.addDefaultProperties(CitrusAgentSettings.getDefaultProperties());

        String testJarPath = CitrusAgentSettings.getTestJar();
        Resource testJar = Resources.create(testJarPath);
        if (testJar.exists()) {
            configuration.setTestJar(testJar.file());
        } else {
            logger.debug("Ignore test jar artifact {} - not found", testJarPath);
        }

        configuration.setTimeToLive(CitrusAgentSettings.getTimeToLive());

        configuration.setModules(CitrusAgentSettings.getModules());
        configuration.setDependencies(CitrusAgentSettings.getDependencies());

        configuration.setOffline(CitrusAgentSettings.isOffline());
        configuration.setInspectCode(CitrusAgentSettings.isInspectCode());

        return configuration;
    }
}
