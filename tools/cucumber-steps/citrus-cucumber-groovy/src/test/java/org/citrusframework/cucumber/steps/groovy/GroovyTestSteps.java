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

package org.citrusframework.cucumber.steps.groovy;

import io.cucumber.java.en.Then;
import org.assertj.core.api.Assertions;
import org.citrusframework.Citrus;
import org.citrusframework.annotations.CitrusFramework;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.context.TestContext;
import org.citrusframework.http.server.HttpServer;

public class GroovyTestSteps {

    @CitrusFramework
    private Citrus citrus;

    @CitrusResource
    private TestContext context;

    @Then("store the port of HTTP server {word} as {word}")
    public void storeHttpServerPort(String endpoint, String variable) {
        HttpServer server = citrus.getCitrusContext().getReferenceResolver().resolve(endpoint, HttpServer.class);
        Assertions.assertThat(server.getPort()).isPositive();
        context.setVariable(variable, server.getPort());
    }

    @Then("^verify endpoint ([^\"\\s]+)$")
    public void verifyEndpointResolvable(String endpoint) {
        Assertions.assertThat(citrus.getCitrusContext().getReferenceResolver().isResolvable(endpoint)).isTrue();
    }

    @Then("^verify bean ([^\"\\s]+)$")
    public void verifyBeanResolvable(String name) {
        Assertions.assertThat(citrus.getCitrusContext().getReferenceResolver().isResolvable(name)).isTrue();
    }
}
