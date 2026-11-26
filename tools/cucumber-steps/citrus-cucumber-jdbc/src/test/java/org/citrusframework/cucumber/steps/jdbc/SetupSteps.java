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

package org.citrusframework.cucumber.steps.jdbc;

import io.cucumber.java.en.Given;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.context.TestContext;
import org.citrusframework.testcontainers.postgresql.PostgreSQLSettings;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class SetupSteps {

    @CitrusResource
    private TestContext context;

    private static final PostgreSQLContainer testdbContainer = createContainer();

    private static PostgreSQLContainer createContainer() {
        return new PostgreSQLContainer(
                DockerImageName.parse(PostgreSQLSettings.getImageName()).withTag(PostgreSQLSettings.getPostgreSQLVersion()))
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("secret")
                .withInitScript("test-db-init.sql");
    }

    @Given("^Start database$")
    public void startDatabase() {
        testdbContainer.start();
    }

    @Given("^Use the database container URL$")
    public void useDatabaseContainerUrl() {
        context.setVariable("database.url", testdbContainer.getJdbcUrl());
    }

    @Given("^Stop database$")
    public void stopDatabase() {
        testdbContainer.stop();
    }
}
