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

package org.citrusframework.jbang.cli.commands;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Option;
import org.aesh.command.option.ParentCommand;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.jbang.cli.CitrusJBangMain;

@CommandDefinition(name = "sessions", description = "List recent distributed test sessions on the broker", generateHelp = true)
public class DistSessions extends CitrusCommand {

    @ParentCommand
    Dist parent;

    @Option(name = "broker-url", shortName = 'b', defaultValue = "http://localhost:8765", description = "Broker base URL")
    String brokerUrl;

    @Option(name = "auth-token", description = "Shared Bearer authentication token")
    String authToken;

    @Option(name = "limit", shortName = 'l', defaultValue = "20", description = "Maximum sessions to display")
    int limit;

    @Option(name = "json", hasValue = false, description = "Output raw JSON")
    boolean json;

    public DistSessions() {
        super(null);
    }

    public DistSessions(CitrusJBangMain main) {
        super(main);
    }

    @Override
    public CitrusJBangMain getMain() {
        if (super.getMain() == null && parent != null) {
            setMain(parent.getMain());
        }
        return super.getMain();
    }

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        try {
            String base = brokerUrl.endsWith("/") ? brokerUrl.substring(0, brokerUrl.length() - 1) : brokerUrl;
            HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions?limit=" + limit))
                    .timeout(Duration.ofSeconds(10))
                    .GET();
            if (authToken != null && !authToken.isBlank()) {
                req.header("Authorization", "Bearer " + authToken);
            }
            HttpResponse<String> response = HttpClient.newHttpClient().send(req.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                printer().println("Broker returned HTTP " + response.statusCode() + ": " + response.body());
                return CommandResult.FAILURE;
            }
            if (json) {
                printer().println(response.body());
                return CommandResult.SUCCESS;
            }
            List<BuildSessionRecord> sessions = Arrays.asList(
                    DistributionJson.fromJson(response.body(), BuildSessionRecord[].class));
            String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, sessions, List.of(
                    new Column().header("SESSION ID").dataAlign(HorizontalAlign.LEFT).with(BuildSessionRecord::sessionId),
                    new Column().header("PROJECT").dataAlign(HorizontalAlign.LEFT).with(BuildSessionRecord::projectName),
                    new Column().header("STATUS").dataAlign(HorizontalAlign.LEFT).with(s -> String.valueOf(s.status())),
                    new Column().header("PASSED").dataAlign(HorizontalAlign.RIGHT).with(s -> Integer.toString(s.passedTests())),
                    new Column().header("FAILED").dataAlign(HorizontalAlign.RIGHT).with(s -> Integer.toString(s.failedTests())),
                    new Column().header("WALL(ms)").dataAlign(HorizontalAlign.RIGHT).with(s -> Long.toString(s.wallDurationMs()))));
            printer().println(table);
            return CommandResult.SUCCESS;
        } catch (Exception e) {
            printer().println("Failed to query sessions from " + brokerUrl + ": " + e.getMessage());
            return CommandResult.FAILURE;
        }
    }
}
