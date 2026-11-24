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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Option;
import org.aesh.command.option.ParentCommand;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.jbang.cli.CitrusJBangMain;

@CommandDefinition(name = "status", description = "Check coordinator health and connected worker pool status", generateHelp = true)
public class DistStatus extends CitrusCommand {

    @ParentCommand
    Dist parent;

    @Option(name = "coordinator-url", shortName = 'u', defaultValue = "http://localhost:8765", description = "Coordinator base URL")
    String coordinatorUrl;

    @Option(name = "auth-token", description = "Shared Bearer authentication token")
    String authToken;

    @Option(name = "json", hasValue = false, description = "Output raw JSON")
    boolean json;

    public DistStatus() {
        super(null);
    }

    public DistStatus(CitrusJBangMain main) {
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
            String base = coordinatorUrl.endsWith("/") ? coordinatorUrl.substring(0, coordinatorUrl.length() - 1) : coordinatorUrl;
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

            HttpResponse<String> health = client.send(
                    HttpRequest.newBuilder(URI.create(base + "/health")).timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());

            HttpRequest.Builder workersReq = HttpRequest.newBuilder(URI.create(base + "/api/v1/workers"))
                    .timeout(Duration.ofSeconds(5)).GET();
            if (authToken != null && !authToken.isBlank()) {
                workersReq.header("Authorization", "Bearer " + authToken);
            }
            HttpResponse<String> workersResp = client.send(workersReq.build(), HttpResponse.BodyHandlers.ofString());
            List<WorkerRecord> workers = workersResp.statusCode() == 200
                    ? Arrays.asList(DistributionJson.fromJson(workersResp.body(), WorkerRecord[].class))
                    : List.of();

            if (json) {
                Map<String, Object> summary = new LinkedHashMap<>();
                summary.put("coordinatorUrl", base);
                summary.put("status", health.statusCode() == 200 ? "UP" : "DOWN");
                summary.put("workersConnected", workers.size());
                summary.put("workers", workers);
                printer().println(DistributionJson.toJson(summary));
                return health.statusCode() == 200 ? CommandResult.SUCCESS : CommandResult.FAILURE;
            }

            printer().println("Coordinator: " + base + " [" + (health.statusCode() == 200 ? "UP" : "HTTP " + health.statusCode()) + "]");
            printer().println("Connected workers: " + workers.size());
            for (WorkerRecord worker : workers) {
                printer().println("  - " + worker.workerId() + " (" + worker.state() + ") " + worker.capabilities());
            }
            return health.statusCode() == 200 ? CommandResult.SUCCESS : CommandResult.FAILURE;
        } catch (Exception e) {
            printer().println("Failed to reach coordinator at " + coordinatorUrl + ": " + e.getMessage());
            return CommandResult.FAILURE;
        }
    }
}
