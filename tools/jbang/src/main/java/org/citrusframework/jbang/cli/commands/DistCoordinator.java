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

import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommand;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.ParentCommand;
import org.citrusframework.jbang.cli.CitrusJBangMain;

@CommandDefinition(name = "coordinator", description = "Manage Citrus Test Distribution coordinator", generateHelp = true,
        groupCommands = {DistCoordinatorStart.class})
public class DistCoordinator extends CitrusCommand implements GroupCommand<CommandInvocation> {

    @ParentCommand
    Dist parent;

    public DistCoordinator() {
        super(null);
    }

    public DistCoordinator(CitrusJBangMain main) {
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
        printer().println(invocation.getHelpInfo());
        return CommandResult.SUCCESS;
    }

    @Override
    public List<Command<CommandInvocation>> getCommands() {
        return List.of(new DistCoordinatorStart(getMain()));
    }
}
