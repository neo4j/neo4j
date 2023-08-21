/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.startup;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
import org.neo4j.cli.ExecutionContext;
import picocli.AutoComplete;
import picocli.CommandLine;

@CommandLine.Command(
        name = "generate-tab-completion",
        description = "Generates tab completion scripts for neo4j & neo4j-admin CLI on Bash and ZSH")
public class TabCompletionGenerator implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Output directory")
    private Path output;

    private void dumpTabCompletionForCommand(Neo4jAdminCommand command) throws IOException {
        CommandLine commandLine = command.getActualAdminCommand(new ExecutionContext(output, output));
        String name = commandLine
                .getCommand()
                .getClass()
                .getAnnotation(CommandLine.Command.class)
                .name();
        String script = AutoComplete.bash(name, commandLine);
        Path file = output.resolve(name + "_completion");
        Files.writeString(file, script, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public Integer call() throws Exception {
        if (Files.exists(output) && !Files.isDirectory(output)) {
            throw new FileAlreadyExistsException(output.toString() + " Needs to be a directory");
        }
        Files.createDirectories(output);

        dumpTabCompletionForCommand(new Neo4jAdminCommand(Environment.SYSTEM));
        dumpTabCompletionForCommand(new Neo4jCommand(Environment.SYSTEM));
        return 0;
    }

    public static void main(String[] args) {
        // No use of System.exit() as this is called by generate-completion-script using exec-maven-plugin from
        // packaging and may terminate the whole build
        new CommandLine(new TabCompletionGenerator()).execute(args);
    }
}
