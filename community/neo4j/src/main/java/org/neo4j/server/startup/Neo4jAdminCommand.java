/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.startup;

import static org.neo4j.server.startup.Bootloader.ARG_EXPAND_COMMANDS;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.neo4j.cli.AdminTool;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine;

@CommandLine.Command(name = "Neo4j Admin", description = "Neo4j Admin CLI.")
class Neo4jAdminCommand extends BootloaderCommand implements Callable<Integer>, VerboseCommand {
    Neo4jAdminCommand(Neo4jAdminBootloaderContext ctx) {
        super(ctx);
    }

    static CommandLine asCommandLine(Neo4jAdminBootloaderContext ctx) {
        return addDefaultOptions(new CommandLine(new Neo4jAdminCommand(ctx)), ctx)
                .setUnmatchedArgumentsAllowed(true)
                .setUnmatchedOptionsArePositionalParams(true)
                .setExpandAtFiles(false);
    }

    @CommandLine.Parameters(hidden = true)
    private List<String> allParameters = List.of();

    @CommandLine.Option(
            names = ARG_EXPAND_COMMANDS,
            hidden = true,
            description = "Allow command expansion in config value evaluation.")
    boolean expandCommands;

    @CommandLine.Option(names = ARG_VERBOSE, hidden = true, description = "Prints additional information.")
    boolean verbose;

    @Override
    public Integer call() throws Exception {
        String[] args = allParameters.toArray(new String[0]);
        ctx.init(expandCommands, verbose, args);

        // Lets verify our arguments before we try to execute the command, avoiding forking the VM if the arguments are
        // invalid and improves error/help messages
        CommandLine actualAdminCommand = AdminTool.getCommandLine(
                new ExecutionContext(ctx.home(), ctx.confDir(), ctx.out, ctx.err, new DefaultFileSystemAbstraction()));

        if (allParameters.isEmpty()) { // No arguments (except expand commands/verbose), print usage
            actualAdminCommand.usage(ctx.out);
            return CommandLine.ExitCode.USAGE;
        }
        try {
            CommandLine.ParseResult result = actualAdminCommand.parseArgs(args); // Check if we can parse it
            Integer code = CommandLine.executeHelpRequest(result); // If help is requested
            if (code != null) {
                return code;
            }
        } catch (CommandLine.ParameterException e) {
            return e.getCommandLine()
                    .getParameterExceptionHandler()
                    .handleParseException(e, args); // Parse error, handle and exit
        }

        // Arguments looks fine! Lets try to execute them for real
        Bootloader bootloader = new Bootloader(ctx);
        return bootloader.admin();
    }

    public static void main(String[] args) {
        int exitCode = Neo4jAdminCommand.asCommandLine(new Neo4jAdminBootloaderContext())
                .execute(args);
        System.exit(exitCode);
    }

    @Override
    public boolean verbose() {
        return verbose;
    }

    static class Neo4jAdminBootloaderContext extends BootloaderContext {
        private static final Class<?> entrypoint = AdminTool.class;

        Neo4jAdminBootloaderContext() {
            super(entrypoint);
        }

        @VisibleForTesting
        Neo4jAdminBootloaderContext(
                PrintStream out,
                PrintStream err,
                Function<String, String> envLookup,
                Function<String, String> propLookup,
                Runtime.Version version) {
            super(out, err, envLookup, propLookup, entrypoint, version, List.of());
        }

        @Override
        void init(boolean expandCommands, boolean verbose, String... additionalArgs) {
            super.init(expandCommands, verbose, additionalArgs);
            if (verbose) {
                this.additionalArgs.add(ARG_VERBOSE);
            }
        }

        @Override
        protected Map<Setting<?>, Object> overriddenDefaultsValues() {
            return Map.of(BootloaderSettings.additional_jvm, "-XX:+UseParallelGC");
        }
    }
}
