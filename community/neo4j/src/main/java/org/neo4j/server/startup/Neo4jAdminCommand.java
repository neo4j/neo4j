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

import static org.neo4j.server.startup.Bootloader.ARG_EXPAND_COMMANDS;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.AdminTool;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(name = "Neo4j Admin", description = "Neo4j Admin CLI.")
public class Neo4jAdminCommand implements Callable<Integer>, VerboseCommand {

    private final Class<?> entrypoint;
    private final Environment environment;

    @Spec
    private CommandSpec spec;

    @Option(
            names = ARG_EXPAND_COMMANDS,
            hidden = true,
            description = "Allow command expansion in config value evaluation.")
    boolean expandCommands;

    @Option(names = ARG_VERBOSE, hidden = true, description = "Prints additional information.")
    boolean verbose;

    public Neo4jAdminCommand(Environment environment) {
        this(AdminTool.class, environment);
    }

    protected Neo4jAdminCommand(Class<?> entrypoint, Environment environment) {
        this.entrypoint = entrypoint;
        this.environment = environment;
    }

    @Override
    public Integer call() throws Exception {
        final var execArgs = buildExecArgs();

        try (var adminBootloader = createAdminBootloader(execArgs.arguments)) {

            // Lets verify our arguments before we try to execute the command, avoiding forking the VM if the arguments
            // are
            // invalid and improves error/help messages
            var ctx = new EnhancedExecutionContext(
                    adminBootloader.home(),
                    adminBootloader.confDir(),
                    environment.out(),
                    environment.err(),
                    new DefaultFileSystemAbstraction(),
                    this::createDbmsBootloader,
                    adminBootloader.getPluginClassLoader());
            CommandLine actualCommand = getActualAdminCommand(ctx);
            if (!execArgs.hasSubCommandArguments) { // No arguments (except expand commands/verbose), print usage
                actualCommand.usage(adminBootloader.environment.err());
                return CommandLine.ExitCode.USAGE;
            }

            ExecutionInfo executionInfo;
            try {
                CommandLine.ParseResult result =
                        actualCommand.parseArgs(execArgs.arguments); // Check if we can parse it
                Integer code = CommandLine.executeHelpRequest(result); // If help is requested
                if (code != null) {
                    return code;
                }
                executionInfo = getExecutionInfo(result);
            } catch (CommandLine.ParameterException e) {
                if (e.getCommandLine() == actualCommand && e instanceof CommandLine.UnmatchedArgumentException) {
                    // We got a mismatch on first layer, can be missing database/dbms/server
                    // Lets add those subcommand to help the suggestion printing
                    // Its safe to do, as we're exiting anyway
                    Map<String, CommandLine> subcommands = actualCommand.getSubcommands();
                    Map<String, CommandLine> permutations = new HashMap<>();
                    subcommands.forEach((outerName, outerSubCommand) -> outerSubCommand
                            .getSubcommands()
                            .keySet()
                            .forEach(innerName -> permutations.put(outerName + " " + innerName, outerSubCommand)));
                    permutations.forEach(actualCommand::addSubcommand);
                }
                return e.getCommandLine()
                        .getParameterExceptionHandler()
                        .handleParseException(e, execArgs.arguments); // Parse error, handle and exit
            }

            // Arguments looks fine! Let's try to execute it for real
            if (executionInfo.forkingAdminCommand) {
                return adminBootloader.admin(executionInfo.additionalConfigs);
            } else {
                return actualCommand.execute(execArgs.arguments);
            }
        }
    }

    private ExecArguments buildExecArgs() {
        final var originalArgs = spec.commandLine().getParseResult().originalArgs();
        var unmatchedCount = originalArgs.size();
        if (expandCommands) {
            unmatchedCount--;
        }
        if (verbose) {
            unmatchedCount--;
        }
        return new ExecArguments(originalArgs.toArray(new String[0]), unmatchedCount > 0);
    }

    private ExecutionInfo getExecutionInfo(CommandLine.ParseResult parseResult) {
        for (CommandLine commandLine : parseResult.asCommandLineList()) {
            if (commandLine.getCommand() instanceof AbstractAdminCommand adminCommand) {
                return new ExecutionInfo(true, adminCommand.getCommandConfigs());
            }
        }
        return new ExecutionInfo(false, List.of());
    }

    protected CommandLine getActualAdminCommand(ExecutionContext executionContext) {
        return AdminTool.getCommandLine(executionContext);
    }

    @VisibleForTesting
    protected Bootloader.Admin createAdminBootloader(String[] args) {
        return new Bootloader.Admin(entrypoint, environment, expandCommands, verbose, args);
    }

    @VisibleForTesting
    protected Bootloader.Dbms createDbmsBootloader() {
        return new Bootloader.Dbms(environment, expandCommands, verbose);
    }

    @Override
    public boolean verbose() {
        return verbose;
    }

    static CommandLine asCommandLine(Neo4jAdminCommand command, Environment environment) {
        return new CommandLine(command)
                .setCaseInsensitiveEnumValuesAllowed(true)
                .setExecutionExceptionHandler(new ExceptionHandler(environment))
                .setOut(new PrintWriter(environment.out(), true))
                .setErr(new PrintWriter(environment.err(), true))
                .setUnmatchedArgumentsAllowed(true)
                .setUnmatchedOptionsArePositionalParams(true)
                .setExpandAtFiles(false);
    }

    public static void main(String[] args) {
        var environment = Environment.SYSTEM;
        int exitCode = Neo4jAdminCommand.asCommandLine(new Neo4jAdminCommand(environment), environment)
                .execute(args);
        System.exit(exitCode);
    }

    private static class ExceptionHandler implements CommandLine.IExecutionExceptionHandler {
        private final Environment environment;

        ExceptionHandler(Environment environment) {
            this.environment = environment;
        }

        @Override
        public int handleExecutionException(
                Exception exception, CommandLine commandLine, CommandLine.ParseResult parseResult) {
            if (commandLine.getCommand() instanceof VerboseCommand
                    && !((VerboseCommand) commandLine.getCommand()).verbose()) {
                environment.err().println(exception.getMessage());
                environment.err().println("Run with '--verbose' for a more detailed error message.");
            } else {
                exception.printStackTrace(environment.err());
            }
            if (exception instanceof CommandFailedException failure) {
                return failure.getExitCode();
            }
            return commandLine.getCommandSpec().exitCodeOnExecutionException();
        }
    }

    private record ExecArguments(String[] arguments, boolean hasSubCommandArguments) {}

    private record ExecutionInfo(boolean forkingAdminCommand, List<Path> additionalConfigs) {}
}
