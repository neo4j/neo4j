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
package org.neo4j.cli;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.neo4j.cli.AdminTool.VersionProvider;
import static picocli.CommandLine.IVersionProvider;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;
import org.neo4j.kernel.internal.Version;
import org.neo4j.service.Services;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.HelpCommand;

@Command(
        name = "neo4j-admin",
        description = "Neo4j database administration tool.",
        mixinStandardHelpOptions = true,
        versionProvider = VersionProvider.class,
        sortOptions = false,
        footerHeading = "\nEnvironment variables:\n",
        footer = {
            "  NEO4J_CONF    Path to directory which contains neo4j.conf.",
            "  NEO4J_DEBUG   Set to anything to enable debug output.",
            "  NEO4J_HOME    Neo4j home directory.",
            "  HEAP_SIZE     Set JVM maximum heap size during command execution. Takes a number and a unit, for example 512m.",
            "  JAVA_OPTS     Used to pass custom setting to Java Virtual Machine. Refer to JVM documentation about the exact format. "
                    + "This variable is incompatible with HEAP_SIZE and takes precedence over HEAP_SIZE."
        })
public final class AdminTool {
    // Accept arguments also used by Neo4jAdminCommand, just to let them show in the usage
    @CommandLine.Option(
            names = "--expand-commands",
            description = "Allow command expansion in config value evaluation.")
    private boolean expandCommands;

    @CommandLine.Option(names = "--verbose", description = "Prints additional information.")
    private boolean verbose;

    private AdminTool() {
        // nope
    }

    public static void main(String[] args) {
        final var homeDir = getDirOrExit("NEO4J_HOME");
        final var confDir = getDirOrExit("NEO4J_CONF");
        final var ctx = new ExecutionContext(homeDir, confDir);
        final var exitCode = execute(ctx, args);
        System.exit(exitCode);
    }

    @SuppressWarnings("InstantiationOfUtilityClass")
    @VisibleForTesting
    public static int execute(ExecutionContext ctx, String... args) {
        final CommandLine cmd = getCommandLine(ctx);
        if (args.length == 0) {
            cmd.usage(cmd.getOut());
            return ExitCode.USAGE;
        }
        return cmd.execute(args);
    }

    public static CommandLine getCommandLine(ExecutionContext ctx) {
        CommandLine cmd = new CommandLine(new AdminTool())
                .setOut(new PrintWriter(ctx.out(), true))
                .setErr(new PrintWriter(ctx.err(), true))
                .setUsageHelpWidth(120)
                .setCaseInsensitiveEnumValuesAllowed(true);
        registerCommands(cmd, ctx, Services.loadAll(CommandProvider.class));
        return cmd;
    }

    private static void registerCommands(
            CommandLine cmd, ExecutionContext ctx, Collection<CommandProvider> commandProviders) {
        cmd.addSubcommand(HelpCommand.class);
        filterCommandProviders(commandProviders).stream()
                .sorted(Comparator.comparing(CommandProvider::commandType))
                .forEach(commandProvider -> cmd.addSubcommand(commandProvider.createCommand(ctx)));
    }

    protected static Collection<CommandProvider> filterCommandProviders(Collection<CommandProvider> commandProviders) {
        return commandProviders.stream()
                .collect(Collectors.toMap(k -> k.commandType(), v -> v, (cp1, cp2) -> {
                    if (cp1.getPriority() == cp2.getPriority()) {
                        throw new IllegalArgumentException(String.format(
                                "Command providers %s and %s create commands with the same priority",
                                cp1.getClass(), cp2.getClass()));
                    }

                    return cp1.getPriority() > cp2.getPriority() ? cp1 : cp2;
                }))
                .values();
    }

    private static Path getDirOrExit(String envVar) {
        final var value = System.getenv(envVar);
        if (isBlank(value)) {
            System.err.printf("Required environment variable '%s' is not set%n", envVar);
            System.exit(ExitCode.USAGE);
        }
        final var path = Path.of(value).toAbsolutePath();
        if (!Files.isDirectory(path)) {
            System.err.printf("%s path doesn't exist or not a directory: %s%n", envVar, path);
            System.exit(ExitCode.USAGE);
        }
        return path;
    }

    static class VersionProvider implements IVersionProvider {
        @Override
        public String[] getVersion() {
            return new String[] {Version.getNeo4jVersion()};
        }
    }
}
