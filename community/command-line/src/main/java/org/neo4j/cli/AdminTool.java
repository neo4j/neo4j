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
package org.neo4j.cli;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static picocli.CommandLine.IVersionProvider;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SystemUtils;
import org.neo4j.kernel.internal.Version;
import org.neo4j.service.Services;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.UsageMessageSpec;

@Command(
        name = "neo4j-admin",
        description = "Neo4j database administration tool.",
        mixinStandardHelpOptions = true,
        versionProvider = AdminTool.VersionProvider.class,
        footerHeading = "\nEnvironment variables:\n",
        subcommands = {AdminTool.VersionCommand.class, HelpCommand.class},
        footer = {
            "  NEO4J_CONF    Path to directory which contains neo4j.conf.",
            "  NEO4J_DEBUG   Set to anything to enable debug output.",
            "  NEO4J_HOME    Neo4j home directory.",
            "  HEAP_SIZE     Set JVM maximum heap size during command execution. Takes a number and a unit, for example 512m.",
            "  JAVA_OPTS     Used to pass custom setting to Java Virtual Machine executing the command. "
                    + "Refer to JVM documentation about the exact format. "
                    + "This variable is incompatible with HEAP_SIZE and takes precedence over HEAP_SIZE."
        })
public class AdminTool {
    private static final String ENV_NEO4J_HOME = "NEO4J_HOME";
    private static final String ENV_NEO4J_CONF = "NEO4J_CONF";

    // Accept arguments also used by Neo4jAdminCommand, just to let them show in the usage
    @CommandLine.Option(
            names = "--expand-commands",
            description = "Allow command expansion in config value evaluation.")
    private boolean expandCommands;

    @CommandLine.Option(names = "--verbose", description = "Prints additional information.")
    private boolean verbose;

    protected AdminTool() {
        // nope
    }

    public static void main(String[] args) {
        final var homeDir = getHomeDir();
        final var confDir = getConfDir(homeDir);
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
        return getCommandLine(ctx, new Strategy() {
            @Override
            public AdminTool createRootCommand() {
                return new AdminTool();
            }

            @Override
            public void registerCommandsFromGroup(
                    CommandGroup commandGroup, CommandLine commandLine, Collection<CommandProvider> commandProviders) {
                var messageSpec = new UsageMessageSpec().description(commandGroup.getDescription());
                CommandSpec commandSpec =
                        CommandSpec.create().name(commandGroup.getDisplayName()).usageMessage(messageSpec);
                CommandLine groupCommandLine = new CommandLine(commandSpec, new ContextInjectingFactory(ctx))
                        .addSubcommand(null, HelpCommand.class, "-h", "--help");
                registerGroupCommands(commandProviders, commandGroup, ctx, groupCommandLine, type -> true);
                commandLine.addSubcommand(groupCommandLine);
            }
        });
    }

    protected static CommandLine getCommandLine(ExecutionContext ctx, Strategy strategy) {
        CommandLine cmd = new CommandLine(strategy.createRootCommand(), new ContextInjectingFactory(ctx));
        registerCommands(cmd, strategy, Services.loadAll(CommandProvider.class));
        cmd.setOut(new PrintWriter(ctx.out(), true))
                .setErr(new PrintWriter(ctx.err(), true))
                .setUsageHelpWidth(120)
                .setCaseInsensitiveEnumValuesAllowed(true);
        return cmd;
    }

    private static void registerCommands(
            CommandLine cmd, Strategy strategy, Collection<CommandProvider> commandProviders) {
        for (CommandGroup commandGroup : CommandGroup.values()) {
            strategy.registerCommandsFromGroup(commandGroup, cmd, commandProviders);
        }
    }

    private static void registerGroupCommands(
            Collection<CommandProvider> commandProviders,
            CommandGroup commandGroup,
            ExecutionContext ctx,
            CommandLine commandLine,
            Predicate<CommandType> commandPredicate) {
        List<Object> subcommands = filterCommandProviders(commandProviders, commandGroup).stream()
                .filter(c -> commandPredicate.test(c.commandType()))
                .map(c -> c.createCommand(ctx))
                .sorted(new CommandNameComparator())
                .toList();
        for (Object subcommand : subcommands) {
            commandLine.addSubcommand(subcommand);
        }
    }

    protected static Collection<CommandProvider> filterCommandProviders(
            Collection<CommandProvider> commandProviders, CommandGroup group) {
        return commandProviders.stream()
                .filter(c -> c.commandType().getCommandGroup() == group)
                .filter(c -> SystemUtils.IS_OS_WINDOWS || c.commandType() != CommandType.NEO4J_SERVICE)
                .collect(Collectors.toMap(CommandProvider::commandType, v -> v, (cp1, cp2) -> {
                    if (cp1.getPriority() == cp2.getPriority()) {
                        throw new IllegalArgumentException(String.format(
                                "Command providers %s and %s create commands with the same priority",
                                cp1.getClass(), cp2.getClass()));
                    }

                    return cp1.getPriority() < cp2.getPriority() ? cp1 : cp2;
                }))
                .values();
    }

    private static Path getHomeDir() {
        var value = System.getenv(ENV_NEO4J_HOME);
        if (isBlank(value)) {
            System.err.printf("Required environment variable '%s' is not set%n", ENV_NEO4J_HOME);
            System.exit(ExitCode.USAGE);
        }
        var path = Path.of(value).toAbsolutePath();
        checkExistsAndIsDirectory(path, true, ENV_NEO4J_HOME);
        return path;
    }

    private static void checkExistsAndIsDirectory(Path path, boolean mustExist, String envVariable) {
        if (!mustExist && !Files.exists(path)) {
            // This directory doesn't need to exist, and it doesn't so don't check any further.
            // This explicit check is done because the below check would yield false for a non-existent path.
            return;
        }

        if (!Files.isDirectory(path)) {
            System.err.printf("%s path doesn't exist or not a directory: %s%n", envVariable, path);
            System.exit(ExitCode.USAGE);
        }
    }

    private static Path getConfDir(Path homeDir) {
        var value = System.getenv(ENV_NEO4J_CONF);
        var isExplicitlySet = !isBlank(value);
        var path = isExplicitlySet ? Path.of(value).toAbsolutePath() : homeDir.resolve("conf");
        if (isExplicitlySet) {
            checkExistsAndIsDirectory(path, false, ENV_NEO4J_CONF);
        }
        return path;
    }

    static class VersionProvider implements IVersionProvider {
        @Override
        public String[] getVersion() {
            return new String[] {Version.getNeo4jVersion()};
        }
    }

    // 'neo4j <command>' is an alias for 'neo4j-admin server <command>'
    // Implementations of this interface contain the code that make this 'alias' behaviour work.
    private interface Strategy {

        AdminTool createRootCommand();

        void registerCommandsFromGroup(
                CommandGroup commandGroup, CommandLine commandLine, Collection<CommandProvider> commandProviders);
    }

    // 'neo4j <command>' is a partial alias for 'neo4j-admin server <command>'
    // This class is the entry point for the alias.
    @Command(
            name = "neo4j",
            description = "A partial alias for 'neo4j-admin server'. Commands for working with DBMS process "
                    + "from 'neo4j-admin server' category can be invoked using this command.")
    public static class Neo4jAlias extends AdminTool {

        private static final Set<CommandType> SUPPORTED_COMMANDS = Set.of(
                CommandType.NEO4J_CONSOLE,
                CommandType.NEO4J_START,
                CommandType.NEO4J_RESTART,
                CommandType.NEO4J_STATUS,
                CommandType.NEO4J_STOP,
                CommandType.NEO4J_SERVICE);

        public static CommandLine getCommandLine(ExecutionContext ctx) {
            return getCommandLine(ctx, new Strategy() {
                @Override
                public AdminTool createRootCommand() {
                    return new Neo4jAlias();
                }

                @Override
                public void registerCommandsFromGroup(
                        CommandGroup commandGroup,
                        CommandLine commandLine,
                        Collection<CommandProvider> commandProviders) {
                    if (commandGroup == CommandGroup.SERVER) {
                        registerGroupCommands(
                                commandProviders, commandGroup, ctx, commandLine, SUPPORTED_COMMANDS::contains);
                    }
                }
            });
        }

        @SuppressWarnings("InstantiationOfUtilityClass")
        @VisibleForTesting
        public static int execute(ExecutionContext ctx, String... args) {
            final CommandLine cmd = getCommandLine(ctx);
            if (args.length == 0) {
                cmd.usage(cmd.getOut());
                return CommandLine.ExitCode.USAGE;
            }
            return cmd.execute(args);
        }

        public static void main(String[] args) {
            final var homeDir = getHomeDir();
            final var confDir = getConfDir(homeDir);
            final var ctx = new ExecutionContext(homeDir, confDir);
            final var exitCode = execute(ctx, args);
            System.exit(exitCode);
        }
    }

    private static class CommandNameComparator implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            return getCommand(o1).name().compareTo(getCommand(o2).name());
        }

        private Command getCommand(Object object) {
            var clazz = object.getClass();
            while (clazz != null) {
                if (clazz.isAnnotationPresent(Command.class)) {
                    return clazz.getAnnotation(Command.class);
                }
                clazz = clazz.getSuperclass();
            }
            throw new IllegalStateException("Instance of " + object.getClass() + " is not a command.");
        }
    }

    @Command(name = "version", description = "Print version information and exit.")
    public static class VersionCommand implements Callable<Integer> {

        private final ExecutionContext ctx;

        public VersionCommand(ExecutionContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public Integer call() throws Exception {
            ctx.out().println("neo4j " + Version.getNeo4jVersion());
            return 0;
        }
    }
}
