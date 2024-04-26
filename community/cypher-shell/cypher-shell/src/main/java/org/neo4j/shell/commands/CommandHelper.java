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
package org.neo4j.shell.commands;

import static java.util.Map.entry;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.Historian;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.terminal.CypherShellTerminal;
import org.neo4j.util.VisibleForTesting;

/**
 * Utility methods for dealing with commands
 */
public class CommandHelper {
    private final TreeMap<String, Command> commands = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public CommandHelper(
            Printer printer,
            Historian historian,
            CypherShell cypherShell,
            CypherShellTerminal terminal,
            ParameterService parameters) {
        var args = new Command.Factory.Arguments(printer, historian, cypherShell, terminal, parameters);
        new CommandFactoryHelper().factoryByName().forEach((key, value) -> commands.put(key, value.executor(args)));
    }

    /**
     * Get a command corresponding to the given name, or null if no such command has been registered.
     */
    public Command getCommand(final String name) {
        return commands.get(name);
    }

    public static String stripEnclosingBackTicks(String input) {
        if (input.startsWith("`") && input.endsWith("`")) {
            return input.substring(1, input.length() - 1);
        }

        return input;
    }

    public static class CommandFactoryHelper {
        private static final Map<Class<? extends Command>, Command.Factory> factoryMap = Map.ofEntries(
                entry(Begin.class, new Begin.Factory()),
                entry(Commit.class, new Commit.Factory()),
                entry(Connect.class, new Connect.Factory()),
                entry(Disconnect.class, new Disconnect.Factory()),
                entry(Exit.class, new Exit.Factory()),
                entry(Help.class, new Help.Factory()),
                entry(History.class, new History.Factory()),
                entry(Param.class, new Param.Factory()),
                entry(Rollback.class, new Rollback.Factory()),
                entry(Source.class, new Source.Factory()),
                entry(Use.class, new Use.Factory()),
                entry(Impersonate.class, new Impersonate.Factory()),
                entry(SysInfo.class, new SysInfo.Factory()),
                entry(AccessMode.class, new AccessMode.Factory()));

        private static final Map<String, Command.Factory> factoryByName = buildFactoryByName();

        public Command.Factory factoryFor(Class<? extends Command> commandClass) {
            return factoryMap.get(commandClass);
        }

        public Command.Factory factoryByName(String name) {
            return factoryByName.get(name);
        }

        public Collection<Command.Factory> factories() {
            return factoryMap.values();
        }

        public Stream<Command.Metadata> metadata() {
            return factoryMap.values().stream().map(Command.Factory::metadata);
        }

        public Map<String, Command.Factory> factoryByName() {
            return factoryByName;
        }

        @VisibleForTesting
        protected Map<Class<? extends Command>, Command.Factory> factoryByClass() {
            return factoryMap;
        }

        private static Map<String, Command.Factory> buildFactoryByName() {
            Map<String, Command.Factory> builder = new HashMap<>();

            for (Command.Factory factory : factoryMap.values()) {
                var metadata = factory.metadata();
                builder.put(metadata.name(), factory);
                metadata.aliases().forEach(alias -> builder.put(alias, factory));
            }

            return Map.copyOf(builder);
        }
    }
}
