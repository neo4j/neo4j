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

import static org.neo4j.shell.printer.AnsiFormattedText.from;

import java.util.List;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.printer.AnsiFormattedText;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.terminal.CypherShellTerminal;

/**
 * A shell command
 */
public interface Command {
    void execute(List<String> args) throws ExitException, CommandException;

    default Metadata metadata() {
        // By default, load from factory, because we're lazy
        return new CommandHelper.CommandFactoryHelper()
                .factoryFor(this.getClass())
                .metadata();
    }

    default void requireArgumentCount(List<String> args, int count) throws CommandException {
        if (args.size() != count) {
            throw new CommandException(incorrectNumberOfArguments());
        }
    }

    default void requireArgumentCount(List<String> args, int min, int max) throws CommandException {
        if (args.size() < min || args.size() > max) {
            throw new CommandException(incorrectNumberOfArguments());
        }
    }

    default AnsiFormattedText incorrectNumberOfArguments() {
        return from("Incorrect number of arguments.\nUsage: ")
                .bold(metadata().name())
                .append(" ")
                .append(metadata().usage());
    }

    record Metadata(String name, String description, String usage, String help, List<String> aliases) {}

    interface Factory {
        record Arguments(
                Printer printer,
                Historian historian,
                CypherShell cypherShell,
                CypherShellTerminal terminal,
                ParameterService parameters) {}

        Metadata metadata();

        Command executor(Arguments args);
    }
}
