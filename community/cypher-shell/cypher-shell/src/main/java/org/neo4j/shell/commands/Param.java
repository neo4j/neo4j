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
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.printer.Printer;

/**
 * This command sets a variable to a name, for use as query parameter.
 */
public record Param(Printer printer, ParameterService parameters) implements Command {
    public static final String NAME = ":param";
    public static final String ALIAS = ":params";

    @Override
    public void execute(List<String> args) throws ExitException, CommandException {
        requireArgumentCount(args, 0, 1);
        if (args.isEmpty() || "list".equalsIgnoreCase(args.get(0).trim())) {
            printer.printOut(parameters.pretty());
        } else if ("clear".equalsIgnoreCase(args.get(0).trim())) {
            parameters.clear();
        } else {
            try {
                final var parsed = parameters.parse(args.get(0));
                parameters.setParameters(parameters.evaluate(parsed));
            } catch (ParameterService.ParameterParsingException e) {
                throw new CommandException(from("Incorrect usage.\nusage: ")
                        .bold(metadata().name())
                        .append(" ")
                        .append(metadata().usage()));
            }
        }
    }

    public static class Factory implements Command.Factory {
        @Override
        public Metadata metadata() {
            var help = "Set the specified query parameter to the value given";
            var usage =
                    """
                    list or :param - Lists all parameters
                    :param clear - Clears all parameters
                    :param {a: 1} - Sets the parameter a to value 1
                    :param {a: 1, b: 1+1} - Sets the parameter a to value 1 and b to value 2.

                    The arrow syntax `:param a => 1` is also supported to set parameters individually.
                    """;
            return new Metadata(NAME, "Set the value of a query parameter", usage, help, List.of(ALIAS));
        }

        @Override
        public Command executor(Arguments args) {
            return new Param(args.printer(), args.parameters());
        }
    }
}
