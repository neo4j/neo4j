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

import static org.neo4j.shell.cli.AccessMode.parse;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.printer.Printer;

/**
 * Command to view or set access mode.
 */
public class AccessMode implements Command {
    private final CypherShell shell;
    private final Printer printer;

    public AccessMode(CypherShell shell, Printer printer) {
        this.shell = shell;
        this.printer = printer;
    }

    @Override
    public void execute(final List<String> args) throws ExitException, CommandException {
        requireArgumentCount(args, 0, 1);
        if (args.isEmpty()) {
            printer.printOut("Access mode " + shell.accessMode());
        } else {
            final var am = parse(args.get(0)).orElseThrow(() -> unknownMode(args.get(0)));
            shell.reconnect(am);
        }
    }

    private CommandException unknownMode(String in) {
        final var available = Arrays.stream(org.neo4j.shell.cli.AccessMode.values())
                .map(Enum::name)
                .collect(Collectors.joining(", "));
        return new CommandException("Unknown access mode %s, available modes are %s".formatted(in, available));
    }

    public static class Factory implements Command.Factory {
        @Override
        public Metadata metadata() {
            var help = "View or set access mode";
            var usage =
                    """
                    - Display current access mode
                    :access-mode read - Reconnect with read access mode
                    :access-mode write - Reconnect with write access mode
                    """;
            return new Metadata(":access-mode", help, usage, help, List.of());
        }

        @Override
        public Command executor(Arguments args) {
            return new AccessMode(args.cypherShell(), args.printer());
        }
    }
}
