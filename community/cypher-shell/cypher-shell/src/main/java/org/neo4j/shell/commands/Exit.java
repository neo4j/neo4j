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

import static org.neo4j.shell.Main.EXIT_SUCCESS;

import java.util.List;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.printer.AnsiFormattedText;

/**
 * Command to exit the logger. Equivalent to hitting Ctrl-D.
 */
public class Exit implements Command {
    @Override
    public void execute(final List<String> args) throws ExitException, CommandException {
        requireArgumentCount(args, 0);
        throw new ExitException(EXIT_SUCCESS);
    }

    public static class Factory implements Command.Factory {
        @Override
        public Metadata metadata() {
            var help = AnsiFormattedText.from("Exit the logger. Corresponds to entering ")
                    .bold("CTRL-D")
                    .append(".")
                    .resetAndRender();
            return new Metadata(":exit", "Exit the logger", "", help, List.of(":quit"));
        }

        @Override
        public Command executor(Arguments args) {
            return new Exit();
        }
    }
}
