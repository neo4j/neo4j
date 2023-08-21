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

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.printer.Printer;

/**
 * Show command history
 */
public class History implements Command {
    private final Printer printer;
    private final Historian historian;

    public History(final Printer printer, final Historian historian) {
        this.printer = printer;
        this.historian = historian;
    }

    @Override
    public void execute(List<String> args) throws ExitException, CommandException {
        requireArgumentCount(args, 0, 1);

        if (args.size() == 1) {
            if ("clear".equalsIgnoreCase(args.get(0))) {
                clearHistory();
            } else {
                throw new CommandException("Unrecognised argument " + args.get(0));
            }
        } else {
            // Calculate starting position
            int lineCount = 16;

            printer.printOut(printHistory(historian.getHistory(), lineCount));
        }
    }

    /**
     * Prints N last lines of history.
     *
     * @param lineCount number of entries to print
     */
    private static String printHistory(final List<String> history, final int lineCount) {
        // for alignment, check the string length of history size
        int colWidth = Integer.toString(history.size()).length();
        String firstLineFormat = " %-" + colWidth + "d  %s%n";
        String continuationLineFormat = " %-" + colWidth + "s  %s%n";
        StringBuilder builder = new StringBuilder();

        for (int i = Math.max(0, history.size() - lineCount); i < history.size(); i++) {
            var statement = history.get(i);
            var lines = statement.split("\\r?\\n");

            builder.append(format(firstLineFormat, i + 1, lines[0]));

            for (int l = 1; l < lines.length; l++) {
                builder.append(format(continuationLineFormat, " ", lines[l]));
            }
        }

        return builder.toString();
    }

    private void clearHistory() throws CommandException {
        try {
            printer.printIfVerbose("Removing history...");
            historian.clear();
        } catch (IOException e) {
            throw new CommandException("Failed to clear history: " + e.getMessage());
        }
    }

    public static class Factory implements Command.Factory {
        @Override
        public Metadata metadata() {
            var help =
                    "':history' prints a list of the last statements executed\n':history clear' removes all entries from the history";
            return new Metadata(":history", "Statement history", "", help, List.of());
        }

        @Override
        public Command executor(Arguments args) {
            return new History(args.printer(), args.historian());
        }
    }
}
