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
package org.neo4j.shell.terminal;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.parser.StatementParser.ParsedStatements;
import org.neo4j.shell.printer.AnsiFormattedText;

/**
 * Handles user input and output.
 */
public interface CypherShellTerminal {
    int PROMPT_MAX_LENGTH = 50;

    /** Start reading statements interactively */
    Reader read();

    /**
     * Read simple string from user, intended to be short-lived.
     *
     * This is a workaround, I couldn't get jline to mask input when output is redirected.
     */
    SimplePrompt simplePrompt();

    /** Write to the terminal */
    Writer write();

    /** Returns true if this terminal supports user interactive input */
    boolean isInteractive();

    /** Returns terminal history */
    Historian getHistory();

    void setHistoryBehaviour(HistoryBehaviour behaviour) throws IOException;

    void bindUserInterruptHandler(UserInterruptHandler handler);

    interface Reader {
        /**
         * Reads cypher shell statements from the terminal.
         *
         * @param prompt user prompt
         * @return the read statements, never null
         * @throws NoMoreInputException if there is no more input (user press ctrl+d for example)
         * @throws UserInterruptException if user interrupted input (user press ctrl+c for example)
         */
        ParsedStatements readStatement(AnsiFormattedText prompt) throws NoMoreInputException, UserInterruptException;

        /**
         * Reads any string from the terminal.
         *
         * @param prompt user prompt
         * @param mask the mask character, null (no mask) or 0 (hide)
         * @return the read line, never null
         * @throws NoMoreInputException if there is no more input (user press ctrl+d for example)
         * @throws UserInterruptException if user interrupted input (user press ctrl+c for example)
         */
        String simplePrompt(String prompt, Character mask) throws NoMoreInputException, UserInterruptException;
    }

    interface Writer {
        /** Prints the specified line to the terminal. */
        void println(String line);

        default void println() {
            println("");
        }
    }

    interface UserInterruptHandler {
        void handleUserInterrupt();
    }

    sealed interface HistoryBehaviour {}

    final class InMemoryHistory implements HistoryBehaviour {}

    final class DefaultHistory implements HistoryBehaviour {}

    record FileHistory(Path historyFile) implements HistoryBehaviour {}
}
