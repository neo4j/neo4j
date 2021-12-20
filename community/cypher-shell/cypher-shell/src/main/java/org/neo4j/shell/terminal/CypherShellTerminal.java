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
package org.neo4j.shell.terminal;

import java.io.File;
import java.util.List;

import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.log.AnsiFormattedText;

/**
 * Handles user input and output.
 */
public interface CypherShellTerminal
{
    int PROMPT_MAX_LENGTH = 50;

    /** Read from terminal */
    Reader read();

    /** Write to the terminal */
    Writer write();

    /** Returns true if this terminal supports user interactive input */
    boolean isInteractive();

    /** Returns terminal history */
    Historian getHistory();

    /** If set history will be saved to the specified file */
    void setHistoryFile( File file );

    void bindUserInterruptHandler( UserInterruptHandler handler );

    interface Reader
    {
        /**
         * Reads a cypher shell statement from the terminal.
         *
         * @param prompt user prompt
         * @return the read statements, never null
         * @throws NoMoreInputException if there is no more input (user press ctrl+d for example)
         * @throws UserInterruptException if user interrupted input (user press ctrl+c for example)
         */
        ParsedStatement readStatement( AnsiFormattedText prompt ) throws NoMoreInputException, UserInterruptException;

        /**
         * Reads any string from the terminal.
         *
         * @param prompt user prompt
         * @param mask the mask character, null (no mask) or 0 (hide)
         * @return the read line, never null
         * @throws NoMoreInputException if there is no more input (user press ctrl+d for example)
         * @throws UserInterruptException if user interrupted input (user press ctrl+c for example)
         */
        String simplePrompt( String prompt, Character mask ) throws NoMoreInputException, UserInterruptException;
    }

    interface Writer
    {
        /** Prints the specified line to the terminal. */
        void println( String line );

        default void println()
        {
            println( "" );
        }
    }

    interface ParsedStatement
    {
        String unparsed();
        List<String> parsed();
    }

    interface UserInterruptHandler
    {
        void handleUserInterrupt();
    }
}
