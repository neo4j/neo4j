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
package org.neo4j.shell;

import java.util.List;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.parser.StatementParser.ParsedStatement;

/**
 * An interface which executes statements
 */
public interface StatementExecuter {

    /**
     * Execute a statement
     *
     * @param statement to execute
     * @throws ExitException    if a command to exit was executed
     * @throws CommandException if something went wrong
     */
    void execute(ParsedStatement statement) throws ExitException, CommandException;

    void execute(List<ParsedStatement> statements) throws ExitException, CommandException;

    /**
     * Stops any running statements
     */
    void reset();

    /**
     * Get the error code from the last executed Cypher statement, or null if the last execution was successful.
     */
    String lastNeo4jErrorCode();
}
