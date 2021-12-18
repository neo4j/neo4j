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
package org.neo4j.shell.parser;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * An object capable of parsing a piece of text and returning a List statements contained within.
 */
public interface StatementParser
{
    /**
     * Consumes and parses all statements of the specified reader, including incomplete statements.
     */
    ParsedStatements parse( Reader reader ) throws IOException;

    /**
     * Consumes and parses all statements of the specified reader, including incomplete statements.
     */
    ParsedStatements parse( String line ) throws IOException;

    record ParsedStatements( List<ParsedStatement> statements )
    {
        public boolean isEmpty()
        {
            return statements.isEmpty();
        }

        public boolean hasIncompleteStatement()
        {
            return statements.stream().anyMatch( statement -> statement instanceof IncompleteStatement );
        }
    }

    sealed interface ParsedStatement permits CommandStatement, CypherStatement, IncompleteStatement
    {
        String statement();
    }
    record CommandStatement( String name, List<String> args ) implements ParsedStatement
    {
        @Override
        public String statement()
        {
            return name + " " + String.join( " ", args );
        }
    }
    record CypherStatement( String statement ) implements ParsedStatement
    { }
    record IncompleteStatement( String statement ) implements ParsedStatement
    { }
}
