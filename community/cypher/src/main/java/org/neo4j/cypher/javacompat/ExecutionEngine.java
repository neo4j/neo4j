/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.javacompat;

import org.neo4j.cypher.SyntaxException;
import org.neo4j.cypher.commands.Query;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

/**
 * To run a {@link Query}, use this class.
 */
public class ExecutionEngine
{
    private org.neo4j.cypher.ExecutionEngine inner;

    /**
     * Creates an execution engine around the give graph database
     * @param database The database to wrap
     */
    public ExecutionEngine( GraphDatabaseService database )
    {
        inner = new org.neo4j.cypher.ExecutionEngine( database );
    }

    /**
     * Executes a {@link Query} and returns an iterable that contains the result set
     * @param query The query to execute
     * @return A ExecutionResult that contains the result set
     * @throws org.neo4j.cypher.SyntaxException If the Query contains errors,
     * a SyntaxException exception might be thrown
     */
    public ExecutionResult execute( Query query ) throws SyntaxException
    {
        return new ExecutionResult(inner.execute( query ));
    }

    /**
     * Executes a {@link Query} and returns an iterable that contains the result set
     * @param query The query to execute
     * @param params Parameters for the query
     * @return A ExecutionResult that contains the result set
     * @throws org.neo4j.cypher.SyntaxException If the Query contains errors,
     * a SyntaxException exception might be thrown
     */
    public ExecutionResult execute( Query query, Map<String, Object> params) throws SyntaxException
    {
        return new ExecutionResult(inner.execute(query, params));
    }
}
