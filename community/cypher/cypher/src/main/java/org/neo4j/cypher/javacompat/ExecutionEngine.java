/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.cypher.CypherException;
import org.neo4j.graphdb.GraphDatabaseService;

import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

/**
 * To run a Cypher query, use this class.
 *
 * This class construct and initialize both the cypher compiler and the cypher runtime, which is a very expensive
 * operation so please make sure this will be constructed only once and properly reused.
 *
 * @deprecated use {@link org.neo4j.graphdb.GraphDatabaseService#execute(String)} instead.
 */
@Deprecated
public class ExecutionEngine
{
    private org.neo4j.cypher.ExecutionEngine inner;

    /**
     * Creates an execution engine around the give graph database
     * @param database The database to wrap
     */
    public ExecutionEngine( GraphDatabaseService database )
    {
        inner = createInnerEngine( database, NullLogProvider.getInstance() );
    }

    /**
     * Creates an execution engine around the give graph database
     * @param database The database to wrap
     * @param logProvider A {@link LogProvider} for cypher-statements
     */
    public ExecutionEngine( GraphDatabaseService database, LogProvider logProvider )
    {
        inner = createInnerEngine( database, logProvider );
    }

    protected
    org.neo4j.cypher.ExecutionEngine createInnerEngine( GraphDatabaseService database, LogProvider logProvider )
    {
        return new org.neo4j.cypher.ExecutionEngine( database, logProvider );
    }

    /**
     * Executes a query and returns an iterable that contains the result set
     * @param query The query to execute
     * @return A ExecutionResult that contains the result set
     * @throws org.neo4j.cypher.SyntaxException If the Query contains errors,
     * a SyntaxException exception might be thrown
     */
    public ExecutionResult execute( String query ) throws CypherException
    {
        return new ExecutionResult( inner.execute( query ) );
    }

    /**
     * Executes a query and returns an iterable that contains the result set
     * @param query The query to execute
     * @param params Parameters for the query
     * @return A ExecutionResult that contains the result set
     * @throws org.neo4j.cypher.SyntaxException If the Query contains errors,
     * a SyntaxException exception might be thrown
     */
    public ExecutionResult execute( String query, Map<String, Object> params) throws CypherException
    {
        return new ExecutionResult( inner.execute( query, params ) );
    }

    /**
     * Profiles a query and returns an iterable that contains the result set.
     * Note that in order to gather profiling information, this actually executes
     * the query as well. You can wrap a call to this in a transaction that you
     * roll back if you don't want the query to have an actual effect on the data.
     *
     * @param query The query to profile
     * @return A ExecutionResult that contains the result set
     * @throws org.neo4j.cypher.SyntaxException If the Query contains errors,
     * a SyntaxException exception might be thrown
     */
    public ExecutionResult profile( String query ) throws CypherException
    {
        return new ExecutionResult( inner.profile( query ) );
    }

    /**
     * Profiles a query and returns an iterable that contains the result set.
     * Note that in order to gather profiling information, this actually executes
     * the query as well. You can wrap a call to this in a transaction that you
     * roll back if you don't want the query to have an actual effect on the data.
     *
     * @param query The query to profile
     * @param params Parameters for the query
     * @return A ExecutionResult that contains the result set
     * @throws org.neo4j.cypher.SyntaxException If the Query contains errors,
     * a SyntaxException exception might be thrown
     */
    public ExecutionResult profile( String query, Map<String, Object> params) throws CypherException
    {
        return new ExecutionResult( inner.profile( query, params ) );
    }

    /**
     * Turns a valid Cypher query and returns it with keywords in uppercase,
     * and new-lines in the appropriate places.
     *
     * @param query The query to make pretty
     * @return The same query, but prettier
     */
    public String prettify( String query )
    {
        return inner.prettify( query );
    }
}
