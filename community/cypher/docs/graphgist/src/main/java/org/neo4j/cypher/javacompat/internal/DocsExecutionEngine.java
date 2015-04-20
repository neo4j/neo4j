/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.javacompat.internal;

import java.util.Collections;
import java.util.Map;

import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

/**
 * To run a Cypher query in internal Neo4j documentation-related tests you can use this class.
 */
public class DocsExecutionEngine
{
    private org.neo4j.cypher.internal.DocsExecutionEngine inner;

    /**
     * Creates an execution engine around the give graph database
     * @param database The database to wrap
     */
    public DocsExecutionEngine( GraphDatabaseService database )
    {
        this( database, NullLogProvider.getInstance() );
    }

    /**
     * Creates an execution engine around the give graph database
     *
     * @param database The database to wrap
     * @param logProvider A Log provider for cypher-statements
     */
    public DocsExecutionEngine( GraphDatabaseService database, LogProvider logProvider )
    {
        inner = (org.neo4j.cypher.internal.DocsExecutionEngine) createInnerEngine( database, logProvider );
    }

    protected
    org.neo4j.cypher.ExecutionEngine createInnerEngine( GraphDatabaseService database, LogProvider logProvider )
    {
        return new org.neo4j.cypher.internal.DocsExecutionEngine( database, logProvider, null, null );
    }

    /**
     * Executes a query and returns an iterable that contains the result set
     * @param query The query to execute
     * @return A ExecutionResult that contains the result set
     * @throws org.neo4j.cypher.SyntaxException If the Query contains errors,
     * a SyntaxException exception might be thrown
     */
    public InternalExecutionResult execute( String query ) throws CypherException
    {
        return (InternalExecutionResult) inner.internalExecute( query, Collections.<String, Object> emptyMap() );
    }

    /**
     * Executes a query and returns an iterable that contains the result set
     * @param query The query to execute
     * @param params Parameters for the query
     * @return A ExecutionResult that contains the result set
     * @throws org.neo4j.cypher.SyntaxException If the Query contains errors,
     * a SyntaxException exception might be thrown
     */
    public InternalExecutionResult execute( String query, Map<String,Object> params ) throws CypherException
    {
        return (InternalExecutionResult) inner.internalExecute( query, params );
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
    public InternalExecutionResult profile( String query ) throws CypherException
    {
        return (InternalExecutionResult) inner.internalProfile( query, Collections.<String, Object> emptyMap() );
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
    public InternalExecutionResult profile( String query, Map<String,Object> params ) throws CypherException
    {
        return (InternalExecutionResult) inner.internalProfile( query, params );
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
