/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.javacompat;

import java.util.Map;

import org.neo4j.cypher.CypherException;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.logging.LogProvider;

/**
 * To run a Cypher query, use this class.
 *
 * This class construct and initialize both the cypher compiler and the cypher runtime, which is a very expensive
 * operation so please make sure this will be constructed only once and properly reused.
 *
 */
public class ExecutionEngine implements QueryExecutionEngine
{
    private org.neo4j.cypher.internal.ExecutionEngine inner;

    /**
     * Creates an execution engine around the give graph database
     * @param queryService The database to wrap
     * @param logProvider A {@link LogProvider} for cypher-statements
     */
    public ExecutionEngine( GraphDatabaseQueryService queryService, LogProvider logProvider )
    {
        inner = new org.neo4j.cypher.internal.ExecutionEngine( queryService, logProvider );
    }

    @Override
    public GraphDatabaseQueryService queryService()
    {
        return inner.queryService();
    }

    @Override
    public Result executeQuery( String query, Map<String, Object> parameters, QuerySession querySession ) throws
            QueryExecutionKernelException
    {
        try
        {
            return new ExecutionResult( inner.execute( query, parameters, querySession ) );
        }
        catch ( CypherException e )
        {
            throw new QueryExecutionKernelException( e );
        }
    }

    @Override
    public Result profileQuery( String query, Map<String, Object> parameters, QuerySession session ) throws QueryExecutionKernelException
    {
        try
        {
            return new ExecutionResult( inner.profile( query, parameters, session ) );
        }
        catch ( CypherException e )
        {
            throw new QueryExecutionKernelException( e );
        }
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return inner.isPeriodicCommit( query );
    }

    /**
     * Turns a valid Cypher query and returns it with keywords in uppercase,
     * and new-lines in the appropriate places.
     *
     * @param query The query to make pretty
     * @return The same query, but prettier
     */
    @Override
    public String prettify( String query )
    {
        return inner.prettify( query );
    }
}
