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
package org.neo4j.cypher.javacompat.internal;

import java.util.Map;

import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.logging.LogProvider;

/**
 * This is a variant of {@link ExecutionEngine} that provides additional
 * callbacks that are used by REST server's transactional endpoints for Cypher
 *
 * This is not public API
 */
public class ServerExecutionEngine extends ExecutionEngine implements QueryExecutionEngine
{
    private org.neo4j.cypher.internal.ServerExecutionEngine serverExecutionEngine;

    public ServerExecutionEngine( GraphDatabaseService database, LogProvider logProvider )
    {
        super( database, logProvider );
    }

    @Override
    protected
    org.neo4j.cypher.ExecutionEngine createInnerEngine( GraphDatabaseService database, LogProvider logProvider )
    {
        return serverExecutionEngine = new org.neo4j.cypher.internal.ServerExecutionEngine( database, logProvider );
    }

    @Override
    public Result executeQuery( String query, Map<String, Object> parameters, QuerySession querySession ) throws
            QueryExecutionKernelException
    {
        try
        {
            return new ExecutionResult( serverExecutionEngine.execute( query, parameters, querySession ) );
        }
        catch ( CypherException e )
        {
            throw new QueryExecutionKernelException( e );
        }
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return serverExecutionEngine.isPeriodicCommit( query );
    }

    @Override
    public Result profileQuery( String query, Map<String, Object> parameters, QuerySession session ) throws QueryExecutionKernelException
    {
        try
        {
            return new ExecutionResult( serverExecutionEngine.profile( query, parameters, session ) );
        }
        catch ( CypherException e )
        {
            throw new QueryExecutionKernelException( e );
        }
    }
}
