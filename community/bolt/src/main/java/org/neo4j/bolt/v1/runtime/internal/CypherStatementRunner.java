/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal;

import java.util.Map;

import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.bolt.v1.runtime.spi.StatementRunner;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QuerySession;

public class CypherStatementRunner implements StatementRunner
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();
    private final QueryExecutionEngine queryExecutionEngine;

    public CypherStatementRunner( QueryExecutionEngine queryExecutionEngine )
    {
        this.queryExecutionEngine = queryExecutionEngine;
    }

    @Override
    public RecordStream run( final SessionState ctx, final String statement, final Map<String,Object> params )
            throws KernelException
    {
        // Temporary until we move parsing to cypher, or run a parser up here
        if ( statement.equalsIgnoreCase( "begin" ) )
        {
            ctx.beginTransaction();
            return RecordStream.EMPTY;
        }
        else if ( statement.equalsIgnoreCase( "commit" ) )
        {
            ctx.commitTransaction();
            return RecordStream.EMPTY;
        }
        else if ( statement.equalsIgnoreCase( "rollback" ) )
        {
            ctx.rollbackTransaction();
            return RecordStream.EMPTY;
        }
        else
        {
            boolean hasTx = ctx.hasTransaction();
            boolean isPeriodicCommit = queryExecutionEngine.isPeriodicCommit( statement );

            if ( !hasTx && !isPeriodicCommit )
            {
                ctx.beginImplicitTransaction();
            }

            QuerySession session = ctx.createSession( queryExecutionEngine.queryService(), locker );
            Result result = queryExecutionEngine.executeQuery( statement, params, session );

            if ( isPeriodicCommit )
            {
                ctx.beginImplicitTransaction();
            }

            return new CypherAdapterStream( result );
        }
    }
}
