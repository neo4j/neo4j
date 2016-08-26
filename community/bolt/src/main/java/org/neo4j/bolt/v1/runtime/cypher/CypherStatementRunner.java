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

package org.neo4j.bolt.v1.runtime.cypher;

import java.util.Map;

import org.neo4j.bolt.v1.runtime.spi.StatementRunner;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.kernel.impl.query.TransactionalContext;

import static java.lang.String.format;
import static org.neo4j.kernel.api.KernelTransaction.Type.implicit;

public class CypherStatementRunner implements StatementRunner
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();

    private final QueryExecutionEngine queryExecutionEngine;
    private final ThreadToStatementContextBridge txBridge;
    private GraphDatabaseQueryService queryService;

    public CypherStatementRunner( QueryExecutionEngine queryExecutionEngine, ThreadToStatementContextBridge txBridge,
            GraphDatabaseQueryService queryService )
    {
        this.queryExecutionEngine = queryExecutionEngine;
        this.txBridge = txBridge;
        this.queryService = queryService;
    }

    @Override
    public Result run( final String querySource, final AuthSubject authSubject, final String statement, final Map<String, Object> params )
            throws KernelException
    {
        InternalTransaction transaction = queryService.beginTransaction( implicit, authSubject );
        TransactionalContext transactionalContext =
                new Neo4jTransactionalContext( queryService, transaction, txBridge.get(), locker );
        QuerySession session = new BoltQuerySession( transactionalContext, querySource );
        return queryExecutionEngine.executeQuery( statement, params, session );
    }

    static class BoltQuerySession extends QuerySession
    {
        private final String querySource;
        private final String username;

        BoltQuerySession( TransactionalContext transactionalContext, String querySource )
        {
            super( transactionalContext );
            this.username = transactionalContext.accessMode().name();
            this.querySource = querySource;
        }

        @Override
        public String toString()
        {
            return format( "bolt-session\t%s\t%s", querySource, username );
        }
    }
}
