/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.dbapi.impl;

import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltQueryExecutor;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.values.virtual.MapValue;

public class BoltQueryExecutorImpl implements BoltQueryExecutor
{
    private final QueryExecutionEngine queryExecutionEngine;
    private final TransactionalContextFactory transactionalContextFactory;
    private final InternalTransaction internalTransaction;

    BoltQueryExecutorImpl( QueryExecutionEngine queryExecutionEngine, TransactionalContextFactory transactionalContextFactory,
            InternalTransaction internalTransaction )
    {
        this.queryExecutionEngine = queryExecutionEngine;
        this.transactionalContextFactory = transactionalContextFactory;
        this.internalTransaction = internalTransaction;
    }

    @Override
    public BoltQueryExecution executeQuery( String query, MapValue parameters, boolean prePopulate, QuerySubscriber subscriber )
            throws QueryExecutionKernelException
    {
        TransactionalContext transactionalContext = transactionalContextFactory.newContext( internalTransaction, query, parameters );
        QueryExecution queryExecution = queryExecutionEngine.executeQuery( query, parameters, transactionalContext, prePopulate, subscriber );
        return new BoltQueryExecutionImpl( queryExecution, transactionalContext );
    }

    private static class BoltQueryExecutionImpl implements BoltQueryExecution
    {
        private final QueryExecution queryExecution;
        private final TransactionalContext transactionalContext;

        BoltQueryExecutionImpl( QueryExecution queryExecution, TransactionalContext transactionalContext )
        {
            this.queryExecution = queryExecution;
            this.transactionalContext = transactionalContext;
        }

        @Override
        public QueryExecution getQueryExecution()
        {
            return queryExecution;
        }

        @Override
        public void close()
        {
            transactionalContext.close();
        }

        @Override
        public void terminate()
        {
            transactionalContext.terminate();
        }
    }
}
