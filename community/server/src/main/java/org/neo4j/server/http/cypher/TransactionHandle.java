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
package org.neo4j.server.http.cypher;

import java.net.URI;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Encapsulates executing statements in a transaction, committing the transaction, or rolling it back.
 *
 * Constructing a {@link TransactionHandle} does not immediately ask the kernel to create a
 * {@link org.neo4j.kernel.api.KernelTransaction}; instead a {@link org.neo4j.kernel.api.KernelTransaction} is
 * only created when the first statements need to be executed.
 *
 * At the end of each statement-executing method, the {@link org.neo4j.kernel.api.KernelTransaction} is either
 * suspended (ready to be resumed by a later operation), or committed, or rolled back.
 *
 * If you acquire instances of this class from {@link TransactionHandleRegistry}, it will prevent concurrent access to
 * the same instance. Therefore the implementation assumes that a single instance will only be accessed from
 * a single thread.
 *
 * All of the public methods on this class are "single-shot"; once you have called one method, the handle returns
 * itself
 * to the registry. If you want to use it again, you'll need to acquire it back from the registry to ensure exclusive
 * use.
 */
class TransactionHandle implements TransactionTerminationHandle
{
    private final TransitionalPeriodTransactionMessContainer txManagerFacade;
    private final QueryExecutionEngine engine;
    private final TransactionRegistry registry;
    private final TransactionUriScheme uriScheme;
    private final Type type;
    private final LoginContext loginContext;
    private final ClientConnectionInfo connectionInfo;
    private long customTransactionTimeoutMillis;
    private final long id;
    private TransitionalTxManagementKernelTransaction context;
    private GraphDatabaseQueryService queryService;
    private long expirationTimestamp = -1;

    TransactionHandle( TransitionalPeriodTransactionMessContainer txManagerFacade, QueryExecutionEngine engine, GraphDatabaseQueryService queryService,
            TransactionRegistry registry, TransactionUriScheme uriScheme, boolean implicitTransaction, LoginContext loginContext,
            ClientConnectionInfo connectionInfo, long customTransactionTimeoutMillis, LogProvider logProvider )
    {
        this.txManagerFacade = txManagerFacade;
        this.engine = engine;
        this.queryService = queryService;
        this.registry = registry;
        this.uriScheme = uriScheme;
        this.type = implicitTransaction ? Type.implicit : Type.explicit;
        this.loginContext = loginContext;
        this.connectionInfo = connectionInfo;
        this.customTransactionTimeoutMillis = customTransactionTimeoutMillis;
        this.id = registry.begin( this );
    }

    URI uri()
    {
        return uriScheme.txUri( id );
    }

    boolean isImplicit()
    {
        return type == Type.implicit;
    }

    long getExpirationTimestamp()
    {
        return expirationTimestamp;
    }

    long getId()
    {
        return id;
    }

    @Override
    public boolean terminate()
    {
        if ( context != null )
        {
            context.terminate();
        }
        return true;
    }

    boolean isPeriodicCommit( String statement )
    {
        return engine.isPeriodicCommit( statement );
    }

    void closeTransactionForPeriodicCommit()
    {
        context.closeTransactionForPeriodicCommit();
    }

    void reopenAfterPeriodicCommit()
    {
        context.reopenAfterPeriodicCommit();
    }

    void ensureActiveTransaction()
    {
        if ( context == null )
        {
            context = txManagerFacade.newTransaction( type, loginContext, connectionInfo, customTransactionTimeoutMillis );
        }
        else
        {
            context.resumeSinceTransactionsAreStillThreadBound();
        }
    }

    Result executeStatement( Statement statement, boolean periodicCommit ) throws QueryExecutionKernelException
    {
        if ( periodicCommit )
        {
            var db = txManagerFacade.getDb();
            Result result;
            try ( Transaction transaction = db.beginTransaction(Type.implicit, loginContext, connectionInfo, customTransactionTimeoutMillis, MILLISECONDS ) )
            {
                result = db.execute( statement.getStatement(), statement.getParameters() );
                transaction.commit();
            }
            return result;
        }
        TransactionalContext tc = txManagerFacade.create( queryService, context.getInternalTransaction(),
                type, loginContext, statement.getStatement(), statement.getParameters() );
        return engine.executeQuery( statement.getStatement(), ValueUtils.asMapValue( statement.getParameters() ), tc, false );
    }

    void forceRollback()
    {
        context.resumeSinceTransactionsAreStillThreadBound();
        context.rollback();
    }

    void suspendTransaction()
    {
        context.suspendSinceTransactionsAreStillThreadBound();
        expirationTimestamp = registry.release( id, this );
    }

    void commit()
    {
        try
        {
            context.commit();
        }
        finally
        {
            registry.forget( id );
        }
    }

    void rollback()
    {
        try
        {
            context.rollback();
        }
        finally
        {
            registry.forget( id );
        }
    }

    boolean hasTransactionContext()
    {
        return context != null;
    }
}
