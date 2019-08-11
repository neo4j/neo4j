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
package org.neo4j.server.security.systemgraph;

import java.util.Map;

import org.neo4j.cypher.internal.javacompat.SystemDatabaseInnerAccessor;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

/**
 * Switches the transactional context of the thread while operating on the system graph.
 */
public class ContextSwitchingSystemGraphQueryExecutor implements QueryExecutor
{
    private final DatabaseManager<?> databaseManager;
    private final ThreadToStatementContextBridge txBridge;

    public ContextSwitchingSystemGraphQueryExecutor( DatabaseManager<?> databaseManager, ThreadToStatementContextBridge txBridge )
    {
        this.databaseManager = databaseManager;
        this.txBridge = txBridge;
    }

    @Override
    public void executeQuery( String query, Map<String,Object> params, ErrorPreservingQuerySubscriber subscriber )
    {
        // pause outer transaction if there is one
        if ( txBridge.hasTransaction() )
        {
            final KernelTransaction outerTx = txBridge.getAndUnbindAnyTransaction();

            try
            {
                systemDbExecute( query, params, subscriber );
            }
            finally
            {
                txBridge.unbindTransactionFromCurrentThread();
                txBridge.bindTransactionToCurrentThread( outerTx );
            }
        }
        else
        {
            systemDbExecute( query, params, subscriber );
        }
    }

    private void systemDbExecute( String query, Map<String,Object> parameters, ErrorPreservingQuerySubscriber subscriber )
    {
        // NOTE: This transaction is executed with AUTH_DISABLED.
        // We need to make sure this method is only accessible from a SecurityContext with admin rights.
        try ( Transaction transaction = getSystemDb().beginTx() )
        {
            systemDbExecuteWithinTransaction( (InternalTransaction) transaction, query, parameters, subscriber );
            if ( !subscriber.hasError() )
            {
                transaction.commit();
            }
        }
    }

    private void systemDbExecuteWithinTransaction( InternalTransaction transaction, String query, Map<String,Object> parameters,
           QuerySubscriber subscriber )
    {
        QueryExecution result = getSystemDb().execute( transaction, query, parameters, subscriber );
        try
        {
            result.consumeAll();
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Failed to access data", e );
        }
    }

    @Override
    public Transaction beginTx()
    {
        final Runnable onClose;

        // pause outer transaction if there is one
        if ( txBridge.hasTransaction() )
        {
            final KernelTransaction outerTx = txBridge.getAndUnbindAnyTransaction();

            onClose = () ->
            {
                // Restore the outer transaction
                txBridge.bindTransactionToCurrentThread( outerTx );
            };
        }
        else
        {
            onClose = () -> {};
        }

        Transaction transaction = getSystemDb().beginTx();

        return new Transaction()
        {
            @Override
            public void terminate()
            {
                transaction.terminate();
            }

            @Override
            public void commit()
            {
                transaction.commit();
            }

            @Override
            public void rollback()
            {
                transaction.rollback();
            }

            @Override
            public void close()
            {
                try
                {
                    transaction.close();
                }
                finally
                {
                    txBridge.unbindTransactionFromCurrentThread();
                    onClose.run();
                }
            }

            @Override
            public Lock acquireWriteLock( PropertyContainer entity )
            {
                return transaction.acquireWriteLock( entity );
            }

            @Override
            public Lock acquireReadLock( PropertyContainer entity )
            {
                return transaction.acquireReadLock( entity );
            }
        };
    }

    private SystemDatabaseInnerAccessor getSystemDb()
    {
        return databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID ).orElseThrow(
                () -> new AuthProviderFailedException( "No database called `" + SYSTEM_DATABASE_NAME + "` was found." ) )
                .dependencies().resolveDependency( SystemDatabaseInnerAccessor.class );
    }
}
