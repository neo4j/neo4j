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
package org.neo4j.kernel.impl.query;

import java.util.function.Supplier;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryRegistryOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;

public class Neo4jTransactionalContext implements TransactionalContext
{
    private final GraphDatabaseQueryService graph;
    private final ThreadToStatementContextBridge txBridge;
    private final KernelTransaction.Type transactionType;
    private final AccessMode mode;
    private final Supplier<Statement> statementSupplier;
    private final DbmsOperations dbmsOperations;
    private final Guard guard;
    private final ExecutingQuery executingQuery;
    private final PropertyContainerLocker locker;

    private InternalTransaction transaction;
    private Statement statement;
    private boolean isOpen = true;

    public Neo4jTransactionalContext(
            GraphDatabaseQueryService graph,
            InternalTransaction initialTransaction,
            KernelTransaction.Type transactionType,
            AccessMode transactionMode,
            Supplier<Statement> statementSupplier,
            ExecutingQuery executingQuery,
            PropertyContainerLocker locker,
            ThreadToStatementContextBridge txBridge,
            DbmsOperations dbmsOperations,
            Guard guard
    ) {
        this.graph = graph;
        this.transaction = initialTransaction;
        this.transactionType = transactionType;
        this.mode = transactionMode;
        this.statementSupplier = statementSupplier;
        this.statement = statementSupplier.get();
        this.executingQuery = executingQuery;
        this.locker = locker;
        this.txBridge = txBridge;
        this.dbmsOperations = dbmsOperations;
        this.guard = guard;
    }

    @Override
    public ExecutingQuery executingQuery()
    {
        return executingQuery;
    }

    @Override
    public ReadOperations readOperations()
    {
        return statement.readOperations();
    }

    @Override
    public DbmsOperations dbmsOperations()
    {
        return dbmsOperations;
    }

    @Override
    public boolean isTopLevelTx()
    {
        return transaction.transactionType() == KernelTransaction.Type.implicit;
    }

    @Override
    public void close( boolean success )
    {
        if ( isOpen )
        {
            try
            {
                statement.queryRegistration().unregisterExecutingQuery( executingQuery );
                statement.close();

                if ( success )
                {
                    transaction.success();
                }
                else
                {
                    transaction.failure();
                }
                transaction.close();
            }
            finally
            {
                statement = null;
                transaction = null;
                isOpen = false;
            }
        }
    }

    @Override // TODO: Make the state of this class a state machine that is a single value and maybe CAS state
    // transitions
    public void terminate()
    {
        if ( isOpen )
        {
            try
            {
                transaction.terminate();
                close( false );
            }
            catch ( NotInTransactionException e )
            {
                // Ok then. Nothing to do
            }
        }
    }

    @Override
    public void commitAndRestartTx()
    {
       /*
        * This method is use by the Cypher runtime to cater for PERIODIC COMMIT, which allows a single query to
        * periodically, after x number of rows, to commit a transaction and spawn a new one.
        *
        * To still keep track of the running stream after switching transactions, we need to open the new transaction
        * before closing the old one. This way, a query will not disappear and appear when switching transactions.
        *
        * Since our transactions are thread bound, we must first unbind the old transaction from the thread before
        * creating a new one. And then we need to do that thread switching again to close the old transaction.
        */

        // (1) Unbind current transaction
        QueryRegistryOperations oldQueryRegistryOperations = statement.queryRegistration();
        InternalTransaction oldTransaction = transaction;
        KernelTransaction oldKernelTx = txBridge.getKernelTransactionBoundToThisThread( true );
        txBridge.unbindTransactionFromCurrentThread();

        // (2) Create, bind, register, and unbind new transaction
        transaction = graph.beginTransaction( transactionType, mode );
        statement = txBridge.get();
        statement.queryRegistration().registerExecutingQuery( executingQuery );
        KernelTransaction kernelTx = txBridge.getKernelTransactionBoundToThisThread( true );
        txBridge.unbindTransactionFromCurrentThread();

        // (3) Rebind old transaction just to commit and close it (and unregister as a side effect of that)
        txBridge.bindTransactionToCurrentThread( oldKernelTx );
        oldQueryRegistryOperations.unregisterExecutingQuery( executingQuery );
        try
        {
            oldTransaction.success();
            oldTransaction.close();
        }
        catch ( Throwable t )
        {
            // Corner case: The old transaction might have been terminated by the user. Now we also need to
            // terminate the new transaction.
            txBridge.bindTransactionToCurrentThread( kernelTx );
            transaction.failure();
            transaction.close();
            txBridge.unbindTransactionFromCurrentThread();
            throw t;
        }

        // (4) Unbind the now closed old transaction and rebind the new transaction for continued execution
        txBridge.unbindTransactionFromCurrentThread();
        txBridge.bindTransactionToCurrentThread( kernelTx );
    }

    @Override
    public void cleanForReuse()
    {
        // close the old statement reference after the statement has been "upgraded"
        // to either a schema data or a schema statement, so that the locks are "handed over".
        statement.queryRegistration().unregisterExecutingQuery( executingQuery );
        statement.close();
        statement = txBridge.get();
        statement.queryRegistration().registerExecutingQuery( executingQuery );
    }

    @Override
    public TransactionalContext getOrBeginNewIfClosed()
    {
        if ( !isOpen )
        {
            transaction = graph.beginTransaction( transactionType, mode );
            statement = statementSupplier.get();
            statement.queryRegistration().registerExecutingQuery( executingQuery );
            isOpen = true;
        }
        return this;
    }

    @Override
    public boolean isOpen()
    {
        return isOpen;
    }

    @Override
    public GraphDatabaseQueryService graph()
    {
        return graph;
    }

    @Override
    public Statement statement()
    {
        return statement;
    }

    @Override
    public void check()
    {
        guard.check( (KernelStatement) statement );
    }

    @Override
    public TxStateHolder stateView()
    {
        return (KernelStatement) statement;
    }

    @Override
    public Lock acquireWriteLock( PropertyContainer p )
    {
        return locker.exclusiveLock( statement, p );
    }

    @Override
    public KernelTransaction.Revertable restrictCurrentTransaction( AccessMode accessMode )
    {
        return transaction.overrideWith( accessMode );
    }

    @Override
    public AccessMode accessMode()
    {
        return mode;
    }
}
