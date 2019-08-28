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
package org.neo4j.kernel.impl.query;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryRegistry;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;
import org.neo4j.values.ValueMapper;

public class Neo4jTransactionalContext implements TransactionalContext
{
    private final GraphDatabaseQueryService graph;
    private final ThreadToStatementContextBridge txBridge;

    public final KernelTransaction.Type transactionType;
    public final SecurityContext securityContext;
    private final ExecutingQuery executingQuery;
    private final ClientConnectionInfo clientInfo;
    private final DatabaseId databaseId;

    private final InternalTransaction transaction;
    private KernelTransaction kernelTransaction;
    private Statement statement;
    private final ValueMapper<Object> valueMapper;
    private final KernelTransactionFactory transactionFactory;
    private volatile boolean isOpen = true;

    private long pageHits;
    private long pageMisses;

    public Neo4jTransactionalContext( GraphDatabaseQueryService graph, ThreadToStatementContextBridge txBridge, InternalTransaction initialTransaction,
            Statement initialStatement, ExecutingQuery executingQuery, ValueMapper<Object> valueMapper, KernelTransactionFactory transactionFactory )
    {
        this.graph = graph;
        this.txBridge = txBridge;
        this.transactionType = initialTransaction.transactionType();
        this.securityContext = initialTransaction.securityContext();
        this.clientInfo = initialTransaction.clientInfo();
        this.executingQuery = executingQuery;

        this.transaction = initialTransaction;
        this.databaseId = executingQuery.databaseId();
        this.kernelTransaction = txBridge.getKernelTransactionBoundToThisThread( true, databaseId );
        this.statement = initialStatement;
        this.valueMapper = valueMapper;
        this.transactionFactory = transactionFactory;
    }

    @Override
    public ValueMapper valueMapper()
    {
        return valueMapper;
    }

    @Override
    public ExecutingQuery executingQuery()
    {
        return executingQuery;
    }

    @Override
    public DbmsOperations dbmsOperations()
    {
        return graph.getDbmsOperations();
    }

    @Override
    public KernelTransaction kernelTransaction()
    {
        return kernelTransaction;
    }

    @Override
    public InternalTransaction transaction()
    {
        return transaction;
    }

    @Override
    public boolean isTopLevelTx()
    {
        return transaction.transactionType() == KernelTransaction.Type.implicit;
    }

    @Override
    public void close()
    {
        if ( isOpen )
        {
            try
            {
                statement.queryRegistration().unregisterExecutingQuery( executingQuery );
                statement.close();
            }
            finally
            {
                statement = null;
                isOpen = false;
            }
        }
    }

    @Override
    public void terminate()
    {
        if ( isOpen )
        {
            transaction.terminate();
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

        checkNotTerminated();

        collectTransactionExecutionStatistic();

        // (1) Unbind current transaction
        QueryRegistry oldQueryRegistry = statement.queryRegistration();
        Statement oldStatement = statement;
        KernelTransaction oldKernelTx = txBridge.getKernelTransactionBoundToThisThread( true, databaseId );
        txBridge.unbindTransactionFromCurrentThread();

        // (2) Create, bind, register, and unbind new transaction
        kernelTransaction = transactionFactory.beginKernelTransaction( transactionType, securityContext, clientInfo );
        statement = kernelTransaction.acquireStatement();
        statement.queryRegistration().registerExecutingQuery( executingQuery );
        txBridge.unbindTransactionFromCurrentThread();

        // (3) Rebind old transaction just to commit and close it (and unregister as a side effect of that)
        txBridge.bindTransactionToCurrentThread( oldKernelTx );
        oldQueryRegistry.unregisterExecutingQuery( executingQuery );
        try
        {
            oldStatement.close();
            oldKernelTx.commit();
        }
        catch ( Throwable t )
        {
            // Corner case: The old transaction might have been terminated by the user. Now we also need to
            // terminate the new transaction.
            txBridge.bindTransactionToCurrentThread( kernelTransaction );
            transaction.rollback();
            txBridge.unbindTransactionFromCurrentThread();
            throw new RuntimeException( t );
        }

        // (4) Unbind the now closed old transaction and rebind the new transaction for continued execution
        txBridge.unbindTransactionFromCurrentThread();
        txBridge.bindTransactionToCurrentThread( kernelTransaction );
        transaction.setTransaction( kernelTransaction );
    }

    @Override
    public void cleanForReuse()
    {
        // close the old statement reference after the statement has been "upgraded"
        // to either a schema data or a schema statement, so that the locks are "handed over".
        statement.queryRegistration().unregisterExecutingQuery( executingQuery );
        statement.close();
        statement = txBridge.get( databaseId );
        statement.queryRegistration().registerExecutingQuery( executingQuery );
    }

    @Override
    public TransactionalContext getOrBeginNewIfClosed()
    {
        checkNotTerminated();

        if ( !isOpen )
        {
            kernelTransaction = txBridge.getKernelTransactionBoundToThisThread( true, databaseId );
            statement = kernelTransaction.acquireStatement();
            statement.queryRegistration().registerExecutingQuery( executingQuery );
            isOpen = true;
        }
        return this;
    }

    private void checkNotTerminated()
    {
        if ( transaction != null )
        {
            transaction.terminationReason().ifPresent( status ->
            {
                throw new TransactionTerminatedException( status );
            } );
        }
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
        kernelTransaction().assertOpen();
    }

    @Override
    public KernelTransaction.Revertable restrictCurrentTransaction( SecurityContext context )
    {
        return transaction.overrideWith( context );
    }

    @Override
    public SecurityContext securityContext()
    {
        return securityContext;
    }

    @Override
    public ResourceTracker resourceTracker()
    {
        // We use the current statement as resourceTracker since it is attached to the KernelTransaction
        // and is guaranteed to be cleaned up on transaction failure.
        return statement;
    }

    @Override
    public StatisticProvider kernelStatisticProvider()
    {
        return new TransactionalContextStatisticProvider( kernelTransaction().executionStatistics() );
    }

    private void collectTransactionExecutionStatistic()
    {
        ExecutionStatistics stats = kernelTransaction().executionStatistics();
        pageHits += stats.pageHits();
        pageMisses += stats.pageFaults();
    }

    interface Creator
    {
        Neo4jTransactionalContext create(
                InternalTransaction tx,
                Statement initialStatement,
                ExecutingQuery executingQuery
        );
    }

    private class TransactionalContextStatisticProvider implements StatisticProvider
    {
        private final ExecutionStatistics executionStatistics;

        private TransactionalContextStatisticProvider( ExecutionStatistics executionStatistics )
        {
            this.executionStatistics = executionStatistics;
        }

        @Override
        public long getPageCacheHits()
        {
            return executionStatistics.pageHits() + pageHits;
        }

        @Override
        public long getPageCacheMisses()
        {
            return executionStatistics.pageFaults() + pageMisses;
        }
    }
}
