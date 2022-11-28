/*
 * Copyright (c) "Neo4j"
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

import java.io.Closeable;
import java.util.function.Consumer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.InnerTransactionHandler;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryRegistry;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;
import org.neo4j.values.ElementIdMapper;

public class Neo4jTransactionalContext implements TransactionalContext {
    private final GraphDatabaseQueryService graph;

    public final KernelTransaction.Type transactionType;
    public final SecurityContext securityContext;
    private final ExecutingQuery executingQuery;
    private final ClientConnectionInfo clientInfo;
    private final NamedDatabaseId namedDatabaseId;

    private final InternalTransaction transaction;
    private KernelTransaction kernelTransaction;
    private KernelStatement statement;
    private QueryRegistry queryRegistry;
    private final ElementIdMapper elementIdMapper;
    private long transactionSequenceNumber;
    private final KernelTransactionFactory transactionFactory;

    private final OnCloseCallback onClose;

    private volatile boolean isOpen = true;

    // The statisticProvider behaves different depending on whether we run a "normal" query or a "PERIODIC COMMIT"
    // query.
    // For normal queries we only include page hits/misses of the current transaction.
    // For PERIODIC COMMIT we also need to include page hits/misses of any committed transactions, because a transaction
    // can be committed/restarted at any point, even during a "profiling event".
    private StatisticProvider statisticProvider;

    public Neo4jTransactionalContext(
            GraphDatabaseQueryService graph,
            InternalTransaction transaction,
            KernelStatement initialStatement,
            ExecutingQuery executingQuery,
            KernelTransactionFactory transactionFactory) {
        this(graph, transaction, initialStatement, executingQuery, transactionFactory, null);
    }

    private Neo4jTransactionalContext(
            GraphDatabaseQueryService graph,
            InternalTransaction transaction,
            KernelStatement initialStatement,
            ExecutingQuery executingQuery,
            KernelTransactionFactory transactionFactory,
            OnCloseCallback onClose) {
        this.graph = graph;
        this.transactionType = transaction.transactionType();
        this.securityContext = transaction.securityContext();
        this.clientInfo = transaction.clientInfo();
        this.executingQuery = executingQuery;

        this.transaction = transaction;
        this.namedDatabaseId = initialStatement.namedDatabaseId();
        this.kernelTransaction = transaction.kernelTransaction();
        this.statement = initialStatement;
        this.queryRegistry = statement.queryRegistry();
        this.elementIdMapper = transaction.elementIdMapper();
        this.transactionSequenceNumber = kernelTransaction.getTransactionSequenceNumber();
        this.transactionFactory = transactionFactory;

        this.statisticProvider = new TransactionalContextStatisticProvider(kernelTransaction.executionStatistics());
        this.onClose = onClose;
    }

    @Override
    public ExecutingQuery executingQuery() {
        return executingQuery;
    }

    @Override
    public KernelTransaction kernelTransaction() {
        return kernelTransaction;
    }

    @Override
    public InternalTransaction transaction() {
        return transaction;
    }

    @Override
    public boolean isTopLevelTx() {
        return transaction.transactionType() == KernelTransaction.Type.IMPLICIT;
    }

    @Override
    public void close() {
        if (isOpen) {
            try {
                if (onClose != null) {
                    onClose.close();
                }
                // Unbind the new transaction/statement from the executingQuery
                beforeUnbind();
                queryRegistry.unbindExecutingQuery(executingQuery, transactionSequenceNumber);
                closeStatement();
            } finally {
                statement = null;
                isOpen = false;
            }
        }
    }

    private void closeStatement() {
        if (statement != null) {
            try {
                statement.close();
            } finally {
                statement = null;
            }
        }
    }

    private void beforeUnbind() {
        if (statement != null) {
            queryRegistry.beforeUnbindExecutingQuery(executingQuery, transactionSequenceNumber);
        }
    }

    @Override
    public void commit() {
        safeTxOperation(Transaction::commit);
    }

    private void safeTxOperation(Consumer<InternalTransaction> operation) {
        RuntimeException exception = null;
        try {
            beforeUnbind();
            closeStatement();
            operation.accept(transaction);
        } catch (RuntimeException e) {
            exception = e;
        } finally {
            try {
                close();
            } catch (RuntimeException e) {
                exception = Exceptions.chain(exception, e);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void rollback() {
        safeTxOperation(Transaction::rollback);
    }

    @Override
    public void terminate() {
        if (isOpen) {
            transaction.terminate();
        }
    }

    @Override
    public long commitAndRestartTx() {
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

        // (1) Remember old statement
        QueryRegistry oldQueryRegistry = queryRegistry;
        KernelStatement oldStatement = statement;
        KernelTransaction oldKernelTx = transaction.kernelTransaction();

        // (2) Unregister the old transaction from the executing query
        oldQueryRegistry.beforeUnbindExecutingQuery(executingQuery, transactionSequenceNumber);
        oldQueryRegistry.unbindExecutingQuery(executingQuery, transactionSequenceNumber);

        // (3) Create and register new transaction
        kernelTransaction = transactionFactory.beginKernelTransaction(transactionType, securityContext, clientInfo);
        statement = (KernelStatement) kernelTransaction.acquireStatement();
        queryRegistry = statement.queryRegistry();
        transactionSequenceNumber = kernelTransaction.getTransactionSequenceNumber();
        queryRegistry.bindExecutingQuery(executingQuery);
        transaction.setTransaction(kernelTransaction);

        // (4) Update statistic provider with new kernel transaction
        updatePeriodicCommitStatisticProvider(kernelTransaction);

        // (5) commit old transaction
        try {
            oldStatement.close();
            try (oldKernelTx) {
                return oldKernelTx.commit();
            }
        } catch (Throwable t) {
            // Corner case: The old transaction might have been terminated by the user. Now we also need to
            // terminate the new transaction.
            transaction.rollback();
            throw new RuntimeException(t);
        }
    }

    @Override
    public Neo4jTransactionalContext contextWithNewTransaction() {
        checkNotTerminated();
        if (transactionType != KernelTransaction.Type.IMPLICIT) {
            throw new TransactionFailureException(
                    "A query with 'CALL { ... } IN TRANSACTIONS' can only be executed in an implicit transaction, "
                            + "but tried to execute in an explicit transaction.");
        }

        // Create new InternalTransaction, creates new KernelTransaction
        InternalTransaction newTransaction = null;
        OnCloseCallback onClose = null;
        try {
            newTransaction = graph.beginTransaction(transactionType, securityContext, clientInfo);
            long newTransactionId = newTransaction.kernelTransaction().getTransactionSequenceNumber();
            InnerTransactionHandler innerTransactionHandler = kernelTransaction.getInnerTransactionHandler();
            onClose = () -> innerTransactionHandler.removeInnerTransaction(newTransactionId);
            innerTransactionHandler.registerInnerTransaction(newTransactionId);

            KernelStatement newStatement =
                    (KernelStatement) newTransaction.kernelTransaction().acquireStatement();
            // Bind the new transaction/statement to the executingQuery
            newStatement.queryRegistry().bindExecutingQuery(executingQuery);

            return new Neo4jTransactionalContext(
                    graph, newTransaction, newStatement, executingQuery, transactionFactory, onClose);
        } catch (Throwable outer) {
            try {
                IOUtils.closeAll(onClose, newTransaction);
            } catch (Throwable inner) {
                outer.addSuppressed(inner);
            }
            throw outer;
        }
    }

    @Override
    public TransactionalContext getOrBeginNewIfClosed() {
        checkNotTerminated();

        if (!isOpen) {
            statement = (KernelStatement) kernelTransaction.acquireStatement();
            queryRegistry = statement.queryRegistry();
            queryRegistry.bindExecutingQuery(executingQuery);
            isOpen = true;
        }
        return this;
    }

    private void checkNotTerminated() {
        transaction.terminationReason().ifPresent(status -> {
            throw new TransactionTerminatedException(status);
        });
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public GraphDatabaseQueryService graph() {
        return graph;
    }

    @Override
    public NamedDatabaseId databaseId() {
        return namedDatabaseId;
    }

    @Override
    public Statement statement() {
        return statement;
    }

    @Override
    public KernelTransaction.Revertable restrictCurrentTransaction(SecurityContext context) {
        return transaction.overrideWith(context);
    }

    @Override
    public SecurityContext securityContext() {
        return securityContext;
    }

    @Override
    public ResourceTracker resourceTracker() {
        // We use the current statement as resourceTracker since it is attached to the KernelTransaction
        // and is guaranteed to be cleaned up on transaction failure.
        return statement;
    }

    @Override
    public ElementIdMapper elementIdMapper() {
        return elementIdMapper;
    }

    @Override
    public StatisticProvider kernelStatisticProvider() {
        return statisticProvider;
    }

    /**
     * Set a new statistic provider that captures hits/misses of the new open transaction plus any hits/misses of already committed transactions.
     */
    private void updatePeriodicCommitStatisticProvider(KernelTransaction kernelTransaction) {
        statisticProvider =
                new PeriodicCommitTransactionalContextStatisticProvider(kernelTransaction.executionStatistics());
    }

    @FunctionalInterface
    interface Creator {
        Neo4jTransactionalContext create(
                InternalTransaction tx, KernelStatement initialStatement, ExecutingQuery executingQuery);
    }

    @FunctionalInterface
    private interface OnCloseCallback extends Closeable {
        @Override
        void close();
    }

    /**
     * Provide statistics using only the page hits/misses of the current transaction.
     */
    private static class TransactionalContextStatisticProvider implements StatisticProvider {
        private final ExecutionStatistics executionStatistics;

        private TransactionalContextStatisticProvider(ExecutionStatistics executionStatistics) {
            this.executionStatistics = executionStatistics;
        }

        @Override
        public long getPageCacheHits() {
            return executionStatistics.pageHits();
        }

        @Override
        public long getPageCacheMisses() {
            return executionStatistics.pageFaults();
        }
    }

    /**
     * Provide statistics using the page hits/misses of the current transaction and any already committed transactions.
     */
    private class PeriodicCommitTransactionalContextStatisticProvider implements StatisticProvider {
        private final ExecutionStatistics executionStatistics;

        private PeriodicCommitTransactionalContextStatisticProvider(ExecutionStatistics executionStatistics) {
            this.executionStatistics = executionStatistics;
        }

        @Override
        public long getPageCacheHits() {
            return executionStatistics.pageHits() + executingQuery.pageHitsOfClosedTransactions();
        }

        @Override
        public long getPageCacheMisses() {
            return executionStatistics.pageFaults() + executingQuery.pageFaultsOfClosedTransactions();
        }
    }
}
