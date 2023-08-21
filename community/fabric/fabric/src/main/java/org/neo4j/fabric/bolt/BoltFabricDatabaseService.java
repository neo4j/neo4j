/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.bolt;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerImpl;
import org.neo4j.fabric.bootstrap.TestOverrides;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.virtual.MapValue;

public class BoltFabricDatabaseService implements BoltGraphDatabaseServiceSPI {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(BoltFabricDatabaseService.class);
    private static final long BOLT_TRANSACTION_SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(BoltTransactionImpl.class);

    private final FabricExecutor fabricExecutor;
    private final DatabaseReference databaseReference;
    private final FabricConfig config;
    private final TransactionManager transactionManager;
    private final LocalGraphTransactionIdTracker transactionIdTracker;
    private final MemoryTracker memoryTracker;

    public BoltFabricDatabaseService(
            DatabaseReference databaseReference,
            FabricExecutor fabricExecutor,
            FabricConfig config,
            TransactionManager transactionManager,
            LocalGraphTransactionIdTracker transactionIdTracker,
            MemoryTracker memoryTracker) {
        this.databaseReference = databaseReference;
        this.config = config;
        this.transactionManager = transactionManager;
        this.fabricExecutor = fabricExecutor;
        this.transactionIdTracker = transactionIdTracker;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public BoltTransaction beginTransaction(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo clientInfo,
            List<String> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            RoutingContext routingContext,
            QueryExecutionConfiguration queryExecutionConfiguration) {
        memoryTracker.allocateHeap(BOLT_TRANSACTION_SHALLOW_SIZE);

        if (txTimeout == null) {
            txTimeout = config.getTransactionTimeout();
        }

        FabricTransactionInfo transactionInfo = new FabricTransactionInfo(
                accessMode,
                loginContext,
                clientInfo,
                databaseReference,
                KernelTransaction.Type.IMPLICIT == type,
                txTimeout,
                txMetadata,
                TestOverrides.routingContext(routingContext),
                queryExecutionConfiguration);

        var parsedBookmarks = BookmarkFormat.parse(bookmarks);
        var transactionBookmarkManager = new TransactionBookmarkManagerImpl(parsedBookmarks);
        // regardless of what we do, System graph must be always up to date
        transactionBookmarkManager
                .getBookmarkForLocalSystemDatabase()
                .ifPresent(
                        localBookmark -> transactionIdTracker.awaitSystemGraphUpToDate(localBookmark.transactionId()));

        FabricTransaction fabricTransaction = transactionManager.begin(transactionInfo, transactionBookmarkManager);
        return new BoltTransactionImpl(transactionInfo, fabricTransaction);
    }

    @Override
    public DatabaseReference getDatabaseReference() {
        return databaseReference;
    }

    public class BoltTransactionImpl implements BoltTransaction {

        private final FabricTransaction fabricTransaction;

        BoltTransactionImpl(FabricTransactionInfo transactionInfo, FabricTransaction fabricTransaction) {
            this.fabricTransaction = fabricTransaction;
        }

        @Override
        public void commit() {
            fabricTransaction.commit();
        }

        @Override
        public void rollback() {
            fabricTransaction.rollback();
        }

        @Override
        public void close() {}

        @Override
        public void markForTermination(Status reason) {
            fabricTransaction.markForTermination(reason);
        }

        @Override
        public void markForTermination() {
            fabricTransaction.markForTermination(Terminated);
        }

        @Override
        public Optional<Status> getReasonIfTerminated() {
            return fabricTransaction.getReasonIfTerminated();
        }

        @Override
        public String getBookmark() {
            QueryRouterBookmark bookmark =
                    fabricTransaction.getBookmarkManager().constructFinalBookmark();
            return BookmarkFormat.serialize(bookmark);
        }

        @Override
        public BoltQueryExecution executeQuery(
                String query, MapValue parameters, boolean prePopulate, QuerySubscriber subscriber) {
            StatementResult statementResult = fabricExecutor.run(fabricTransaction, query, parameters);
            final BoltQueryExecutionImpl queryExecution =
                    new BoltQueryExecutionImpl(statementResult, subscriber, config);
            try {
                queryExecution.initialize();
            } catch (Exception e) {
                QuerySubscriber.safelyOnError(subscriber, e);
            }
            return queryExecution;
        }

        /**
         * This is a hack to be able to get an InternalTransaction for the TestFabricTransaction tx wrapper
         */
        @Deprecated
        public FabricTransaction getFabricTransaction() {
            return fabricTransaction;
        }
    }
}
