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
package org.neo4j.fabric.executor;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bookmark.LocalBookmark;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.QueryStatementLifecycles.StatementLifecycle;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.fabric.transaction.parent.CompoundTransaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.virtual.MapValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FabricLocalExecutor {
    private final FabricConfig config;
    private final FabricDatabaseManager dbms;
    private final LocalGraphTransactionIdTracker transactionIdTracker;

    public FabricLocalExecutor(
            FabricConfig config, FabricDatabaseManager dbms, LocalGraphTransactionIdTracker transactionIdTracker) {
        this.config = config;
        this.dbms = dbms;
        this.transactionIdTracker = transactionIdTracker;
    }

    public LocalTransactionContext startTransactionContext(
            CompoundTransaction<SingleDbTransaction> parentTransaction,
            FabricTransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager) {
        return new LocalTransactionContext(parentTransaction, transactionInfo, bookmarkManager);
    }

    public class LocalTransactionContext implements AutoCloseable {
        private final Map<UUID, KernelTxWrapper> kernelTransactions = new ConcurrentHashMap<>();
        private final Set<InternalTransaction> internalTransactions = ConcurrentHashMap.newKeySet();

        private final CompoundTransaction<SingleDbTransaction> parentTransaction;
        private final FabricTransactionInfo transactionInfo;
        private final TransactionBookmarkManager bookmarkManager;

        private LocalTransactionContext(
                CompoundTransaction<SingleDbTransaction> parentTransaction,
                FabricTransactionInfo transactionInfo,
                TransactionBookmarkManager bookmarkManager) {
            this.parentTransaction = parentTransaction;
            this.transactionInfo = transactionInfo;
            this.bookmarkManager = bookmarkManager;
        }

        public StatementResult run(
                Location.Local location,
                TransactionMode transactionMode,
                StatementLifecycle parentLifecycle,
                FullyParsedQuery query,
                MapValue params,
                Flux<Record> input,
                ExecutionOptions executionOptions,
                Boolean targetsComposite) {
            var kernelTransaction = getOrCreateTx(location, transactionMode, targetsComposite);
            return kernelTransaction.run(query, params, input, parentLifecycle, executionOptions);
        }

        public StatementResult runInAutocommitTransaction(
                Location.Local location,
                StatementLifecycle parentLifecycle,
                FullyParsedQuery query,
                MapValue params,
                Flux<Record> input,
                ExecutionOptions executionOptions) {
            var databaseFacade = getDatabaseFacade(location);
            var kernelTransaction = beginKernelTx(databaseFacade);

            var driverResult = kernelTransaction.run(query, params, input, parentLifecycle, executionOptions);
            var result = new AutocommitLocalStatementResult(
                    driverResult, kernelTransaction, bookmarkManager, transactionIdTracker, location);
            parentTransaction.registerAutocommitQuery(result);
            return result;
        }

        @Override
        public void close() {}

        public Set<InternalTransaction> getInternalTransactions() {
            return internalTransactions;
        }

        public FabricKernelTransaction getOrCreateTx(
                Location.Local location, TransactionMode transactionMode, Boolean targetsComposite) {
            var existingTx = kernelTransactions.get(location.getUuid());
            if (!targetsComposite && existingTx != null) {
                maybeUpgradeToWritingTransaction(existingTx, transactionMode);
                return existingTx.fabricKernelTransaction;
            }

            // it is important to try to get the facade before handling bookmarks
            // Unlike the bookmark logic, this will fail gracefully if the database is not available
            var databaseFacade = getDatabaseFacade(location);

            bookmarkManager
                    .getBookmarkForLocal(location)
                    .ifPresent(bookmark -> transactionIdTracker.awaitGraphUpToDate(location, bookmark.transactionId()));
            return kernelTransactions.computeIfAbsent(
                            location.getUuid(),
                            dbUuid -> parentTransaction.registerNewChildTransaction(location, transactionMode, () -> {
                                var tx = beginKernelTx(databaseFacade);
                                return new KernelTxWrapper(tx, bookmarkManager, location);
                            }))
                    .fabricKernelTransaction;
        }

        private void maybeUpgradeToWritingTransaction(KernelTxWrapper tx, TransactionMode transactionMode) {
            if (transactionMode == TransactionMode.DEFINITELY_WRITE) {
                parentTransaction.upgradeToWritingTransaction(tx);
            }
        }

        private FabricKernelTransaction beginKernelTx(GraphDatabaseAPI databaseFacade) {
            var dependencyResolver = databaseFacade.getDependencyResolver();
            var executionEngine = dependencyResolver.resolveDependency(ExecutionEngine.class);

            var internalTransaction = beginInternalTransaction(databaseFacade, transactionInfo);

            var transactionalContextFactory = dependencyResolver.resolveDependency(TransactionalContextFactory.class);

            return new FabricKernelTransaction(
                    executionEngine, transactionalContextFactory, internalTransaction, config, transactionInfo);
        }

        private GraphDatabaseAPI getDatabaseFacade(Location.Local location) {
            try {
                var facade = dbms.getDatabaseFacade(location.getDatabaseName());
                if (!Objects.equals(facade.databaseId().databaseId().uuid(), location.getUuid())) {
                    throw new FabricException(
                            Status.Transaction.Outdated,
                            "The locations associated with the graph name %s have "
                                    + "changed whilst the transaction was running.",
                            location.getDatabaseName());
                }
                return facade;
            } catch (UnavailableException e) {
                throw new FabricException(Status.General.DatabaseUnavailable, e);
            }
        }

        private InternalTransaction beginInternalTransaction(
                GraphDatabaseAPI databaseAPI, FabricTransactionInfo transactionInfo) {
            KernelTransaction.Type kernelTransactionType = getKernelTransactionType(transactionInfo);

            InternalTransaction internalTransaction = databaseAPI.beginTransaction(
                    kernelTransactionType,
                    transactionInfo.getLoginContext(),
                    transactionInfo.getClientConnectionInfo(),
                    transactionInfo.getTxTimeout().toMillis(),
                    TimeUnit.MILLISECONDS,
                    parentTransaction::childTransactionTerminated,
                    this::transformTerminalOperationError);

            if (transactionInfo.getTxMetadata() != null) {
                internalTransaction.setMetaData(transactionInfo.getTxMetadata());
            }

            internalTransactions.add(internalTransaction);

            return internalTransaction;
        }

        private KernelTransaction.Type getKernelTransactionType(FabricTransactionInfo fabricTransactionInfo) {
            if (fabricTransactionInfo.isImplicitTransaction()) {
                return KernelTransaction.Type.IMPLICIT;
            }

            return KernelTransaction.Type.EXPLICIT;
        }

        private RuntimeException transformTerminalOperationError(Exception e) {
            // The main purpose of this is mapping of checked exceptions
            // while preserving status codes
            if (e instanceof Status.HasStatus) {
                if (e instanceof RuntimeException) {
                    return (RuntimeException) e;
                }
                return new FabricException(((Status.HasStatus) e).status(), e.getMessage(), e);
            }

            // We don't know what operation is being executed,
            // so it is not possible to come up with a reasonable status code here.
            // The error is wrapped into a generic one
            // and a proper status code will be added later.
            throw new TransactionFailureException("Unable to complete transaction.", e, Status.General.UnknownError);
        }
    }

    private class KernelTxWrapper implements SingleDbTransaction {

        private final FabricKernelTransaction fabricKernelTransaction;
        private final TransactionBookmarkManager bookmarkManager;
        private final Location.Local location;

        KernelTxWrapper(
                FabricKernelTransaction fabricKernelTransaction,
                TransactionBookmarkManager bookmarkManager,
                Location.Local location) {
            this.fabricKernelTransaction = fabricKernelTransaction;
            this.bookmarkManager = bookmarkManager;
            this.location = location;
        }

        @Override
        public Mono<Void> commit() {
            return Mono.fromRunnable(this::doCommit);
        }

        @Override
        public Mono<Void> rollback() {
            return Mono.fromRunnable(this::doRollback);
        }

        private void doCommit() {
            fabricKernelTransaction.commit();
            long transactionId = transactionIdTracker.getTransactionId(location);
            bookmarkManager.localTransactionCommitted(location, new LocalBookmark(transactionId));
        }

        private void doRollback() {
            fabricKernelTransaction.rollback();
        }

        @Override
        public Mono<Void> terminate(Status reason) {
            return Mono.fromRunnable(() -> fabricKernelTransaction.terminate(reason));
        }

        @Override
        public Location location() {
            return location;
        }
    }
}
