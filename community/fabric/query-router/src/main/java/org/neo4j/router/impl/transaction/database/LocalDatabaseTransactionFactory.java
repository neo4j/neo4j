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
package org.neo4j.router.impl.transaction.database;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.router.QueryRouterException;
import org.neo4j.router.transaction.DatabaseTransaction;
import org.neo4j.router.transaction.DatabaseTransactionFactory;
import org.neo4j.router.transaction.TransactionInfo;

public class LocalDatabaseTransactionFactory implements DatabaseTransactionFactory<Location.Local> {
    private final DatabaseContextProvider<?> databaseContextProvider;
    private final LocalGraphTransactionIdTracker transactionIdTracker;

    public LocalDatabaseTransactionFactory(
            DatabaseContextProvider<?> databaseContextProvider, LocalGraphTransactionIdTracker transactionIdTracker) {
        this.databaseContextProvider = databaseContextProvider;
        this.transactionIdTracker = transactionIdTracker;
    }

    @Override
    public DatabaseTransaction beginTransaction(
            Location.Local location,
            TransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager,
            Consumer<Status> terminationCallback,
            ConstituentTransactionFactory constituentTransactionFactory) {
        var databaseContext = databaseContextProvider
                .getDatabaseContext(location.databaseReference().databaseId())
                .orElseThrow(databaseNotFound(location.getDatabaseName()));

        var databaseApi = databaseContext.databaseFacade();
        var resolver = databaseContext.dependencies();

        try {
            databaseContext.database().getDatabaseAvailabilityGuard().assertDatabaseAvailable();
        } catch (UnavailableException e) {
            throw new QueryRouterException(e.status(), e);
        }

        var queryExecutionEngine = resolver.resolveDependency(QueryExecutionEngine.class);

        TransactionalContextFactory transactionalContextFactory = getTransactionalContextFactory(location, resolver);

        bookmarkManager
                .getBookmarkForLocal(location)
                .ifPresent(bookmark -> transactionIdTracker.awaitGraphUpToDate(location, bookmark.transactionId()));

        InternalTransaction internalTransaction =
                beginInternalTransaction(databaseApi, transactionInfo, terminationCallback);

        return new LocalDatabaseTransaction(
                location,
                transactionInfo,
                internalTransaction,
                transactionalContextFactory,
                queryExecutionEngine,
                bookmarkManager,
                transactionIdTracker,
                constituentTransactionFactory);
    }

    protected TransactionalContextFactory getTransactionalContextFactory(
            Location.Local location, DependencyResolver resolver) {
        return Neo4jTransactionalContextFactory.create(
                resolver.provideDependency(GraphDatabaseQueryService.class),
                resolver.resolveDependency(KernelTransactionFactory.class));
    }

    private InternalTransaction beginInternalTransaction(
            GraphDatabaseAPI databaseApi, TransactionInfo transactionInfo, Consumer<Status> terminationCallback) {

        InternalTransaction internalTransaction = databaseApi.beginTransaction(
                transactionInfo.type(),
                transactionInfo.loginContext(),
                transactionInfo.clientInfo(),
                transactionInfo.txTimeout().toMillis(),
                TimeUnit.MILLISECONDS,
                terminationCallback,
                this::transformTerminalOperationError);

        internalTransaction.setMetaData(transactionInfo.txMetadata());

        return internalTransaction;
    }

    private RuntimeException transformTerminalOperationError(Exception e) {
        // The main purpose of this is mapping of checked exceptions
        // while preserving status codes
        if (e instanceof Status.HasStatus se) {
            if (e instanceof RuntimeException re) {
                return re;
            }
            return new QueryRouterException(se.status(), e.getMessage(), e);
        }

        // We don't know what operation is being executed,
        // so it is not possible to come up with a reasonable status code here.
        // The error is wrapped into a generic one
        // and a proper status code will be added later.
        throw new TransactionFailureException("Unable to complete transaction.", e);
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound(String databaseNameRaw) {
        return () -> new DatabaseNotFoundException("Database " + databaseNameRaw + " not found");
    }
}
