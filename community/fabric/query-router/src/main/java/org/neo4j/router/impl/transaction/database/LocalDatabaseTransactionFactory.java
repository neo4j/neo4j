/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.router.impl.transaction.database;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.router.transaction.DatabaseTransaction;
import org.neo4j.router.transaction.DatabaseTransactionFactory;
import org.neo4j.router.transaction.TransactionInfo;

public class LocalDatabaseTransactionFactory implements DatabaseTransactionFactory<Location.Local> {
    private final DatabaseContextProvider<?> databaseContextProvider;

    public LocalDatabaseTransactionFactory(DatabaseContextProvider<?> databaseContextProvider) {
        this.databaseContextProvider = databaseContextProvider;
    }

    @Override
    public DatabaseTransaction beginTransaction(
            Location.Local location, TransactionInfo transactionInfo, TransactionBookmarkManager bookmarkManager) {
        var databaseContext = databaseContextProvider
                .getDatabaseContext(location.databaseReference().databaseId())
                .orElseThrow(databaseNotFound(location.getDatabaseName()));

        var databaseApi = databaseContext.databaseFacade();
        var resolver = databaseContext.dependencies();
        var transactionIdTracker =
                databaseContext.dependencies().resolveDependency(LocalGraphTransactionIdTracker.class);

        var queryExecutionEngine = resolver.resolveDependency(QueryExecutionEngine.class);

        var transactionalContextFactory = Neo4jTransactionalContextFactory.create(
                resolver.provideDependency(GraphDatabaseQueryService.class),
                resolver.resolveDependency(KernelTransactionFactory.class));

        bookmarkManager
                .getBookmarkForLocal(location)
                .ifPresent(bookmark -> transactionIdTracker.awaitGraphUpToDate(location, bookmark.transactionId()));

        InternalTransaction internalTransaction = beginInternalTransaction(databaseApi, transactionInfo);

        return new LocalDatabaseTransaction(
                location,
                transactionInfo,
                internalTransaction,
                transactionalContextFactory,
                queryExecutionEngine,
                bookmarkManager,
                transactionIdTracker);
    }

    private InternalTransaction beginInternalTransaction(
            GraphDatabaseAPI databaseApi, TransactionInfo transactionInfo) {

        InternalTransaction internalTransaction = databaseApi.beginTransaction(
                transactionInfo.type(),
                transactionInfo.loginContext(),
                transactionInfo.clientInfo(),
                transactionInfo.txTimeout().toMillis(),
                TimeUnit.MILLISECONDS);

        internalTransaction.setMetaData(transactionInfo.txMetadata());

        return internalTransaction;
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound(String databaseNameRaw) {
        return () -> new DatabaseNotFoundException("Database " + databaseNameRaw + " not found");
    }
}
