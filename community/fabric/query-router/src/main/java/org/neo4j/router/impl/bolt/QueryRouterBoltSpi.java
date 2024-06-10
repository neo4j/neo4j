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
package org.neo4j.router.impl.bolt;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.fabric.bootstrap.TestOverrides;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.router.QueryRouter;
import org.neo4j.router.query.DatabaseReferenceResolver;
import org.neo4j.router.query.Query;
import org.neo4j.router.transaction.RouterTransaction;
import org.neo4j.router.transaction.RouterTransactionContext;
import org.neo4j.router.transaction.TransactionInfo;
import org.neo4j.values.virtual.MapValue;

public class QueryRouterBoltSpi {

    public static class DatabaseManagementService implements BoltGraphDatabaseManagementServiceSPI {

        private final QueryRouter queryRouter;
        private final DatabaseReferenceResolver databaseReferenceResolver;
        private final BoltGraphDatabaseManagementServiceSPI compositeStack;
        private final boolean useQueryRouterForCompositeQueries;

        public DatabaseManagementService(
                QueryRouter queryRouter,
                DatabaseReferenceResolver databaseReferenceResolver,
                BoltGraphDatabaseManagementServiceSPI compositeStack,
                boolean useQueryRouterForCompositeQueries) {
            this.queryRouter = queryRouter;
            this.databaseReferenceResolver = databaseReferenceResolver;
            this.compositeStack = compositeStack;
            this.useQueryRouterForCompositeQueries = useQueryRouterForCompositeQueries;
        }

        @Override
        public BoltGraphDatabaseServiceSPI database(String databaseName, MemoryTracker memoryTracker)
                throws UnavailableException, DatabaseNotFoundException {
            return new Database(
                    databaseName,
                    queryRouter,
                    databaseReferenceResolver,
                    compositeStack.database(databaseName, memoryTracker),
                    useQueryRouterForCompositeQueries);
        }
    }

    private record Database(
            String sessionDatabaseName,
            QueryRouter queryRouter,
            DatabaseReferenceResolver databaseReferenceResolver,
            BoltGraphDatabaseServiceSPI compositeStack,
            boolean useQueryRouterForCompositeQueries)
            implements BoltGraphDatabaseServiceSPI {

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
            var normalizedSessionDatabaseName = new NormalizedDatabaseName(sessionDatabaseName);
            var sessionDatabaseReference =
                    databaseReferenceResolver.resolve(new NormalizedDatabaseName(sessionDatabaseName));

            if (!useQueryRouterForCompositeQueries && sessionDatabaseReference.isComposite()) {
                return compositeStack.beginTransaction(
                        type,
                        loginContext,
                        clientInfo,
                        bookmarks,
                        txTimeout,
                        accessMode,
                        txMetadata,
                        TestOverrides.routingContext(routingContext),
                        queryExecutionConfiguration);
            }
            TransactionInfo transactionInfo = new TransactionInfo(
                    sessionDatabaseReference,
                    type,
                    loginContext,
                    clientInfo,
                    bookmarks,
                    txTimeout,
                    accessMode,
                    txMetadata,
                    TestOverrides.routingContext(routingContext),
                    queryExecutionConfiguration);

            return new Transaction(queryRouter, queryRouter.beginTransaction(transactionInfo));
        }

        @Override
        public DatabaseReference getDatabaseReference() {
            return databaseReferenceResolver.resolve(sessionDatabaseName);
        }
    }

    private static class Transaction implements BoltTransaction {

        private final QueryRouter queryRouter;
        private final RouterTransactionContext routerTransactionContext;

        private final RouterTransaction routerTransaction;

        Transaction(QueryRouter queryRouter, RouterTransactionContext routerTransactionContext) {
            this.queryRouter = queryRouter;
            this.routerTransactionContext = routerTransactionContext;
            this.routerTransaction = routerTransactionContext.routerTransaction();
        }

        @Override
        public BoltQueryExecution executeQuery(
                String query, MapValue parameters, boolean prePopulate, QuerySubscriber subscriber) {
            return new Execution(
                    queryRouter.executeQuery(routerTransactionContext, Query.of(query, parameters), subscriber));
        }

        @Override
        public void commit() {
            routerTransaction.commit();
        }

        @Override
        public void rollback() {
            routerTransaction.rollback();
        }

        @Override
        public void close() {}

        @Override
        public void markForTermination(Status reason) {
            routerTransaction.markForTermination(reason);
        }

        @Override
        public void markForTermination() {
            routerTransaction.markForTermination(Terminated);
        }

        @Override
        public Optional<Status> getReasonIfTerminated() {
            return routerTransaction.getReasonIfTerminated();
        }

        @Override
        public String getBookmark() {
            return BookmarkFormat.serialize(
                    routerTransactionContext.txBookmarkManager().constructFinalBookmark());
        }
    }

    private record Execution(QueryExecution queryExecution) implements BoltQueryExecution {

        @Override
        public void close() {
            queryExecution.cancel();
        }

        @Override
        public void terminate() {
            queryExecution.cancel();
        }
    }
}
