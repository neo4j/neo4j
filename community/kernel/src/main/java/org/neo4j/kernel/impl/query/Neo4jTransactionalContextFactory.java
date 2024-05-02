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
package org.neo4j.kernel.impl.query;

import static org.neo4j.function.Suppliers.lazySingleton;

import java.util.function.Supplier;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.values.virtual.MapValue;

public class Neo4jTransactionalContextFactory implements TransactionalContextFactory {
    private final Neo4jTransactionalContext.Creator contextCreator;

    public static TransactionalContextFactory create(
            Supplier<GraphDatabaseQueryService> queryServiceSupplier,
            KernelTransactionFactory transactionFactory,
            TransactionalContext.DatabaseMode dbMode) {
        Supplier<GraphDatabaseQueryService> queryService = lazySingleton(queryServiceSupplier);
        Neo4jTransactionalContext.Creator contextCreator =
                (tx, initialStatement, executingQuery, queryExecutionConfiguration) -> new Neo4jTransactionalContext(
                        queryService.get(),
                        tx,
                        initialStatement,
                        executingQuery,
                        transactionFactory,
                        queryExecutionConfiguration,
                        dbMode);
        return new Neo4jTransactionalContextFactory(contextCreator);
    }

    public static TransactionalContextFactory create(
            Supplier<GraphDatabaseQueryService> queryServiceSupplier, KernelTransactionFactory transactionFactory) {
        Supplier<GraphDatabaseQueryService> queryService = lazySingleton(queryServiceSupplier);
        Neo4jTransactionalContext.Creator contextCreator =
                (tx, initialStatement, executingQuery, queryExecutionConfiguration) -> new Neo4jTransactionalContext(
                        queryService.get(),
                        tx,
                        initialStatement,
                        executingQuery,
                        transactionFactory,
                        queryExecutionConfiguration);
        return new Neo4jTransactionalContextFactory(contextCreator);
    }

    @Deprecated
    public static TransactionalContextFactory create(GraphDatabaseQueryService queryService) {
        var resolver = queryService.getDependencyResolver();
        var transactionFactory = resolver.resolveDependency(KernelTransactionFactory.class);
        Neo4jTransactionalContext.Creator contextCreator =
                (tx, initialStatement, executingQuery, queryExecutionConfiguration) -> new Neo4jTransactionalContext(
                        queryService,
                        tx,
                        initialStatement,
                        executingQuery,
                        transactionFactory,
                        queryExecutionConfiguration);
        return new Neo4jTransactionalContextFactory(contextCreator);
    }

    // Please use the factory methods above to actually construct an instance
    private Neo4jTransactionalContextFactory(Neo4jTransactionalContext.Creator contextCreator) {
        this.contextCreator = contextCreator;
    }

    @Override
    public TransactionalContext newContext(
            InternalTransaction tx,
            String queryText,
            ExecutingQuery parentQuery,
            MapValue queryParameters,
            QueryExecutionConfiguration queryExecutionConfiguration) {
        KernelStatement initialStatement =
                (KernelStatement) tx.kernelTransaction().acquireStatement();
        var executingQuery = initialStatement.queryRegistry().startAndBindExecutingQuery(queryText, queryParameters);
        executingQuery.setParentTransaction(parentQuery.databaseId().get().name(), parentQuery.getOuterTransactionId());
        return contextCreator.create(tx, initialStatement, executingQuery, queryExecutionConfiguration);
    }

    @Override
    public final Neo4jTransactionalContext newContext(
            InternalTransaction tx,
            String queryText,
            MapValue queryParameters,
            QueryExecutionConfiguration queryExecutionConfiguration) {
        KernelStatement initialStatement =
                (KernelStatement) tx.kernelTransaction().acquireStatement();
        var executingQuery = initialStatement.queryRegistry().startAndBindExecutingQuery(queryText, queryParameters);
        return contextCreator.create(tx, initialStatement, executingQuery, queryExecutionConfiguration);
    }

    @Override
    public TransactionalContext newContextForQuery(
            InternalTransaction tx,
            ExecutingQuery executingQuery,
            QueryExecutionConfiguration queryExecutionConfiguration) {
        KernelStatement initialStatement =
                (KernelStatement) tx.kernelTransaction().acquireStatement();
        initialStatement.queryRegistry().bindExecutingQuery(executingQuery);
        return contextCreator.create(tx, initialStatement, executingQuery, queryExecutionConfiguration);
    }

    @Override
    public TransactionalContext newContextForQuery(
            InternalTransaction tx,
            ExecutingQuery executingQuery,
            QueryExecutionConfiguration queryExecutionConfiguration,
            ConstituentTransactionFactory
                    constituentTransactionFactory // Ignore as you only have constituents on composite databases.
            ) {
        return newContextForQuery(tx, executingQuery, queryExecutionConfiguration);
    }
}
