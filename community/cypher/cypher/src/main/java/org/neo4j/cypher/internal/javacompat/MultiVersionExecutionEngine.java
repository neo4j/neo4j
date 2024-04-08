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
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.cache.CypherQueryCaches;
import org.neo4j.cypher.internal.runtime.InputDataStream;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.values.virtual.MapValue;

public class MultiVersionExecutionEngine extends ExecutionEngine {
    private final int maxQueryExecutionAttempts;

    public MultiVersionExecutionEngine(
            GraphDatabaseQueryService queryService,
            Config config,
            CypherQueryCaches queryCaches,
            InternalLogProvider logProvider,
            CompilerFactory compilerFactory) {
        super(queryService, queryCaches, logProvider, compilerFactory);
        this.maxQueryExecutionAttempts = config.get(GraphDatabaseInternalSettings.snapshot_query_retries);
    }

    @Override
    public QueryExecution executeQuery(
            String query,
            MapValue parameters,
            TransactionalContext context,
            boolean prePopulate,
            QuerySubscriber subscriber)
            throws QueryExecutionKernelException {
        return executeWithRetry(context, () -> super.executeQuery(query, parameters, context, prePopulate, subscriber));
    }

    @Override
    public QueryExecution executeQuery(
            String query,
            MapValue parameters,
            TransactionalContext context,
            boolean prePopulate,
            QuerySubscriber subscriber,
            QueryExecutionMonitor monitor)
            throws QueryExecutionKernelException {
        return executeWithRetry(
                context, () -> super.executeQuery(query, parameters, context, prePopulate, subscriber, monitor));
    }

    public QueryExecution executeQuery(
            FullyParsedQuery query,
            MapValue parameters,
            TransactionalContext context,
            boolean prePopulate,
            InputDataStream input,
            QueryExecutionMonitor queryMonitor,
            QuerySubscriber subscriber)
            throws QueryExecutionKernelException {
        return executeWithRetry(
                context,
                () -> super.executeQuery(query, parameters, context, prePopulate, input, queryMonitor, subscriber));
    }

    private QueryExecution executeWithRetry(TransactionalContext context, QueryExecutor executor)
            throws QueryExecutionKernelException {
        //noinspection resource
        var kernelTransaction = context.kernelTransaction();
        // this must be the first query in the transaction
        if (kernelTransaction.aquireStatementCounter() > 1) {
            return executor.execute();
        }

        int attempts = 0;
        QueryExecution result;
        VersionContext versionContext = getCursorContext(context);
        do {
            if (attempts > 0) {
                kernelTransaction.releaseStorageEngineResources();
                ((TxStateHolder) kernelTransaction).txState().reset();
                context.executingQuery().onRetryAttempted();
                versionContext.initRead();
                versionContext.resetObsoleteHeadState();
            }

            result = executor.execute();
            if (!(kernelTransaction instanceof TxStateHolder txStateHolder) || !txStateHolder.hasTxStateWithChanges()) {
                // skip query that does not have transaction state changes
                break;
            }
            if (versionContext.initializedForWrite()) {
                // query appended some chunks, can't retry
                break;
            }
            if (!versionContext.invisibleHeadObserved()) {
                // haven't seen stale data, no need to retry
                break;
            }
        } while (attempts++ < maxQueryExecutionAttempts);
        return result;
    }

    private static VersionContext getCursorContext(TransactionalContext context) {
        //noinspection resource
        return context.kernelTransaction().cursorContext().getVersionContext();
    }

    @FunctionalInterface
    private interface QueryExecutor {
        QueryExecution execute() throws QueryExecutionKernelException;
    }
}
