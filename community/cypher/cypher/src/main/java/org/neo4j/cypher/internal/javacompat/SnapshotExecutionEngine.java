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
import org.neo4j.graphdb.Result;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.values.virtual.MapValue;

/**
 * {@link ExecutionEngine} engine that will try to run cypher query with guarantee that query will never see any data
 * that coming from transaction that are newer then transaction that was the last closed on a moment when
 * {@link VersionContext} was initialised. Observed behaviour is the same as executing query on top data snapshot for
 * that version.
 */
public class SnapshotExecutionEngine extends ExecutionEngine {
    private final int maxQueryExecutionAttempts;

    SnapshotExecutionEngine(
            GraphDatabaseQueryService queryService,
            Config config,
            CypherQueryCaches queryCaches,
            InternalLogProvider logProvider,
            CompilerFactory compilerFactory) {
        super(queryService, queryCaches, logProvider, compilerFactory);
        this.maxQueryExecutionAttempts = config.get(GraphDatabaseInternalSettings.snapshot_query_retries);
    }

    @Override
    public Result executeQuery(String query, MapValue parameters, TransactionalContext context, boolean prePopulate)
            throws QueryExecutionKernelException {
        QueryExecutor queryExecutor =
                querySubscriber -> super.executeQuery(query, parameters, context, prePopulate, querySubscriber);
        ResultSubscriber resultSubscriber = new ResultSubscriber(context);
        MaterialisedResult materialisedResult = executeWithRetries(query, context, queryExecutor);
        QueryExecution queryExecution = materialisedResult.stream(resultSubscriber);
        resultSubscriber.init(queryExecution);
        return resultSubscriber;
    }

    @Override
    public QueryExecution executeQuery(
            String query,
            MapValue parameters,
            TransactionalContext context,
            boolean prePopulate,
            QuerySubscriber subscriber)
            throws QueryExecutionKernelException {
        QueryExecutor queryExecutor =
                querySubscriber -> super.executeQuery(query, parameters, context, prePopulate, querySubscriber);
        MaterialisedResult materialisedResult = executeWithRetries(query, context, queryExecutor);
        return materialisedResult.stream(subscriber);
    }

    @Override
    public QueryExecution executeQuery(
            FullyParsedQuery query,
            MapValue parameters,
            TransactionalContext context,
            boolean prePopulate,
            InputDataStream input,
            QueryExecutionMonitor queryMonitor,
            QuerySubscriber subscriber)
            throws QueryExecutionKernelException {
        QueryExecutor queryExecutor = querySubscriber ->
                super.executeQuery(query, parameters, context, prePopulate, input, queryMonitor, querySubscriber);
        MaterialisedResult materialisedResult = executeWithRetries(query.description(), context, queryExecutor);
        return materialisedResult.stream(subscriber);
    }

    protected MaterialisedResult executeWithRetries(String query, TransactionalContext context, QueryExecutor executor)
            throws QueryExecutionKernelException {
        VersionContext versionContext = getCursorContext(context);
        MaterialisedResult materialisedResult;
        int attempt = 0;
        boolean dirtySnapshot;
        do {
            if (attempt == maxQueryExecutionAttempts) {
                throw new QueryExecutionKernelException(new UnstableSnapshotException(
                        "Unable to get clean data snapshot for query '%s' after %d attempts.", query, attempt));
            }

            if (attempt > 0) {
                context.executingQuery().onRetryAttempted();
            }

            attempt++;
            versionContext.initRead();

            materialisedResult = new MaterialisedResult();

            QueryExecution queryExecution = executor.execute(materialisedResult);
            materialisedResult.consumeAll(queryExecution);

            // we always allow indexes/constraints to be created since their population/verification should see the
            // latest data and uniqueness of those schema
            // objects is guaranteed by schema and not by execution engine
            if (context.transaction().terminationReason().isEmpty()
                    && context.transaction().kernelTransaction().isSchemaTransaction()) {
                return materialisedResult;
            }
            dirtySnapshot = versionContext.isDirty();
            if (isUnstableSnapshot(materialisedResult, dirtySnapshot)) {
                throw new QueryExecutionKernelException(new UnstableSnapshotException(
                        "Unable to get clean data snapshot for query '%s' that performs updates.", query, attempt));
            }
        } while (dirtySnapshot);

        return materialisedResult;
    }

    private boolean isUnstableSnapshot(MaterialisedResult materialisedResult, boolean dirtySnapshot) {
        return dirtySnapshot && materialisedResult.getQueryStatistics().containsUpdates();
    }

    private static VersionContext getCursorContext(TransactionalContext context) {
        return context.kernelTransaction().cursorContext().getVersionContext();
    }

    @FunctionalInterface
    protected interface QueryExecutor {
        QueryExecution execute(MaterialisedResult materialisedResult) throws QueryExecutionKernelException;
    }
}
