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

import static org.neo4j.configuration.GraphDatabaseInternalSettings.multi_version_transaction_visibility_refresh;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.multi_version_transaction_visibility_refresh_all;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.snapshot_query_retries;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.system_snapshot_query_retries;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.result.FailedExecutionResult;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;

final class MultiVersionQueryRetry {
    private final int maxQueryExecutionAttempts;
    private final Config config;
    private final boolean refreshAllTransactionTypes;

    MultiVersionQueryRetry(Config config, boolean refreshAllTransactionTypes, boolean isSystemDb) {
        this.maxQueryExecutionAttempts =
                isSystemDb ? config.get(system_snapshot_query_retries) : config.get(snapshot_query_retries);
        this.config = config;
        this.refreshAllTransactionTypes = refreshAllTransactionTypes;
    }

    QueryExecution executeWithRetry(TransactionalContext context, QueryExecutor executor)
            throws QueryExecutionKernelException {
        var kernelTransaction = context.kernelTransaction();
        // this must be the first query in the transaction
        if (kernelTransaction.aquireStatementCounter() > 1) {
            return executor.execute();
        }

        int attempts = 0;
        boolean allowRefresh = config.get(multi_version_transaction_visibility_refresh);
        boolean allowRefreshAllTypes =
                refreshAllTransactionTypes || config.get(multi_version_transaction_visibility_refresh_all);
        QueryExecution result;
        VersionContext versionContext = getCursorContext(context);
        do {
            if (attempts > 0) {
                context.executingQuery().onRetryAttempted();
                kernelTransaction.retryQuery();
                versionContext.initRead();
                versionContext.resetObsoleteHeadState();

                kernelTransaction.reportVisibilityBoundaryRefresh();
            }

            result = executor.execute();
            if (result instanceof FailedExecutionResult) {
                // execution failed and the error was already delivered to the subscriber;
                // retrying cannot change a genuine error and the subscriber cannot be driven twice
                break;
            }
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

            if (!allowRefresh) {
                break;
            }

            if (kernelTransaction.clientInfo().isEmbedded() && !allowRefreshAllTypes) {
                break;
            }

        } while (attempts++ < maxQueryExecutionAttempts);
        return result;
    }

    private static VersionContext getCursorContext(TransactionalContext context) {
        return context.kernelTransaction().cursorContext().getVersionContext();
    }

    @FunctionalInterface
    interface QueryExecutor {
        QueryExecution execute() throws QueryExecutionKernelException;
    }
}
