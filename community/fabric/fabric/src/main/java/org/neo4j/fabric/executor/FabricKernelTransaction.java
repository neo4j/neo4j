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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.cypher.internal.runtime.InputDataStream;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.QueryStatementLifecycles.StatementLifecycle;
import org.neo4j.fabric.stream.InputDataStreamImpl;
import org.neo4j.fabric.stream.QuerySubject;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Rx2SyncStream;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.StatementResults;
import org.neo4j.fabric.stream.StatementResults.SubscribableExecution;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.values.virtual.MapValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FabricKernelTransaction {
    private final ExecutionEngine queryExecutionEngine;
    private final TransactionalContextFactory transactionalContextFactory;
    private final InternalTransaction internalTransaction;
    private final FabricConfig config;
    private final Set<TransactionalContext> openExecutionContexts = ConcurrentHashMap.newKeySet();

    private final FabricTransactionInfo transactionInfo;

    FabricKernelTransaction(
            ExecutionEngine queryExecutionEngine,
            TransactionalContextFactory transactionalContextFactory,
            InternalTransaction internalTransaction,
            FabricConfig config,
            FabricTransactionInfo transactionInfo) {
        this.queryExecutionEngine = queryExecutionEngine;
        this.transactionalContextFactory = transactionalContextFactory;
        this.internalTransaction = internalTransaction;
        this.config = config;
        this.transactionInfo = transactionInfo;
    }

    public StatementResult run(
            FullyParsedQuery query,
            MapValue params,
            Flux<Record> input,
            StatementLifecycle parentLifecycle,
            ExecutionOptions executionOptions) {
        var childExecutionContext = makeChildTransactionalContext(parentLifecycle);
        parentLifecycle.startExecution(true);
        var childQueryMonitor = parentLifecycle.getChildQueryMonitor();
        openExecutionContexts.add(childExecutionContext);

        SubscribableExecution execution =
                execute(query, params, childExecutionContext, convert(input), childQueryMonitor);

        QuerySubject subject = executionOptions.addSourceTag()
                ? new QuerySubject.CompositeQuerySubject(executionOptions.sourceId())
                : new QuerySubject.BasicQuerySubject();

        StatementResult result = StatementResults.connectVia(execution, subject);
        return new ContextClosingResultInterceptor(result, childExecutionContext);
    }

    private SubscribableExecution execute(
            FullyParsedQuery query,
            MapValue params,
            TransactionalContext executionContext,
            InputDataStream input,
            QueryExecutionMonitor queryMonitor) {
        return subscriber -> {
            try {
                return queryExecutionEngine.executeQuery(
                        query, params, executionContext, true, input, queryMonitor, subscriber);
            } catch (QueryExecutionKernelException e) {
                // all exception thrown from execution engine are wrapped in QueryExecutionKernelException,
                // let's see if there is something better hidden in it
                Throwable cause = e.getCause() == null ? e : e.getCause();
                throw Exceptions.transform(
                        Status.Statement.ExecutionFailed,
                        cause,
                        executionContext.executingQuery().internalQueryId());
            }
        };
    }

    private TransactionalContext makeChildTransactionalContext(StatementLifecycle lifecycle) {
        var parentQuery = lifecycle.getMonitoredQuery();
        var queryExecutionConfiguration = transactionInfo.getQueryExecutionConfiguration();

        if (lifecycle.isParentChildMonitoringMode()) {
            // Cypher engine reports separately for each child query
            String queryText = "Internal query for parent query id: " + parentQuery.id();
            MapValue params = MapValue.EMPTY;
            return transactionalContextFactory.newContext(
                    internalTransaction, queryText, parentQuery, params, queryExecutionConfiguration);
        } else {
            // Cypher engine reports directly to parent query
            return transactionalContextFactory.newContextForQuery(
                    internalTransaction, parentQuery, queryExecutionConfiguration);
        }
    }

    private InputDataStream convert(Flux<Record> input) {
        return new InputDataStreamImpl(
                new Rx2SyncStream(input, config.getDataStream().getBatchSize()));
    }

    public void commit() {
        synchronized (internalTransaction) {
            if (internalTransaction.isOpen()) {
                closeContexts();
                internalTransaction.commit();
            }
        }
    }

    public void rollback() {
        synchronized (internalTransaction) {
            if (internalTransaction.isOpen()) {
                closeContexts();
                internalTransaction.rollback();
            }
        }
    }

    private void closeContexts() {
        openExecutionContexts.forEach(TransactionalContext::close);
    }

    public void terminate(Status reason) {
        terminateIfPossible(reason);
    }

    public void terminateIfPossible(Status reason) {
        if (internalTransaction.isOpen()
                && internalTransaction.terminationReason().isEmpty()) {
            internalTransaction.terminate(reason);
        }
    }

    /**
     * This is a hack to be able to get an InternalTransaction for the TestFabricTransaction tx wrapper
     */
    @Deprecated
    public InternalTransaction getInternalTransaction() {
        return internalTransaction;
    }

    public long transactionSequenceNumber() {
        return internalTransaction.kernelTransaction().getTransactionSequenceNumber();
    }

    private class ContextClosingResultInterceptor implements StatementResult {
        private final StatementResult wrappedResult;
        private final TransactionalContext executionContext;

        ContextClosingResultInterceptor(StatementResult wrappedResult, TransactionalContext executionContext) {
            this.wrappedResult = wrappedResult;
            this.executionContext = executionContext;
        }

        @Override
        public List<String> columns() {
            return wrappedResult.columns();
        }

        @Override
        public Flux<Record> records() {
            // We care only about the case when the statement completes successfully.
            // All contexts will be closed upon a failure in the rollback
            return wrappedResult.records().doOnComplete(() -> {
                openExecutionContexts.remove(executionContext);
                executionContext.close();
            });
        }

        @Override
        public Mono<Summary> summary() {
            return wrappedResult.summary();
        }

        @Override
        public Mono<QueryExecutionType> executionType() {
            return wrappedResult.executionType();
        }
    }
}
