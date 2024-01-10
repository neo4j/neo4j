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

import static scala.jdk.javaapi.CollectionConverters.asJava;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.ast.SubqueryCall;
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral;
import org.neo4j.cypher.internal.logical.plans.TransactionForeach$;
import org.neo4j.fabric.eval.Catalog;
import org.neo4j.fabric.eval.UseEvaluation;
import org.neo4j.fabric.planning.FabricPlan;
import org.neo4j.fabric.planning.FabricPlanner;
import org.neo4j.fabric.planning.Fragment;
import org.neo4j.fabric.stream.Prefetcher;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Records;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.summary.MergedQueryStatistics;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.graphdb.Notification;
import org.neo4j.kernel.impl.query.QueryRoutingMonitor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class CallInTransactionsExecutor extends SingleQueryFragmentExecutor {

    private final Fragment.Apply callInTransactions;
    private final Fragment.Exec innerFragment;
    private final int batchSize;
    private final List<BufferedInputRow> inputRowsBuffer;
    private Catalog.Graph batchGraph;
    private TransactionMode batchTransactionMode;

    CallInTransactionsExecutor(
            Fragment.Apply callInTransactions,
            FabricPlanner.PlannerInstance plannerInstance,
            Executor fabricWorkerExecutor,
            FabricTransaction.FabricExecutionContext ctx,
            UseEvaluation.Instance useEvaluator,
            FabricPlan plan,
            MapValue queryParams,
            AccessMode accessMode,
            Set<Notification> notifications,
            QueryStatementLifecycles.StatementLifecycle lifecycle,
            Prefetcher prefetcher,
            QueryRoutingMonitor queryRoutingMonitor,
            MergedQueryStatistics statistics,
            Tracer tracer,
            FragmentExecutor fragmentExecutor) {
        super(
                plannerInstance,
                fabricWorkerExecutor,
                ctx,
                useEvaluator,
                plan,
                queryParams,
                accessMode,
                notifications,
                lifecycle,
                prefetcher,
                queryRoutingMonitor,
                statistics,
                tracer,
                fragmentExecutor);
        this.callInTransactions = callInTransactions;
        this.innerFragment = (Fragment.Exec) callInTransactions.inner();
        this.batchSize = batchSize();
        inputRowsBuffer = new ArrayList<>(batchSize);
    }

    Flux<Record> run(Record argument) {
        Flux<Record> input =
                fragmentExecutor().run(callInTransactions.input(), argument).records();
        Flux<Record> resultPipe = input.flatMap(this::processInputRecord, 1, 1);
        return Flux.concat(resultPipe, Flux.defer(this::processBufferedInputRows));
    }

    private int batchSize() {
        return callInTransactions
                .inTransactionsParameters()
                .flatMap(SubqueryCall.InTransactionsParameters::batchParams)
                .map(SubqueryCall.InTransactionsBatchParameters::batchSize)
                .map(expression -> {
                    if (expression instanceof SignedDecimalIntegerLiteral literal) {
                        return literal.value().intValue();
                    }

                    throw new IllegalArgumentException("Unexpected batch size expression: " + expression);
                })
                .getOrElse(() -> (int) TransactionForeach$.MODULE$.defaultBatchSize());
    }

    private Flux<Record> processInputRecord(Record argument) {
        PrepareResult prepareResult = prepare(innerFragment, argument);

        if (batchGraph == null) {
            batchGraph = prepareResult.graph();
            batchTransactionMode = prepareResult.transactionMode();
        }

        if (!batchGraph.equals(prepareResult.graph())) {
            Flux<Record> result = processBufferedInputRows();
            batchGraph = prepareResult.graph();
            batchTransactionMode = prepareResult.transactionMode();
            inputRowsBuffer.add(new BufferedInputRow(prepareResult.argumentValues(), argument));
            return result;
        }

        inputRowsBuffer.add(new BufferedInputRow(prepareResult.argumentValues(), argument));
        if (inputRowsBuffer.size() == batchSize) {
            return processBufferedInputRows();
        }

        return Flux.empty();
    }

    private Flux<Record> processBufferedInputRows() {
        if (inputRowsBuffer.isEmpty()) {
            return Flux.empty();
        }

        MapValue params = addParamsFromInputRows();

        var result = doExecuteFragment(
                innerFragment,
                params,
                batchGraph,
                batchTransactionMode,
                // Inner part of CALL IN TRANSACTIONS does not have a child plan node
                // Unlike standard query execution with which most logic is shared
                () -> new FragmentResult(Flux.just(Records.empty()), Mono.empty(), Mono.empty()));
        var inputRecords = new ArrayList<>(inputRowsBuffer);
        Flux<Record> resultStream;
        if (callInTransactions.outputColumns().isEmpty()) {
            resultStream = result.records().map(outputRecord -> getMatchingInputRecord(outputRecord, inputRecords));
        } else {
            resultStream = result.records()
                    .map(outputRecord ->
                            Records.join(getMatchingInputRecord(outputRecord, inputRecords), outputRecord));
        }
        batchGraph = null;
        batchTransactionMode = null;
        inputRowsBuffer.clear();
        return resultStream;
    }

    private Record getMatchingInputRecord(Record outputRecord, List<BufferedInputRow> inputRecords) {
        // We are using the knowledge that Stitcher adds this always as the last column.
        var rowIdColumn = innerFragment.outputColumns().size() - 1;
        var rowId = (LongValue) outputRecord.getValue(rowIdColumn);
        return inputRecords.get((int) rowId.value()).record;
    }

    private MapValue addParamsFromInputRows() {
        List<String> bindings = asJava(innerFragment.argumentColumns());

        var rowListBuilder = ListValueBuilder.newListBuilder(inputRowsBuffer.size());
        for (int i = 0; i < inputRowsBuffer.size(); i++) {
            MapValue rowParams = rowToParams(inputRowsBuffer.get(i), bindings, i);
            rowListBuilder.add(rowParams);
        }
        var rows = rowListBuilder.build();

        MapValueBuilder builder = new MapValueBuilder(queryParams().size() + 1);
        queryParams().foreach(builder::add);
        builder.add(Fragment.Apply$.MODULE$.CALL_IN_TX_ROWS(), rows);
        return builder.build();
    }

    private MapValue rowToParams(BufferedInputRow inputRow, List<String> bindings, int rowId) {
        MapValueBuilder builder = new MapValueBuilder(bindings.size() + 1);
        bindings.forEach(
                var -> builder.add(var, validateValue(inputRow.argumentValues().get(var))));
        builder.add(Fragment.Apply$.MODULE$.CALL_IN_TX_ROW_ID(), Values.intValue(rowId));
        return builder.build();
    }

    @Override
    Mono<StatementResult> runRemote(
            Location.Remote location,
            ExecutionOptions options,
            String query,
            TransactionMode transactionMode,
            MapValue params) {
        return Mono.just(
                ctx().getRemote().runInAutocommitTransaction(location, options, query, transactionMode, params));
    }

    @Override
    StatementResult runLocal(
            Location.Local location,
            TransactionMode transactionMode,
            QueryStatementLifecycles.StatementLifecycle parentLifecycle,
            FullyParsedQuery query,
            MapValue params,
            Flux<Record> input,
            ExecutionOptions executionOptions,
            Boolean targetsComposite) {
        throw new IllegalStateException("Local execution is not supported yet");
    }

    private record BufferedInputRow(Map<String, AnyValue> argumentValues, Record record) {}
}
