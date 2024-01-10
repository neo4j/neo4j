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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.ast.GraphSelection;
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter;
import org.neo4j.cypher.internal.expressions.Expression;
import org.neo4j.cypher.internal.runtime.CypherRow;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.fabric.eval.Catalog;
import org.neo4j.fabric.eval.UseEvaluation;
import org.neo4j.fabric.planning.FabricPlan;
import org.neo4j.fabric.planning.FabricPlanner;
import org.neo4j.fabric.planning.FabricQuery;
import org.neo4j.fabric.planning.Fragment;
import org.neo4j.fabric.planning.QueryType;
import org.neo4j.fabric.stream.CompletionDelegatingOperator;
import org.neo4j.fabric.stream.Prefetcher;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Records;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.summary.MergedQueryStatistics;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.impl.query.QueryRoutingMonitor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

abstract class SingleQueryFragmentExecutor {

    private final FabricPlanner.PlannerInstance plannerInstance;
    private final Executor fabricWorkerExecutor;
    private final FabricTransaction.FabricExecutionContext ctx;
    private final UseEvaluation.Instance useEvaluator;
    private final FabricPlan plan;
    private final MapValue queryParams;
    private final AccessMode accessMode;
    private final Set<Notification> notifications;
    private final QueryStatementLifecycles.StatementLifecycle lifecycle;
    private final Prefetcher prefetcher;
    private final QueryRoutingMonitor queryRoutingMonitor;
    private final MergedQueryStatistics statistics;
    private final Tracer tracer;
    private final FragmentExecutor fragmentExecutor;

    SingleQueryFragmentExecutor(
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
        this.plannerInstance = plannerInstance;
        this.fabricWorkerExecutor = fabricWorkerExecutor;
        this.ctx = ctx;
        this.useEvaluator = useEvaluator;
        this.plan = plan;
        this.queryParams = queryParams;
        this.accessMode = accessMode;
        this.notifications = notifications;
        this.lifecycle = lifecycle;
        this.prefetcher = prefetcher;
        this.queryRoutingMonitor = queryRoutingMonitor;
        this.statistics = statistics;
        this.tracer = tracer;
        this.fragmentExecutor = fragmentExecutor;
    }

    MapValue queryParams() {
        return queryParams;
    }

    FabricTransaction.FabricExecutionContext ctx() {
        return ctx;
    }

    FragmentExecutor fragmentExecutor() {
        return fragmentExecutor;
    }

    PrepareResult prepare(Fragment.Exec fragment, Record argument) {
        ctx.validateStatementType(fragment.statementType());
        Map<String, AnyValue> argumentValues = argumentValues(fragment, argument);

        Catalog.Graph graph =
                evalUse(fragment.use().graphSelection(), argumentValues, ctx.getSessionDatabaseReference());

        validateCanUseGraph(graph, ctx.getSessionDatabaseReference());

        var transactionMode =
                getTransactionMode(fragment.queryType(), graph.reference().toPrettyString());
        return new PrepareResult(graph, argumentValues, transactionMode);
    }

    FragmentResult doExecuteFragment(
            Fragment.Exec fragment,
            MapValue parameters,
            Catalog.Graph graph,
            TransactionMode transactionMode,
            Supplier<FragmentResult> executeFragmentInput) {
        var location = this.ctx.locationOf(graph, transactionMode.requiresWrite());

        if (location instanceof Location.Local local) {
            FragmentResult input = executeFragmentInput.get();
            if (fragment.executable()) {
                FabricQuery.LocalQuery localQuery = plannerInstance.asLocal(fragment);
                var targetsComposite = plannerInstance.targetsComposite(fragment);
                FragmentResult fragmentResult = runLocalQueryAt(
                        local, transactionMode, localQuery.query(), parameters, targetsComposite, input.records());
                Mono<QueryExecutionType> executionType =
                        mergeExecutionType(input.executionType(), fragmentResult.executionType());
                return new FragmentResult(fragmentResult.records(), fragmentResult.planDescription(), executionType);
            } else {
                return input;
            }
        } else if (location instanceof Location.Remote remote) {
            FabricQuery.RemoteQuery remoteQuery = plannerInstance.asRemote(fragment);
            var extracted = asJava(remoteQuery.extractedLiterals());
            var builder = new MapValueBuilder();
            var evaluator = useEvaluator.evaluator();
            for (Map.Entry<AutoExtractedParameter, Expression> entry : extracted.entrySet()) {
                builder.add(
                        entry.getKey().name(),
                        evaluator.evaluate(entry.getValue(), VirtualValues.EMPTY_MAP, CypherRow.empty()));
            }
            MapValue fullParams = parameters.updatedWith(builder.build());

            return runRemoteQueryAt(remote, transactionMode, remoteQuery.query(), fullParams);
        } else {
            throw notImplemented("Invalid graph location", location);
        }
    }

    abstract Mono<StatementResult> runRemote(
            Location.Remote location,
            ExecutionOptions options,
            String query,
            TransactionMode transactionMode,
            MapValue params);

    abstract StatementResult runLocal(
            Location.Local location,
            TransactionMode transactionMode,
            QueryStatementLifecycles.StatementLifecycle parentLifecycle,
            FullyParsedQuery query,
            MapValue params,
            Flux<Record> input,
            ExecutionOptions executionOptions,
            Boolean targetsComposite);

    private RuntimeException notImplemented(String msg, Object object) {
        return notImplemented(msg, object.toString());
    }

    private RuntimeException notImplemented(String msg, String info) {
        return new InvalidSemanticsException(msg + ": " + info);
    }

    private FragmentResult runRemoteQueryAt(
            Location.Remote location, TransactionMode transactionMode, String queryString, MapValue parameters) {
        var recordTracer = tracer.remoteQueryStart(location, queryString);
        ExecutionOptions executionOptions =
                plan.inCompositeContext() ? new ExecutionOptions(location.graphId()) : new ExecutionOptions();

        lifecycle.startExecution(true);
        Mono<StatementResult> statementResult =
                runRemote(location, executionOptions, queryString, transactionMode, parameters);
        Flux<Record> records = statementResult.flatMapMany(
                sr -> sr.records().doOnComplete(() -> sr.summary().subscribe(this::updateSummary)));

        // 'onComplete' signal coming from an inner stream might cause more data being requested from an upstream
        // operator
        // and the request will be done using the thread that invoked 'onComplete'.
        // Since 'onComplete' is invoked by driver IO thread ( Netty event loop ), this might cause the driver
        // thread to block
        // or perform a computationally intensive operation in an upstream operator if the upstream operator is
        // Cypher local execution
        // that produces records directly in 'request' call.
        Flux<Record> recordsWithCompletionDelegation = new CompletionDelegatingOperator(records, fabricWorkerExecutor);
        Flux<Record> prefetchedRecords = prefetcher.addPrefetch(recordsWithCompletionDelegation);
        Mono<ExecutionPlanDescription> planDescription =
                statementResult.flatMap(StatementResult::summary).map(Summary::executionPlanDescription);

        // TODO: We currently need to override here since we can't get it from remote properly
        // but our result here is not as accurate as what the remote might report.
        Mono<QueryExecutionType> executionType = Mono.just(EffectiveQueryType.queryExecutionType(plan, accessMode));

        if (location instanceof Location.Remote.Internal) {
            queryRoutingMonitor.queryRoutedRemoteInternal();
        } else if (location instanceof Location.Remote.External) {
            queryRoutingMonitor.queryRoutedRemoteExternal();
        }

        return recordTracer.traceRecords(new FragmentResult(prefetchedRecords, planDescription, executionType));
    }

    private FragmentResult runLocalQueryAt(
            Location.Local location,
            TransactionMode transactionMode,
            FullyParsedQuery query,
            MapValue parameters,
            boolean targetsComposite,
            Flux<Record> input) {
        var recordTracer = tracer.localQueryStart(location, query);

        ExecutionOptions executionOptions = plan.inCompositeContext() && !targetsComposite
                ? new ExecutionOptions(location.graphId())
                : new ExecutionOptions();

        StatementResult localStatementResult = runLocal(
                location, transactionMode, lifecycle, query, parameters, input, executionOptions, targetsComposite);
        Flux<Record> records = localStatementResult
                .records()
                .doOnComplete(() -> localStatementResult.summary().subscribe(this::updateSummary));

        Mono<ExecutionPlanDescription> planDescription = localStatementResult
                .summary()
                .map(Summary::executionPlanDescription)
                .map(pd -> new TaggingPlanDescriptionWrapper(pd, location.getDatabaseName()));

        queryRoutingMonitor.queryRoutedLocal();

        return recordTracer.traceRecords(
                new FragmentResult(records, planDescription, localStatementResult.executionType()));
    }

    private Map<String, AnyValue> argumentValues(Fragment fragment, Record argument) {
        if (argument == null) {
            return Map.of();
        } else {
            return Records.asMap(argument, asJava(fragment.argumentColumns()));
        }
    }

    private Catalog.Graph evalUse(GraphSelection selection, Map<String, AnyValue> record, DatabaseReference sessionDb) {
        return useEvaluator.evaluate(selection, queryParams, record, sessionDb);
    }

    private void validateCanUseGraph(Catalog.Graph accessedGraph, DatabaseReference sessionDatabaseReference) {
        var sessionGraph = useEvaluator.resolveGraph(sessionDatabaseReference.alias());

        if (sessionGraph instanceof Catalog.Composite) {
            if (!useEvaluator.isConstituentOrSelf(accessedGraph, sessionGraph)) {
                if (!useEvaluator.isSystem(accessedGraph)) {
                    throw new InvalidSemanticsException(cantAccessOutsideCompositeMessage(sessionGraph, accessedGraph));
                }
            }
        } else {
            if (!useEvaluator.isDatabaseOrAliasInRoot(accessedGraph)) {
                throw new InvalidSemanticsException(
                        cantAccessCompositeConstituentsMessage(sessionGraph, accessedGraph));
            }
        }
    }

    private String cantAccessOutsideCompositeMessage(Catalog.Graph sessionDatabase, Catalog.Graph accessed) {
        return "When connected to a composite database, access is allowed only to its constituents. "
                + "Attempted to access '%s' while connected to '%s'"
                        .formatted(
                                useEvaluator.qualifiedNameString(accessed),
                                useEvaluator.qualifiedNameString(sessionDatabase));
    }

    private String cantAccessCompositeConstituentsMessage(Catalog.Graph sessionDatabase, Catalog.Graph accessed) {
        return "Accessing a composite database and its constituents is only allowed when connected to it. "
                + "Attempted to access '%s' while connected to '%s'"
                        .formatted(
                                useEvaluator.qualifiedNameString(accessed),
                                useEvaluator.qualifiedNameString(sessionDatabase));
    }

    private TransactionMode getTransactionMode(QueryType queryType, String graph) {
        var executionType = plan.executionType();
        var queryMode = EffectiveQueryType.effectiveAccessMode(accessMode, executionType, queryType);

        if (accessMode == AccessMode.WRITE) {
            if (queryMode == AccessMode.WRITE) {
                return TransactionMode.DEFINITELY_WRITE;
            } else {
                return TransactionMode.MAYBE_WRITE;
            }
        } else {
            if (queryMode == AccessMode.WRITE) {
                throw new FabricException(
                        Status.Statement.AccessMode,
                        FabricExecutor.WRITING_IN_READ_NOT_ALLOWED_MSG + ". Attempted write to %s",
                        graph);
            } else {
                return TransactionMode.DEFINITELY_READ;
            }
        }
    }

    AnyValue validateValue(AnyValue value) {
        if (value instanceof VirtualNodeValue) {
            throw new FabricException(
                    Status.Statement.TypeError,
                    "Importing node values in remote subqueries is currently not supported");
        } else if (value instanceof VirtualRelationshipValue) {
            throw new FabricException(
                    Status.Statement.TypeError,
                    "Importing relationship values in remote subqueries is currently not supported");
        } else if (value instanceof PathValue) {
            throw new FabricException(
                    Status.Statement.TypeError,
                    "Importing path values in remote subqueries is currently not supported");
        } else {
            return value;
        }
    }

    private Mono<QueryExecutionType> mergeExecutionType(Mono<QueryExecutionType> lhs, Mono<QueryExecutionType> rhs) {
        return Mono.zip(lhs, rhs)
                .map(both -> QueryTypes.merge(both.getT1(), both.getT2()))
                .switchIfEmpty(lhs)
                .switchIfEmpty(rhs);
    }

    private void updateSummary(Summary summary) {
        if (summary != null) {
            this.statistics.add(summary.getQueryStatistics());
            this.notifications.addAll(summary.getNotifications());
        }
    }

    record PrepareResult(Catalog.Graph graph, Map<String, AnyValue> argumentValues, TransactionMode transactionMode) {}

    interface Tracer {

        RecordTracer remoteQueryStart(Location.Remote location, String queryString);

        RecordTracer localQueryStart(Location.Local location, FullyParsedQuery query);
    }

    interface RecordTracer {
        FragmentResult traceRecords(FragmentResult fragmentResult);
    }

    interface FragmentExecutor {
        FragmentResult run(Fragment fragment, Record argument);
    }
}
