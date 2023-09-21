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

import static org.neo4j.fabric.stream.StatementResults.withErrorMapping;
import static scala.jdk.javaapi.CollectionConverters.asJava;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.ast.GraphSelection;
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver;
import org.neo4j.cypher.internal.evaluator.StaticEvaluation;
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter;
import org.neo4j.cypher.internal.expressions.Expression;
import org.neo4j.cypher.internal.runtime.CypherRow;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.eval.Catalog;
import org.neo4j.fabric.eval.UseEvaluation;
import org.neo4j.fabric.executor.QueryStatementLifecycles.StatementLifecycle;
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
import org.neo4j.fabric.stream.StatementResults;
import org.neo4j.fabric.stream.summary.MergedQueryStatistics;
import org.neo4j.fabric.stream.summary.MergedSummary;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.kernel.impl.query.QueryRoutingMonitor;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FabricExecutor {
    public static final String WRITING_IN_READ_NOT_ALLOWED_MSG = "Writing in read access mode not allowed";
    private final FabricConfig.DataStream dataStreamConfig;
    private final FabricPlanner planner;
    private final UseEvaluation useEvaluation;
    private final InternalLog log;
    private final QueryStatementLifecycles statementLifecycles;
    private final Executor fabricWorkerExecutor;
    private final QueryRoutingMonitor queryRoutingMonitor;

    public FabricExecutor(
            FabricConfig config,
            FabricPlanner planner,
            UseEvaluation useEvaluation,
            InternalLogProvider internalLog,
            QueryStatementLifecycles statementLifecycles,
            Executor fabricWorkerExecutor,
            Monitors monitors) {
        this.dataStreamConfig = config.getDataStream();
        this.planner = planner;
        this.useEvaluation = useEvaluation;
        this.log = internalLog.getLog(getClass());
        this.statementLifecycles = statementLifecycles;
        this.fabricWorkerExecutor = fabricWorkerExecutor;
        this.queryRoutingMonitor = monitors.newMonitor(QueryRoutingMonitor.class);
    }

    public StatementResult run(FabricTransaction fabricTransaction, String statement, MapValue parameters) {
        var transactionBinding = fabricTransaction.transactionBinding();
        var lifecycle = statementLifecycles.create(
                fabricTransaction.getTransactionInfo(), statement, parameters, transactionBinding);

        lifecycle.startProcessing();

        var procedures = fabricTransaction.contextlessProcedures();
        var signatureResolver = SignatureResolver.from(procedures);
        var evaluator = StaticEvaluation.from(procedures);

        try {
            var defaultGraphName = fabricTransaction
                    .getTransactionInfo()
                    .getSessionDatabaseReference()
                    .alias()
                    .name();

            var catalog = fabricTransaction.getCatalogSnapshot();
            var plannerInstance = planner.instance(
                    signatureResolver,
                    statement,
                    parameters,
                    defaultGraphName,
                    catalog,
                    fabricTransaction.cancellationChecker());
            var plan = plannerInstance.plan();
            var query = plan.query();

            lifecycle.doneFabricProcessing(plan);

            var accessMode = fabricTransaction.getTransactionInfo().getAccessMode();

            if (plan.debugOptions().logPlan()) {
                log.debug(String.format("Fabric plan: %s", Fragment.pretty().asString(query)));
            }

            var statementResult = fabricTransaction.execute(ctx -> {
                var useEvaluator = useEvaluation.instance(evaluator, signatureResolver, statement, catalog);
                FabricStatementExecution execution;
                if (plan.debugOptions().logRecords()) {
                    execution = new FabricLoggingStatementExecution(
                            plan,
                            plannerInstance,
                            useEvaluator,
                            parameters,
                            accessMode,
                            ctx,
                            log,
                            lifecycle,
                            dataStreamConfig,
                            fabricTransaction
                                    .getTransactionInfo()
                                    .getQueryExecutionConfiguration()
                                    .notificationFilters());
                } else {
                    execution = new FabricStatementExecution(
                            plan,
                            plannerInstance,
                            useEvaluator,
                            parameters,
                            accessMode,
                            ctx,
                            lifecycle,
                            dataStreamConfig,
                            fabricTransaction
                                    .getTransactionInfo()
                                    .getQueryExecutionConfiguration()
                                    .notificationFilters());
                }
                return execution.run();
            });

            return withErrorMapping(
                    statementResult, FabricSecondaryException.class, FabricSecondaryException::getPrimaryException);
        } catch (RuntimeException e) {
            lifecycle.endFailure(e);
            // NOTE: We should not rollback the transaction here, since that is the responsibility of outer layers,
            //       and it should happen after all active statements/queries have been closed.
            throw e;
        }
    }

    public long clearQueryCachesForDatabase(String databaseName) {
        return planner.queryCache().clearByContext(databaseName);
    }

    private class FabricStatementExecution {
        private final FabricPlan plan;
        private final FabricPlanner.PlannerInstance plannerInstance;
        private final UseEvaluation.Instance useEvaluator;
        private final MapValue queryParams;
        private final FabricTransaction.FabricExecutionContext ctx;
        private final MergedQueryStatistics statistics = new MergedQueryStatistics();
        private final Set<Notification> notifications = ConcurrentHashMap.newKeySet();
        private final StatementLifecycle lifecycle;
        private final Prefetcher prefetcher;
        private final AccessMode accessMode;
        private final NotificationConfiguration notificationConfiguration;

        FabricStatementExecution(
                FabricPlan plan,
                FabricPlanner.PlannerInstance plannerInstance,
                UseEvaluation.Instance useEvaluator,
                MapValue queryParams,
                AccessMode accessMode,
                FabricTransaction.FabricExecutionContext ctx,
                StatementLifecycle lifecycle,
                FabricConfig.DataStream dataStreamConfig,
                NotificationConfiguration notificationConfiguration) {
            this.plan = plan;
            this.plannerInstance = plannerInstance;
            this.useEvaluator = useEvaluator;
            this.queryParams = queryParams;
            this.ctx = ctx;
            this.lifecycle = lifecycle;
            this.prefetcher = new Prefetcher(dataStreamConfig);
            this.accessMode = accessMode;
            this.notificationConfiguration = notificationConfiguration;
        }

        StatementResult run() {
            var filteredNotifications = plan.notifications()
                    .filter(notificationConfiguration::includes)
                    .toList();
            notifications.addAll(asJava(filteredNotifications));

            lifecycle.startExecution(false);
            var query = plan.query();

            // EXPLAIN for multi-graph queries returns only fabric plan,
            // because it is very hard to produce anything better without actually executing the query
            if (plan.executionType() == FabricPlan.EXPLAIN() && plan.inCompositeContext()) {
                lifecycle.endSuccess();
                return StatementResults.create(
                        asJava(query.outputColumns()),
                        Flux.empty(),
                        Mono.just(new MergedSummary(Mono.just(plan.query().description()), statistics, notifications)),
                        Mono.just(EffectiveQueryType.queryExecutionType(plan, accessMode)));
            } else {
                FragmentResult fragmentResult = run(query, null);

                List<String> columns;
                Flux<Record> records;
                if (query.producesResults()) {
                    columns = asJava(query.outputColumns());
                    records = fragmentResult.records;
                } else {
                    columns = Collections.emptyList();
                    records = fragmentResult.records.then(Mono.<Record>empty()).flux();
                }

                Mono<Summary> summary =
                        Mono.just(new MergedSummary(fragmentResult.planDescription, statistics, notifications));

                return StatementResults.create(
                        columns,
                        records.doOnComplete(lifecycle::endSuccess)
                                .doOnCancel(lifecycle::endSuccess)
                                .doOnError(lifecycle::endFailure),
                        summary,
                        fragmentResult.executionType);
            }
        }

        FragmentResult run(Fragment fragment, Record argument) {

            if (fragment instanceof Fragment.Init) {
                return runInit();
            } else if (fragment instanceof Fragment.Apply) {
                return runApply((Fragment.Apply) fragment, argument);
            } else if (fragment instanceof Fragment.Union) {
                return runUnion((Fragment.Union) fragment, argument);
            } else if (fragment instanceof Fragment.Exec) {
                return runExec((Fragment.Exec) fragment, argument);
            } else {
                throw notImplemented("Invalid query fragment", fragment);
            }
        }

        FragmentResult runInit() {
            return new FragmentResult(Flux.just(Records.empty()), Mono.empty(), Mono.empty());
        }

        FragmentResult runApply(Fragment.Apply apply, Record argument) {
            FragmentResult input = run(apply.input(), argument);

            Function<Record, Publisher<Record>> runInner =
                    apply.inner().outputColumns().isEmpty()
                            ? (Record record) -> runAndProduceOnlyRecord(apply.inner(), record) // Unit subquery
                            : (Record record) -> runAndProduceJoinedResult(apply.inner(), record); // Returning subquery

            Flux<Record> resultRecords = input.records.flatMap(runInner, dataStreamConfig.getConcurrency(), 1);

            // TODO: merge executionType here for subqueries
            // For now, just return global value as seen by fabric
            Mono<QueryExecutionType> executionType = Mono.just(EffectiveQueryType.queryExecutionType(plan, accessMode));

            return new FragmentResult(resultRecords, Mono.empty(), executionType);
        }

        private Flux<Record> runAndProduceJoinedResult(Fragment fragment, Record record) {
            return run(fragment, record).records.map(outputRecord -> Records.join(record, outputRecord));
        }

        private Mono<Record> runAndProduceOnlyRecord(Fragment fragment, Record record) {
            return run(fragment, record).records.then(Mono.just(record));
        }

        FragmentResult runUnion(Fragment.Union union, Record argument) {
            FragmentResult lhs = run(union.lhs(), argument);
            FragmentResult rhs = run(union.rhs(), argument);
            Flux<Record> merged = Flux.merge(lhs.records, rhs.records);
            Mono<QueryExecutionType> executionType = mergeExecutionType(lhs.executionType, rhs.executionType);
            if (union.distinct()) {
                return new FragmentResult(merged.distinct(), Mono.empty(), executionType);
            } else {
                return new FragmentResult(merged, Mono.empty(), executionType);
            }
        }

        FragmentResult runExec(Fragment.Exec fragment, Record argument) {
            ctx.validateStatementType(fragment.statementType());
            Map<String, AnyValue> argumentValues = argumentValues(fragment, argument);

            Catalog.Graph graph =
                    evalUse(fragment.use().graphSelection(), argumentValues, ctx.getSessionDatabaseReference());

            validateCanUseGraph(graph, ctx.getSessionDatabaseReference());

            var transactionMode =
                    getTransactionMode(fragment.queryType(), graph.reference().toPrettyString());

            MapValue parameters = addParamsFromRecord(queryParams, argumentValues, asJava(fragment.parameters()));

            var location = this.ctx.locationOf(graph, transactionMode.requiresWrite());

            if (location instanceof Location.Local local) {
                FragmentResult input = run(fragment.input(), argument);
                if (fragment.executable()) {
                    FabricQuery.LocalQuery localQuery = plannerInstance.asLocal(fragment);
                    var targetsComposite = plannerInstance.targetsComposite(fragment);
                    FragmentResult fragmentResult = runLocalQueryAt(
                            local, transactionMode, localQuery.query(), parameters, targetsComposite, input.records);
                    Mono<QueryExecutionType> executionType =
                            mergeExecutionType(input.executionType, fragmentResult.executionType);
                    return new FragmentResult(fragmentResult.records, fragmentResult.planDescription, executionType);
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

        private void validateCanUseGraph(Catalog.Graph accessedGraph, DatabaseReference sessionDatabaseReference) {
            var sessionGraph = useEvaluator.resolveGraph(sessionDatabaseReference.alias());

            if (sessionGraph instanceof Catalog.Composite) {
                if (!useEvaluator.isConstituentOrSelf(accessedGraph, sessionGraph)) {
                    if (!useEvaluator.isSystem(accessedGraph)) {
                        throw new InvalidSemanticsException(
                                cantAccessOutsideCompositeMessage(sessionGraph, accessedGraph));
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

        FragmentResult runLocalQueryAt(
                Location.Local location,
                TransactionMode transactionMode,
                FullyParsedQuery query,
                MapValue parameters,
                boolean targetsComposite,
                Flux<Record> input) {

            ExecutionOptions executionOptions = plan.inCompositeContext() && !targetsComposite
                    ? new ExecutionOptions(location.graphId())
                    : new ExecutionOptions();

            StatementResult localStatementResult = ctx.getLocal()
                    .run(
                            location,
                            transactionMode,
                            lifecycle,
                            query,
                            parameters,
                            input,
                            executionOptions,
                            targetsComposite);
            Flux<Record> records = localStatementResult
                    .records()
                    .doOnComplete(() -> localStatementResult.summary().subscribe(this::updateSummary));

            Mono<ExecutionPlanDescription> planDescription = localStatementResult
                    .summary()
                    .map(Summary::executionPlanDescription)
                    .map(pd -> new TaggingPlanDescriptionWrapper(pd, location.getDatabaseName()));

            queryRoutingMonitor.queryRoutedLocal();

            return new FragmentResult(records, planDescription, localStatementResult.executionType());
        }

        FragmentResult runRemoteQueryAt(
                Location.Remote location, TransactionMode transactionMode, String queryString, MapValue parameters) {
            ExecutionOptions executionOptions =
                    plan.inCompositeContext() ? new ExecutionOptions(location.graphId()) : new ExecutionOptions();

            lifecycle.startExecution(true);
            Mono<StatementResult> statementResult =
                    ctx.getRemote().run(location, executionOptions, queryString, transactionMode, parameters);
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
            Flux<Record> recordsWithCompletionDelegation =
                    new CompletionDelegatingOperator(records, fabricWorkerExecutor);
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

            return new FragmentResult(prefetchedRecords, planDescription, executionType);
        }

        private Map<String, AnyValue> argumentValues(Fragment fragment, Record argument) {
            if (argument == null) {
                return Map.of();
            } else {
                return Records.asMap(argument, asJava(fragment.argumentColumns()));
            }
        }

        private Catalog.Graph evalUse(
                GraphSelection selection, Map<String, AnyValue> record, DatabaseReference sessionDb) {
            return useEvaluator.evaluate(selection, queryParams, record, sessionDb);
        }

        private MapValue addParamsFromRecord(
                MapValue params, Map<String, AnyValue> record, Map<String, String> bindings) {
            int resultSize = params.size() + bindings.size();
            if (resultSize == 0) {
                return VirtualValues.EMPTY_MAP;
            }
            MapValueBuilder builder = new MapValueBuilder(resultSize);
            params.foreach(builder::add);
            bindings.forEach((var, par) -> builder.add(par, validateValue(record.get(var))));
            return builder.build();
        }

        private AnyValue validateValue(AnyValue value) {
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

        private void updateSummary(Summary summary) {
            if (summary != null) {
                this.statistics.add(summary.getQueryStatistics());
                this.notifications.addAll(summary.getNotifications());
            }
        }

        private Mono<QueryExecutionType> mergeExecutionType(
                Mono<QueryExecutionType> lhs, Mono<QueryExecutionType> rhs) {
            return Mono.zip(lhs, rhs)
                    .map(both -> QueryTypes.merge(both.getT1(), both.getT2()))
                    .switchIfEmpty(lhs)
                    .switchIfEmpty(rhs);
        }

        private RuntimeException notImplemented(String msg, Object object) {
            return notImplemented(msg, object.toString());
        }

        private RuntimeException notImplemented(String msg, String info) {
            return new InvalidSemanticsException(msg + ": " + info);
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
                            WRITING_IN_READ_NOT_ALLOWED_MSG + ". Attempted write to %s",
                            graph);
                } else {
                    return TransactionMode.DEFINITELY_READ;
                }
            }
        }
    }

    private static class FragmentResult {
        private final Flux<Record> records;
        private final Mono<ExecutionPlanDescription> planDescription;
        private final Mono<QueryExecutionType> executionType;

        FragmentResult(
                Flux<Record> records,
                Mono<ExecutionPlanDescription> planDescription,
                Mono<QueryExecutionType> executionType) {
            this.records = records;
            this.planDescription = planDescription;
            this.executionType = executionType;
        }
    }

    private class FabricLoggingStatementExecution extends FabricStatementExecution {
        private final AtomicInteger step;
        private final InternalLog log;

        FabricLoggingStatementExecution(
                FabricPlan plan,
                FabricPlanner.PlannerInstance plannerInstance,
                UseEvaluation.Instance useEvaluator,
                MapValue params,
                AccessMode accessMode,
                FabricTransaction.FabricExecutionContext ctx,
                InternalLog log,
                StatementLifecycle lifecycle,
                FabricConfig.DataStream dataStreamConfig,
                NotificationConfiguration notificationConfiguration) {
            super(
                    plan,
                    plannerInstance,
                    useEvaluator,
                    params,
                    accessMode,
                    ctx,
                    lifecycle,
                    dataStreamConfig,
                    notificationConfiguration);
            this.step = new AtomicInteger(0);
            this.log = log;
        }

        @Override
        FragmentResult runLocalQueryAt(
                Location.Local location,
                TransactionMode transactionMode,
                FullyParsedQuery query,
                MapValue parameters,
                boolean targetsComposite,
                Flux<Record> input) {
            String id = executionId();
            trace(id, "local " + nameString(location), compact(query.description()));
            return traceRecords(
                    id, super.runLocalQueryAt(location, transactionMode, query, parameters, targetsComposite, input));
        }

        @Override
        FragmentResult runRemoteQueryAt(
                Location.Remote location, TransactionMode transactionMode, String queryString, MapValue parameters) {
            String id = executionId();
            trace(id, "remote " + nameString(location), compact(queryString));
            return traceRecords(id, super.runRemoteQueryAt(location, transactionMode, queryString, parameters));
        }

        private static String nameString(Location location) {
            var namespace = location.databaseReference().namespace().map(NormalizedDatabaseName::name).stream();
            var name = Stream.of(location.databaseReference().alias().name());
            return Stream.concat(namespace, name).collect(Collectors.joining("."));
        }

        private String compact(String in) {
            return in.replaceAll("\\r?\\n", " ").replaceAll("\\s+", " ");
        }

        private FragmentResult traceRecords(String id, FragmentResult fragmentResult) {
            var records = fragmentResult
                    .records
                    .doOnNext(record -> {
                        String rec = IntStream.range(0, record.size())
                                .mapToObj(i -> record.getValue(i).toString())
                                .collect(Collectors.joining(", ", "[", "]"));
                        trace(id, "output", rec);
                    })
                    .doOnError(err -> {
                        String rec = err.getClass().getSimpleName() + ": " + err.getMessage();
                        trace(id, "error", rec);
                    })
                    .doOnCancel(() -> trace(id, "cancel", "cancel"))
                    .doOnComplete(() -> trace(id, "complete", "complete"));
            return new FragmentResult(records, fragmentResult.planDescription, fragmentResult.executionType);
        }

        private void trace(String id, String event, String data) {
            log.debug(String.format("%s: %s: %s", id, event, data));
        }

        private String executionId() {
            String stmtId = idString(this.hashCode());
            String step = idString(this.step.getAndIncrement());
            return String.format("%s/%s", stmtId, step);
        }

        private String idString(int code) {
            return String.format("%08X", code);
        }
    }
}
