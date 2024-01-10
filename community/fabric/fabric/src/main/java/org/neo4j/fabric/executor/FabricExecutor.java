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
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver;
import org.neo4j.cypher.internal.evaluator.StaticEvaluation;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.eval.UseEvaluation;
import org.neo4j.fabric.executor.QueryStatementLifecycles.StatementLifecycle;
import org.neo4j.fabric.planning.FabricPlan;
import org.neo4j.fabric.planning.FabricPlanner;
import org.neo4j.fabric.planning.Fragment;
import org.neo4j.fabric.stream.Prefetcher;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Records;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.StatementResults;
import org.neo4j.fabric.stream.summary.MergedQueryStatistics;
import org.neo4j.fabric.stream.summary.MergedSummary;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.kernel.impl.query.QueryRoutingMonitor;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.values.virtual.MapValue;
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
                    records = fragmentResult.records();
                } else {
                    columns = Collections.emptyList();
                    records =
                            fragmentResult.records().then(Mono.<Record>empty()).flux();
                }

                Mono<Summary> summary =
                        Mono.just(new MergedSummary(fragmentResult.planDescription(), statistics, notifications));

                return StatementResults.create(
                        columns,
                        records.doOnComplete(lifecycle::endSuccess)
                                .doOnCancel(lifecycle::endSuccess)
                                .doOnError(lifecycle::endFailure),
                        summary,
                        fragmentResult.executionType());
            }
        }

        FragmentResult run(Fragment fragment, Record argument) {

            if (fragment instanceof Fragment.Init) {
                return runInit();
            } else if (fragment instanceof Fragment.Apply apply) {
                if (apply.inTransactionsParameters().isEmpty()) {
                    return runApply(apply, argument);
                } else {
                    return runCallInTransactions(apply, argument);
                }

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

            Flux<Record> resultRecords = input.records().flatMap(runInner, dataStreamConfig.getConcurrency(), 1);

            // TODO: merge executionType here for subqueries
            // For now, just return global value as seen by fabric
            Mono<QueryExecutionType> executionType = Mono.just(EffectiveQueryType.queryExecutionType(plan, accessMode));

            return new FragmentResult(resultRecords, Mono.empty(), executionType);
        }

        private Flux<Record> runAndProduceJoinedResult(Fragment fragment, Record record) {
            return run(fragment, record).records().map(outputRecord -> Records.join(record, outputRecord));
        }

        private Mono<Record> runAndProduceOnlyRecord(Fragment fragment, Record record) {
            return run(fragment, record).records().then(Mono.just(record));
        }

        FragmentResult runUnion(Fragment.Union union, Record argument) {
            FragmentResult lhs = run(union.lhs(), argument);
            FragmentResult rhs = run(union.rhs(), argument);
            Flux<Record> merged = Flux.merge(lhs.records(), rhs.records());
            Mono<QueryExecutionType> executionType = mergeExecutionType(lhs.executionType(), rhs.executionType());
            if (union.distinct()) {
                return new FragmentResult(merged.distinct(), Mono.empty(), executionType);
            } else {
                return new FragmentResult(merged, Mono.empty(), executionType);
            }
        }

        FragmentResult runExec(Fragment.Exec fragment, Record argument) {
            return new StandardQueryExecutor(
                            fragment,
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
                            tracer(),
                            FabricStatementExecution.this::run)
                    .run(argument);
        }

        FragmentResult runCallInTransactions(Fragment.Apply fragment, Record argument) {
            var resultRecords = new CallInTransactionsExecutor(
                            fragment,
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
                            tracer(),
                            FabricStatementExecution.this::run)
                    .run(argument);

            Mono<QueryExecutionType> executionType = Mono.just(EffectiveQueryType.queryExecutionType(plan, accessMode));
            return new FragmentResult(resultRecords, Mono.empty(), executionType);
        }

        SingleQueryFragmentExecutor.Tracer tracer() {
            return new SingleQueryFragmentExecutor.Tracer() {

                @Override
                public SingleQueryFragmentExecutor.RecordTracer remoteQueryStart(
                        Location.Remote location, String queryString) {
                    return fragmentResult -> fragmentResult;
                }

                @Override
                public SingleQueryFragmentExecutor.RecordTracer localQueryStart(
                        Location.Local location, FullyParsedQuery query) {
                    return fragmentResult -> fragmentResult;
                }
            };
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

        SingleQueryFragmentExecutor.Tracer tracer() {
            return new SingleQueryFragmentExecutor.Tracer() {

                @Override
                public SingleQueryFragmentExecutor.RecordTracer remoteQueryStart(
                        Location.Remote location, String queryString) {
                    String id = executionId();
                    trace(id, "remote " + nameString(location), compact(queryString));

                    return fragmentResult -> doTraceRecords(id, fragmentResult);
                }

                @Override
                public SingleQueryFragmentExecutor.RecordTracer localQueryStart(
                        Location.Local location, FullyParsedQuery query) {
                    String id = executionId();
                    trace(id, "local " + nameString(location), compact(query.description()));

                    return fragmentResult -> doTraceRecords(id, fragmentResult);
                }
            };
        }

        private static String nameString(Location location) {
            var namespace = location.databaseReference().namespace().map(NormalizedDatabaseName::name).stream();
            var name = Stream.of(location.databaseReference().alias().name());
            return Stream.concat(namespace, name).collect(Collectors.joining("."));
        }

        private String compact(String in) {
            return in.replaceAll("\\r?\\n", " ").replaceAll("\\s+", " ");
        }

        private FragmentResult doTraceRecords(String id, FragmentResult fragmentResult) {
            var records = fragmentResult
                    .records()
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
            return new FragmentResult(records, fragmentResult.planDescription(), fragmentResult.executionType());
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
