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
import static scala.jdk.javaapi.CollectionConverters.asScala;

import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.neo4j.boltmessages.AccessMode;
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver;
import org.neo4j.cypher.internal.evaluator.StaticEvaluation;
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStats;
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage;
import org.neo4j.cypher.internal.preparser.FullyParsedQuery;
import org.neo4j.dbms.systemgraph.DefaultQueryLanguageLookup;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.config.FabricConstants;
import org.neo4j.fabric.eval.Catalog;
import org.neo4j.fabric.eval.UseEvaluation;
import org.neo4j.fabric.executor.QueryStatementLifecycles.StatementLifecycle;
import org.neo4j.fabric.planning.FabricPlan;
import org.neo4j.fabric.planning.FabricPlanner;
import org.neo4j.fabric.planning.Fragment;
import org.neo4j.fabric.stream.DelegatingFragmentResult;
import org.neo4j.fabric.stream.FragmentResult;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Records;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.StatementResults;
import org.neo4j.fabric.stream.summary.MergedSummary;
import org.neo4j.fabric.stream.summary.PlanlessSummary;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.kernel.impl.query.QueryRoutingMonitor;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.notifications.NotificationImplementation;
import org.neo4j.notifications.StandardGqlStatusObject;
import org.neo4j.scheduler.CallableExecutor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

public class FabricExecutor {
    public static final String WRITING_IN_READ_NOT_ALLOWED_MSG = "Writing in read access mode not allowed";
    private final FabricConfig.DataStream dataStreamConfig;
    private final Supplier<FabricConfig.Profiling> profilingConfig;
    private final FabricPlanner planner;
    private final UseEvaluation useEvaluation;
    private final InternalLog log;
    private final QueryStatementLifecycles statementLifecycles;
    private final CallableExecutor fabricWorkerExecutor;
    private final QueryRoutingMonitor queryRoutingMonitor;
    private final InternalUsageStats internalUsageStats;
    private final DefaultQueryLanguageLookup defaultQueryLanguageLookup;
    private final Clock clock;

    public FabricExecutor(
            FabricConfig config,
            FabricPlanner planner,
            UseEvaluation useEvaluation,
            InternalLogProvider internalLog,
            QueryStatementLifecycles statementLifecycles,
            CallableExecutor fabricWorkerExecutor,
            Monitors monitors,
            InternalUsageStats internalUsageStats,
            DefaultQueryLanguageLookup defaultQueryLanguageLookup,
            Clock clock) {
        this.dataStreamConfig = config.getDataStream();
        this.profilingConfig = config::getProfiling;
        this.planner = planner;
        this.useEvaluation = useEvaluation;
        this.log = internalLog.getLog(getClass());
        this.statementLifecycles = statementLifecycles;
        this.fabricWorkerExecutor = fabricWorkerExecutor;
        this.queryRoutingMonitor = monitors.newMonitor(QueryRoutingMonitor.class);
        this.internalUsageStats = internalUsageStats;
        this.defaultQueryLanguageLookup = defaultQueryLanguageLookup;
        this.clock = clock;
    }

    public StatementResult run(FabricTransaction fabricTransaction, String statement, MapValue parameters) {
        var transactionBinding = fabricTransaction.transactionBinding();
        var lifecycle = statementLifecycles.create(
                fabricTransaction.getTransactionInfo(), statement, parameters, transactionBinding);

        lifecycle.startProcessing();

        var procedures = fabricTransaction.contextlessProcedures();
        var signatureResolver = SignatureResolver.from(procedures);

        try {
            var defaultGraphName = fabricTransaction.getTransactionInfo().getSessionDatabaseReference();
            var catalog = fabricTransaction.getCatalogSnapshot();

            final var defaultLanguage = defaultQueryLanguageLookup.dbDefaultQueryLanguage(
                    fabricTransaction.defaultQueryLanguageScope(),
                    defaultGraphName.namedDatabaseId(),
                    planner.cypherConfig().systemDefaultLanguage());

            final var shadowedFunctions = fabricTransaction
                    .contextlessProcedures()
                    .shadowedNamespaces(QueryLanguage.toKernelScope(defaultLanguage));

            var plannerInstance = planner.instance(
                    signatureResolver,
                    statement,
                    parameters,
                    defaultGraphName,
                    catalog,
                    internalUsageStats,
                    fabricTransaction.cancellationChecker(),
                    defaultLanguage,
                    asScala(shadowedFunctions).toSet());
            lifecycle.donePreParsing(plannerInstance.query());
            var plan = plannerInstance.plan();
            var query = plan.query();

            lifecycle.doneFabricProcessing(
                    plan, plannerInstance.query().options().offset().offset());

            var accessMode = fabricTransaction.getTransactionInfo().getAccessMode();

            if (plan.debugOptions().logPlan()) {
                log.debug(String.format("Fabric plan: %s", Fragment.pretty().asString(query)));
            }

            var evaluator =
                    StaticEvaluation.from(procedures, plannerInstance.query().resolvedLanguage());
            var statementResult = fabricTransaction.execute(ctx -> {
                var useEvaluator =
                        useEvaluation.instance(evaluator, plannerInstance.signatureResolver(), statement, catalog);
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

            return statementResult;
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
        private final List<NotificationImplementation> planNotifications;
        private final StatementLifecycle lifecycle;
        private final AccessMode accessMode;
        private final ProfilingContext profilingContext;

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
            this.accessMode = accessMode;
            var filteredNotifications = plan.notifications()
                    .filter(notificationConfiguration::includes)
                    .toList();
            planNotifications = asJava(filteredNotifications);

            if (plan.executionType() == FabricPlan.PROFILE()) {
                profilingContext = new ProfilingContextImpl(
                        lifecycle.getMonitoredQuery(), profilingConfig.get().outputDir(), clock);
            } else {
                profilingContext = ProfilingContext.NO_OP;
            }
        }

        StatementResult run() {
            lifecycle.startExecution(false);
            var query = plan.query();

            // EXPLAIN for multi-graph queries returns only fabric plan,
            // because it is very hard to produce anything better without actually executing the query
            if (plan.executionType() == FabricPlan.EXPLAIN() && plan.inCompositeContext()) {
                lifecycle.endSuccess();

                Set<GqlStatusObject> gqlStatusObjects = new HashSet<>(planNotifications);
                // EXPLAIN queries always give OMITTED RESULT
                gqlStatusObjects.add(StandardGqlStatusObject.OMITTED_RESULT);
                return StatementResults.emptyStream(
                        asJava(query.resultColumns()),
                        new MergedSummary(
                                plan.query().description(),
                                QueryStatistics.EMPTY,
                                new HashSet<>(planNotifications),
                                gqlStatusObjects),
                        EffectiveQueryType.queryExecutionType(plan, accessMode));
            } else {
                FragmentResult fragmentResult = run(query, null);
                return new FabricExecutorResult(
                        fragmentResult, planNotifications, query.producesResults(), lifecycle, profilingContext);
            }
        }

        private FragmentResult run(Fragment fragment, Record argument) {
            return switch (fragment) {
                case Fragment.Init init -> runInit();
                case Fragment.Apply apply ->
                    apply.inTransactionsParameters().isEmpty()
                            ? runApply(apply, argument)
                            : runCallInTransactions(apply, argument);
                case Fragment.Union union -> runUnion(union, argument);
                case Fragment.Exec exec -> runExec(exec, argument);
                default -> throw notImplemented("Invalid query fragment", fragment);
            };
        }

        private FragmentResult runInit() {
            return StatementResults.oneRecord(List.of(), Records.empty(), null);
        }

        private FragmentResult runApply(Fragment.Apply apply, Record argument) {
            // TODO: merge executionType here for subqueries
            // For now, just return global value as seen by fabric
            var queryExecutionType = EffectiveQueryType.queryExecutionType(plan, accessMode);
            FragmentResult input = run(apply.input(), argument);

            var remoteBatchExecutor = new RemoteBatchExecutor(
                    fabricWorkerExecutor,
                    record -> run(apply.inner(), record),
                    FabricConstants.BUFFER_SIZE,
                    FabricConstants.STREAM_CONCURRENCY);
            return new ApplyExecutor(
                    asJava(apply.outputColumns()),
                    input,
                    apply.inner().outputColumns().isEmpty(),
                    queryExecutionType,
                    remoteBatchExecutor,
                    record -> run(apply.inner(), record),
                    record -> isRemoteFragment(apply.inner(), record));
        }

        private boolean isRemoteFragment(Fragment fragment, Record argument) {
            if (fragment instanceof Fragment.Exec exec) {
                Map<String, AnyValue> argumentValues = SingleQueryFragmentExecutor.argumentValues(fragment, argument);
                Catalog.Graph graph = useEvaluator
                        .evaluate(
                                exec.use().graphSelection(),
                                queryParams,
                                argumentValues,
                                ctx.getSessionDatabaseReference())
                        .graph();
                TransactionMode transactionMode = SingleQueryFragmentExecutor.getTransactionMode(
                        plan, accessMode, exec.queryType(), graph.reference().toPrettyString());

                var location = ctx.locationOf(graph, transactionMode.requiresWrite());
                return location instanceof Location.Remote;
            }

            return false;
        }

        private FragmentResult runUnion(Fragment.Union union, Record argument) {
            FragmentResult lhs = run(union.lhs(), argument);
            FragmentResult rhs = run(union.rhs(), argument);
            FragmentResult mergedResult = StatementResults.mergeUnion(lhs, rhs);

            if (union.distinct()) {
                return StatementResults.distinct(mergedResult);
            }

            return mergedResult;
        }

        private FragmentResult runExec(Fragment.Exec fragment, Record argument) {
            return new StandardQueryExecutor(
                            fragment,
                            plannerInstance,
                            ctx,
                            useEvaluator,
                            plan,
                            queryParams,
                            accessMode,
                            lifecycle,
                            queryRoutingMonitor,
                            tracer(),
                            profilingContext,
                            FabricStatementExecution.this::run)
                    .run(argument);
        }

        private FragmentResult runCallInTransactions(Fragment.Apply fragment, Record argument) {
            return new CallInTransactionsExecutor(
                            fragment,
                            plannerInstance,
                            ctx,
                            useEvaluator,
                            plan,
                            queryParams,
                            accessMode,
                            lifecycle,
                            queryRoutingMonitor,
                            tracer(),
                            EffectiveQueryType.queryExecutionType(plan, accessMode),
                            profilingContext,
                            FabricStatementExecution.this::run)
                    .run(argument);
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

        private RuntimeException notImplemented(String msg, Object object) {
            return notImplemented(msg, object.toString());
        }

        private RuntimeException notImplemented(String msg, String info) {
            return InvalidSemanticsException.internalError(this.getClass().getSimpleName(), msg + ": " + info);
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
            return new DelegatingFragmentResult(fragmentResult) {

                boolean completed = false;

                @Override
                public Record next() {
                    Record record;
                    try {
                        record = super.next();
                    } catch (RuntimeException e) {
                        String rec = e.getClass().getSimpleName() + ": " + e.getMessage();
                        trace(id, "error", rec);
                        throw e;
                    }
                    if (record == null) {
                        completed = true;
                        trace(id, "complete", "complete");
                    } else {
                        String rec = IntStream.range(0, record.size())
                                .mapToObj(i -> record.getValue(i).toString())
                                .collect(Collectors.joining(", ", "[", "]"));
                        trace(id, "output", rec);
                    }

                    return record;
                }

                @Override
                public PlanlessSummary consume() {
                    if (!completed) {
                        trace(id, "cancel", "cancel");
                    }

                    return delegate.consume();
                }
            };
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
