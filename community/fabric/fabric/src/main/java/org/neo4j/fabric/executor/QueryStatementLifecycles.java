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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingChangeListener;
import org.neo4j.cypher.internal.CypherDeprecationNotificationsProvider;
import org.neo4j.cypher.internal.CypherQueryObfuscator;
import org.neo4j.cypher.internal.notification.InternalNotification;
import org.neo4j.cypher.internal.preparser.PreParsedQuery;
import org.neo4j.cypher.internal.util.InputPosition;
import org.neo4j.cypher.internal.util.ObfuscationMetadata;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.planning.FabricPlan;
import org.neo4j.fabric.transaction.StatementLifecycleTransactionInfo;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.ExtendedQueryStatistics;
import org.neo4j.kernel.impl.api.ExecutingQueryFactory;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.lock.LockTracer;
import org.neo4j.memory.HeapHighWaterMarkTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class QueryStatementLifecycles {
    private final DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider;
    private final QueryExecutionMonitor dbmsMonitor;
    private final ExecutingQueryFactory executingQueryFactory;
    private final boolean shardQueryLogEnabled;
    private final Config config;
    private final boolean exposeFullyObfuscatedQueryView;

    public QueryStatementLifecycles(
            DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider,
            Monitors dbmsMonitors,
            Config config,
            LockTracer systemLockTracer,
            SystemNanoClock systemNanoClock) {
        this.databaseContextProvider = databaseContextProvider;
        this.dbmsMonitor = dbmsMonitors.newMonitor(QueryExecutionMonitor.class);
        this.executingQueryFactory =
                new ExecutingQueryFactory(systemNanoClock, setupCpuClockAtomicReference(config), systemLockTracer);
        this.shardQueryLogEnabled = config.get(GraphDatabaseInternalSettings.shard_query_log_enabled);
        this.config = config;
        this.exposeFullyObfuscatedQueryView =
                config.get(GraphDatabaseInternalSettings.expose_fully_obfuscated_query_view);
    }

    private boolean obfuscateLiterals() {
        return config.get(GraphDatabaseSettings.log_queries_obfuscate_literals);
    }

    private static AtomicReference<CpuClock> setupCpuClockAtomicReference(Config config) {
        AtomicReference<CpuClock> cpuClock = new AtomicReference<>(CpuClock.NOT_AVAILABLE);
        SettingChangeListener<Boolean> cpuClockUpdater = (before, after) -> {
            if (after) {
                cpuClock.set(CpuClock.CPU_CLOCK);
            } else {
                cpuClock.set(CpuClock.NOT_AVAILABLE);
            }
        };
        cpuClockUpdater.accept(null, config.get(GraphDatabaseSettings.track_query_cpu_time));
        config.addListener(GraphDatabaseSettings.track_query_cpu_time, cpuClockUpdater);
        return cpuClock;
    }

    public StatementLifecycle create(
            StatementLifecycleTransactionInfo transactionInfo,
            String statement,
            MapValue params,
            ExecutingQuery.TransactionBinding transactionBinding) {
        var executingQuery = executingQueryFactory.createUnbound(
                statement,
                params,
                transactionInfo.getClientConnectionInfo(),
                transactionInfo.getLoginContext().subject().executingUser(),
                transactionInfo.getLoginContext().subject().authenticatedUser(),
                transactionInfo.getTxMetadata());

        if (transactionBinding != null) {
            executingQuery.onTransactionBound(transactionBinding);
        }

        if (!shardQueryLogEnabled
                && transactionInfo.getSessionDatabaseReference().isShard()) {
            return new EmptyStatementLifecycle(executingQuery);
        }

        return new StatementLifecycleImpl(executingQuery);
    }

    private static class EmptyStatementLifecycle implements StatementLifecycle {

        private final ExecutingQuery executingQuery;

        private EmptyStatementLifecycle(ExecutingQuery executingQuery) {
            this.executingQuery = executingQuery;
        }

        @Override
        public void startProcessing() {}

        @Override
        public void donePreParsing(PreParsedQuery preParsedQuery) {}

        @Override
        public void doneFabricProcessing(FabricPlan plan, int preParserOffset) {}

        @Override
        public void onObfuscatorReady(ObfuscationMetadata obfuscationMetadata, InputPosition preParserOffset) {}

        @Override
        public void doneRouterProcessing(
                ObfuscationMetadata obfuscateMetadata,
                InputPosition preParserOffset,
                boolean inCompositeContext,
                Set<InternalNotification> notifications) {}

        @Override
        public void startExecution(boolean shouldLogIfSingleQuery) {}

        @Override
        public void endSuccess() {}

        @Override
        public void endFailure(Throwable failure) {}

        @Override
        public void endFailure(String reason, Status status, ErrorGqlStatusObject errorGqlStatusObject) {}

        @Override
        public ExecutingQuery getMonitoredQuery() {
            return executingQuery;
        }

        @Override
        public QueryExecutionMonitor getChildQueryMonitor() {
            return QueryExecutionMonitor.NO_OP;
        }
    }

    public class StatementLifecycleImpl implements StatementLifecycle {
        private final ExecutingQuery executingQuery;

        private QueryExecutionMonitor dbMonitor;
        private MonitoringMode monitoringMode;

        private StatementLifecycleImpl(ExecutingQuery executingQuery) {
            this.executingQuery = executingQuery;
        }

        @Override
        public void startProcessing() {
            getQueryExecutionMonitor().startProcessing(executingQuery);
        }

        @Override
        public void donePreParsing(PreParsedQuery preParsedQuery) {
            executingQuery.onPreparseReady(preParsedQuery.resolvedLanguage());
        }

        @Override
        public void doneFabricProcessing(FabricPlan plan, int preParserOffset) {
            executingQuery.onObfuscatorReady(
                    CypherQueryObfuscator.apply(
                            plan.obfuscationMetadata(), obfuscateLiterals(), exposeFullyObfuscatedQueryView),
                    preParserOffset);
            executingQuery.onFabricDeprecationNotificationsProviderReady(plan.deprecationNotificationsProvider());

            if (plan.inCompositeContext()) {
                monitoringMode = new ParentChildMonitoringMode();
            } else {
                monitoringMode = new SingleQueryMonitoringMode();
            }
        }

        @Override
        public void onObfuscatorReady(ObfuscationMetadata obfuscationMetadata, InputPosition preParserOffset) {
            executingQuery.onObfuscatorReady(
                    CypherQueryObfuscator.apply(
                            obfuscationMetadata, obfuscateLiterals(), exposeFullyObfuscatedQueryView),
                    preParserOffset.offset());
        }

        @Override
        public void doneRouterProcessing(
                ObfuscationMetadata obfuscateMetadata,
                InputPosition preParserOffset,
                boolean inCompositeContext,
                Set<InternalNotification> notifications) {
            executingQuery.onObfuscatorReady(
                    CypherQueryObfuscator.apply(obfuscateMetadata, obfuscateLiterals(), exposeFullyObfuscatedQueryView),
                    preParserOffset.offset());
            executingQuery.onFabricDeprecationNotificationsProviderReady(
                    CypherDeprecationNotificationsProvider.fromJava(preParserOffset, notifications));

            if (inCompositeContext) {
                monitoringMode = new ParentChildMonitoringMode();
            } else {
                monitoringMode = new SingleQueryMonitoringMode();
            }
        }

        @Override
        public void startExecution(boolean shouldLogIfSingleQuery) {
            monitoringMode.startExecution(shouldLogIfSingleQuery);
        }

        @Override
        public void endSuccess() {
            QueryExecutionMonitor monitor = getQueryExecutionMonitor();
            monitor.beforeEnd(executingQuery, true);
            monitor.endSuccess(executingQuery);
        }

        private QueryExecutionMonitor getQueryExecutionMonitor() {
            return getDbMonitor().orElse(dbmsMonitor);
        }

        @Override
        public void endFailure(Throwable failure) {
            QueryExecutionMonitor monitor = getQueryExecutionMonitor();
            Status status = failure instanceof Status.HasStatus s ? s.status() : null;
            ErrorGqlStatusObject errorGqlStatusObject = failure instanceof ErrorGqlStatusObject eg ? eg : null;
            monitor.beforeEnd(executingQuery, false);
            monitor.endFailure(executingQuery, failure.getMessage(), status, errorGqlStatusObject);
        }

        @Override
        public void endFailure(String reason, Status status, ErrorGqlStatusObject errorGqlStatusObject) {
            QueryExecutionMonitor monitor = getQueryExecutionMonitor();
            monitor.beforeEnd(executingQuery, false);
            monitor.endFailure(executingQuery, reason, status, errorGqlStatusObject);
        }

        private Optional<QueryExecutionMonitor> getDbMonitor() {
            if (dbMonitor == null) {
                executingQuery
                        .databaseId()
                        .flatMap(databaseContextProvider::getDatabaseContext)
                        .map(dbm -> dbm.dependencies().resolveDependency(Monitors.class))
                        .map(monitors -> monitors.newMonitor(QueryExecutionMonitor.class))
                        .ifPresent(monitor -> dbMonitor = monitor);
            }

            return Optional.ofNullable(dbMonitor);
        }

        @Override
        public ExecutingQuery getMonitoredQuery() {
            return executingQuery;
        }

        @Override
        public QueryExecutionMonitor getChildQueryMonitor() {
            return monitoringMode.getChildQueryMonitor();
        }

        boolean isParentChildMonitoringMode() {
            return monitoringMode.isParentChildMonitoringMode();
        }

        private abstract static class MonitoringMode {
            abstract boolean isParentChildMonitoringMode();

            abstract QueryExecutionMonitor getChildQueryMonitor();

            abstract void startExecution(Boolean shouldLogIfSingleQuery);
        }

        private class SingleQueryMonitoringMode extends MonitoringMode {
            @Override
            boolean isParentChildMonitoringMode() {
                return false;
            }

            @Override
            void startExecution(Boolean shouldLogIfSingleQuery) {
                // Query state events triggered by cypher engine
                if (shouldLogIfSingleQuery) {
                    getQueryExecutionMonitor().startExecution(executingQuery);
                }
            }

            @Override
            QueryExecutionMonitor getChildQueryMonitor() {
                // Query monitoring events handled by fabric
                return QueryExecutionMonitor.NO_OP;
            }
        }

        private class ParentChildMonitoringMode extends MonitoringMode {
            @Override
            boolean isParentChildMonitoringMode() {
                return true;
            }

            @Override
            void startExecution(Boolean shouldLogIfSingleQuery) {
                if (!shouldLogIfSingleQuery) {
                    getQueryExecutionMonitor().startExecution(executingQuery);
                    executingQuery.onCompilationCompleted(null, null, null, 0);
                    executingQuery.onExecutionStarted(
                            HeapHighWaterMarkTracker.NONE, () -> ExtendedQueryStatistics.EMPTY);
                }
            }

            @Override
            QueryExecutionMonitor getChildQueryMonitor() {
                return getQueryExecutionMonitor();
            }
        }
    }

    public interface StatementLifecycle {

        void startProcessing();

        void donePreParsing(PreParsedQuery preParsedQuery);

        void doneFabricProcessing(FabricPlan plan, int preParserOffset);

        // Wire the obfuscator onto the {@link ExecutingQuery} as soon as obfuscation metadata is available.
        void onObfuscatorReady(ObfuscationMetadata obfuscationMetadata, InputPosition preParserOffset);

        void doneRouterProcessing(
                ObfuscationMetadata obfuscateMetadata,
                InputPosition preParserOffset,
                boolean inCompositeContext,
                Set<InternalNotification> notifications);

        void startExecution(boolean shouldLogIfSingleQuery);

        void endSuccess();

        void endFailure(Throwable failure);

        void endFailure(String reason, Status status, ErrorGqlStatusObject errorGqlStatusObject);

        ExecutingQuery getMonitoredQuery();

        QueryExecutionMonitor getChildQueryMonitor();
    }
}
