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
package org.neo4j.router;

import static org.neo4j.cypher.internal.tracing.CompilationTracer.NO_COMPILATION_TRACING;
import static org.neo4j.router.impl.query.ProcessedQueryInfoCache.MONITOR_TAG;
import static org.neo4j.scheduler.Group.CYPHER_CACHE;
import static org.neo4j.scheduler.JobMonitoringParams.systemJob;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.PreParser;
import org.neo4j.cypher.internal.cache.CacheTracer;
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory;
import org.neo4j.cypher.internal.compiler.CypherParsing;
import org.neo4j.cypher.internal.compiler.CypherParsingConfig;
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration;
import org.neo4j.cypher.internal.config.CypherConfiguration;
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bootstrap.CommonQueryRouterBoostrap;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.QueryStatementLifecycles;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitorScheduler;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.kernel.impl.query.QueryRoutingMonitor;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.router.impl.CommunityLocationService;
import org.neo4j.router.impl.QueryRouterImpl;
import org.neo4j.router.impl.bolt.QueryRouterBoltSpi;
import org.neo4j.router.impl.query.DefaultDatabaseReferenceResolver;
import org.neo4j.router.impl.query.ProcessedQueryInfoCache;
import org.neo4j.router.impl.query.QueryProcessorImpl;
import org.neo4j.router.impl.transaction.QueryRouterTransactionMonitor;
import org.neo4j.router.impl.transaction.RouterTransactionManager;
import org.neo4j.router.impl.transaction.database.LocalDatabaseTransactionFactory;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.transaction.DatabaseTransactionFactory;
import org.neo4j.router.transaction.RoutingInfo;
import org.neo4j.router.transaction.TransactionLookup;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

public class CommunityQueryRouterBoostrap extends CommonQueryRouterBoostrap {

    private final LogService logService;
    private final DatabaseContextProvider<? extends DatabaseContext> databaseProvider;
    private final DatabaseReferenceRepository databaseReferenceRepo;
    private final AbstractSecurityLog securityLog;

    public CommunityQueryRouterBoostrap(
            LifeSupport lifeSupport,
            Dependencies dependencies,
            LogService logService,
            DatabaseContextProvider<? extends DatabaseContext> databaseProvider,
            DatabaseReferenceRepository databaseReferenceRepo,
            AbstractSecurityLog securityLog) {
        super(lifeSupport, dependencies, databaseProvider);
        this.logService = logService;
        this.databaseProvider = databaseProvider;
        this.databaseReferenceRepo = databaseReferenceRepo;
        this.securityLog = securityLog;
    }

    public BoltGraphDatabaseManagementServiceSPI bootstrapServices(
            DatabaseManagementService databaseManagementService) {
        bootstrapCommonServices(databaseManagementService, logService);
        return createBoltDatabaseManagementServiceProvider();
    }

    protected BoltGraphDatabaseManagementServiceSPI getCompositeDatabaseStack() {
        return (databaseName, memoryTracker) -> new BoltGraphDatabaseServiceSPI() {

            @Override
            public BoltTransaction beginTransaction(
                    KernelTransaction.Type type,
                    LoginContext loginContext,
                    ClientConnectionInfo clientInfo,
                    List<String> bookmarks,
                    Duration txTimeout,
                    AccessMode accessMode,
                    Map<String, Object> txMetadata,
                    RoutingContext routingContext,
                    QueryExecutionConfiguration queryExecutionConfiguration) {
                // If a piece of code tries to use this in Community edition, it means a bug
                throw new InvalidSemanticsException("Composite database is not supported in Community Edition");
            }

            @Override
            public DatabaseReference getDatabaseReference() {
                // If a piece of code tries to use this in Community edition, it means a bug
                throw new InvalidSemanticsException("Composite database is not supported in Community Edition");
            }
        };
    }

    protected LocationService createLocationService(RoutingInfo routingInfo) {
        return new CommunityLocationService();
    }

    protected DatabaseTransactionFactory<Location.Remote> createRemoteDatabaseTransactionFactory() {
        // If a piece of code tries to use this in Community edition, it means a bug
        return (location, transactionInfo, bookmarkManager, terminationCallback, constituentTransactionFactory) -> {
            throw new IllegalStateException("Remote transactions are not supported in Community Edition");
        };
    }

    protected BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider() {
        var config = resolve(Config.class);
        var cypherConfig = CypherConfiguration.fromConfig(config);
        var jobScheduler = resolve(JobScheduler.class);
        var monitoredExecutor = jobScheduler.monitoredJobExecutor(CYPHER_CACHE);
        var monitors = resolve(Monitors.class);
        var cacheFactory = new ExecutorBasedCaffeineCacheFactory(
                job -> monitoredExecutor.execute(systemJob("Query plan cache maintenance"), job));
        var targetCache = new ProcessedQueryInfoCache(
                cacheFactory, cypherConfig.queryCacheSize(), monitors.newMonitor(CacheTracer.class, MONITOR_TAG));
        var preParser = new PreParser(cypherConfig);
        CypherPlannerConfiguration plannerConfig =
                CypherPlannerConfiguration.fromCypherConfiguration(cypherConfig, config, true, false);
        var parsing = new CypherParsing(
                null,
                CypherParsingConfig.fromCypherPlannerConfiguration(plannerConfig),
                plannerConfig.queryRouterEnabled(),
                plannerConfig.queryRouterForCompositeQueriesEnabled(),
                resolve(InternalSyntaxUsageStats.class));
        DefaultDatabaseReferenceResolver databaseReferenceResolver =
                new DefaultDatabaseReferenceResolver(databaseReferenceRepo);
        var databaseManager = (DatabaseContextProvider<DatabaseContext>) resolve(DatabaseContextProvider.class);
        var tracers = resolve(Tracers.class);
        SystemNanoClock systemNanoClock = resolve(SystemNanoClock.class);
        var statementLifecycles = new QueryStatementLifecycles(
                databaseManager, monitors, config, tracers.getLockTracer(), systemNanoClock);
        var globalProcedures = resolve(GlobalProcedures.class);
        var useQueryRouterForCompositeQueries =
                config.get(GraphDatabaseInternalSettings.composite_queries_with_query_router);

        var transactionIdTracker = resolve(LocalGraphTransactionIdTracker.class);
        var txMonitor = new QueryRouterTransactionMonitor(config, systemNanoClock, this.logService);
        var transactionCheckInterval = config.get(GraphDatabaseSettings.transaction_monitor_check_interval)
                .toMillis();
        registerWithLifecycle(new TransactionMonitorScheduler(txMonitor, jobScheduler, transactionCheckInterval, null));
        var routerTxManager = new RouterTransactionManager(txMonitor);
        dependencies.satisfyDependency(routerTxManager);
        TransactionManager compositeTxManager = null;
        if (dependencies.containsDependency(TransactionManager.class)) {
            compositeTxManager = dependencies.resolveDependency(TransactionManager.class);
        }
        var transactionLookup = new TransactionLookup(routerTxManager, compositeTxManager);
        dependencies.satisfyDependency(transactionLookup);
        var queryRouter = new QueryRouterImpl(
                config,
                databaseReferenceResolver,
                this::createLocationService,
                new QueryProcessorImpl(
                        targetCache, preParser, parsing, NO_COMPILATION_TRACING, globalProcedures, databaseProvider),
                getLocalDatabaseTransactionFactory(databaseProvider, transactionIdTracker),
                createRemoteDatabaseTransactionFactory(),
                new ErrorReporter(this.logService),
                systemNanoClock,
                transactionIdTracker,
                statementLifecycles,
                monitors.newMonitor(QueryRoutingMonitor.class),
                routerTxManager,
                securityLog);
        dependencies.satisfyDependency(queryRouter);
        return new QueryRouterBoltSpi.DatabaseManagementService(
                queryRouter, databaseReferenceResolver, getCompositeDatabaseStack(), useQueryRouterForCompositeQueries);
    }

    protected LocalDatabaseTransactionFactory getLocalDatabaseTransactionFactory(
            DatabaseContextProvider<? extends DatabaseContext> databaseProvider,
            LocalGraphTransactionIdTracker transactionIdTracker) {
        return new LocalDatabaseTransactionFactory(databaseProvider, transactionIdTracker);
    }

    @Override
    protected <T> T resolve(Class<T> type) {
        return dependencies.resolveDependency(type);
    }

    protected LogService getLogService() {
        return logService;
    }
}
