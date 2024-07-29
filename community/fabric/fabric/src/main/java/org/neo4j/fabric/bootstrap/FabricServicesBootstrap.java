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
package org.neo4j.fabric.bootstrap;

import static org.neo4j.scheduler.Group.CYPHER_CACHE;
import static org.neo4j.scheduler.Group.FABRIC_WORKER;
import static org.neo4j.scheduler.JobMonitoringParams.systemJob;

import java.util.concurrent.Executor;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory;
import org.neo4j.cypher.internal.config.CypherConfiguration;
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bolt.BoltFabricDatabaseManagementService;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.eval.CommunityCatalogManager;
import org.neo4j.fabric.eval.DatabaseLookup;
import org.neo4j.fabric.eval.UseEvaluation;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.fabric.executor.QueryStatementLifecycles;
import org.neo4j.fabric.executor.ThrowingFabricRemoteExecutor;
import org.neo4j.fabric.planning.FabricPlanner;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.fabric.transaction.FabricTransactionMonitor;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitorScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.SystemNanoClock;

public abstract class FabricServicesBootstrap extends CommonQueryRouterBootstrap {

    private final FabricConfig fabricConfig;
    protected final LogService logService;
    private final Config config;
    private final AvailabilityGuard availabilityGuard;
    protected final DatabaseContextProvider<? extends DatabaseContext> databaseProvider;
    protected final AbstractSecurityLog securityLog;
    protected final DatabaseReferenceRepository databaseReferenceRepo;

    public FabricServicesBootstrap(
            LifeSupport lifeSupport,
            Dependencies dependencies,
            LogService logService,
            AbstractSecurityLog securityLog,
            DatabaseContextProvider<? extends DatabaseContext> databaseProvider,
            DatabaseReferenceRepository databaseReferenceRepo) {
        super(lifeSupport, dependencies, databaseProvider);

        this.logService = logService;
        this.securityLog = securityLog;
        this.databaseProvider = databaseProvider;
        this.databaseReferenceRepo = databaseReferenceRepo;

        config = dependencies.resolveDependency(Config.class);
        availabilityGuard = dependencies.resolveDependency(AvailabilityGuard.class);

        fabricConfig = bootstrapFabricConfig();
    }

    public BoltGraphDatabaseManagementServiceSPI bootstrapServices(
            DatabaseManagementService databaseManagementService) {
        super.bootstrapCommonServices(databaseManagementService, logService);
        InternalLogProvider internalLogProvider = logService.getInternalLogProvider();

        @SuppressWarnings("unchecked")
        var databaseManager = (DatabaseContextProvider<DatabaseContext>) resolve(DatabaseContextProvider.class);
        var fabricDatabaseManager = register(createFabricDatabaseManager(fabricConfig), FabricDatabaseManager.class);

        var jobScheduler = resolve(JobScheduler.class);
        var monitors = resolve(Monitors.class);
        var tracers = resolve(Tracers.class);

        var remoteExecutor = bootstrapRemoteStack();
        var systemNanoClock = resolve(SystemNanoClock.class);

        var localGraphTransactionIdTracker = resolve(LocalGraphTransactionIdTracker.class);
        var localExecutor = register(
                new FabricLocalExecutor(fabricConfig, fabricDatabaseManager, localGraphTransactionIdTracker),
                FabricLocalExecutor.class);

        var transactionMonitor = register(
                new FabricTransactionMonitor(config, systemNanoClock, logService, fabricConfig),
                FabricTransactionMonitor.class);

        var transactionCheckInterval = config.get(GraphDatabaseSettings.transaction_monitor_check_interval)
                .toMillis();
        register(
                new TransactionMonitorScheduler(transactionMonitor, jobScheduler, transactionCheckInterval, null),
                TransactionMonitorScheduler.class);

        var errorReporter = new ErrorReporter(logService);
        var catalogManager = register(createCatalogManager(fabricDatabaseManager), CatalogManager.class);

        var globalProcedures = resolve(GlobalProcedures.class);

        var internalSyntaxUsageStats = resolve(InternalSyntaxUsageStats.class);

        register(
                new TransactionManager(
                        remoteExecutor,
                        localExecutor,
                        catalogManager,
                        transactionMonitor,
                        securityLog,
                        systemNanoClock,
                        config,
                        availabilityGuard,
                        errorReporter,
                        globalProcedures),
                TransactionManager.class);

        var cypherConfig = CypherConfiguration.fromConfig(config);
        var statementLifecycles = new QueryStatementLifecycles(
                databaseManager, monitors, config, tracers.getLockTracer(), systemNanoClock);
        var monitoredExecutor = jobScheduler.monitoredJobExecutor(CYPHER_CACHE);
        var cacheFactory = new ExecutorBasedCaffeineCacheFactory(
                job -> monitoredExecutor.execute(systemJob("Query plan cache maintenance"), job));
        var planner =
                register(new FabricPlanner(fabricConfig, cypherConfig, monitors, cacheFactory), FabricPlanner.class);
        var useEvaluation = register(new UseEvaluation(), UseEvaluation.class);

        register(new FabricReactorHooksService(errorReporter), FabricReactorHooksService.class);

        Executor fabricWorkerExecutor = jobScheduler.executor(FABRIC_WORKER);
        var fabricExecutor = new FabricExecutor(
                fabricConfig,
                planner,
                useEvaluation,
                internalLogProvider,
                statementLifecycles,
                fabricWorkerExecutor,
                monitors,
                internalSyntaxUsageStats);
        register(fabricExecutor, FabricExecutor.class);
        return createBoltDatabaseManagementServiceProvider();
    }

    protected DatabaseLookup createDatabaseLookup(FabricDatabaseManager fabricDatabaseManager) {
        return new DatabaseLookup.Default(fabricDatabaseManager);
    }

    private BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider() {
        FabricExecutor fabricExecutor = resolve(FabricExecutor.class);
        TransactionManager transactionManager = resolve(TransactionManager.class);
        FabricDatabaseManager fabricDatabaseManager = resolve(FabricDatabaseManager.class);
        LocalGraphTransactionIdTracker localGraphTransactionIdTracker = resolve(LocalGraphTransactionIdTracker.class);
        return new BoltFabricDatabaseManagementService(
                fabricExecutor,
                fabricConfig,
                transactionManager,
                fabricDatabaseManager,
                localGraphTransactionIdTracker);
    }

    protected abstract FabricDatabaseManager createFabricDatabaseManager(FabricConfig fabricConfig);

    protected abstract CatalogManager createCatalogManager(FabricDatabaseManager fabricDatabaseManager);

    protected abstract FabricRemoteExecutor bootstrapRemoteStack();

    protected abstract FabricConfig bootstrapFabricConfig();

    public static class Community extends FabricServicesBootstrap {
        public Community(
                LifeSupport lifeSupport,
                Dependencies dependencies,
                LogService logService,
                DatabaseContextProvider<? extends DatabaseContext> databaseProvider,
                DatabaseReferenceRepository databaseReferenceRepo) {
            super(
                    lifeSupport,
                    dependencies,
                    logService,
                    CommunitySecurityLog.NULL_LOG,
                    databaseProvider,
                    databaseReferenceRepo);
        }

        @Override
        protected FabricDatabaseManager createFabricDatabaseManager(FabricConfig fabricConfig) {
            return new FabricDatabaseManager(fabricConfig, databaseProvider, databaseReferenceRepo);
        }

        @Override
        protected CatalogManager createCatalogManager(FabricDatabaseManager fabricDatabaseManager) {
            return new CommunityCatalogManager(
                    createDatabaseLookup(fabricDatabaseManager), this::getSystemDbTransactionIdStore);
        }

        private TransactionIdStore getSystemDbTransactionIdStore() {
            return databaseProvider
                    .getSystemDatabaseContext()
                    .dependencies()
                    .resolveDependency(TransactionIdStore.class);
        }

        @Override
        protected FabricRemoteExecutor bootstrapRemoteStack() {
            return new ThrowingFabricRemoteExecutor();
        }

        @Override
        protected FabricConfig bootstrapFabricConfig() {
            var config = resolve(Config.class);
            return FabricConfig.from(config);
        }
    }
}
