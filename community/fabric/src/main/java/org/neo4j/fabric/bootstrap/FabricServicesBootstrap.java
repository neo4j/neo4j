/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.bootstrap;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory;
import org.neo4j.cypher.internal.config.CypherConfiguration;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bolt.BoltFabricDatabaseManagementService;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerFactory;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.eval.CommunityCatalogManager;
import org.neo4j.fabric.eval.DatabaseLookup;
import org.neo4j.fabric.eval.UseEvaluation;
import org.neo4j.fabric.executor.FabricDatabaseAccess;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.fabric.executor.FabricStatementLifecycles;
import org.neo4j.fabric.executor.ThrowingFabricRemoteExecutor;
import org.neo4j.fabric.pipeline.SignatureResolver;
import org.neo4j.fabric.planning.FabricPlanner;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.fabric.transaction.FabricTransactionMonitor;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitorScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.scheduler.Group.CYPHER_CACHE;
import static org.neo4j.scheduler.Group.FABRIC_WORKER;
import static org.neo4j.scheduler.JobMonitoringParams.systemJob;

public abstract class FabricServicesBootstrap
{

    private final FabricConfig fabricConfig;
    private final Dependencies dependencies;
    private final LogService logService;
    private final ServiceBootstrapper serviceBootstrapper;
    private final Config config;

    public FabricServicesBootstrap( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
    {
        this.dependencies = dependencies;
        this.logService = logService;

        serviceBootstrapper = new ServiceBootstrapper( lifeSupport, dependencies );

        config = dependencies.resolveDependency( Config.class );

        fabricConfig = bootstrapFabricConfig();
    }

    protected <T> T register( T dependency, Class<T> dependencyType )
    {
        return serviceBootstrapper.registerService( dependency, dependencyType );
    }

    protected <T> T resolve( Class<T> type )
    {
        return dependencies.resolveDependency( type );
    }

    public void bootstrapServices()
    {
        LogProvider internalLogProvider = logService.getInternalLogProvider();

        @SuppressWarnings( "unchecked" )
        var databaseManager = (DatabaseManager<DatabaseContext>) resolve( DatabaseManager.class );
        var fabricDatabaseManager = register( createFabricDatabaseManager( fabricConfig ), FabricDatabaseManager.class );

        var jobScheduler = resolve( JobScheduler.class );
        var monitors = resolve( Monitors.class );

        var databaseAccess = createFabricDatabaseAccess();
        var remoteExecutor = bootstrapRemoteStack();
        var localExecutor = register( new FabricLocalExecutor( fabricConfig, fabricDatabaseManager, databaseAccess ), FabricLocalExecutor.class );

        var systemNanoClock = resolve( SystemNanoClock.class );
        var transactionMonitor = new FabricTransactionMonitor( systemNanoClock, logService, fabricConfig );

        var transactionCheckInterval = config.get( GraphDatabaseSettings.transaction_monitor_check_interval ).toMillis();
        register( new TransactionMonitorScheduler( transactionMonitor, jobScheduler, transactionCheckInterval, null ), TransactionMonitorScheduler.class );

        var errorReporter = new ErrorReporter( logService );
        register( new TransactionManager( remoteExecutor, localExecutor, errorReporter, fabricConfig, transactionMonitor ), TransactionManager.class );

        var cypherConfig = CypherConfiguration.fromConfig( config );

        Supplier<GlobalProcedures> proceduresSupplier = () -> resolve( GlobalProcedures.class );
        var catalogManager = register( createCatalogManger(), CatalogManager.class );
        var signatureResolver = new SignatureResolver( proceduresSupplier );
        var statementLifecycles = new FabricStatementLifecycles( databaseManager, monitors, config, systemNanoClock );
        var monitoredExecutor = jobScheduler.monitoredJobExecutor( CYPHER_CACHE );
        var cacheFactory = new ExecutorBasedCaffeineCacheFactory( job -> monitoredExecutor.execute( systemJob( "Query plan cache maintenance" ), job ) );
        var planner = register( new FabricPlanner( fabricConfig, cypherConfig, monitors, cacheFactory, signatureResolver ), FabricPlanner.class );
        var useEvaluation = register( new UseEvaluation( catalogManager, proceduresSupplier, signatureResolver ), UseEvaluation.class );

        register( new FabricReactorHooksService( errorReporter ), FabricReactorHooksService.class );

        Executor fabricWorkerExecutor = jobScheduler.executor( FABRIC_WORKER );
        var fabricExecutor = new FabricExecutor(
                fabricConfig, planner, useEvaluation, catalogManager, internalLogProvider, statementLifecycles, fabricWorkerExecutor );
        register( fabricExecutor, FabricExecutor.class );

        register( new TransactionBookmarkManagerFactory( fabricDatabaseManager ), TransactionBookmarkManagerFactory.class );
    }

    protected DatabaseLookup createDatabaseLookup()
    {
        Supplier<DatabaseManager<DatabaseContext>> databaseManagerProvider = () ->
                (DatabaseManager<DatabaseContext>) dependencies.resolveDependency( DatabaseManager.class );
        return new DatabaseLookup.Default( databaseManagerProvider );
    }

    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider(
            BoltGraphDatabaseManagementServiceSPI kernelDatabaseManagementService, DatabaseManagementService managementService, Monitors monitors,
            SystemNanoClock clock )
    {
        FabricExecutor fabricExecutor = dependencies.resolveDependency( FabricExecutor.class );
        TransactionManager transactionManager = dependencies.resolveDependency( TransactionManager.class );
        FabricDatabaseManager fabricDatabaseManager = dependencies.resolveDependency( FabricDatabaseManager.class );

        var serverConfig = dependencies.resolveDependency( Config.class );
        var bookmarkTimeout = serverConfig.get( GraphDatabaseSettings.bookmark_ready_timeout );

        var transactionIdTracker = new TransactionIdTracker( managementService, monitors, clock );

        var databaseManager = (DatabaseManager<DatabaseContext>) dependencies.resolveDependency( DatabaseManager.class );
        var databaseIdRepository = databaseManager.databaseIdRepository();
        var transactionBookmarkManagerFactory = dependencies.resolveDependency( TransactionBookmarkManagerFactory.class );

        var localGraphTransactionIdTracker = new LocalGraphTransactionIdTracker( transactionIdTracker, databaseIdRepository, bookmarkTimeout );
        var fabricDatabaseManagementService = dependencies.satisfyDependency(
                new BoltFabricDatabaseManagementService( fabricExecutor, fabricConfig, transactionManager, fabricDatabaseManager,
                                                         localGraphTransactionIdTracker, transactionBookmarkManagerFactory ) );

        return new BoltGraphDatabaseManagementServiceSPI()
        {

            @Override
            public BoltGraphDatabaseServiceSPI database( String databaseName ) throws UnavailableException, DatabaseNotFoundException
            {
                if ( fabricDatabaseManager.hasMultiGraphCapabilities( databaseName ) )
                {
                    return fabricDatabaseManagementService.database( databaseName );
                }

                return kernelDatabaseManagementService.database( databaseName );
            }

            @Override
            public Optional<CustomBookmarkFormatParser> getCustomBookmarkFormatParser()
            {
                return fabricDatabaseManagementService.getCustomBookmarkFormatParser();
            }
        };
    }

    protected abstract FabricDatabaseManager createFabricDatabaseManager( FabricConfig fabricConfig );

    protected abstract CatalogManager createCatalogManger();

    protected abstract FabricDatabaseAccess createFabricDatabaseAccess();

    protected abstract FabricRemoteExecutor bootstrapRemoteStack();

    protected abstract FabricConfig bootstrapFabricConfig();

    public static class Community extends FabricServicesBootstrap
    {
        public Community( LifeSupport lifeSupport, Dependencies dependencies, LogService logService )
        {
            super( lifeSupport, dependencies, logService );
        }

        @Override
        protected FabricDatabaseManager createFabricDatabaseManager( FabricConfig fabricConfig )
        {
            var databaseManager = (DatabaseManager<DatabaseContext>) resolve( DatabaseManager.class );
            return new FabricDatabaseManager.Community( fabricConfig, databaseManager );
        }

        @Override
        protected CatalogManager createCatalogManger()
        {
            var databaseManagementService = resolve( DatabaseManagementService.class );
            return new CommunityCatalogManager( createDatabaseLookup(), databaseManagementService );
        }

        @Override
        protected FabricDatabaseAccess createFabricDatabaseAccess()
        {
            return FabricDatabaseAccess.NO_RESTRICTION;
        }

        @Override
        protected FabricRemoteExecutor bootstrapRemoteStack()
        {
            return new ThrowingFabricRemoteExecutor();
        }

        @Override
        protected FabricConfig bootstrapFabricConfig()
        {
            var config = resolve( Config.class );
            return FabricConfig.from( config );
        }
    }

    private static class ServiceBootstrapper
    {
        private final LifeSupport lifeSupport;
        private final Dependencies dependencies;

        ServiceBootstrapper( LifeSupport lifeSupport, Dependencies dependencies )
        {
            this.lifeSupport = lifeSupport;
            this.dependencies = dependencies;
        }

        <T> T registerService( T dependency, Class<T> dependencyType )
        {
            dependencies.satisfyDependency( dependency );

            if ( LifecycleAdapter.class.isAssignableFrom( dependencyType ) )
            {
                lifeSupport.add( (LifecycleAdapter) dependency );
            }

            return dependencies.resolveDependency( dependencyType );
        }
    }
}
