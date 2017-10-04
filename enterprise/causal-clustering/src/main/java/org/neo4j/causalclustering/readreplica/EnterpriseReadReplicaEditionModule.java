/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.readreplica;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupServer;
import org.neo4j.causalclustering.catchup.CheckpointerSupplier;
import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.catchup.tx.BatchingTxApplier;
import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.schedule.DelayedRenewableTimeoutService;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.HostnameResolver;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.discovery.TopologyServiceMultiRetryStrategy;
import org.neo4j.causalclustering.discovery.TopologyServiceRetryStrategy;
import org.neo4j.causalclustering.discovery.procedures.ReadReplicaRoleProcedure;
import org.neo4j.causalclustering.handlers.NoOpPipelineHandlerAppenderFactory;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppenderFactory;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.core.DelegatingLabelTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingPropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingRelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.enterprise.StandardBoltConnectionTracker;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.stats.IdBasedStoreEntityCounters;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.DefaultKernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.time.Clocks;
import org.neo4j.udc.UsageData;

import static org.neo4j.causalclustering.discovery.ResolutionResolverFactory.chooseResolver;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise Read Replica edition.
 */
public class EnterpriseReadReplicaEditionModule extends EditionModule
{
    EnterpriseReadReplicaEditionModule( final PlatformModule platformModule,
                                        final DiscoveryServiceFactory discoveryServiceFactory, MemberId myself )
    {
        LogService logging = platformModule.logging;

        ioLimiter = new ConfigurableIOLimiter( platformModule.config );

        org.neo4j.kernel.impl.util.Dependencies dependencies = platformModule.dependencies;
        Config config = platformModule.config;
        config.augment( backupDisabledSettings() );
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        PageCache pageCache = platformModule.pageCache;
        File storeDir = platformModule.storeDir;
        LifeSupport life = platformModule.life;
        Monitors monitors = platformModule.monitors;

        eligibleForIdReuse = IdReuseEligibility.ALWAYS;

        this.accessCapability = new ReadOnly();

        watcherService = createFileSystemWatcherService( fileSystem, storeDir, logging, platformModule.jobScheduler, fileWatcherFileNameFilter() );
        dependencies.satisfyDependencies( watcherService );

        GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;

        lockManager = dependencies.satisfyDependency( new ReadReplicaLockManager() );

        statementLocksFactory = new StatementLocksFactorySelector( lockManager, config, logging ).select();

        idTypeConfigurationProvider = new EnterpriseIdTypeConfigurationProvider( config );
        idGeneratorFactory = dependencies.satisfyDependency( new DefaultIdGeneratorFactory( fileSystem, idTypeConfigurationProvider ) );
        idController = createDefaultIdController();
        dependencies.satisfyDependency( idGeneratorFactory );
        dependencies.satisfyDependency( idController );
        dependencies.satisfyDependency( new IdBasedStoreEntityCounters( this.idGeneratorFactory ) );

        propertyKeyTokenHolder = life.add( dependencies.satisfyDependency( new DelegatingPropertyKeyTokenHolder( new ReadOnlyTokenCreator() ) ) );
        labelTokenHolder = life.add( dependencies.satisfyDependency( new DelegatingLabelTokenHolder( new ReadOnlyTokenCreator() ) ) );
        relationshipTypeTokenHolder = life.add( dependencies.satisfyDependency( new DelegatingRelationshipTypeTokenHolder( new ReadOnlyTokenCreator() ) ) );

        life.add( dependencies.satisfyDependency( new DefaultKernelData( fileSystem, pageCache, storeDir, config, graphDatabaseFacade ) ) );

        headerInformationFactory = TransactionHeaderInformationFactory.DEFAULT;

        schemaWriteGuard = () ->
        {
        };

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout ).toMillis();

        constraintSemantics = new EnterpriseConstraintSemantics();

        coreAPIAvailabilityGuard = new CoreAPIAvailabilityGuard( platformModule.availabilityGuard, transactionStartTimeout );

        registerRecovery( platformModule.databaseInfo, life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );
        commitProcessFactory = readOnly();

        LogProvider logProvider = platformModule.logging.getInternalLogProvider();
        LogProvider userLogProvider = platformModule.logging.getUserLogProvider();

        logProvider.getLog( getClass() ).info( String.format( "Generated new id: %s", myself ) );

        HostnameResolver hostnameResolver = chooseResolver( config, logProvider, userLogProvider );

        configureDiscoveryService( discoveryServiceFactory, dependencies, config, logProvider );

        TopologyService topologyService =
                discoveryServiceFactory.topologyService( config, logProvider, platformModule.jobScheduler, myself,
                        hostnameResolver, resolveStrategy( config ) );

        life.add( dependencies.satisfyDependency( topologyService ) );

        // We need to satisfy the dependency here to keep users of it, such as BoltKernelExtension, happy.
        dependencies.satisfyDependency( SslPolicyLoader.create( config, logProvider ) );

        PipelineHandlerAppenderFactory appenderFactory = appenderFactory();
        PipelineHandlerAppender handlerAppender = appenderFactory.create( config, dependencies, logProvider );

        long inactivityTimeoutMillis = config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ).toMillis();
        CatchUpClient catchUpClient =
                life.add( new CatchUpClient( logProvider, Clocks.systemClock(), inactivityTimeoutMillis, monitors, handlerAppender ) );

        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        Supplier<TransactionCommitProcess> writableCommitProcess =
                () -> new TransactionRepresentationCommitProcess( dependencies.resolveDependency( TransactionAppender.class ),
                        dependencies.resolveDependency( StorageEngine.class ) );

        LifeSupport txPulling = new LifeSupport();
        int maxBatchSize = config.get( CausalClusteringSettings.read_replica_transaction_applier_batch_size );
        BatchingTxApplier batchingTxApplier =
                new BatchingTxApplier( maxBatchSize, dependencies.provideDependency( TransactionIdStore.class ), writableCommitProcess, platformModule.monitors,
                        logProvider );

        DelayedRenewableTimeoutService catchupTimeoutService = new DelayedRenewableTimeoutService( Clocks.systemClock(), logProvider );

        StoreFiles storeFiles = new StoreFiles( fileSystem, pageCache );

        LocalDatabase localDatabase =
                new LocalDatabase( platformModule.storeDir, storeFiles, platformModule.dataSourceManager, databaseHealthSupplier, watcherService,
                        platformModule.availabilityGuard, logProvider );

        RemoteStore remoteStore = new RemoteStore( platformModule.logging.getInternalLogProvider(), fileSystem, platformModule.pageCache,
                new StoreCopyClient( catchUpClient, logProvider ), new TxPullClient( catchUpClient, platformModule.monitors ),
                new TransactionLogCatchUpFactory(), platformModule.monitors );

        CopiedStoreRecovery copiedStoreRecovery = new CopiedStoreRecovery( config, platformModule.kernelExtensions.listFactories(), platformModule.pageCache );

        txPulling.add( copiedStoreRecovery );

        LifeSupport servicesToStopOnStoreCopy = new LifeSupport();

        StoreCopyProcess storeCopyProcess = new StoreCopyProcess( fileSystem, pageCache, localDatabase, copiedStoreRecovery, remoteStore, logProvider );

        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, config, logProvider, myself );

        UpstreamDatabaseStrategiesLoader loader;
        if ( config.get( CausalClusteringSettings.multi_dc_license ) )
        {
            loader = new UpstreamDatabaseStrategiesLoader( topologyService, config, myself, logProvider );
            logProvider.getLog( getClass() ).info( "Multi-Data Center option enabled." );
        }
        else
        {
            loader = new NoOpUpstreamDatabaseStrategiesLoader();
        }

        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                new UpstreamDatabaseStrategySelector( defaultStrategy, loader, myself, logProvider );

        CatchupPollingProcess catchupProcess =
                new CatchupPollingProcess( logProvider, localDatabase, servicesToStopOnStoreCopy, catchUpClient, upstreamDatabaseStrategySelector,
                        catchupTimeoutService, config.get( CausalClusteringSettings.pull_interval ).toMillis(), batchingTxApplier, platformModule.monitors,
                        storeCopyProcess, databaseHealthSupplier, topologyService );
        dependencies.satisfyDependencies( catchupProcess );

        txPulling.add( batchingTxApplier );
        txPulling.add( catchupProcess );
        txPulling.add( catchupTimeoutService );
        txPulling.add( new WaitForUpToDateStore( catchupProcess, logProvider ) );

        ExponentialBackoffStrategy retryStrategy = new ExponentialBackoffStrategy( 1, 30, TimeUnit.SECONDS );
        life.add( new ReadReplicaStartupProcess( remoteStore, localDatabase, txPulling, upstreamDatabaseStrategySelector, retryStrategy, logProvider,
                platformModule.logging.getUserLogProvider(), storeCopyProcess, topologyService ) );

        CatchupServer catchupServer =
                new CatchupServer( platformModule.logging.getInternalLogProvider(), platformModule.logging.getUserLogProvider(), localDatabase::storeId,
                        platformModule.dependencies.provideDependency( TransactionIdStore.class ),
                        platformModule.dependencies.provideDependency( LogicalTransactionStore.class ), localDatabase::dataSource, localDatabase::isAvailable,
                        null, config, platformModule.monitors, new CheckpointerSupplier( platformModule.dependencies ), fileSystem, pageCache,
                        platformModule.storeCopyCheckPointMutex, handlerAppender );

        servicesToStopOnStoreCopy.add( catchupServer );

        dependencies.satisfyDependency( createSessionTracker() );

        life.add( catchupServer ); // must start last and stop first, since it handles external requests
    }

    protected void configureDiscoveryService( DiscoveryServiceFactory discoveryServiceFactory, Dependencies dependencies,
                                              Config config, LogProvider logProvider )
    {
    }

    protected PipelineHandlerAppenderFactory appenderFactory()
    {
        return new NoOpPipelineHandlerAppenderFactory();
    }

    static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any( fileName -> fileName.startsWith( PhysicalLogFile.DEFAULT_NAME ),
                fileName -> fileName.startsWith( IndexConfigStore.INDEX_DB_FILE_NAME ), filename -> filename.startsWith( StoreUtil.BRANCH_SUBDIRECTORY ),
                filename -> filename.startsWith( StoreUtil.TEMP_COPY_DIRECTORY_NAME ) );
    }

    @Override
    public void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException
    {
        procedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        procedures.register( new ReadReplicaRoleProcedure() );
    }

    private void registerRecovery( final DatabaseInfo databaseInfo, LifeSupport life, final DependencyResolver dependencyResolver )
    {
        life.addLifecycleListener( ( instance, from, to ) ->
        {
            if ( instance instanceof DatabaseAvailability && to.equals( LifecycleStatus.STARTED ) )
            {
                doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );
            }
        } );
    }

    private CommitProcessFactory readOnly()
    {
        return ( appender, storageEngine, config ) -> new ReadOnlyTransactionCommitProcess();
    }

    @Override
    protected BoltConnectionTracker createSessionTracker()
    {
        return new StandardBoltConnectionTracker();
    }

    @Override
    public void setupSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        EnterpriseEditionModule.setupEnterpriseSecurityModule( platformModule, procedures );
    }

    private class NoOpUpstreamDatabaseStrategiesLoader extends UpstreamDatabaseStrategiesLoader
    {
        NoOpUpstreamDatabaseStrategiesLoader()
        {
            super( null, null, null, NullLogProvider.getInstance() );
        }

        @Override
        public Iterator<UpstreamDatabaseSelectionStrategy> iterator()
        {
            return new Iterator<UpstreamDatabaseSelectionStrategy>()
            {
                @Override
                public boolean hasNext()
                {
                    return false;
                }

                @Override
                public UpstreamDatabaseSelectionStrategy next()
                {
                    return null;
                }
            };
        }
    }

    private static TopologyServiceRetryStrategy resolveStrategy( Config config )
    {
        long refreshPeriodMillis = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        int pollingFrequencyWithinRefreshWindow = 2;
        int numberOfRetries =
                pollingFrequencyWithinRefreshWindow + 1; // we want to have more retries at the given frequency than there is time in a refresh period
        return new TopologyServiceMultiRetryStrategy( refreshPeriodMillis / pollingFrequencyWithinRefreshWindow, numberOfRetries );
    }

    private static Map<String,String> backupDisabledSettings()
    {
        Map<String,String> overrideBackupSettings = new HashMap<>(  );
        overrideBackupSettings.put( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE );
        return overrideBackupSettings;
    }
}
