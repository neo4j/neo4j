/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.edge;

import java.io.File;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.coreedge.catchup.CatchUpClient;
import org.neo4j.coreedge.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyClient;
import org.neo4j.coreedge.catchup.storecopy.StoreFetcher;
import org.neo4j.coreedge.catchup.storecopy.StoreFiles;
import org.neo4j.coreedge.catchup.tx.BatchingTxApplier;
import org.neo4j.coreedge.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.TxPollingClient;
import org.neo4j.coreedge.catchup.tx.TxPullClient;
import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.consensus.ContinuousJob;
import org.neo4j.coreedge.core.consensus.schedule.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.core.state.machines.tx.ExponentialBackoffStrategy;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.TopologyService;
import org.neo4j.coreedge.discovery.procedures.EdgeRoleProcedure;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.coreedge.messaging.routing.ConnectToRandomCoreMember;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
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
import org.neo4j.kernel.impl.enterprise.StandardBoltConnectionTracker;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.stats.IdBasedStoreEntityCounters;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.DefaultKernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.time.Clocks;
import org.neo4j.udc.UsageData;

import static java.util.Collections.singletonMap;
import static org.neo4j.kernel.impl.factory.CommunityEditionModule.createLockManager;
import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.NEW_THREAD;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise Edge edition that provides an edge cluster.
 */
public class EnterpriseEdgeEditionModule extends EditionModule
{
    @Override
    public void registerProcedures( Procedures procedures )
    {
        try
        {
            procedures.register( new EdgeRoleProcedure() );
        }
        catch ( ProcedureException e )
        {
            throw new RuntimeException( e );
        }
    }

    EnterpriseEdgeEditionModule( final PlatformModule platformModule,
                                 final DiscoveryServiceFactory discoveryServiceFactory )
    {
        LogService logging = platformModule.logging;
        Log userLog = logging.getUserLog( EnterpriseEdgeEditionModule.class );
        if ( platformModule.config.get( OnlineBackupSettings.online_backup_enabled ) )
        {
            userLog.warn( "Backup is not supported on edge servers. Ignoring the configuration setting: "
                    + OnlineBackupSettings.online_backup_enabled );
            platformModule.config.augment( singletonMap( OnlineBackupSettings.online_backup_enabled.name(), Settings
                    .FALSE ) );
        }

        ioLimiter = new ConfigurableIOLimiter( platformModule.config );

        org.neo4j.kernel.impl.util.Dependencies dependencies = platformModule.dependencies;
        Config config = platformModule.config;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        PageCache pageCache = platformModule.pageCache;
        File storeDir = platformModule.storeDir;
        LifeSupport life = platformModule.life;

        GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;

        lockManager = dependencies.satisfyDependency( createLockManager( config, logging ) );

        statementLocksFactory = new StatementLocksFactorySelector( lockManager, config, logging ).select();

        idTypeConfigurationProvider = new EnterpriseIdTypeConfigurationProvider( config );
        idGeneratorFactory = dependencies.satisfyDependency( new DefaultIdGeneratorFactory( fileSystem,
                idTypeConfigurationProvider ) );
        dependencies.satisfyDependency( new IdBasedStoreEntityCounters( this.idGeneratorFactory ) );

        propertyKeyTokenHolder = life.add( dependencies.satisfyDependency(
                new DelegatingPropertyKeyTokenHolder( new ReadOnlyTokenCreator() ) ) );
        labelTokenHolder = life.add( dependencies.satisfyDependency(
                new DelegatingLabelTokenHolder( new ReadOnlyTokenCreator() ) ) );
        relationshipTypeTokenHolder = life.add( dependencies.satisfyDependency(
                new DelegatingRelationshipTypeTokenHolder( new ReadOnlyTokenCreator() ) ) );

        life.add( dependencies.satisfyDependency(
                new DefaultKernelData( fileSystem, pageCache, storeDir, config, graphDatabaseFacade ) ) );

        life.add( dependencies.satisfyDependency( createAuthManager( config, logging ) ) );

        headerInformationFactory = TransactionHeaderInformationFactory.DEFAULT;

        schemaWriteGuard = () -> {
        };

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );

        constraintSemantics = new EnterpriseConstraintSemantics();

        coreAPIAvailabilityGuard = new CoreAPIAvailabilityGuard( platformModule.availabilityGuard,
                transactionStartTimeout );

        registerRecovery( platformModule.databaseInfo, life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );
        commitProcessFactory = readOnly();

        LogProvider logProvider = platformModule.logging.getInternalLogProvider();

        DelayedRenewableTimeoutService refreshEdgeTimeoutService = life.add( new DelayedRenewableTimeoutService(
                Clocks.systemClock(), logProvider ) );

        long edgeTimeToLiveTimeout = config.get( CoreEdgeClusterSettings.edge_time_to_live );
        long edgeRefreshRate = config.get( CoreEdgeClusterSettings.edge_refresh_rate );

        TopologyService discoveryService = discoveryServiceFactory.edgeDiscoveryService( config,
                extractBoltAddress( config ), logProvider, refreshEdgeTimeoutService, edgeTimeToLiveTimeout,
                edgeRefreshRate );
        life.add( dependencies.satisfyDependency( discoveryService ) );

        Clock clock = Clocks.systemClock();
        CatchUpClient catchUpClient = life.add( new CatchUpClient( discoveryService, logProvider, clock ) );

        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        Supplier<TransactionCommitProcess> writableCommitProcess = () -> new TransactionRepresentationCommitProcess(
                dependencies.resolveDependency( TransactionAppender.class ),
                dependencies.resolveDependency( StorageEngine.class ) );

        LifeSupport txPulling = new LifeSupport();
        int maxBatchSize = config.get( CoreEdgeClusterSettings.edge_transaction_applier_batch_size );
        BatchingTxApplier batchingTxApplier = new BatchingTxApplier( maxBatchSize, dependencies.provideDependency(
                TransactionIdStore.class ),
                writableCommitProcess, databaseHealthSupplier, platformModule.monitors, logProvider );
        ContinuousJob txApplyJob = new ContinuousJob( platformModule.jobScheduler, new JobScheduler.Group(
                "tx-applier", NEW_THREAD ), batchingTxApplier );

        DelayedRenewableTimeoutService txPullerTimeoutService = new DelayedRenewableTimeoutService( clock
                , logProvider );

        LocalDatabase localDatabase = new LocalDatabase( platformModule.storeDir,
                new CopiedStoreRecovery( config, platformModule.kernelExtensions.listFactories(),
                        platformModule.pageCache ),
                new StoreFiles( new DefaultFileSystemAbstraction() ),
                platformModule.dataSourceManager,
                dependencies.provideDependency( TransactionIdStore.class ),
                databaseHealthSupplier, logProvider );

        TxPollingClient txPuller = new TxPollingClient( logProvider,
                localDatabase, catchUpClient, new ConnectToRandomCoreMember( discoveryService ),
                txPullerTimeoutService, config.get( CoreEdgeClusterSettings.pull_interval ), batchingTxApplier,
                platformModule.monitors );

        dependencies.satisfyDependencies( txPuller );

        txPulling.add( batchingTxApplier );
        txPulling.add( txApplyJob );
        txPulling.add( txPuller );
        txPulling.add( txPullerTimeoutService );

        StoreFetcher storeFetcher = new StoreFetcher( platformModule.logging.getInternalLogProvider(),
                new DefaultFileSystemAbstraction(), platformModule.pageCache,
                new StoreCopyClient( catchUpClient ), new TxPullClient( catchUpClient, platformModule.monitors ),
                new TransactionLogCatchUpFactory() );

        life.add( new EdgeStartupProcess( storeFetcher,
                localDatabase,
                txPulling, new ConnectToRandomCoreMember( discoveryService ),
                new ExponentialBackoffStrategy( 1, TimeUnit.SECONDS ), logProvider ) );

        dependencies.satisfyDependency( createSessionTracker() );
    }

    public static AdvertisedSocketAddress extractBoltAddress( Config config )
    {
        HostnamePort address = config.get( GraphDatabaseSettings.bolt_advertised_address );
        return new AdvertisedSocketAddress( address.toString() );
    }

    private void registerRecovery( final DatabaseInfo databaseInfo, LifeSupport life,
                                   final DependencyResolver dependencyResolver )
    {
        life.addLifecycleListener( ( instance, from, to ) -> {
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
}
