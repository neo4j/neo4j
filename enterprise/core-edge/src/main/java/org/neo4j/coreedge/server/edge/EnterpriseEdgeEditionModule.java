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
package org.neo4j.coreedge.server.edge;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreFiles;
import org.neo4j.coreedge.catchup.storecopy.edge.CopiedStoreRecovery;
import org.neo4j.coreedge.catchup.storecopy.edge.EdgeToCoreClient;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreCopyClient;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.catchup.tx.edge.ApplyPulledTransactions;
import org.neo4j.coreedge.catchup.tx.edge.TransactionApplier;
import org.neo4j.coreedge.catchup.tx.edge.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.edge.TxPollingClient;
import org.neo4j.coreedge.catchup.tx.edge.TxPullClient;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.EdgeTopologyService;
import org.neo4j.coreedge.raft.replication.tx.ExponentialBackoffStrategy;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.NonBlockingChannels;
import org.neo4j.coreedge.server.core.NoBoltConnectivityException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.core.DelegatingLabelTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingPropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingRelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.stats.IdBasedStoreEntityCounters;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.udc.UsageData;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.GroupSettingSupport.enumerate;
import static org.neo4j.kernel.impl.factory.CommunityEditionModule.createLockManager;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise Edge edition that provides an edge cluster.
 */
public class EnterpriseEdgeEditionModule extends EditionModule
{
    public EnterpriseEdgeEditionModule( final PlatformModule platformModule,
                                        DiscoveryServiceFactory discoveryServiceFactory )
    {
        LogService logging = platformModule.logging;
        Log userLog = logging.getUserLog( EnterpriseEdgeEditionModule.class );
        if ( platformModule.config.get( OnlineBackupSettings.online_backup_enabled ) )
        {
            userLog.warn( "Backup is not supported on edge servers. Ignoring the configuration setting: "
                          + OnlineBackupSettings.online_backup_enabled );
            platformModule.config.augment( singletonMap( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE ) );
        }

        ioLimiter = new ConfigurableIOLimiter( platformModule.config );
        formats = StandardV3_0.RECORD_FORMATS;

        org.neo4j.kernel.impl.util.Dependencies dependencies = platformModule.dependencies;
        Config config = platformModule.config;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        PageCache pageCache = platformModule.pageCache;
        File storeDir = platformModule.storeDir;
        LifeSupport life = platformModule.life;

        GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;

        lockManager = dependencies.satisfyDependency( createLockManager( config, logging ) );

        idGeneratorFactory = dependencies.satisfyDependency( new DefaultIdGeneratorFactory( fileSystem ) );
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

        schemaWriteGuard = () -> {};

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );

        constraintSemantics = new EnterpriseConstraintSemantics();

        coreAPIAvailabilityGuard = new CoreAPIAvailabilityGuard( platformModule.availabilityGuard, transactionStartTimeout );

        registerRecovery( platformModule.databaseInfo, life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );
        commitProcessFactory = readOnly();

        LogProvider logProvider = platformModule.logging.getInternalLogProvider();

        EdgeTopologyService discoveryService = discoveryServiceFactory.edgeDiscoveryService( config, logProvider);
        life.add(dependencies.satisfyDependency( discoveryService ));

        Supplier<TransactionApplier> transactionApplierSupplier =
                () -> new TransactionApplier( platformModule.dependencies );

        NonBlockingChannels nonBlockingChannels = new NonBlockingChannels();
        EdgeToCoreClient.ChannelInitializer channelInitializer = new EdgeToCoreClient.ChannelInitializer( logProvider, nonBlockingChannels );
        int maxQueueSize = config.get( CoreEdgeClusterSettings.outgoing_queue_size );
        EdgeToCoreClient edgeToCoreClient = life.add( new EdgeToCoreClient( logProvider,
                channelInitializer, platformModule.monitors, maxQueueSize, nonBlockingChannels ) );
        channelInitializer.setOwner( edgeToCoreClient );

        Supplier<TransactionIdStore> transactionIdStoreSupplier =
                () -> platformModule.dependencies.resolveDependency( TransactionIdStore.class ) ;

        ApplyPulledTransactions applyPulledTransactions =
                new ApplyPulledTransactions( logProvider, transactionApplierSupplier, transactionIdStoreSupplier,
                        platformModule.monitors );

        TxPollingClient txPollingClient = life.add(
                new TxPollingClient( platformModule.jobScheduler, config.get( CoreEdgeClusterSettings.pull_interval ),
                        platformModule.dependencies.provideDependency( TransactionIdStore.class ), edgeToCoreClient,
                        applyPulledTransactions, new ConnectToRandomCoreServer( discoveryService ), NullLogProvider
                        .getInstance() ) );

        StoreFetcher storeFetcher = new StoreFetcher( platformModule.logging.getInternalLogProvider(),
                new DefaultFileSystemAbstraction(), platformModule.pageCache,
                new StoreCopyClient( edgeToCoreClient ), new TxPullClient( edgeToCoreClient ),
                new TransactionLogCatchUpFactory() );

        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        life.add( new EdgeServerStartupProcess( storeFetcher,
                new LocalDatabase( platformModule.storeDir,
                        new CopiedStoreRecovery( config, platformModule.kernelExtensions.listFactories(), platformModule.pageCache ),
                        new StoreFiles( new DefaultFileSystemAbstraction() ),
                        dependencies.provideDependency( NeoStoreDataSource.class ),
                        dependencies.provideDependency( TransactionIdStore.class ),
                        databaseHealthSupplier),
                txPollingClient, platformModule.dataSourceManager, new ConnectToRandomCoreServer( discoveryService ),
                new ExponentialBackoffStrategy( 1, TimeUnit.SECONDS ), logProvider, discoveryService, config ) );
    }

    public static HostnamePort extractBoltAddress( Config config )
    {
        List<HostnamePort> addresses = config
                .view( enumerate( GraphDatabaseSettings.Connector.class ) )
                .map( GraphDatabaseSettings.BoltConnector::new )
                .filter( ( connConfig ) -> BOLT.equals( config.get( connConfig.type ) )
                        && config.get( connConfig.enabled ) )
                .map( ( connConfig ) -> config.get( connConfig.address ) )
                .collect( toList() );
        if ( addresses.size() == 0 )
        {
            throw new NoBoltConnectivityException();
        }
        else
        {
            // Just use the first.
            return addresses.get( 0 );
        }
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

    private final class DefaultKernelData extends KernelData implements Lifecycle
    {
        private final GraphDatabaseAPI graphDb;

        DefaultKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir,
                           Config config, GraphDatabaseAPI graphDb )
        {
            super( fileSystem, pageCache, storeDir, config );
            this.graphDb = graphDb;
        }

        @Override
        public Version version()
        {
            return Version.getKernel();
        }

        @Override
        public GraphDatabaseAPI graphDatabase()
        {
            return graphDb;
        }

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
        }

        @Override
        public void stop() throws Throwable
        {
        }
    }
}
