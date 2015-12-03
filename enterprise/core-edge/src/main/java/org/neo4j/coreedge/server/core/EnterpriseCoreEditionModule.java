/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.server.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.neo4j.coreedge.catchup.StoreIdSupplier;
import org.neo4j.coreedge.catchup.CatchupServer;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.catchup.CheckpointerSupplier;
import org.neo4j.coreedge.catchup.DataSourceSupplier;
import org.neo4j.coreedge.discovery.CoreDiscoveryService;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.RaftDiscoveryServiceConnector;
import org.neo4j.coreedge.raft.net.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.raft.locks.CoreServiceManager;
import org.neo4j.coreedge.raft.locks.CoreServiceRegistry;
import org.neo4j.coreedge.raft.replication.LocalReplicator;
import org.neo4j.coreedge.raft.membership.MembershipWaiter;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdGeneratorFactory;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdRangeAcquirer;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTracker;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.raft.replication.token.ReplicatedLabelTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedPropertyKeyTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedRelationshipTypeTokenHolder;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionCommitProcess;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionStateMachine;
import org.neo4j.coreedge.server.Expiration;
import org.neo4j.coreedge.server.SenderService;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.replication.RaftReplicator;
import org.neo4j.coreedge.raft.membership.CoreMemberSetBuilder;
import org.neo4j.coreedge.raft.log.NaiveDurableRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.replication.shipping.RaftLogShippingManager;
import org.neo4j.coreedge.raft.membership.RaftMembershipManager;
import org.neo4j.coreedge.server.ExpiryScheduler;
import org.neo4j.coreedge.raft.net.LoggingInbound;
import org.neo4j.coreedge.raft.net.LoggingOutbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.net.RaftChannelInitializer;
import org.neo4j.coreedge.raft.net.RaftOutbound;
import org.neo4j.coreedge.raft.RaftServer;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.replication.RaftContentSerializer;
import org.neo4j.coreedge.raft.state.DurableTermStore;
import org.neo4j.coreedge.raft.state.DurableVoteStore;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.catchup.storecopy.edge.CopiedStoreRecovery;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreFiles;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.coreedge.server.logging.BetterMessageLogger;
import org.neo4j.coreedge.server.logging.MessageLogger;
import org.neo4j.coreedge.raft.ScheduledTimeoutService;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.logging.LogProvider;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.join_catch_up_timeout;
import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class EnterpriseCoreEditionModule
        extends EditionModule
{
    private final RaftInstance<CoreMember> raft;

    public RaftInstance<CoreMember> raft()
    {
        return raft;
    }

    public EnterpriseCoreEditionModule( final PlatformModule platformModule,
                                        DiscoveryServiceFactory discoveryServiceFactory )
    {
        org.neo4j.kernel.impl.util.Dependencies dependencies = platformModule.dependencies;
        Config config = platformModule.config;
        LogService logging = platformModule.logging;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        File storeDir = platformModule.storeDir;
        LifeSupport life = platformModule.life;
        GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;

        LogProvider logProvider = logging.getInternalLogProvider();

        final CoreReplicatedContentMarshal marshall = new CoreReplicatedContentMarshal();
        final SenderService senderService = new SenderService(
                new ExpiryScheduler( platformModule.jobScheduler ), new Expiration( SYSTEM_CLOCK ),
                new RaftChannelInitializer( marshall ), logProvider );
        life.add( senderService );

        final CoreMember myself = new CoreMember(
                config.get( CoreEdgeClusterSettings.discovery_advertised_address ),
                config.get( CoreEdgeClusterSettings.transaction_advertised_address ),
                config.get( CoreEdgeClusterSettings.raft_advertised_address ) );

        final MessageLogger<AdvertisedSocketAddress> messageLogger =
                new BetterMessageLogger<>( myself.getRaftAddress(), raftMessagesLog( storeDir ) );

        LoggingOutbound<AdvertisedSocketAddress> loggingOutbound = new LoggingOutbound<>(
                senderService, myself.getRaftAddress(), messageLogger );

        File raftLogsDirectory = createRaftLogsDirectory( platformModule.storeDir, fileSystem );

        RaftLog raftLog = new NaiveDurableRaftLog( fileSystem, raftLogsDirectory, new RaftContentSerializer(),
                platformModule.monitors );
        dependencies.satisfyDependencies( raftLog );

        ListenSocketAddress raftListenAddress = config.get( CoreEdgeClusterSettings.raft_listen_address );
        RaftServer<CoreMember> raftServer = new RaftServer<>( marshall, raftListenAddress, logProvider );

        final ScheduledTimeoutService raftTimeoutService = new ScheduledTimeoutService();

        Long electionTimeout = config.get( CoreEdgeClusterSettings.leader_election_timeout );

        RaftMembershipManager<CoreMember> membershipManager = createMembershipManager( loggingOutbound, config, raftLog,
                myself, logProvider, electionTimeout );

        CoreDiscoveryService discoveryService =
                discoveryServiceFactory.coreDiscoveryService( config, logProvider, membershipManager );
        dependencies.satisfyDependency( discoveryService );

        raft = createRaft( life, loggingOutbound, discoveryService, config,
                messageLogger, raftLog, myself, raftLogsDirectory, fileSystem, logProvider, raftServer,
                raftTimeoutService );

        RaftReplicator<CoreMember> replicator = new RaftReplicator<>( raft, myself,
                new RaftOutbound( loggingOutbound ) );

        LocalSessionPool localSessionPool = new LocalSessionPool( myself );
        GlobalSessionTracker sessionTracker = new GlobalSessionTracker();

        commitProcessFactory = createCommitProcessFactory( replicator, localSessionPool, sessionTracker, dependencies );

        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( myself );
        replicator.subscribe( idAllocationStateMachine );

        // TODO: AllocationChunk should be configurable and per type. The retry timeout should also be configurable.
        ReplicatedIdRangeAcquirer idRangeAcquirer = new ReplicatedIdRangeAcquirer( replicator,
                idAllocationStateMachine, 1024, 1000, myself, logProvider );

        raftLog.registerListener( replicator );

        MembershipWaiter<CoreMember> membershipWaiter =
                new MembershipWaiter<>( myself, platformModule.jobScheduler, electionTimeout );

        CoreServiceRegistry coreServices = new CoreServiceRegistry( SYSTEM_CLOCK );

        raft.registerLeadershipChangeListener(
                new CoreServiceManager( replicator, coreServices, myself, logProvider ) );

        ReplicatedIdGeneratorFactory replicatedIdGeneratorFactory =
                createIdGeneratorFactory( fileSystem, idRangeAcquirer, logProvider );

        this.idGeneratorFactory = dependencies.satisfyDependency( replicatedIdGeneratorFactory );

        this.relationshipTypeTokenHolder = life.add( dependencies.satisfyDependency( new
                ReplicatedRelationshipTypeTokenHolder( replicator, this.idGeneratorFactory, dependencies ) ) );
        this.propertyKeyTokenHolder = life.add( dependencies.satisfyDependency( new ReplicatedPropertyKeyTokenHolder(
                replicator, this.idGeneratorFactory, dependencies ) ) );
        this.labelTokenHolder = life.add( dependencies.satisfyDependency( new ReplicatedLabelTokenHolder( replicator,
                this.idGeneratorFactory, dependencies ) ) );

        dependencies.satisfyDependency( createKernelData( fileSystem, platformModule.pageCache, storeDir,
                config, graphDatabaseFacade, life ) );

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );

        upgradeConfiguration = new ConfigMapUpgradeConfiguration( config );

        constraintSemantics = new EnterpriseConstraintSemantics();

        registerRecovery( config.get( GraphDatabaseFacadeFactory.Configuration.editionName ), life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ) );

        ExpiryScheduler expiryScheduler = new ExpiryScheduler( platformModule.jobScheduler );
        Expiration expiration = new Expiration( SYSTEM_CLOCK );

        CoreToCoreClient.ChannelInitializer channelInitializer = new CoreToCoreClient.ChannelInitializer( logProvider );
        CoreToCoreClient coreToCoreClient = life.add( new CoreToCoreClient( logProvider, expiryScheduler, expiration,
                channelInitializer ) );
        channelInitializer.setOwner( coreToCoreClient );

        lockManager = dependencies.satisfyDependency( createLockManager( config, logging ) );

        LocalDatabase localDatabase =
                new LocalDatabase( platformModule.storeDir,
                        new CopiedStoreRecovery( config, platformModule.kernelExtensions.listFactories(),
                                platformModule.pageCache ),
                        new StoreFiles( new DefaultFileSystemAbstraction() ),
                        dependencies.provideDependency( NeoStoreDataSource.class ),
                        platformModule.dependencies.provideDependency( TransactionIdStore.class ) );

        CatchupServer catchupServer = new CatchupServer( logProvider,
                new StoreIdSupplier( platformModule ),
                platformModule.dependencies.provideDependency( TransactionIdStore.class ),
                platformModule.dependencies.provideDependency( LogicalTransactionStore.class ),
                new DataSourceSupplier( platformModule ),
                new CheckpointerSupplier( platformModule.dependencies ),
                config.get( CoreEdgeClusterSettings.transaction_listen_address ) );

        life.add( CoreServerStartupProcess.createLifeSupport(
                platformModule.dataSourceManager, replicatedIdGeneratorFactory, raftServer,
                catchupServer, raftTimeoutService, discoveryService,
                new RaftDiscoveryServiceConnector( discoveryService, raft ),
                new DeleteStoreOnStartUp( localDatabase ), new RaftLogReplay( raftLog ),
                new WaitToCatchUp<>( membershipWaiter, config.get( join_catch_up_timeout ), raft )
        ));
    }

    public boolean isLeader()
    {
        return raft.currentRole() == Role.LEADER;
    }

    private File createRaftLogsDirectory( File dir, FileSystemAbstraction fileSystem )
    {
        File raftLogDir = new File( dir, "raft-logs" );

        try
        {
            fileSystem.mkdirs( raftLogDir );
            return raftLogDir;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

    }

    public static CommitProcessFactory createCommitProcessFactory( final Replicator replicator,
                                                                   final LocalSessionPool localSessionPool, final
                                                                   GlobalSessionTracker sessionTracker, final
                                                                   Dependencies dependencies )
    {
        return ( appender, applier, indexUpdatesValidator, config ) -> {
            TransactionRepresentationCommitProcess localCommit =
                    new TransactionRepresentationCommitProcess( appender, applier, indexUpdatesValidator );
            dependencies.satisfyDependencies( localCommit );

            ReplicatedTransactionStateMachine replicatedTxListener = new ReplicatedTransactionStateMachine(
                    localCommit, sessionTracker, localSessionPool.getGlobalSession() );

            return new ReplicatedTransactionCommitProcess( replicator, localSessionPool, replicatedTxListener );
        };
    }

    private static RaftInstance<CoreMember> createRaft( LifeSupport life,
                                                        Outbound<AdvertisedSocketAddress> outbound,
                                                        CoreDiscoveryService discoveryService,
                                                        Config config,
                                                        MessageLogger<AdvertisedSocketAddress> messageLogger,
                                                        RaftLog raftLog,
                                                        CoreMember myself,
                                                        File raftLogsDirectory,
                                                        FileSystemAbstraction fileSystem,
                                                        LogProvider logProvider,
                                                        RaftServer<CoreMember> raftServer,
                                                        ScheduledTimeoutService raftTimeoutService )
    {
        LoggingInbound loggingRaftInbound = new LoggingInbound( raftServer, messageLogger, myself.getRaftAddress() );

        long electionTimeout = config.get( CoreEdgeClusterSettings.leader_election_timeout );
        long heartbeatInterval = electionTimeout / 3;

        long leaderWaitTimeout = config.get( CoreEdgeClusterSettings.leader_wait_timeout );

        Integer expectedClusterSize = config.get( CoreEdgeClusterSettings.expected_core_cluster_size );

        CoreMemberSetBuilder memberSetBuilder = new CoreMemberSetBuilder();

        DurableTermStore termStore = new DurableTermStore( fileSystem, raftLogsDirectory );
        DurableVoteStore voteStore = new DurableVoteStore( fileSystem, raftLogsDirectory );

        Replicator localReplicator = new LocalReplicator<>( myself, myself.getRaftAddress(), outbound );

        RaftMembershipManager<CoreMember> raftMembershipManager = new RaftMembershipManager<>( localReplicator, memberSetBuilder, raftLog,
                logProvider, expectedClusterSize, electionTimeout, SYSTEM_CLOCK, config.get( join_catch_up_timeout ) );

        RaftLogShippingManager<CoreMember> logShipping = new RaftLogShippingManager<>( new RaftOutbound( outbound ), logProvider, raftLog,
                SYSTEM_CLOCK, myself, raftMembershipManager, electionTimeout, config.get( CoreEdgeClusterSettings.catchup_batch_size ),
                config.get( CoreEdgeClusterSettings.log_shipping_max_lag ) );

        return new RaftInstance<>(
                myself, termStore, voteStore, raftLog, electionTimeout, heartbeatInterval,
                raftTimeoutService, loggingRaftInbound,
                new RaftOutbound( outbound ), leaderWaitTimeout, logProvider,
                raftMembershipManager, logShipping );
    }

    private static RaftMembershipManager<CoreMember> createMembershipManager( Outbound<AdvertisedSocketAddress> outbound, Config config, RaftLog raftLog, CoreMember myself, LogProvider logProvider, long electionTimeout )
    {
        Integer expectedClusterSize = config.get( CoreEdgeClusterSettings.expected_core_cluster_size );

        CoreMemberSetBuilder memberSetBuilder = new CoreMemberSetBuilder();


        Replicator localReplicator = new LocalReplicator<>( myself, myself.getRaftAddress(), outbound );

        return new RaftMembershipManager<>( localReplicator, memberSetBuilder, raftLog,
                logProvider, expectedClusterSize, electionTimeout, SYSTEM_CLOCK, config.get( join_catch_up_timeout ) );
    }

    private static PrintWriter raftMessagesLog( File storeDir )
    {
        storeDir.mkdirs();
        try
        {
            return new PrintWriter( new FileOutputStream( new File( storeDir, "raft-messages.log" ), true ) );
        }
        catch ( FileNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void publishEditionInfo( UsageData sysInfo )
    {
        sysInfo.set( UsageDataKeys.edition, UsageDataKeys.Edition.enterprise );
        sysInfo.set( UsageDataKeys.operationalMode, UsageDataKeys.OperationalMode.core );
    }

    protected SchemaWriteGuard createSchemaWriteGuard()
    {
        return () -> {};
    }

    protected KernelData createKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir,
                                           Config config, GraphDatabaseAPI graphAPI, LifeSupport life )
    {
        DefaultKernelData kernelData = new DefaultKernelData( fileSystem, pageCache, storeDir, config, graphAPI );
        return life.add( kernelData );
    }

    protected ReplicatedIdGeneratorFactory createIdGeneratorFactory( FileSystemAbstraction fileSystem,
                                                                     final ReplicatedIdRangeAcquirer idRangeAcquirer,
                                                                     final LogProvider logProvider )
    {
        return new ReplicatedIdGeneratorFactory( fileSystem, idRangeAcquirer, logProvider );
    }

    protected Locks createLockManager( final Config config, final LogService logging )
    {
        Locks local = CommunityEditionModule.createLockManager( config, logging );

        return local;
    }

    protected TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return TransactionHeaderInformationFactory.DEFAULT;
    }

    protected void registerRecovery( final String editionName, LifeSupport life,
                                     final DependencyResolver dependencyResolver )
    {
        life.addLifecycleListener( ( instance, from, to ) -> {
            if ( instance instanceof DatabaseAvailability && to.equals( LifecycleStatus.STARTED ) )
            {
                doAfterRecoveryAndStartup( editionName, dependencyResolver );
            }
        } );
    }

    @Override
    protected void doAfterRecoveryAndStartup( String editionName, DependencyResolver dependencyResolver )
    {
        super.doAfterRecoveryAndStartup( editionName, dependencyResolver );

        new RemoveOrphanConstraintIndexesOnStartup( dependencyResolver.resolveDependency( NeoStoreDataSource.class )
                .getKernel(), dependencyResolver.resolveDependency( LogService.class ).getInternalLogProvider() )
                .perform();
    }

    protected final class DefaultKernelData extends KernelData implements Lifecycle
    {
        private final GraphDatabaseAPI graphDb;

        public DefaultKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir,
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
