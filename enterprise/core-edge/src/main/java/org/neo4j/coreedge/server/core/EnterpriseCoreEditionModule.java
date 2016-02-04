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
package org.neo4j.coreedge.server.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.CatchupServer;
import org.neo4j.coreedge.catchup.CheckpointerSupplier;
import org.neo4j.coreedge.catchup.DataSourceSupplier;
import org.neo4j.coreedge.catchup.StoreIdSupplier;
import org.neo4j.coreedge.discovery.CoreDiscoveryService;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.RaftDiscoveryServiceConnector;
import org.neo4j.coreedge.raft.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.RaftServer;
import org.neo4j.coreedge.raft.log.MonitoredRaftLog;
import org.neo4j.coreedge.raft.log.NaiveDurableRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.membership.CoreMemberSetBuilder;
import org.neo4j.coreedge.raft.membership.MembershipWaiter;
import org.neo4j.coreedge.raft.membership.RaftMembershipManager;
import org.neo4j.coreedge.raft.net.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.raft.net.LoggingInbound;
import org.neo4j.coreedge.raft.net.LoggingOutbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.net.RaftChannelInitializer;
import org.neo4j.coreedge.raft.net.RaftOutbound;
import org.neo4j.coreedge.raft.replication.LeaderOnlyReplicator;
import org.neo4j.coreedge.raft.replication.RaftContentSerializer;
import org.neo4j.coreedge.raft.replication.RaftReplicator;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.id.IdAllocationState;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdGeneratorFactory;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdRangeAcquirer;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.raft.replication.session.OnDiskGlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.shipping.RaftLogShippingManager;
import org.neo4j.coreedge.raft.replication.token.ReplicatedLabelTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedPropertyKeyTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedRelationshipTypeTokenHolder;
import org.neo4j.coreedge.raft.replication.tx.CommittingTransactions;
import org.neo4j.coreedge.raft.replication.tx.CommittingTransactionsRegistry;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionCommitProcess;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionStateMachine;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.id_allocation.OnDiskIdAllocationState;
import org.neo4j.coreedge.raft.state.membership.OnDiskRaftMembershipState;
import org.neo4j.coreedge.raft.state.membership.RaftMembershipState;
import org.neo4j.coreedge.raft.state.term.MonitoredTermState;
import org.neo4j.coreedge.raft.state.term.OnDiskTermState;
import org.neo4j.coreedge.raft.state.term.TermState;
import org.neo4j.coreedge.raft.state.vote.OnDiskVoteState;
import org.neo4j.coreedge.raft.state.vote.VoteState;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.CoreMember.CoreMemberMarshal;
import org.neo4j.coreedge.server.Expiration;
import org.neo4j.coreedge.server.ExpiryScheduler;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.coreedge.server.SenderService;
import org.neo4j.coreedge.server.core.locks.LeaderOnlyLockManager;
import org.neo4j.coreedge.server.core.locks.LockTokenManager;
import org.neo4j.coreedge.server.core.locks.OnDiskReplicatedLockTokenState;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.coreedge.server.logging.BetterMessageLogger;
import org.neo4j.coreedge.server.logging.MessageLogger;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Clock;
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
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.stats.IdBasedStoreEntityCounters;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.udc.UsageData;

import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class EnterpriseCoreEditionModule
        extends EditionModule
{
    public static final String CLUSTER_STATE_DIRECTORY_NAME = "cluster-state";
    private final RaftInstance<CoreMember> raft;

    public RaftInstance<CoreMember> raft()
    {
        return raft;
    }

    public EnterpriseCoreEditionModule( final PlatformModule platformModule,
                                        DiscoveryServiceFactory discoveryServiceFactory )
    {
        final org.neo4j.kernel.impl.util.Dependencies dependencies = platformModule.dependencies;
        final Config config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final File storeDir = platformModule.storeDir;
        final File clusterStateDirectory = createClusterStateDirectory( storeDir, fileSystem );
        final LifeSupport life = platformModule.life;
        final GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;

        LogProvider logProvider = logging.getInternalLogProvider();

        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        CoreDiscoveryService discoveryService =
                discoveryServiceFactory.coreDiscoveryService( config );
        life.add( dependencies.satisfyDependency( discoveryService ) );

        final CoreReplicatedContentMarshal marshal = new CoreReplicatedContentMarshal();
        int maxQueueSize = config.get( CoreEdgeClusterSettings.outgoing_queue_size );
        final SenderService senderService = new SenderService(
                new ExpiryScheduler( platformModule.jobScheduler ), new Expiration( SYSTEM_CLOCK ),
                new RaftChannelInitializer( marshal ), logProvider, platformModule.monitors, maxQueueSize );
        life.add( senderService );

        final CoreMember myself = new CoreMember(
                config.get( CoreEdgeClusterSettings.transaction_advertised_address ),
                config.get( CoreEdgeClusterSettings.raft_advertised_address ) );

        final MessageLogger<AdvertisedSocketAddress> messageLogger =
                new BetterMessageLogger<>( myself.getRaftAddress(), raftMessagesLog( storeDir ) );

        LoggingOutbound<AdvertisedSocketAddress> loggingOutbound = new LoggingOutbound<>(
                senderService, myself.getRaftAddress(), messageLogger );

        ListenSocketAddress raftListenAddress = config.get( CoreEdgeClusterSettings.raft_listen_address );
        RaftServer<CoreMember> raftServer = new RaftServer<>( marshal, raftListenAddress, logProvider );

        final DelayedRenewableTimeoutService raftTimeoutService =
                new DelayedRenewableTimeoutService( Clock.SYSTEM_CLOCK, logProvider );

        NaiveDurableRaftLog raftLog = life.add( new NaiveDurableRaftLog( fileSystem,
                new File( clusterStateDirectory, NaiveDurableRaftLog.DIRECTORY_NAME ),
                new RaftContentSerializer(), logProvider ) );

        MonitoredRaftLog monitoredRaftLog = new MonitoredRaftLog( raftLog, platformModule.monitors );

        TermState termState;
        try
        {
            OnDiskTermState onDiskTermState = life.add( new OnDiskTermState( fileSystem,
                    new File( clusterStateDirectory, OnDiskTermState.DIRECTORY_NAME ),
                    config.get( CoreEdgeClusterSettings.term_state_size ), databaseHealthSupplier, logProvider ) );
            termState = new MonitoredTermState( onDiskTermState, platformModule.monitors );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        VoteState<CoreMember> voteState;
        try
        {
            voteState = life.add( new OnDiskVoteState<>( fileSystem,
                    new File( clusterStateDirectory, OnDiskVoteState.DIRECTORY_NAME ),
                    config.get( CoreEdgeClusterSettings.vote_state_size ), databaseHealthSupplier,
                    new CoreMemberMarshal(), logProvider ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }


        RaftMembershipState<CoreMember> raftMembershipState;
        try
        {
            raftMembershipState = life.add( new OnDiskRaftMembershipState<>( fileSystem,
                    new File( clusterStateDirectory, OnDiskRaftMembershipState.DIRECTORY_NAME ),
                    config.get( CoreEdgeClusterSettings.raft_membership_state_size ),
                    databaseHealthSupplier, new CoreMemberMarshal(), logProvider ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        raft = createRaft( life, loggingOutbound, discoveryService, config, messageLogger, monitoredRaftLog,
                termState, voteState, myself, logProvider, raftServer, raftTimeoutService,
                databaseHealthSupplier, raftMembershipState, platformModule.monitors );

        dependencies.satisfyDependency( raft );

        dependencies.satisfyDependency( raft );

        RaftReplicator<CoreMember> replicator = new RaftReplicator<>( raft, myself,
                new RaftOutbound( loggingOutbound ) );

        LocalSessionPool localSessionPool = new LocalSessionPool( myself );


        OnDiskReplicatedLockTokenState<CoreMember> onDiskReplicatedLockTokenState;
        try
        {
            onDiskReplicatedLockTokenState = life.add( new OnDiskReplicatedLockTokenState<>( fileSystem,
                    new File( clusterStateDirectory, OnDiskReplicatedLockTokenState.DIRECTORY_NAME ),
                    config.get( CoreEdgeClusterSettings.replicated_lock_token_state_size ),
                    new CoreMember.CoreMemberMarshal(), databaseHealthSupplier, logProvider ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        ReplicatedLockTokenStateMachine<CoreMember> replicatedLockTokenStateMachine =
                new ReplicatedLockTokenStateMachine<>( replicator, onDiskReplicatedLockTokenState );

        OnDiskGlobalSessionTrackerState<CoreMember> onDiskGlobalSessionTrackerState;
        try
        {
            onDiskGlobalSessionTrackerState = new
                    OnDiskGlobalSessionTrackerState<>( fileSystem,
                    new File( clusterStateDirectory, OnDiskGlobalSessionTrackerState.DIRECTORY_NAME ),
                    new CoreMemberMarshal(),
                    config.get( CoreEdgeClusterSettings.global_session_tracker_state_size ),
                    databaseHealthSupplier, logProvider );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        commitProcessFactory = createCommitProcessFactory( replicator, localSessionPool,
                replicatedLockTokenStateMachine,
                dependencies, logging, platformModule.monitors, onDiskGlobalSessionTrackerState );

        final IdAllocationState idAllocationState;
        try
        {
            idAllocationState = life.add( new OnDiskIdAllocationState( fileSystem,
                    new File( clusterStateDirectory, OnDiskIdAllocationState.DIRECTORY_NAME ),
                    config.get( CoreEdgeClusterSettings.id_alloc_state_size ), databaseHealthSupplier, logProvider ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine(
                myself, idAllocationState, logProvider );

        replicator.subscribe( idAllocationStateMachine );

        // TODO: AllocationChunk should be configurable and per type. The retry timeout should also be configurable.
        ReplicatedIdRangeAcquirer idRangeAcquirer = new ReplicatedIdRangeAcquirer( replicator,
                idAllocationStateMachine, 1024, 1000, myself, logProvider );

        monitoredRaftLog.registerListener( replicator );

        long electionTimeout = config.get( CoreEdgeClusterSettings.leader_election_timeout );
        MembershipWaiter<CoreMember> membershipWaiter =
                new MembershipWaiter<>( myself, platformModule.jobScheduler, electionTimeout * 4, logProvider );

        ReplicatedIdGeneratorFactory replicatedIdGeneratorFactory =
                createIdGeneratorFactory( fileSystem, idRangeAcquirer, logProvider );

        this.idGeneratorFactory = dependencies.satisfyDependency( replicatedIdGeneratorFactory );
        dependencies.satisfyDependency( new IdBasedStoreEntityCounters( this.idGeneratorFactory ) );

        Long tokenCreationTimeout = config.get( CoreEdgeClusterSettings.token_creation_timeout );
        ReplicatedRelationshipTypeTokenHolder relationshipTypeTokenHolder = new ReplicatedRelationshipTypeTokenHolder(
                replicator, this.idGeneratorFactory, dependencies, tokenCreationTimeout, logProvider );
        ReplicatedPropertyKeyTokenHolder propertyKeyTokenHolder = new ReplicatedPropertyKeyTokenHolder(
                replicator, this.idGeneratorFactory, dependencies, tokenCreationTimeout, logProvider );
        ReplicatedLabelTokenHolder labelTokenHolder = new ReplicatedLabelTokenHolder(
                replicator, this.idGeneratorFactory, dependencies, tokenCreationTimeout, logProvider );

        LifeSupport tokenLife = new LifeSupport();
        this.relationshipTypeTokenHolder = tokenLife.add( relationshipTypeTokenHolder );
        this.propertyKeyTokenHolder = tokenLife.add( propertyKeyTokenHolder );
        this.labelTokenHolder = tokenLife.add( labelTokenHolder );

        dependencies.satisfyDependency( createKernelData( fileSystem, platformModule.pageCache, storeDir,
                config, graphDatabaseFacade, life ) );

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );

        constraintSemantics = new EnterpriseConstraintSemantics();

        registerRecovery( platformModule.databaseInfo, life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );

        ExpiryScheduler expiryScheduler = new ExpiryScheduler( platformModule.jobScheduler );
        Expiration expiration = new Expiration( SYSTEM_CLOCK );

        CoreToCoreClient.ChannelInitializer channelInitializer = new CoreToCoreClient.ChannelInitializer( logProvider );
        CoreToCoreClient coreToCoreClient = life.add( new CoreToCoreClient( logProvider, expiryScheduler, expiration,
                channelInitializer, platformModule.monitors, maxQueueSize ) );
        channelInitializer.setOwner( coreToCoreClient );

        long leaderLockTokenTimeout = config.get( CoreEdgeClusterSettings.leader_lock_token_timeout );
        lockManager = dependencies.satisfyDependency( createLockManager( config, logging, replicator, myself,
                replicatedLockTokenStateMachine, raft, leaderLockTokenTimeout ) );

        CatchupServer catchupServer = new CatchupServer( logProvider,
                new StoreIdSupplier( platformModule ),
                platformModule.dependencies.provideDependency( TransactionIdStore.class ),
                platformModule.dependencies.provideDependency( LogicalTransactionStore.class ),
                new DataSourceSupplier( platformModule ),
                new CheckpointerSupplier( platformModule.dependencies ),
                config.get( CoreEdgeClusterSettings.transaction_listen_address ),
                platformModule.monitors );

        life.add( CoreServerStartupProcess.createLifeSupport(
                platformModule.dataSourceManager, replicatedIdGeneratorFactory, raft, new RaftLogReplay(
                        monitoredRaftLog,
                        logProvider ), raftServer,
                catchupServer, raftTimeoutService, membershipWaiter,
                config.get( CoreEdgeClusterSettings.join_catch_up_timeout ),
                new RecoverTransactionLogState( dependencies, logProvider,
                        relationshipTypeTokenHolder, propertyKeyTokenHolder, labelTokenHolder ),
                tokenLife
        ) );
    }

    public boolean isLeader()
    {
        return raft.currentRole() == Role.LEADER;
    }

    private File createClusterStateDirectory( File dir, FileSystemAbstraction fileSystem )
    {
        File raftLogDir = new File( dir, CLUSTER_STATE_DIRECTORY_NAME );

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
                                                                   final LocalSessionPool localSessionPool,
                                                                   final LockTokenManager
                                                                           currentReplicatedLockState,
                                                                   final Dependencies dependencies,
                                                                   final LogService logging,
                                                                   Monitors monitors,
                                                                   GlobalSessionTrackerState<CoreMember> globalSessionTrackerState )
    {
        return ( appender, applier, config ) -> {
            TransactionRepresentationCommitProcess localCommit =
                    new TransactionRepresentationCommitProcess( appender, applier );
            dependencies.satisfyDependencies( localCommit );

            CommittingTransactions committingTransactions = new CommittingTransactionsRegistry();
            ReplicatedTransactionStateMachine<CoreMember> replicatedTxStateMachine = new ReplicatedTransactionStateMachine<>(
                    localCommit, localSessionPool.getGlobalSession(), currentReplicatedLockState,
                    committingTransactions, globalSessionTrackerState, logging.getInternalLogProvider() );

            dependencies.satisfyDependencies( replicatedTxStateMachine );

            replicator.subscribe( replicatedTxStateMachine );

            return new ReplicatedTransactionCommitProcess( replicator, localSessionPool,
                    replicatedTxStateMachine,
                    config.get( CoreEdgeClusterSettings.tx_replication_retry_interval ),
                    logging, committingTransactions, monitors
            );
        };
    }

    private static RaftInstance<CoreMember> createRaft( LifeSupport life,
                                                        Outbound<AdvertisedSocketAddress> outbound,
                                                        CoreDiscoveryService discoveryService,
                                                        Config config,
                                                        MessageLogger<AdvertisedSocketAddress> messageLogger,
                                                        RaftLog raftLog,
                                                        TermState termState,
                                                        VoteState<CoreMember> voteState,
                                                        CoreMember myself,
                                                        LogProvider logProvider,
                                                        RaftServer<CoreMember> raftServer,
                                                        DelayedRenewableTimeoutService raftTimeoutService,
                                                        Supplier<DatabaseHealth> databaseHealthSupplier,
                                                        RaftMembershipState<CoreMember> raftMembershipState, Monitors
                                                                monitors )
    {
        LoggingInbound loggingRaftInbound = new LoggingInbound( raftServer, messageLogger, myself.getRaftAddress() );

        long electionTimeout = config.get( CoreEdgeClusterSettings.leader_election_timeout );
        long heartbeatInterval = electionTimeout / 3;

        long leaderWaitTimeout = config.get( CoreEdgeClusterSettings.leader_wait_timeout );

        Integer expectedClusterSize = config.get( CoreEdgeClusterSettings.expected_core_cluster_size );

        CoreMemberSetBuilder memberSetBuilder = new CoreMemberSetBuilder();

        Replicator localReplicator = new LeaderOnlyReplicator<>( myself, myself.getRaftAddress(), outbound );

        RaftMembershipManager<CoreMember> raftMembershipManager = new RaftMembershipManager<>( localReplicator,
                memberSetBuilder, raftLog, logProvider, expectedClusterSize, electionTimeout, SYSTEM_CLOCK,
                config.get( CoreEdgeClusterSettings.join_catch_up_timeout ), raftMembershipState );

        RaftLogShippingManager<CoreMember> logShipping = new RaftLogShippingManager<>( new RaftOutbound( outbound ),
                logProvider, raftLog,
                SYSTEM_CLOCK, myself, raftMembershipManager, electionTimeout,
                config.get( CoreEdgeClusterSettings.catchup_batch_size ),
                config.get( CoreEdgeClusterSettings.log_shipping_max_lag ) );

        RaftInstance<CoreMember> raftInstance = new RaftInstance<>(
                myself, termState, voteState, raftLog, electionTimeout, heartbeatInterval,
                raftTimeoutService, loggingRaftInbound,
                new RaftOutbound( outbound ), leaderWaitTimeout, logProvider,
                raftMembershipManager, logShipping, databaseHealthSupplier, Clock.SYSTEM_CLOCK, monitors );

        life.add( new RaftDiscoveryServiceConnector( discoveryService, raftInstance ) );

        return raftInstance;
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

    protected SchemaWriteGuard createSchemaWriteGuard()
    {
        return () -> {
        };
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

    protected Locks createLockManager( final Config config, final LogService logging, final Replicator replicator,
                                       CoreMember myself, LockTokenManager lockTokenManager,
                                       LeaderLocator<CoreMember> leaderLocator, long leaderLockTokenTimeout )
    {
        Locks local = CommunityEditionModule.createLockManager( config, logging );

        return new LeaderOnlyLockManager<>( myself, replicator, leaderLocator, local, lockTokenManager,
                leaderLockTokenTimeout );
    }

    protected TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return () -> new TransactionHeaderInformation( -1, -1, new byte[0] );
    }

    protected void registerRecovery( final DatabaseInfo databaseInfo, LifeSupport life,
                                     final DependencyResolver dependencyResolver )
    {
        life.addLifecycleListener( ( instance, from, to ) -> {
            if ( instance instanceof DatabaseAvailability && to.equals( LifecycleStatus.STARTED ) )
            {
                doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );
            }
        } );
    }

    @Override
    protected void doAfterRecoveryAndStartup( DatabaseInfo databaseInfo, DependencyResolver dependencyResolver )
    {
        super.doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );

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
