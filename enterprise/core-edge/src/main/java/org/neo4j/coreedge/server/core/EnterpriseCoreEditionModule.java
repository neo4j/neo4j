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
import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.CatchupServer;
import org.neo4j.coreedge.catchup.CheckpointerSupplier;
import org.neo4j.coreedge.catchup.DataSourceSupplier;
import org.neo4j.coreedge.catchup.StoreIdSupplier;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.catchup.storecopy.StoreFiles;
import org.neo4j.coreedge.catchup.storecopy.core.CoreToCoreClient;
import org.neo4j.coreedge.catchup.storecopy.edge.CopiedStoreRecovery;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreCopyClient;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.catchup.tx.edge.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.edge.TxPullClient;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.RaftDiscoveryServiceConnector;
import org.neo4j.coreedge.raft.BatchingMessageHandler;
import org.neo4j.coreedge.raft.ContinuousJob;
import org.neo4j.coreedge.raft.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.RaftServer;
import org.neo4j.coreedge.raft.RaftStateMachine;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.MonitoredRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftLogMetadataCache;
import org.neo4j.coreedge.raft.log.naive.NaiveDurableRaftLog;
import org.neo4j.coreedge.raft.log.physical.PhysicalRaftLog;
import org.neo4j.coreedge.raft.log.physical.PhysicalRaftLogFile;
import org.neo4j.coreedge.raft.log.pruning.PruningScheduler;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.log.segmented.SegmentedRaftLog;
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
import org.neo4j.coreedge.raft.replication.ProgressTrackerImpl;
import org.neo4j.coreedge.raft.replication.RaftReplicator;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdGeneratorFactory;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdRangeAcquirer;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
import org.neo4j.coreedge.raft.replication.shipping.RaftLogShippingManager;
import org.neo4j.coreedge.raft.replication.token.ReplicatedLabelTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedPropertyKeyTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedRelationshipTypeTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenStateMachine;
import org.neo4j.coreedge.raft.replication.token.TokenRegistry;
import org.neo4j.coreedge.raft.replication.tx.ExponentialBackoffStrategy;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionCommitProcess;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionStateMachine;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.CoreState;
import org.neo4j.coreedge.raft.state.CoreStateApplier;
import org.neo4j.coreedge.raft.state.CoreStateDownloader;
import org.neo4j.coreedge.raft.state.CoreStateMachines;
import org.neo4j.coreedge.raft.state.DurableStateStorage;
import org.neo4j.coreedge.raft.state.LongIndexMarshal;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.raft.state.id_allocation.IdAllocationState;
import org.neo4j.coreedge.raft.state.membership.RaftMembershipState;
import org.neo4j.coreedge.raft.state.term.MonitoredTermStateStorage;
import org.neo4j.coreedge.raft.state.term.TermState;
import org.neo4j.coreedge.raft.state.vote.VoteState;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.CoreMember.CoreMemberMarshal;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.coreedge.server.NonBlockingChannels;
import org.neo4j.coreedge.server.SenderService;
import org.neo4j.coreedge.server.core.locks.LeaderOnlyLockManager;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenState;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.coreedge.server.logging.BetterMessageLogger;
import org.neo4j.coreedge.server.logging.MessageLogger;
import org.neo4j.coreedge.server.logging.NullMessageLogger;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.stats.IdBasedStoreEntityCounters;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.Token;
import org.neo4j.udc.UsageData;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.NEW_THREAD;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class EnterpriseCoreEditionModule
        extends EditionModule implements CoreEditionSPI
{
    public static final String CLUSTER_STATE_DIRECTORY_NAME = "cluster-state";

    private final RaftInstance<CoreMember> raft;
    private final CoreState coreState;
    private final CoreMember myself;
    private final CoreTopologyService discoveryService;
    private final LogProvider logProvider;

    @Override
    public CoreMember id()
    {
        return myself;
    }

    @Override
    public Role currentRole()
    {
        return raft.currentRole();
    }

    @Override
    public void downloadSnapshot( AdvertisedSocketAddress source ) throws InterruptedException, StoreCopyFailedException
    {
        coreState.downloadSnapshot( source );
    }

    @Override
    public void compact() throws IOException
    {
        coreState.compact();
    }

    public enum RaftLogImplementation
    {
        NAIVE,
        IN_MEMORY,
        PHYSICAL,
        SEGMENTED;

        public static RaftLogImplementation fromString( String value )
        {
            try
            {
                return RaftLogImplementation.valueOf( value );
            }
            catch ( IllegalArgumentException ex )
            {
                return NAIVE;
            }
        }
    }

    @Override
    public void registerProcedures( Procedures procedures )
    {
        try
        {
            procedures.register( new DiscoverMembersProcedure( discoveryService, logProvider ) );
            procedures.register( new AcquireEndpointsProcedure( discoveryService, raft, logProvider ) );
        }
        catch ( ProcedureException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    protected SPI spi()
    {
        return this;
    }

    public EnterpriseCoreEditionModule( final PlatformModule platformModule,
                                        DiscoveryServiceFactory discoveryServiceFactory )
    {
        ioLimiter = new ConfigurableIOLimiter( platformModule.config );

        final Dependencies dependencies = platformModule.dependencies;
        final Config config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final File storeDir = platformModule.storeDir;
        final File clusterStateDirectory = createClusterStateDirectory( storeDir, fileSystem );
        final LifeSupport life = platformModule.life;
        final GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;

        logProvider = logging.getInternalLogProvider();

        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        discoveryService = discoveryServiceFactory.coreDiscoveryService( config, logProvider );

        life.add( dependencies.satisfyDependency( discoveryService ) );

        final CoreReplicatedContentMarshal marshal = new CoreReplicatedContentMarshal();
        int maxQueueSize = config.get( CoreEdgeClusterSettings.outgoing_queue_size );
        final SenderService senderService = new SenderService(
                new RaftChannelInitializer( marshal ), logProvider, platformModule.monitors, maxQueueSize,
                new NonBlockingChannels() );
        life.add( senderService );

        myself = new CoreMember(
                config.get( CoreEdgeClusterSettings.transaction_advertised_address ),
                config.get( CoreEdgeClusterSettings.raft_advertised_address )
        );

        final MessageLogger<AdvertisedSocketAddress> messageLogger;
        if ( config.get( CoreEdgeClusterSettings.raft_messages_log_enable ) )
        {
            messageLogger = life.add(
                    new BetterMessageLogger<>( myself.getRaftAddress(), raftMessagesLog( storeDir ) )
            );
        }
        else
        {
            messageLogger = new NullMessageLogger<>();
        }

        LoggingOutbound<AdvertisedSocketAddress> loggingOutbound = new LoggingOutbound<>(
                senderService, myself.getRaftAddress(), messageLogger );

        ListenSocketAddress raftListenAddress = config.get( CoreEdgeClusterSettings.raft_listen_address );
        RaftServer<CoreMember> raftServer = new RaftServer<>( marshal, raftListenAddress, logProvider );

        final DelayedRenewableTimeoutService raftTimeoutService =
                new DelayedRenewableTimeoutService( Clock.systemUTC(), logProvider );

        RaftLog underlyingLog = createRaftLog(
                config, life, fileSystem, clusterStateDirectory, marshal, logProvider, databaseHealthSupplier );

        MonitoredRaftLog raftLog = new MonitoredRaftLog( underlyingLog, platformModule.monitors );

        LocalDatabase localDatabase = new LocalDatabase( platformModule.storeDir,
                new CopiedStoreRecovery( config, platformModule.kernelExtensions.listFactories(),
                        platformModule.pageCache ),
                new StoreFiles( new DefaultFileSystemAbstraction() ),
                dependencies.provideDependency( NeoStoreDataSource.class ),
                platformModule.dependencies.provideDependency( TransactionIdStore.class ), databaseHealthSupplier );

        NonBlockingChannels nonBlockingChannels = new NonBlockingChannels();

        CoreToCoreClient.ChannelInitializer channelInitializer = new CoreToCoreClient.ChannelInitializer(
                logProvider, nonBlockingChannels );
        CoreToCoreClient coreToCoreClient = life.add( new CoreToCoreClient( logProvider,
                channelInitializer, platformModule.monitors, maxQueueSize, nonBlockingChannels ) );
        channelInitializer.setOwner( coreToCoreClient );

        StoreFetcher storeFetcher = new StoreFetcher( logProvider, fileSystem, platformModule.pageCache,
                new StoreCopyClient( coreToCoreClient ), new TxPullClient( coreToCoreClient ),
                new TransactionLogCatchUpFactory() );

        GlobalSession<CoreMember> myGlobalSession = new GlobalSession<>( UUID.randomUUID(), myself );
        LocalSessionPool<CoreMember> sessionPool = new LocalSessionPool<>( myGlobalSession );
        ProgressTrackerImpl progressTracker = new ProgressTrackerImpl( myGlobalSession );

        try
        {
            DurableStateStorage<Long> lastFlushedStorage = life.add( new DurableStateStorage<>(
                    fileSystem, new File( clusterStateDirectory, "last-flushed-state" ), "last-flushed",
                    new LongIndexMarshal(), config.get( CoreEdgeClusterSettings.last_flushed_state_size ),
                    databaseHealthSupplier, logProvider ) );

            DurableStateStorage<Long> lastApplyingStorage = life.add( new DurableStateStorage<>(
                    fileSystem, new File( clusterStateDirectory, "last-applying-state" ), "last-applying",
                    new LongIndexMarshal(), config.get( CoreEdgeClusterSettings.last_flushed_state_size ),
                    databaseHealthSupplier, logProvider ) );

            StateStorage<GlobalSessionTrackerState<CoreMember>> sessionTrackerStorage;
            try
            {
                sessionTrackerStorage = life.add( new DurableStateStorage<>(
                        fileSystem, new File( clusterStateDirectory, "session-tracker-state" ), "session-tracker",
                        new GlobalSessionTrackerState.Marshal<>( new CoreMemberMarshal() ),
                        config.get( CoreEdgeClusterSettings.global_session_tracker_state_size ),
                        databaseHealthSupplier, logProvider
                ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }

            CoreStateApplier applier = new CoreStateApplier( logProvider );
            CoreStateDownloader downloader = new CoreStateDownloader( localDatabase, storeFetcher, coreToCoreClient,
                    logProvider );

            InFlightMap<Long, RaftLogEntry> inFlightMap = new InFlightMap<>();

            coreState = new CoreState(
                    raftLog, config.get( CoreEdgeClusterSettings.state_machine_apply_max_batch_size ),
                    config.get( CoreEdgeClusterSettings.state_machine_flush_window_size ),
                    databaseHealthSupplier, logProvider, progressTracker, lastFlushedStorage, lastApplyingStorage,
                    sessionTrackerStorage, new NotMyselfSelectionStrategy( discoveryService, myself ), applier,
                    downloader, inFlightMap, platformModule.monitors );

            raft = createRaft( life, loggingOutbound, discoveryService, config, messageLogger, raftLog,
                    coreState, fileSystem, clusterStateDirectory, myself, logProvider, raftServer,
                    raftTimeoutService, databaseHealthSupplier, inFlightMap, platformModule.monitors,
                    platformModule.jobScheduler );

            life.add( new PruningScheduler( coreState, platformModule.jobScheduler,
                    config.get( CoreEdgeClusterSettings.raft_log_pruning_frequency ) ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        RaftReplicator<CoreMember> replicator = new RaftReplicator<>( raft, myself,
                new RaftOutbound( loggingOutbound ), sessionPool, progressTracker, new ExponentialBackoffStrategy(
                10, SECONDS ) );

        dependencies.satisfyDependency( raft );

        StateStorage<ReplicatedLockTokenState<CoreMember>> lockTokenState;
        try
        {
            lockTokenState = life.add( new DurableStateStorage<>(
                    fileSystem, new File( clusterStateDirectory, "lock-token-state" ), "lock-token",
                    new ReplicatedLockTokenState.Marshal<>( new CoreMemberMarshal() ),
                    config.get( CoreEdgeClusterSettings.replicated_lock_token_state_size ),
                    databaseHealthSupplier, logProvider
            ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        final StateStorage<IdAllocationState> idAllocationState;
        try
        {
            idAllocationState = life.add( new DurableStateStorage<>(
                    fileSystem, new File( clusterStateDirectory, "id-allocation-state" ), "id-allocation",
                    new IdAllocationState.Marshal(),
                    config.get( CoreEdgeClusterSettings.id_alloc_state_size ), databaseHealthSupplier, logProvider
            ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine(
                idAllocationState );

        int allocationChunk = 1024; // TODO: AllocationChunk should be configurable and per type.
        ReplicatedIdRangeAcquirer idRangeAcquirer = new ReplicatedIdRangeAcquirer( replicator,
                idAllocationStateMachine, allocationChunk, myself, logProvider );

        long electionTimeout = config.get( CoreEdgeClusterSettings.leader_election_timeout );
        MembershipWaiter<CoreMember> membershipWaiter =
                new MembershipWaiter<>( myself, platformModule.jobScheduler, electionTimeout * 4, logProvider );

        ReplicatedIdGeneratorFactory replicatedIdGeneratorFactory =
                createIdGeneratorFactory( fileSystem, idRangeAcquirer, logProvider );

        this.idGeneratorFactory = dependencies.satisfyDependency( replicatedIdGeneratorFactory );
        dependencies.satisfyDependency( new IdBasedStoreEntityCounters( this.idGeneratorFactory ) );

        Long tokenCreationTimeout = config.get( CoreEdgeClusterSettings.token_creation_timeout );

        TokenRegistry<RelationshipTypeToken> relationshipTypeTokenRegistry = new TokenRegistry<>( "RelationshipType" );
        ReplicatedRelationshipTypeTokenHolder relationshipTypeTokenHolder = new ReplicatedRelationshipTypeTokenHolder(
                relationshipTypeTokenRegistry, replicator, this.idGeneratorFactory, dependencies,
                tokenCreationTimeout );

        TokenRegistry<Token> propertyKeyTokenRegistry = new TokenRegistry<>( "PropertyKey" );
        ReplicatedPropertyKeyTokenHolder propertyKeyTokenHolder = new ReplicatedPropertyKeyTokenHolder(
                propertyKeyTokenRegistry, replicator, this.idGeneratorFactory, dependencies, tokenCreationTimeout );

        TokenRegistry<Token> labelTokenRegistry = new TokenRegistry<>( "Label" );
        ReplicatedLabelTokenHolder labelTokenHolder = new ReplicatedLabelTokenHolder(
                labelTokenRegistry, replicator, this.idGeneratorFactory, dependencies, tokenCreationTimeout );

        ReplicatedLockTokenStateMachine<CoreMember> replicatedLockTokenStateMachine =
                new ReplicatedLockTokenStateMachine<>( lockTokenState );

        RecoverTransactionLogState txLogState = new RecoverTransactionLogState( dependencies, logProvider );

        ReplicatedTokenStateMachine<Token>
                labelTokenStateMachine = new ReplicatedTokenStateMachine<>(
                labelTokenRegistry, new Token.Factory(),
                logProvider );

        ReplicatedTokenStateMachine<Token>
                propertyKeyTokenStateMachine = new ReplicatedTokenStateMachine<>(
                propertyKeyTokenRegistry, new Token.Factory(),
                logProvider );

        ReplicatedTokenStateMachine<RelationshipTypeToken>
                relationshipTypeTokenStateMachine = new ReplicatedTokenStateMachine<>(
                relationshipTypeTokenRegistry, new RelationshipTypeToken.Factory(),
                logProvider );

        ReplicatedTransactionStateMachine<CoreMember> replicatedTxStateMachine =
                new ReplicatedTransactionStateMachine<>( replicatedLockTokenStateMachine,
                        config.get( CoreEdgeClusterSettings.state_machine_apply_max_batch_size ),
                        logging.getInternalLogProvider() );

        dependencies.satisfyDependencies( replicatedTxStateMachine );

        CoreStateMachines coreStateMachines = new CoreStateMachines(
                replicatedTxStateMachine, labelTokenStateMachine, relationshipTypeTokenStateMachine,
                propertyKeyTokenStateMachine, replicatedLockTokenStateMachine, idAllocationStateMachine,
                coreState, txLogState, raftLog );

        commitProcessFactory = ( appender, applier, ignored ) ->
        {
            TransactionRepresentationCommitProcess localCommit = new TransactionRepresentationCommitProcess(
                    appender, applier );
            coreStateMachines.refresh( localCommit ); // This gets called when a core-to-core download is performed.
            return new ReplicatedTransactionCommitProcess( replicator );
        };

        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;

        dependencies.satisfyDependency( createKernelData( fileSystem, platformModule.pageCache, storeDir,
                config, graphDatabaseFacade, life ) );

        life.add( dependencies.satisfyDependency( createAuthManager( config, logging ) ) );

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );

        constraintSemantics = new EnterpriseConstraintSemantics();

        coreAPIAvailabilityGuard = new CoreAPIAvailabilityGuard( platformModule.availabilityGuard,
                transactionStartTimeout );

        registerRecovery( platformModule.databaseInfo, life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );

        long leaderLockTokenTimeout = config.get( CoreEdgeClusterSettings.leader_lock_token_timeout );
        Locks lockManager = createLockManager( config, logging, replicator, myself,
                raft, leaderLockTokenTimeout, replicatedLockTokenStateMachine );

        this.lockManager = dependencies.satisfyDependency( lockManager );

        CatchupServer catchupServer = new CatchupServer( logProvider,
                new StoreIdSupplier( platformModule ),
                platformModule.dependencies.provideDependency( TransactionIdStore.class ),
                platformModule.dependencies.provideDependency( LogicalTransactionStore.class ),
                new DataSourceSupplier( platformModule ),
                new CheckpointerSupplier( platformModule.dependencies ),
                coreState,
                config.get( CoreEdgeClusterSettings.transaction_listen_address ),
                platformModule.monitors );

        long joinCatchupTimeout = config.get( CoreEdgeClusterSettings.join_catch_up_timeout );

        life.add( CoreServerStartupProcess.createLifeSupport(
                platformModule.dataSourceManager, replicatedIdGeneratorFactory, raft,
                coreState, raftServer,
                catchupServer, raftTimeoutService, membershipWaiter,
                joinCatchupTimeout, logProvider ) );
    }

    private RaftLog createRaftLog(
            Config config, LifeSupport life, FileSystemAbstraction fileSystem, File clusterStateDirectory,
            CoreReplicatedContentMarshal marshal, LogProvider logProvider,
            Supplier<DatabaseHealth> databaseHealthSupplier )
    {
        RaftLogImplementation raftLogImplementation = RaftLogImplementation.fromString(
                config.get( CoreEdgeClusterSettings.raft_log_implementation ) );
        switch ( raftLogImplementation )
        {
            case IN_MEMORY:
                return new InMemoryRaftLog();
            case PHYSICAL:
            {
                long rotateAtSize = config.get( CoreEdgeClusterSettings.raft_log_rotation_size );
                String pruneConf = config.get( CoreEdgeClusterSettings.raft_log_pruning_strategy );
                int entryCacheSize = config.get( CoreEdgeClusterSettings.raft_log_entry_cache_size );
                int metaDataCacheSize = config.get( CoreEdgeClusterSettings.raft_log_meta_data_cache_size );
                int headerCacheSize = config.get( CoreEdgeClusterSettings.raft_log_header_cache_size );

                return life.add( new PhysicalRaftLog(
                        fileSystem,
                        new File( clusterStateDirectory, PhysicalRaftLog.PHYSICAL_LOG_DIRECTORY_NAME ),
                        rotateAtSize, pruneConf, entryCacheSize, headerCacheSize,
                        new PhysicalRaftLogFile.Monitor.Adapter(), marshal, databaseHealthSupplier, logProvider,
                        new RaftLogMetadataCache( metaDataCacheSize ) ) );
            }

            case SEGMENTED:
            {
                long rotateAtSize = config.get( CoreEdgeClusterSettings.raft_log_rotation_size );
                int metaDataCacheSize = config.get( CoreEdgeClusterSettings.raft_log_meta_data_cache_size );
                String pruningStrategyConfig = config.get( CoreEdgeClusterSettings.raft_log_pruning_strategy );
                int entryCacheSize = config.get( CoreEdgeClusterSettings.raft_log_entry_cache_size );

                return life.add( new SegmentedRaftLog(
                        fileSystem,
                        new File( clusterStateDirectory, PhysicalRaftLog.PHYSICAL_LOG_DIRECTORY_NAME ),
                        rotateAtSize,
                        marshal,
                        logProvider,
                        entryCacheSize,
                        pruningStrategyConfig ) );
            }

            case NAIVE:
            default:
                return life.add( new NaiveDurableRaftLog(
                        fileSystem,
                        new File( clusterStateDirectory, NaiveDurableRaftLog.NAIVE_LOG_DIRECTORY_NAME ),
                        marshal, logProvider ) );
        }
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

    private static RaftInstance<CoreMember> createRaft( LifeSupport life,
                                                        Outbound<AdvertisedSocketAddress> outbound,
                                                        CoreTopologyService discoveryService,
                                                        Config config,
                                                        MessageLogger<AdvertisedSocketAddress> messageLogger,
                                                        RaftLog raftLog,
                                                        RaftStateMachine raftStateMachine,
                                                        FileSystemAbstraction fileSystem,
                                                        File clusterStateDirectory,
                                                        CoreMember myself,
                                                        LogProvider logProvider,
                                                        RaftServer<CoreMember> raftServer,
                                                        DelayedRenewableTimeoutService raftTimeoutService,
                                                        Supplier<DatabaseHealth> databaseHealthSupplier,
                                                        InFlightMap<Long, RaftLogEntry> inFlightMap,
                                                        Monitors monitors,
                                                        JobScheduler jobScheduler )
    {
        StateStorage<TermState> termState;
        try
        {
            StateStorage<TermState> durableTermState = life.add( new DurableStateStorage<>(
                    fileSystem, new File( clusterStateDirectory, "term-state" ), "term-state",
                    new TermState.Marshal(),
                    config.get( CoreEdgeClusterSettings.term_state_size ), databaseHealthSupplier, logProvider
            ) );
            termState = new MonitoredTermStateStorage( durableTermState, monitors );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        StateStorage<VoteState<CoreMember>> voteState;
        try
        {
            voteState = life.add( new DurableStateStorage<>( fileSystem,
                    new File( clusterStateDirectory, "vote-state" ), "vote-state",
                    new VoteState.Marshal<>( new CoreMemberMarshal() ),
                    config.get( CoreEdgeClusterSettings.vote_state_size ), databaseHealthSupplier, logProvider
            ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        StateStorage<RaftMembershipState<CoreMember>> raftMembershipStorage;
        try
        {
            raftMembershipStorage = life.add( new DurableStateStorage<>( fileSystem,
                    new File( clusterStateDirectory, "membership-state" ), "membership-state",
                    new RaftMembershipState.Marshal<>( new CoreMemberMarshal() ),
                    config.get( CoreEdgeClusterSettings.raft_membership_state_size ),
                    databaseHealthSupplier, logProvider
            ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        LoggingInbound<RaftMessages.RaftMessage<CoreMember>>
                loggingRaftInbound = new LoggingInbound<>( raftServer, messageLogger, myself.getRaftAddress() );

        long electionTimeout = config.get( CoreEdgeClusterSettings.leader_election_timeout );
        long heartbeatInterval = electionTimeout / 3;

        Integer expectedClusterSize = config.get( CoreEdgeClusterSettings.expected_core_cluster_size );

        CoreMemberSetBuilder memberSetBuilder = new CoreMemberSetBuilder();

        LeaderOnlyReplicator leaderOnlyReplicator = new LeaderOnlyReplicator<>( myself, myself.getRaftAddress(),
                outbound );

        RaftMembershipManager<CoreMember> raftMembershipManager = new RaftMembershipManager<>( leaderOnlyReplicator,
                memberSetBuilder, raftLog, logProvider, expectedClusterSize, electionTimeout, Clock.systemUTC(),
                config.get( CoreEdgeClusterSettings.join_catch_up_timeout ), raftMembershipStorage );

        RaftLogShippingManager<CoreMember> logShipping = new RaftLogShippingManager<>( new RaftOutbound( outbound ),
                logProvider, raftLog,
                Clock.systemUTC(), myself, raftMembershipManager, electionTimeout,
                config.get( CoreEdgeClusterSettings.catchup_batch_size ),
                config.get( CoreEdgeClusterSettings.log_shipping_max_lag ),
                inFlightMap );

        RaftInstance<CoreMember> raftInstance = new RaftInstance<>(
                myself, termState, voteState, raftLog, raftStateMachine, electionTimeout, heartbeatInterval,
                raftTimeoutService,
                new RaftOutbound( outbound ), logProvider,
                raftMembershipManager, logShipping, databaseHealthSupplier, inFlightMap, monitors );

        int queueSize = config.get( CoreEdgeClusterSettings.raft_in_queue_size );
        int maxBatch = config.get( CoreEdgeClusterSettings.raft_in_queue_max_batch );
        BatchingMessageHandler<CoreMember> batchingMessageHandler =
                new BatchingMessageHandler<>( raftInstance, logProvider, queueSize, maxBatch );

        life.add( new ContinuousJob( jobScheduler, new JobScheduler.Group( "raft-batch-handler", NEW_THREAD ),
                batchingMessageHandler ) );

        loggingRaftInbound.registerHandler( batchingMessageHandler );

        life.add( new RaftDiscoveryServiceConnector( discoveryService, raftInstance ) );

        life.add( new LifecycleAdapter()
        {
            @Override
            public void shutdown() throws Throwable
            {
                logShipping.destroy();
            }
        } );

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

    private SchemaWriteGuard createSchemaWriteGuard()
    {
        return () -> {};
    }

    private KernelData createKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir,
                                         Config config, GraphDatabaseAPI graphAPI, LifeSupport life )
    {
        DefaultKernelData kernelData = new DefaultKernelData( fileSystem, pageCache, storeDir, config, graphAPI );
        return life.add( kernelData );
    }

    private ReplicatedIdGeneratorFactory createIdGeneratorFactory( FileSystemAbstraction fileSystem,
                                                                   final ReplicatedIdRangeAcquirer idRangeAcquirer,
                                                                   final LogProvider logProvider )
    {
        return new ReplicatedIdGeneratorFactory( fileSystem, idRangeAcquirer, logProvider );
    }

    private Locks createLockManager( final Config config, final LogService logging, final Replicator replicator,
                                     CoreMember myself, LeaderLocator<CoreMember> leaderLocator,
                                     long leaderLockTokenTimeout,
                                     ReplicatedLockTokenStateMachine lockTokenStateMachine )
    {
        Locks localLocks = CommunityEditionModule.createLockManager( config, logging );

        return new LeaderOnlyLockManager<>( myself, replicator, leaderLocator, localLocks,
                leaderLockTokenTimeout, lockTokenStateMachine );
    }

    private TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return () -> new TransactionHeaderInformation( -1, -1, new byte[0] );
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

    @Override
    protected void doAfterRecoveryAndStartup( DatabaseInfo databaseInfo, DependencyResolver dependencyResolver )
    {
        super.doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );

        new RemoveOrphanConstraintIndexesOnStartup( dependencyResolver.resolveDependency( NeoStoreDataSource.class )
                .getKernel(), dependencyResolver.resolveDependency( LogService.class ).getInternalLogProvider() )
                .perform();
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
