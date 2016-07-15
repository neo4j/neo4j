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
import java.util.UUID;
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.CatchupServer;
import org.neo4j.coreedge.catchup.CheckpointerSupplier;
import org.neo4j.coreedge.catchup.DataSourceSupplier;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreFiles;
import org.neo4j.coreedge.catchup.storecopy.core.CoreToCoreClient;
import org.neo4j.coreedge.catchup.storecopy.edge.CopiedStoreRecovery;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreCopyClient;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.catchup.tx.edge.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.edge.TxPullClient;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.raft.BatchingMessageHandler;
import org.neo4j.coreedge.raft.ConsensusModule;
import org.neo4j.coreedge.raft.ContinuousJob;
import org.neo4j.coreedge.raft.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.RaftServer;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.pruning.PruningScheduler;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.membership.MembershipWaiter;
import org.neo4j.coreedge.raft.net.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.raft.net.LoggingInbound;
import org.neo4j.coreedge.raft.net.LoggingOutbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.net.RaftChannelInitializer;
import org.neo4j.coreedge.raft.net.RaftOutbound;
import org.neo4j.coreedge.raft.replication.ProgressTrackerImpl;
import org.neo4j.coreedge.raft.replication.RaftReplicator;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdGeneratorFactory;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdRangeAcquirer;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.session.LocalSessionPool;
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
import org.neo4j.kernel.api.bolt.SessionTracker;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.StandardSessionTracker;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
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
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.Token;
import org.neo4j.udc.UsageData;

import static java.time.Clock.systemUTC;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.NEW_THREAD;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class EnterpriseCoreEditionModule extends EditionModule
{
    public static final String CLUSTER_STATE_DIRECTORY_NAME = "cluster-state";

    private final ConsensusModule consensusModule;
    private final CoreTopologyService discoveryService;
    private final LogProvider logProvider;

    public enum RaftLogImplementation
    {
        IN_MEMORY, SEGMENTED
    }

    @Override
    public void registerProcedures( Procedures procedures )
    {
        try
        {
            procedures.register( new DiscoverMembersProcedure( discoveryService, logProvider ) );
            procedures.register( new AcquireEndpointsProcedure( discoveryService, consensusModule.raftInstance(), logProvider ) );
            procedures.register( new ClusterOverviewProcedure( discoveryService, consensusModule.raftInstance(), logProvider ) );
        }
        catch ( ProcedureException e )
        {
            throw new RuntimeException( e );
        }
    }

    EnterpriseCoreEditionModule( final PlatformModule platformModule,
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

        CoreMember myself;
        StateStorage<IdAllocationState> idAllocationState;
        StateStorage<Long> lastFlushedStorage;
        StateStorage<GlobalSessionTrackerState> sessionTrackerStorage;
        StateStorage<ReplicatedLockTokenState> lockTokenState;

        try
        {
            StateStorage<CoreMember> idStorage = life.add( new DurableStateStorage<>(
                    fileSystem, clusterStateDirectory, "raft-member-id", new CoreMemberMarshal(), 1,
                    databaseHealthSupplier, logProvider ) );
            CoreMember member = idStorage.getInitialState();
            if ( member == null )
            {
                member = new CoreMember( UUID.randomUUID() );
                idStorage.persistStoreData( member );
            }
            myself = member;

            lastFlushedStorage = life.add(
                    new DurableStateStorage<>( fileSystem, new File( clusterStateDirectory, "last-flushed-state" ),
                            "last-flushed", new LongIndexMarshal(), config.get( CoreEdgeClusterSettings.last_flushed_state_size ),
                            databaseHealthSupplier, logProvider ) );

            sessionTrackerStorage = life.add( new DurableStateStorage<>( fileSystem,
                    new File( clusterStateDirectory, "session-tracker-state" ), "session-tracker",
                    new GlobalSessionTrackerState.Marshal( new CoreMemberMarshal() ),
                    config.get( CoreEdgeClusterSettings.global_session_tracker_state_size ), databaseHealthSupplier,
                    logProvider ) );

            lockTokenState = life.add(
                    new DurableStateStorage<>( fileSystem, new File( clusterStateDirectory, "lock-token-state" ),
                            "lock-token", new ReplicatedLockTokenState.Marshal( new CoreMemberMarshal() ),
                            config.get( CoreEdgeClusterSettings.replicated_lock_token_state_size ),
                            databaseHealthSupplier, logProvider ) );

            idAllocationState = life.add(
                    new DurableStateStorage<>( fileSystem, new File( clusterStateDirectory, "id-allocation-state" ),
                            "id-allocation", new IdAllocationState.Marshal(),
                            config.get( CoreEdgeClusterSettings.id_alloc_state_size ), databaseHealthSupplier,
                            logProvider ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        discoveryService = discoveryServiceFactory.coreDiscoveryService( config, myself, logProvider );

        life.add( dependencies.satisfyDependency( discoveryService ) );

        final CoreReplicatedContentMarshal marshal = new CoreReplicatedContentMarshal();
        int maxQueueSize = config.get( CoreEdgeClusterSettings.outgoing_queue_size );
        long logThresholdMillis = config.get( CoreEdgeClusterSettings.unknown_address_logging_throttle );
        final SenderService senderService =
                new SenderService( new RaftChannelInitializer( marshal, logProvider ), logProvider, platformModule.monitors,
                        maxQueueSize, new NonBlockingChannels() );
        life.add( senderService );

        final MessageLogger<CoreMember> messageLogger;
        if ( config.get( CoreEdgeClusterSettings.raft_messages_log_enable ) )
        {
            File logsDir = config.get( GraphDatabaseSettings.logs_directory );
            messageLogger = life.add( new BetterMessageLogger<>( myself, raftMessagesLog( logsDir ) ) );
        }
        else
        {
            messageLogger = new NullMessageLogger<>();
        }

        ListenSocketAddress raftListenAddress = config.get( CoreEdgeClusterSettings.raft_listen_address );

        CopiedStoreRecovery copiedStoreRecovery = new CopiedStoreRecovery( config,
                platformModule.kernelExtensions.listFactories(), platformModule.pageCache );

        LocalDatabase localDatabase = new LocalDatabase( platformModule.storeDir, copiedStoreRecovery,
                new StoreFiles( new DefaultFileSystemAbstraction() ),
                platformModule.dataSourceManager,
                platformModule.dependencies.provideDependency( TransactionIdStore.class ), databaseHealthSupplier,
                logProvider);

        final DelayedRenewableTimeoutService raftTimeoutService =
                new DelayedRenewableTimeoutService( systemUTC(), logProvider );

        NonBlockingChannels nonBlockingChannels = new NonBlockingChannels();

        CoreToCoreClient.ChannelInitializer channelInitializer =
                new CoreToCoreClient.ChannelInitializer( logProvider, nonBlockingChannels );
        CoreToCoreClient coreToCoreClient = life.add(
                new CoreToCoreClient( logProvider, channelInitializer, platformModule.monitors, maxQueueSize,
                        nonBlockingChannels, discoveryService, logThresholdMillis ) );
        channelInitializer.setOwner( coreToCoreClient );

        StoreFetcher storeFetcher = new StoreFetcher( logProvider, fileSystem, platformModule.pageCache,
                new StoreCopyClient( coreToCoreClient ), new TxPullClient( coreToCoreClient ),
                new TransactionLogCatchUpFactory() );

        GlobalSession myGlobalSession = new GlobalSession( UUID.randomUUID(), myself );
        LocalSessionPool sessionPool = new LocalSessionPool( myGlobalSession );
        ProgressTrackerImpl progressTracker = new ProgressTrackerImpl( myGlobalSession );
        RaftOutbound raftOutbound =
                new RaftOutbound( discoveryService, senderService, localDatabase, logProvider, logThresholdMillis );
        Outbound<CoreMember,RaftMessages.RaftMessage> loggingOutbound = new LoggingOutbound<>(
                raftOutbound, myself, messageLogger );

        RaftServer raftServer;
        CoreState coreState;

        CoreStateApplier coreStateApplier = new CoreStateApplier( logProvider );
        CoreStateDownloader downloader = new CoreStateDownloader( localDatabase, storeFetcher,
                coreToCoreClient, logProvider );

        InFlightMap<Long,RaftLogEntry> inFlightMap = new InFlightMap<>();

        NotMyselfSelectionStrategy someoneElse = new NotMyselfSelectionStrategy( discoveryService, myself );

        consensusModule =
                new ConsensusModule( myself, platformModule, raftOutbound, clusterStateDirectory, raftTimeoutService,
                        discoveryService, lastFlushedStorage.getInitialState() );

        coreState = dependencies.satisfyDependency( new CoreState(
                consensusModule.raftLog(), config.get( CoreEdgeClusterSettings.state_machine_apply_max_batch_size ),
                config.get( CoreEdgeClusterSettings.state_machine_flush_window_size ),
                databaseHealthSupplier, logProvider, progressTracker, lastFlushedStorage,
                sessionTrackerStorage, someoneElse, coreStateApplier, downloader, inFlightMap, platformModule.monitors ) );

        raftServer = new RaftServer( marshal, raftListenAddress, logProvider );

        LoggingInbound<RaftMessages.StoreIdAwareMessage> loggingRaftInbound =
                new LoggingInbound<>( raftServer, messageLogger, myself );

        int queueSize = config.get( CoreEdgeClusterSettings.raft_in_queue_size );
        int maxBatch = config.get( CoreEdgeClusterSettings.raft_in_queue_max_batch );
        BatchingMessageHandler batchingMessageHandler =
                new BatchingMessageHandler( consensusModule.raftInstance(), logProvider, queueSize, maxBatch, localDatabase, coreState );

        life.add( new ContinuousJob( platformModule.jobScheduler, new JobScheduler.Group( "raft-batch-handler", NEW_THREAD ),
                batchingMessageHandler ) );

        loggingRaftInbound.registerHandler( batchingMessageHandler );

        dependencies.satisfyDependency( consensusModule.raftInstance() );

        life.add( new PruningScheduler( coreState, platformModule.jobScheduler,
                config.get( CoreEdgeClusterSettings.raft_log_pruning_frequency ) ) );

        RaftReplicator replicator =
                new RaftReplicator( consensusModule.raftInstance(), myself,
                        loggingOutbound,
                        sessionPool, progressTracker,
                        new ExponentialBackoffStrategy( 10, SECONDS ) );

        ReplicatedIdAllocationStateMachine idAllocationStateMachine =
                new ReplicatedIdAllocationStateMachine( idAllocationState );

        int allocationChunk = 1024; // TODO: AllocationChunk should be configurable and per type.
        ReplicatedIdRangeAcquirer idRangeAcquirer =
                new ReplicatedIdRangeAcquirer( replicator, idAllocationStateMachine, allocationChunk, myself,
                        logProvider );

        long electionTimeout = config.get( CoreEdgeClusterSettings.leader_election_timeout );
        MembershipWaiter membershipWaiter =
                new MembershipWaiter( myself, platformModule.jobScheduler, electionTimeout * 4, batchingMessageHandler, logProvider );

        idTypeConfigurationProvider = new EnterpriseIdTypeConfigurationProvider( config );
        ReplicatedIdGeneratorFactory replicatedIdGeneratorFactory =
                createIdGeneratorFactory( fileSystem, idRangeAcquirer, logProvider, idTypeConfigurationProvider );

        this.idGeneratorFactory = dependencies.satisfyDependency( replicatedIdGeneratorFactory );
        dependencies.satisfyDependency( new IdBasedStoreEntityCounters( this.idGeneratorFactory ) );

        Long tokenCreationTimeout = config.get( CoreEdgeClusterSettings.token_creation_timeout );

        TokenRegistry<RelationshipTypeToken> relationshipTypeTokenRegistry = new TokenRegistry<>( "RelationshipType" );
        ReplicatedRelationshipTypeTokenHolder relationshipTypeTokenHolder =
                new ReplicatedRelationshipTypeTokenHolder( relationshipTypeTokenRegistry, replicator,
                        this.idGeneratorFactory, dependencies, tokenCreationTimeout );

        TokenRegistry<Token> propertyKeyTokenRegistry = new TokenRegistry<>( "PropertyKey" );
        ReplicatedPropertyKeyTokenHolder propertyKeyTokenHolder =
                new ReplicatedPropertyKeyTokenHolder( propertyKeyTokenRegistry, replicator, this.idGeneratorFactory,
                        dependencies, tokenCreationTimeout );

        TokenRegistry<Token> labelTokenRegistry = new TokenRegistry<>( "Label" );
        ReplicatedLabelTokenHolder labelTokenHolder =
                new ReplicatedLabelTokenHolder( labelTokenRegistry, replicator, this.idGeneratorFactory, dependencies,
                        tokenCreationTimeout );

        ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine =
                new ReplicatedLockTokenStateMachine( lockTokenState );

        RecoverTransactionLogState txLogState = new RecoverTransactionLogState( dependencies, logProvider );

        ReplicatedTokenStateMachine<Token> labelTokenStateMachine =
                new ReplicatedTokenStateMachine<>( labelTokenRegistry, new Token.Factory(), logProvider );

        ReplicatedTokenStateMachine<Token> propertyKeyTokenStateMachine =
                new ReplicatedTokenStateMachine<>( propertyKeyTokenRegistry, new Token.Factory(), logProvider );

        ReplicatedTokenStateMachine<RelationshipTypeToken> relationshipTypeTokenStateMachine =
                new ReplicatedTokenStateMachine<>( relationshipTypeTokenRegistry, new RelationshipTypeToken.Factory(),
                        logProvider );

        ReplicatedTransactionStateMachine replicatedTxStateMachine =
                new ReplicatedTransactionStateMachine( replicatedLockTokenStateMachine,
                        config.get( CoreEdgeClusterSettings.state_machine_apply_max_batch_size ),
                        logging.getInternalLogProvider() );

        dependencies.satisfyDependencies( replicatedTxStateMachine );

        CoreStateMachines coreStateMachines = new CoreStateMachines( replicatedTxStateMachine, labelTokenStateMachine,
                relationshipTypeTokenStateMachine, propertyKeyTokenStateMachine, replicatedLockTokenStateMachine,
                idAllocationStateMachine, coreState, txLogState, consensusModule.raftLog(), localDatabase );

        commitProcessFactory = ( appender, applier, ignored ) -> {
            TransactionRepresentationCommitProcess localCommit =
                    new TransactionRepresentationCommitProcess( appender, applier );
            coreStateMachines.refresh( localCommit ); // This gets called when a core-to-core download is performed.
            return new ReplicatedTransactionCommitProcess( replicator );
        };

        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;

        dependencies.satisfyDependency(
                createKernelData( fileSystem, platformModule.pageCache, storeDir, config, graphDatabaseFacade, life ) );

        life.add( dependencies.satisfyDependency( createAuthManager( config, logging ) ) );

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );

        constraintSemantics = new EnterpriseConstraintSemantics();

        coreAPIAvailabilityGuard =
                new CoreAPIAvailabilityGuard( platformModule.availabilityGuard, transactionStartTimeout );

        registerRecovery( platformModule.databaseInfo, life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );

        long leaderLockTokenTimeout = config.get( CoreEdgeClusterSettings.leader_lock_token_timeout );
        Locks lockManager = createLockManager( config, logging, replicator, myself, consensusModule.raftInstance(), leaderLockTokenTimeout,
                replicatedLockTokenStateMachine );

        this.lockManager = dependencies.satisfyDependency( lockManager );

        CatchupServer catchupServer = new CatchupServer( logProvider, localDatabase,
                platformModule.dependencies.provideDependency( TransactionIdStore.class ),
                platformModule.dependencies.provideDependency( LogicalTransactionStore.class ),
                new DataSourceSupplier( platformModule ), new CheckpointerSupplier( platformModule.dependencies ),
                coreState, config.get( CoreEdgeClusterSettings.transaction_listen_address ), platformModule.monitors );

        long joinCatchupTimeout = config.get( CoreEdgeClusterSettings.join_catch_up_timeout );

        life.add( CoreServerStartupProcess.createLifeSupport(
                platformModule.dataSourceManager, replicatedIdGeneratorFactory, consensusModule.raftInstance(), coreState, raftServer,
                catchupServer, raftTimeoutService, membershipWaiter, joinCatchupTimeout, logProvider ) );

        dependencies.satisfyDependency( createSessionTracker() );
    }

    public boolean isLeader()
    {
        return consensusModule.raftInstance().currentRole() == Role.LEADER;
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

    private static PrintWriter raftMessagesLog( File logsDir )
    {
        //noinspection ResultOfMethodCallIgnored
        logsDir.mkdirs();
        try
        {

            return new PrintWriter( new FileOutputStream( new File( logsDir, "raft-messages.log" ), true ) );
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
            final ReplicatedIdRangeAcquirer idRangeAcquirer, final LogProvider logProvider,
            IdTypeConfigurationProvider idTypeConfigurationProvider )
    {
        return new ReplicatedIdGeneratorFactory( fileSystem, idRangeAcquirer, logProvider, idTypeConfigurationProvider );
    }

    private Locks createLockManager( final Config config, final LogService logging, final Replicator replicator,
            CoreMember myself, LeaderLocator leaderLocator, long leaderLockTokenTimeout,
            ReplicatedLockTokenStateMachine lockTokenStateMachine )
    {
        Locks localLocks = CommunityEditionModule.createLockManager( config, logging );

        return new LeaderOnlyLockManager( myself, replicator, leaderLocator, localLocks, leaderLockTokenTimeout,
                lockTokenStateMachine );
    }

    private TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return () -> new TransactionHeaderInformation( -1, -1, new byte[0] );
    }

    private void registerRecovery( final DatabaseInfo databaseInfo, LifeSupport life,
            final DependencyResolver dependencyResolver )
    {
        life.addLifecycleListener( ( instance, from, to ) -> {
            if ( instance instanceof DatabaseAvailability && LifecycleStatus.STARTED.equals( to ) )
            {
                doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );
            }
        } );
    }

    @Override
    protected void doAfterRecoveryAndStartup( DatabaseInfo databaseInfo, DependencyResolver dependencyResolver )
    {
        super.doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );

        if ( dependencyResolver.resolveDependency( RaftInstance.class ).isLeader() )
        {
            new RemoveOrphanConstraintIndexesOnStartup(
                    dependencyResolver.resolveDependency( NeoStoreDataSource.class ).getKernel(),
                    dependencyResolver.resolveDependency( LogService.class ).getInternalLogProvider() ).perform();
        }
    }

    private final class DefaultKernelData extends KernelData implements Lifecycle
    {
        private final GraphDatabaseAPI graphDb;

        DefaultKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir, Config config,
                GraphDatabaseAPI graphDb )
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

    @Override
    protected SessionTracker createSessionTracker()
    {
        return new StandardSessionTracker();
    }
}
