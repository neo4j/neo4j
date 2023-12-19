/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.server;

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.causalclustering.ReplicationModule;
import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupClientBuilder;
import org.neo4j.causalclustering.catchup.CatchupProtocolServerInstaller;
import org.neo4j.causalclustering.catchup.CatchupServerBuilder;
import org.neo4j.causalclustering.catchup.CatchupServerHandler;
import org.neo4j.causalclustering.catchup.CheckPointerService;
import org.neo4j.causalclustering.catchup.CheckpointerSupplier;
import org.neo4j.causalclustering.catchup.RegularCatchupServerHandler;
import org.neo4j.causalclustering.catchup.storecopy.CommitStateHelper;
import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.MemberIdRepository;
import org.neo4j.causalclustering.core.SupportedProtocolCreator;
import org.neo4j.causalclustering.core.TransactionBackupServiceProvider;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.pruning.PruningScheduler;
import org.neo4j.causalclustering.core.consensus.membership.MembershipWaiter;
import org.neo4j.causalclustering.core.consensus.membership.MembershipWaiterLifecycle;
import org.neo4j.causalclustering.core.state.ClusteringModule;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.core.state.CoreLife;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.CoreState;
import org.neo4j.causalclustering.core.state.LongIndexMarshal;
import org.neo4j.causalclustering.core.state.RaftLogPruner;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateDownloader;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateDownloaderService;
import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.helper.CompositeSuspendable;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.causalclustering.helper.Suspendable;
import org.neo4j.causalclustering.net.InstalledProtocolHandler;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.transaction_listen_address;
import static org.neo4j.time.Clocks.systemClock;

public class CoreServerModule
{
    public static final String CLUSTER_ID_NAME = "cluster-id";
    public static final String LAST_FLUSHED_NAME = "last-flushed";
    public static final String DB_NAME = "db-name";

    public final MembershipWaiterLifecycle membershipWaiterLifecycle;
    private final Server catchupServer;
    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    private final Optional<Server> backupServer;
    private final MemberIdRepository memberIdRepository;
    private final CoreStateMachinesModule coreStateMachinesModule;
    private final ConsensusModule consensusModule;
    private final ClusteringModule clusteringModule;
    private final LocalDatabase localDatabase;
    private final Supplier<DatabaseHealth> dbHealthSupplier;
    private final CommandApplicationProcess commandApplicationProcess;
    private final CoreSnapshotService snapshotService;
    private final CoreStateDownloaderService downloadService;
    private final Config config;
    private final JobScheduler jobScheduler;
    private final LogProvider logProvider;
    private final PlatformModule platformModule;

    public CoreServerModule( MemberIdRepository memberIdRepository, final PlatformModule platformModule, ConsensusModule consensusModule,
            CoreStateMachinesModule coreStateMachinesModule, ClusteringModule clusteringModule, ReplicationModule replicationModule,
            LocalDatabase localDatabase, Supplier<DatabaseHealth> dbHealthSupplier, File clusterStateDirectory,
            NettyPipelineBuilderFactory clientPipelineBuilderFactory, NettyPipelineBuilderFactory serverPipelineBuilderFactory,
            NettyPipelineBuilderFactory backupServerPipelineBuilderFactory, InstalledProtocolHandler installedProtocolsHandler )
    {
        this.memberIdRepository = memberIdRepository;
        this.coreStateMachinesModule = coreStateMachinesModule;
        this.consensusModule = consensusModule;
        this.clusteringModule = clusteringModule;
        this.localDatabase = localDatabase;
        this.dbHealthSupplier = dbHealthSupplier;
        this.platformModule = platformModule;

        this.config = platformModule.config;
        this.jobScheduler = platformModule.jobScheduler;

        final Dependencies dependencies = platformModule.dependencies;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final LifeSupport life = platformModule.life;

        this.logProvider = logging.getInternalLogProvider();
        LogProvider userLogProvider = logging.getUserLogProvider();

        CompositeSuspendable servicesToStopOnStoreCopy = new CompositeSuspendable();

        StateStorage<Long> lastFlushedStorage = platformModule.life.add(
                new DurableStateStorage<>( platformModule.fileSystem, clusterStateDirectory, LAST_FLUSHED_NAME, new LongIndexMarshal(),
                        platformModule.config.get( CausalClusteringSettings.last_flushed_state_size ), logProvider ) );

        consensusModule.raftMembershipManager().setRecoverFromIndexSupplier( lastFlushedStorage::getInitialState );

        CoreState coreState = new CoreState( coreStateMachinesModule.coreStateMachines,
                replicationModule.getSessionTracker(), lastFlushedStorage );

        final Supplier<DatabaseHealth> databaseHealthSupplier = platformModule.dependencies.provideDependency( DatabaseHealth.class );
        commandApplicationProcess = new CommandApplicationProcess(
                consensusModule.raftLog(),
                platformModule.config.get( CausalClusteringSettings.state_machine_apply_max_batch_size ),
                platformModule.config.get( CausalClusteringSettings.state_machine_flush_window_size ),
                databaseHealthSupplier,
                logProvider,
                replicationModule.getProgressTracker(),
                replicationModule.getSessionTracker(),
                coreState,
                consensusModule.inFlightCache(),
                platformModule.monitors );

        platformModule.dependencies.satisfyDependency( commandApplicationProcess ); // lastApplied() for CC-robustness

        this.snapshotService = new CoreSnapshotService( commandApplicationProcess, coreState, consensusModule.raftLog(), consensusModule.raftMachine() );

        CatchUpClient catchUpClient = createCatchupClient( clientPipelineBuilderFactory );
        CoreStateDownloader downloader = createCoreStateDownloader( servicesToStopOnStoreCopy, catchUpClient );

        this.downloadService = new CoreStateDownloaderService( platformModule.jobScheduler, downloader, commandApplicationProcess, logProvider,
                new ExponentialBackoffStrategy( 1, 30, SECONDS ).newTimeout(), databaseHealthSupplier );

        this.membershipWaiterLifecycle = createMembershipWaiterLifecycle();

        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        ApplicationSupportedProtocols supportedCatchupProtocols = supportedProtocolCreator.createSupportedCatchupProtocol();
        Collection<ModifierSupportedProtocols> supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();

        ApplicationProtocolRepository catchupProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), supportedCatchupProtocols );
        ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), supportedModifierProtocols );

        CheckPointerService checkPointerService =
                new CheckPointerService( new CheckpointerSupplier( platformModule.dependencies ), jobScheduler, JobScheduler.Groups.checkPoint );
        CatchupServerHandler catchupServerHandler = new RegularCatchupServerHandler( platformModule.monitors,
                logProvider, localDatabase::storeId, platformModule.dependencies.provideDependency( TransactionIdStore.class ),
                platformModule.dependencies.provideDependency( LogicalTransactionStore.class ), localDatabase::dataSource, localDatabase::isAvailable,
                fileSystem, platformModule.pageCache, platformModule.storeCopyCheckPointMutex, snapshotService,
                checkPointerService );

        CatchupProtocolServerInstaller.Factory catchupProtocolServerInstaller = new CatchupProtocolServerInstaller.Factory( serverPipelineBuilderFactory,
                logProvider, catchupServerHandler );

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                singletonList( catchupProtocolServerInstaller ), ModifierProtocolInstaller.allServerInstallers );

        HandshakeServerInitializer handshakeServerInitializer = new HandshakeServerInitializer( catchupProtocolRepository, modifierProtocolRepository,
                protocolInstallerRepository, serverPipelineBuilderFactory, logProvider );

        catchupServer = new CatchupServerBuilder( catchupServerHandler )
                .serverHandler( installedProtocolsHandler )
                .catchupProtocols( supportedCatchupProtocols )
                .modifierProtocols( supportedModifierProtocols )
                .pipelineBuilder( serverPipelineBuilderFactory )
                .userLogProvider( userLogProvider )
                .debugLogProvider( logProvider )
                .listenAddress( config.get( transaction_listen_address ) )
                .serverName( "catchup-server" )
                .build();

        TransactionBackupServiceProvider transactionBackupServiceProvider =
                new TransactionBackupServiceProvider( logProvider,
                        userLogProvider,
                        supportedCatchupProtocols,
                        supportedModifierProtocols,
                        backupServerPipelineBuilderFactory,
                        catchupServerHandler,
                        installedProtocolsHandler );

        backupServer = transactionBackupServiceProvider.resolveIfBackupEnabled( config );

        RaftLogPruner raftLogPruner = new RaftLogPruner( consensusModule.raftMachine(), commandApplicationProcess, platformModule.clock );
        dependencies.satisfyDependency( raftLogPruner );

        life.add( new PruningScheduler( raftLogPruner, jobScheduler,
                config.get( CausalClusteringSettings.raft_log_pruning_frequency ).toMillis(), logProvider ) );

        servicesToStopOnStoreCopy.add( this.catchupServer );
        backupServer.ifPresent( servicesToStopOnStoreCopy::add );
    }

    private CatchUpClient createCatchupClient( NettyPipelineBuilderFactory clientPipelineBuilderFactory )
    {
        SupportedProtocolCreator supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        ApplicationSupportedProtocols supportedCatchupProtocols = supportedProtocolCreator.createSupportedCatchupProtocol();
        Collection<ModifierSupportedProtocols> supportedModifierProtocols = supportedProtocolCreator.createSupportedModifierProtocols();
        Duration handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
        long inactivityTimeoutMillis = platformModule.config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ).toMillis();

        CatchUpClient catchUpClient = new CatchupClientBuilder( supportedCatchupProtocols, supportedModifierProtocols, clientPipelineBuilderFactory,
                handshakeTimeout, inactivityTimeoutMillis, logProvider, systemClock() ).build();
        platformModule.life.add( catchUpClient );
        return catchUpClient;
    }

    private CoreStateDownloader createCoreStateDownloader( Suspendable servicesToSuspendOnStoreCopy, CatchUpClient catchUpClient )
    {
        ExponentialBackoffStrategy storeCopyBackoffStrategy =
                new ExponentialBackoffStrategy( 1, config.get( CausalClusteringSettings.store_copy_backoff_max_wait ).toMillis(), TimeUnit.MILLISECONDS );

        RemoteStore remoteStore = new RemoteStore( logProvider, platformModule.fileSystem, platformModule.pageCache,
                new StoreCopyClient( catchUpClient, platformModule.monitors, logProvider, storeCopyBackoffStrategy ),
                new TxPullClient( catchUpClient, platformModule.monitors ), new TransactionLogCatchUpFactory(), config, platformModule.monitors );

        CopiedStoreRecovery copiedStoreRecovery = platformModule.life.add(
                new CopiedStoreRecovery( platformModule.config, platformModule.kernelExtensions.listFactories(), platformModule.pageCache ) );

        StoreCopyProcess storeCopyProcess = new StoreCopyProcess( platformModule.fileSystem, platformModule.pageCache, localDatabase,
                copiedStoreRecovery, remoteStore, logProvider );

        CommitStateHelper commitStateHelper = new CommitStateHelper( platformModule.pageCache, platformModule.fileSystem, config );
        return new CoreStateDownloader( localDatabase, servicesToSuspendOnStoreCopy, remoteStore, catchUpClient, logProvider,
                                        storeCopyProcess, coreStateMachinesModule.coreStateMachines, snapshotService, commitStateHelper );
    }

    private MembershipWaiterLifecycle createMembershipWaiterLifecycle()
    {
        long electionTimeout = config.get( CausalClusteringSettings.leader_election_timeout ).toMillis();
        MembershipWaiter membershipWaiter = new MembershipWaiter( memberIdRepository.myself(), jobScheduler,
                dbHealthSupplier, electionTimeout * 4, logProvider );
        long joinCatchupTimeout = config.get( CausalClusteringSettings.join_catch_up_timeout ).toMillis();
        return new MembershipWaiterLifecycle( membershipWaiter, joinCatchupTimeout, consensusModule.raftMachine(), logProvider );
    }

    public Server catchupServer()
    {
        return catchupServer;
    }

    public Optional<Server> backupServer()
    {
        return backupServer;
    }

    public CoreLife createCoreLife( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> handler )
    {
        return new CoreLife( consensusModule.raftMachine(),
                localDatabase, clusteringModule.clusterBinder(), commandApplicationProcess, coreStateMachinesModule.coreStateMachines,
                handler, snapshotService, downloadService );
    }

    public CommandApplicationProcess commandApplicationProcess()
    {
        return commandApplicationProcess;
    }

    public CoreStateDownloaderService downloadService()
    {
        return downloadService;
    }
}
