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
package org.neo4j.causalclustering.core.server;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.causalclustering.ReplicationModule;
import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupServer;
import org.neo4j.causalclustering.catchup.CheckpointerSupplier;
import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.IdentityModule;
import org.neo4j.causalclustering.core.consensus.ConsensusModule;
import org.neo4j.causalclustering.core.consensus.ContinuousJob;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftServer;
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
import org.neo4j.causalclustering.core.state.RaftMessageHandler;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateDownloader;
import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.logging.MessageLogger;
import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.LoggingInbound;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.time.Clocks;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.NEW_THREAD;

public class CoreServerModule
{
    public static final String CLUSTER_ID_NAME = "cluster-id";
    public static final String LAST_FLUSHED_NAME = "last-flushed";

    public final MembershipWaiterLifecycle membershipWaiterLifecycle;

    public CoreServerModule( IdentityModule identityModule, final PlatformModule platformModule,
            ConsensusModule consensusModule, CoreStateMachinesModule coreStateMachinesModule,
            ReplicationModule replicationModule, File clusterStateDirectory, ClusteringModule clusteringModule,
            LocalDatabase localDatabase, MessageLogger<MemberId> messageLogger,
            Supplier<DatabaseHealth> dbHealthSupplier, SslPolicy sslPolicy )
    {
        final Dependencies dependencies = platformModule.dependencies;
        final Config config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final LifeSupport life = platformModule.life;
        final Monitors monitors = platformModule.monitors;
        final JobScheduler jobScheduler = platformModule.jobScheduler;
        final TopologyService topologyService = clusteringModule.topologyService();

        LogProvider logProvider = logging.getInternalLogProvider();
        LogProvider userLogProvider = logging.getUserLogProvider();

        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        StateStorage<Long> lastFlushedStorage = life
                .add( new DurableStateStorage<>( fileSystem, clusterStateDirectory, LAST_FLUSHED_NAME,
                        new LongIndexMarshal(), config.get( CausalClusteringSettings.last_flushed_state_size ),
                        logProvider ) );

        consensusModule.raftMembershipManager().setRecoverFromIndexSupplier( lastFlushedStorage::getInitialState );

        RaftServer raftServer = new RaftServer( new CoreReplicatedContentMarshal(), sslPolicy, config, logProvider,
                userLogProvider, monitors );

        LoggingInbound<RaftMessages.ClusterIdAwareMessage> loggingRaftInbound = new LoggingInbound<>( raftServer,
                messageLogger, identityModule.myself() );

        long inactivityTimeoutMillis = config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout ).toMillis();
        CatchUpClient catchUpClient = life
                .add(new CatchUpClient(  logProvider, Clocks.systemClock(), inactivityTimeoutMillis, monitors, sslPolicy ) );

        RemoteStore remoteStore = new RemoteStore( logProvider, fileSystem, platformModule.pageCache, new StoreCopyClient( catchUpClient, logProvider ),
                new TxPullClient( catchUpClient, platformModule.monitors ), new TransactionLogCatchUpFactory(), platformModule.monitors );

        CopiedStoreRecovery copiedStoreRecovery = new CopiedStoreRecovery( config,
                platformModule.kernelExtensions.listFactories(), platformModule.pageCache );
        life.add( copiedStoreRecovery );

        StoreCopyProcess storeCopyProcess = new StoreCopyProcess( fileSystem, platformModule.pageCache, localDatabase,
                copiedStoreRecovery, remoteStore, logProvider );

        LifeSupport servicesToStopOnStoreCopy = new LifeSupport();

        if ( config.get( OnlineBackupSettings.online_backup_enabled ) )
        {
            platformModule.dataSourceManager.addListener( new DataSourceManager.Listener()
            {
                @Override
                public void registered( NeoStoreDataSource dataSource )
                {
                    servicesToStopOnStoreCopy.add( pickBackupExtension( dataSource ) );
                }

                @Override
                public void unregistered( NeoStoreDataSource dataSource )
                {
                    servicesToStopOnStoreCopy.remove( pickBackupExtension( dataSource ) );
                }

                private OnlineBackupKernelExtension pickBackupExtension( NeoStoreDataSource dataSource )
                {
                    return dataSource.getDependencyResolver().resolveDependency( OnlineBackupKernelExtension.class );
                }
            } );
        }

        CoreState coreState = new CoreState( coreStateMachinesModule.coreStateMachines,
                replicationModule.getSessionTracker(), lastFlushedStorage );

        CommandApplicationProcess commandApplicationProcess = new CommandApplicationProcess(
                consensusModule.raftLog(),
                config.get( CausalClusteringSettings.state_machine_apply_max_batch_size ),
                config.get( CausalClusteringSettings.state_machine_flush_window_size ), databaseHealthSupplier,
                logProvider, replicationModule.getProgressTracker(),
                replicationModule.getSessionTracker(), coreState, consensusModule.inFlightCache(),
                platformModule.monitors );
        dependencies.satisfyDependency( commandApplicationProcess ); // lastApplied() for CC-robustness

        CoreSnapshotService snapshotService = new CoreSnapshotService( commandApplicationProcess,
                coreState, consensusModule.raftLog(), consensusModule.raftMachine() );

        CoreStateDownloader downloader = new CoreStateDownloader( localDatabase, servicesToStopOnStoreCopy,
                remoteStore, catchUpClient, logProvider, storeCopyProcess, coreStateMachinesModule.coreStateMachines,
                snapshotService, commandApplicationProcess, topologyService );

        RaftMessageHandler messageHandler = new RaftMessageHandler( localDatabase, logProvider,
                consensusModule.raftMachine(), downloader, commandApplicationProcess );

        CoreLife coreLife = new CoreLife( consensusModule.raftMachine(), localDatabase,
                clusteringModule.clusterBinder(), commandApplicationProcess,
                coreStateMachinesModule.coreStateMachines, messageHandler, snapshotService );

        RaftLogPruner raftLogPruner = new RaftLogPruner( consensusModule.raftMachine(), commandApplicationProcess );
        dependencies.satisfyDependency( raftLogPruner );

        life.add( new PruningScheduler( raftLogPruner, jobScheduler,
                config.get( CausalClusteringSettings.raft_log_pruning_frequency ).toMillis(), logProvider ) );

        int queueSize = config.get( CausalClusteringSettings.raft_in_queue_size );
        int maxBatch = config.get( CausalClusteringSettings.raft_in_queue_max_batch );

        BatchingMessageHandler batchingMessageHandler = new BatchingMessageHandler( messageHandler, queueSize, maxBatch,
                logProvider );

        long electionTimeout = config.get( CausalClusteringSettings.leader_election_timeout ).toMillis();

        MembershipWaiter membershipWaiter = new MembershipWaiter( identityModule.myself(), jobScheduler,
                dbHealthSupplier, electionTimeout * 4, logProvider );
        long joinCatchupTimeout = config.get( CausalClusteringSettings.join_catch_up_timeout ).toMillis();
        membershipWaiterLifecycle = new MembershipWaiterLifecycle( membershipWaiter, joinCatchupTimeout,
                consensusModule.raftMachine(), logProvider );

        loggingRaftInbound.registerHandler( batchingMessageHandler );

        CatchupServer catchupServer = new CatchupServer( logProvider, userLogProvider, localDatabase::storeId,
                platformModule.dependencies.provideDependency( TransactionIdStore.class ),
                platformModule.dependencies.provideDependency( LogicalTransactionStore.class ),
                localDatabase::dataSource, localDatabase::isAvailable, snapshotService, config, platformModule.monitors,
                new CheckpointerSupplier( platformModule.dependencies ), fileSystem, platformModule.pageCache,
                platformModule.storeCopyCheckPointMutex, sslPolicy );

        // Exposes this so that tests can start/stop the catchup server
        dependencies.satisfyDependency( catchupServer );

        servicesToStopOnStoreCopy.add( catchupServer );

        // batches messages from raft server -> core state
        // core state will drop messages if not ready
        life.add( batchingMessageHandler );
        life.add( new ContinuousJob( jobScheduler, new JobScheduler.Group( "raft-batch-handler", NEW_THREAD ),
                batchingMessageHandler, logProvider ) );

        life.add( raftServer ); // must start before core state so that it can trigger snapshot downloads when necessary
        life.add( coreLife );
        life.add( catchupServer ); // must start last and stop first, since it handles external requests
    }
}
