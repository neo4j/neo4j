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
package org.neo4j.coreedge.core.server;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.coreedge.ReplicationModule;
import org.neo4j.coreedge.catchup.CatchUpClient;
import org.neo4j.coreedge.catchup.CatchupServer;
import org.neo4j.coreedge.catchup.CheckpointerSupplier;
import org.neo4j.coreedge.catchup.DataSourceSupplier;
import org.neo4j.coreedge.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyClient;
import org.neo4j.coreedge.catchup.storecopy.StoreFetcher;
import org.neo4j.coreedge.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.TxPullClient;
import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.consensus.ConsensusModule;
import org.neo4j.coreedge.core.consensus.ContinuousJob;
import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.RaftServer;
import org.neo4j.coreedge.core.consensus.log.pruning.PruningScheduler;
import org.neo4j.coreedge.core.consensus.membership.MembershipWaiter;
import org.neo4j.coreedge.core.consensus.membership.MembershipWaiterLifecycle;
import org.neo4j.coreedge.core.state.BindingService;
import org.neo4j.coreedge.core.state.CommandApplicationProcess;
import org.neo4j.coreedge.core.state.CoreBootstrapper;
import org.neo4j.coreedge.core.state.CoreState;
import org.neo4j.coreedge.core.state.CoreStateApplier;
import org.neo4j.coreedge.core.state.LongIndexMarshal;
import org.neo4j.coreedge.core.state.machines.CoreStateMachinesModule;
import org.neo4j.coreedge.core.state.snapshot.CoreStateDownloader;
import org.neo4j.coreedge.core.state.storage.DurableStateStorage;
import org.neo4j.coreedge.core.state.storage.SimpleFileStorage;
import org.neo4j.coreedge.core.state.storage.SimpleStorage;
import org.neo4j.coreedge.core.state.storage.StateStorage;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.logging.MessageLogger;
import org.neo4j.coreedge.messaging.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.messaging.LoggingInbound;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

import static java.lang.Thread.sleep;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.NEW_THREAD;

public class CoreServerModule
{
    private static final String CLUSTER_ID_NAME = "cluster-id";
    public static final String LAST_FLUSHED_NAME = "last-flushed";

    public final MembershipWaiterLifecycle membershipWaiterLifecycle;

    public CoreServerModule( MemberId myself, final PlatformModule platformModule, ConsensusModule consensusModule,
            CoreStateMachinesModule coreStateMachinesModule, ReplicationModule replicationModule,
            File clusterStateDirectory, CoreTopologyService discoveryService,
            LocalDatabase localDatabase, MessageLogger<MemberId> messageLogger, Supplier<DatabaseHealth> dbHealthSupplier )
    {
        final Dependencies dependencies = platformModule.dependencies;
        final Config config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final LifeSupport life = platformModule.life;
        final Monitors monitors = platformModule.monitors;

        LogProvider logProvider = logging.getInternalLogProvider();
        LogProvider userLogProvider = logging.getUserLogProvider();

        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        StateStorage<Long> lastFlushedStorage;

        lastFlushedStorage = life.add(
                new DurableStateStorage<>( fileSystem, clusterStateDirectory, LAST_FLUSHED_NAME,
                        new LongIndexMarshal(), config.get( CoreEdgeClusterSettings.last_flushed_state_size ),
                        logProvider ) );

        consensusModule.raftMembershipManager().setRecoverFromIndexSupplier( lastFlushedStorage::getInitialState );

        RaftServer raftServer =
                new RaftServer( new CoreReplicatedContentMarshal(), config, logProvider, userLogProvider, monitors );

        LoggingInbound<RaftMessages.StoreIdAwareMessage> loggingRaftInbound =
                new LoggingInbound<>( raftServer, messageLogger, myself );

        CatchUpClient catchUpClient =
                life.add( new CatchUpClient( discoveryService, logProvider, Clocks.systemClock(), monitors ) );

        StoreFetcher storeFetcher = new StoreFetcher( logProvider, fileSystem, platformModule.pageCache,
                new StoreCopyClient( catchUpClient ), new TxPullClient( catchUpClient, platformModule.monitors ),
                new TransactionLogCatchUpFactory() );

        CoreStateApplier coreStateApplier = new CoreStateApplier( logProvider );

        CopiedStoreRecovery copiedStoreRecovery = new CopiedStoreRecovery( config,
                platformModule.kernelExtensions.listFactories(), platformModule.pageCache );
        CoreStateDownloader downloader = new CoreStateDownloader( localDatabase, storeFetcher,
                catchUpClient, logProvider, copiedStoreRecovery );

        SimpleStorage<ClusterId> clusterIdStorage = new SimpleFileStorage<>( fileSystem, clusterStateDirectory,
                CLUSTER_ID_NAME, new ClusterId.Marshal(), logProvider );

        CoreBootstrapper coreBootstrapper = new CoreBootstrapper( platformModule.storeDir, platformModule.pageCache,
                fileSystem, config );

        BindingService bindingService = new BindingService( clusterIdStorage, discoveryService, logProvider,
                Clocks.systemClock(), () -> sleep( 100 ), 300_000, coreBootstrapper );

        CoreState coreState = new CoreState(
                consensusModule.raftMachine(), localDatabase,
                logProvider,
                downloader,
                bindingService,
                new CommandApplicationProcess( coreStateMachinesModule.coreStateMachines, consensusModule.raftLog(),
                        config.get( CoreEdgeClusterSettings.state_machine_apply_max_batch_size ),
                        config.get( CoreEdgeClusterSettings.state_machine_flush_window_size ),
                        databaseHealthSupplier, logProvider, replicationModule.getProgressTracker(),
                        lastFlushedStorage, replicationModule.getSessionTracker(), coreStateApplier,
                        consensusModule.inFlightMap(), platformModule.monitors ) );

        dependencies.satisfyDependency( coreState );

        life.add( new PruningScheduler( coreState, platformModule.jobScheduler,
                config.get( CoreEdgeClusterSettings.raft_log_pruning_frequency ), logProvider ) );

        int queueSize = config.get( CoreEdgeClusterSettings.raft_in_queue_size );
        int maxBatch = config.get( CoreEdgeClusterSettings.raft_in_queue_max_batch );

        BatchingMessageHandler batchingMessageHandler =
                new BatchingMessageHandler( coreState, queueSize, maxBatch, logProvider );

        long electionTimeout = config.get( CoreEdgeClusterSettings.leader_election_timeout );

        MembershipWaiter membershipWaiter =
                new MembershipWaiter( myself, platformModule.jobScheduler, dbHealthSupplier,
                        electionTimeout * 4, logProvider );
        long joinCatchupTimeout = config.get( CoreEdgeClusterSettings.join_catch_up_timeout );
        membershipWaiterLifecycle = new MembershipWaiterLifecycle( membershipWaiter,
                joinCatchupTimeout, consensusModule.raftMachine(), logProvider );

        loggingRaftInbound.registerHandler( batchingMessageHandler );

        CatchupServer catchupServer = new CatchupServer( logProvider, userLogProvider, localDatabase,
                platformModule.dependencies.provideDependency( TransactionIdStore.class ),
                platformModule.dependencies.provideDependency( LogicalTransactionStore.class ),
                new DataSourceSupplier( platformModule ), new CheckpointerSupplier( platformModule.dependencies ),
                coreState, config, platformModule.monitors );

        life.add( raftServer );
        life.add( new ContinuousJob( platformModule.jobScheduler, new JobScheduler.Group( "raft-batch-handler", NEW_THREAD ),
                batchingMessageHandler, logProvider ) );
        life.add( coreState );
        life.add( catchupServer );
    }
}
