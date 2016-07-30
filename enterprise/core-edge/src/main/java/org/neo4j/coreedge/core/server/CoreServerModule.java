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
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.coreedge.core.state.CoreStateMachinesModule;
import org.neo4j.coreedge.ReplicationModule;
import org.neo4j.coreedge.catchup.CatchupServer;
import org.neo4j.coreedge.catchup.CheckpointerSupplier;
import org.neo4j.coreedge.catchup.DataSourceSupplier;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.core.CoreToCoreClient;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreCopyClient;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.catchup.tx.edge.TransactionLogCatchUpFactory;
import org.neo4j.coreedge.catchup.tx.edge.TxPullClient;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.raft.ConsensusModule;
import org.neo4j.coreedge.raft.ContinuousJob;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.RaftServer;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.pruning.PruningScheduler;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.membership.MembershipWaiter;
import org.neo4j.coreedge.raft.net.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.raft.net.LoggingInbound;
import org.neo4j.coreedge.core.state.CoreState;
import org.neo4j.coreedge.core.state.CoreStateApplier;
import org.neo4j.coreedge.core.state.CoreStateDownloader;
import org.neo4j.coreedge.core.state.DurableStateStorage;
import org.neo4j.coreedge.core.state.LongIndexMarshal;
import org.neo4j.coreedge.core.state.StateStorage;
import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.messaging.ListenSocketAddress;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.NonBlockingChannels;
import org.neo4j.coreedge.raft.membership.MembershipWaiterLifecycle;
import org.neo4j.coreedge.messaging.NotMyselfSelectionStrategy;
import org.neo4j.coreedge.logging.MessageLogger;
import org.neo4j.coreedge.raft.state.CommandApplicationProcess;
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
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.NEW_THREAD;

public class CoreServerModule
{
    public final LifeSupport startupLifecycle;
    public final MembershipWaiterLifecycle membershipWaiterLifecycle;

    public CoreServerModule( MemberId myself, final PlatformModule platformModule, ConsensusModule consensusModule, CoreStateMachinesModule coreStateMachinesModule, ReplicationModule replicationModule, File clusterStateDirectory, CoreTopologyService
            discoveryService, LocalDatabase localDatabase, MessageLogger<MemberId> messageLogger )
    {
        final Dependencies dependencies = platformModule.dependencies;
        final Config config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final LifeSupport life = platformModule.life;
        LogProvider logProvider = logging.getInternalLogProvider();

        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        StateStorage<Long> lastFlushedStorage;

        try
        {
            lastFlushedStorage = life.add(
                    new DurableStateStorage<>( fileSystem, clusterStateDirectory, ReplicationModule.LAST_FLUSHED_NAME,
                            new LongIndexMarshal(), config.get( CoreEdgeClusterSettings.last_flushed_state_size ),
                            databaseHealthSupplier, logProvider ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        consensusModule.raftMembershipManager().setRecoverFromIndex( lastFlushedStorage.getInitialState() );

        ListenSocketAddress raftListenAddress = config.get( CoreEdgeClusterSettings.raft_listen_address );

        RaftServer raftServer = new RaftServer( new CoreReplicatedContentMarshal(), raftListenAddress, logProvider );

        LoggingInbound<RaftMessages.StoreIdAwareMessage> loggingRaftInbound =
                new LoggingInbound<>( raftServer, messageLogger, myself );

        NonBlockingChannels nonBlockingChannels = new NonBlockingChannels();

        CoreToCoreClient.ChannelInitializer channelInitializer =
                new CoreToCoreClient.ChannelInitializer( logProvider, nonBlockingChannels );

        int maxQueueSize = config.get( CoreEdgeClusterSettings.outgoing_queue_size );
        long logThresholdMillis = config.get( CoreEdgeClusterSettings.unknown_address_logging_throttle );

        CoreToCoreClient coreToCoreClient = life.add(
                new CoreToCoreClient( logProvider, channelInitializer, platformModule.monitors, maxQueueSize,
                        nonBlockingChannels, discoveryService, logThresholdMillis ) );
        channelInitializer.setOwner( coreToCoreClient );

        StoreFetcher storeFetcher = new StoreFetcher( logProvider, fileSystem, platformModule.pageCache,
                new StoreCopyClient( coreToCoreClient ), new TxPullClient( coreToCoreClient ),
                new TransactionLogCatchUpFactory() );

        CoreStateApplier coreStateApplier = new CoreStateApplier( logProvider );
        CoreStateDownloader downloader = new CoreStateDownloader( localDatabase, storeFetcher,
                coreToCoreClient, logProvider );

        InFlightMap<Long,RaftLogEntry> inFlightMap = new InFlightMap<>();

        NotMyselfSelectionStrategy someoneElse = new NotMyselfSelectionStrategy( discoveryService, myself );

        CoreState coreState = new CoreState(
                consensusModule.raftInstance(), localDatabase,
                logProvider,
                someoneElse, downloader,
                new CommandApplicationProcess( coreStateMachinesModule.coreStateMachines, consensusModule.raftLog(), config.get( CoreEdgeClusterSettings.state_machine_apply_max_batch_size ),

                        config.get( CoreEdgeClusterSettings.state_machine_flush_window_size ), databaseHealthSupplier, logProvider, replicationModule.getProgressTracker(), lastFlushedStorage, replicationModule.getSessionTracker(), coreStateApplier,
                        inFlightMap, platformModule.monitors ) );

        dependencies.satisfyDependency( coreState );

        life.add( new PruningScheduler( coreState, platformModule.jobScheduler,
                config.get( CoreEdgeClusterSettings.raft_log_pruning_frequency ) ) );

        int queueSize = config.get( CoreEdgeClusterSettings.raft_in_queue_size );
        int maxBatch = config.get( CoreEdgeClusterSettings.raft_in_queue_max_batch );

        BatchingMessageHandler batchingMessageHandler =
                new BatchingMessageHandler( coreState, queueSize, maxBatch, logProvider );

        long electionTimeout = config.get( CoreEdgeClusterSettings.leader_election_timeout );

        MembershipWaiter membershipWaiter =
                new MembershipWaiter( myself, platformModule.jobScheduler, electionTimeout * 4, coreState, logProvider );
        long joinCatchupTimeout = config.get( CoreEdgeClusterSettings.join_catch_up_timeout );
        membershipWaiterLifecycle = new MembershipWaiterLifecycle( membershipWaiter,
                joinCatchupTimeout, consensusModule.raftInstance(), logProvider );

        life.add( new ContinuousJob( platformModule.jobScheduler, new JobScheduler.Group( "raft-batch-handler", NEW_THREAD ),
                batchingMessageHandler ) );

        loggingRaftInbound.registerHandler( batchingMessageHandler );

        CatchupServer catchupServer = new CatchupServer( logProvider, localDatabase,
                platformModule.dependencies.provideDependency( TransactionIdStore.class ),
                platformModule.dependencies.provideDependency( LogicalTransactionStore.class ),
                new DataSourceSupplier( platformModule ), new CheckpointerSupplier( platformModule.dependencies ),
                coreState, config.get( CoreEdgeClusterSettings.transaction_listen_address ), platformModule.monitors );

        startupLifecycle = new LifeSupport();
        startupLifecycle.add( coreState );
        startupLifecycle.add( raftServer );
        startupLifecycle.add( catchupServer );
    }
}
