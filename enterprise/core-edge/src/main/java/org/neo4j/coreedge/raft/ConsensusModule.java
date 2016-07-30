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
package org.neo4j.coreedge.raft;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.function.Supplier;

import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.RaftDiscoveryServiceConnector;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.MonitoredRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.log.segmented.SegmentedRaftLog;
import org.neo4j.coreedge.raft.membership.MemberIdSetBuilder;
import org.neo4j.coreedge.raft.membership.RaftMembershipManager;
import org.neo4j.coreedge.raft.net.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.raft.net.LoggingOutbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.net.RaftChannelInitializer;
import org.neo4j.coreedge.raft.net.RaftOutbound;
import org.neo4j.coreedge.core.replication.SendToMyself;
import org.neo4j.coreedge.raft.schedule.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.raft.shipping.RaftLogShippingManager;
import org.neo4j.coreedge.core.state.DurableStateStorage;
import org.neo4j.coreedge.core.state.StateStorage;
import org.neo4j.coreedge.raft.membership.RaftMembershipState;
import org.neo4j.coreedge.raft.term.MonitoredTermStateStorage;
import org.neo4j.coreedge.raft.term.TermState;
import org.neo4j.coreedge.raft.vote.VoteState;
import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.NonBlockingChannels;
import org.neo4j.coreedge.messaging.SenderService;
import org.neo4j.coreedge.core.EnterpriseCoreEditionModule;
import org.neo4j.coreedge.logging.BetterMessageLogger;
import org.neo4j.coreedge.logging.MessageLogger;
import org.neo4j.coreedge.logging.NullMessageLogger;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;

import static java.time.Clock.systemUTC;

public class ConsensusModule
{
    public static final String RAFT_MEMBERSHIP_NAME = "membership";
    public static final String RAFT_TERM_NAME = "term";
    public static final String RAFT_VOTE_NAME = "vote";

    private final MonitoredRaftLog raftLog;
    private final RaftInstance raftInstance;
    private final DelayedRenewableTimeoutService raftTimeoutService;
    private final RaftMembershipManager raftMembershipManager;

    public ConsensusModule( MemberId myself, final PlatformModule platformModule,
                            RaftOutbound raftOutbound, File clusterStateDirectory,
                            CoreTopologyService discoveryService )
    {
        final Dependencies dependencies = platformModule.dependencies;
        final Config config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final LifeSupport life = platformModule.life;

        LogProvider logProvider = logging.getInternalLogProvider();
        final Supplier<DatabaseHealth> databaseHealthSupplier = dependencies.provideDependency( DatabaseHealth.class );

        final CoreReplicatedContentMarshal marshal = new CoreReplicatedContentMarshal();
        int maxQueueSize = config.get( CoreEdgeClusterSettings.outgoing_queue_size );
        final SenderService senderService =
                new SenderService( new RaftChannelInitializer( marshal, logProvider ), logProvider, platformModule.monitors,
                        maxQueueSize, new NonBlockingChannels() );
        life.add( senderService );

        final MessageLogger<MemberId> messageLogger;
        if ( config.get( CoreEdgeClusterSettings.raft_messages_log_enable ) )
        {
            File logsDir = config.get( GraphDatabaseSettings.logs_directory );
            messageLogger = life.add( new BetterMessageLogger<>( myself, raftMessagesLog( logsDir ) ) );
        }
        else
        {
            messageLogger = new NullMessageLogger<>();
        }

        RaftLog underlyingLog = createRaftLog( config, life, fileSystem, clusterStateDirectory, marshal, logProvider );

        raftLog = new MonitoredRaftLog( underlyingLog, platformModule.monitors );

        Outbound<MemberId,RaftMessages.RaftMessage> loggingOutbound = new LoggingOutbound<>(
                raftOutbound, myself, messageLogger );

        InFlightMap<Long,RaftLogEntry> inFlightMap = new InFlightMap<>();

        StateStorage<TermState> termState;
        StateStorage<VoteState> voteState;
        StateStorage<RaftMembershipState> raftMembershipStorage;

        try
        {
            StateStorage<TermState> durableTermState = life.add(
                    new DurableStateStorage<>( fileSystem, clusterStateDirectory, RAFT_TERM_NAME,
                            new TermState.Marshal(), config.get( CoreEdgeClusterSettings.term_state_size ),
                            databaseHealthSupplier, logProvider ) );

            termState = new MonitoredTermStateStorage( durableTermState, platformModule.monitors );

            voteState = life.add(
                    new DurableStateStorage<>( fileSystem, clusterStateDirectory, RAFT_VOTE_NAME,
                            new VoteState.Marshal( new MemberId.MemberIdMarshal() ),
                            config.get( CoreEdgeClusterSettings.vote_state_size ), databaseHealthSupplier,
                            logProvider ) );

            raftMembershipStorage = life.add(
                    new DurableStateStorage<>( fileSystem, clusterStateDirectory, RAFT_MEMBERSHIP_NAME,
                            new RaftMembershipState.Marshal(), config.get( CoreEdgeClusterSettings.raft_membership_state_size ),
                            databaseHealthSupplier, logProvider ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        long electionTimeout1 = config.get( CoreEdgeClusterSettings.leader_election_timeout );
        long heartbeatInterval = electionTimeout1 / 3;

        Integer expectedClusterSize = config.get( CoreEdgeClusterSettings.expected_core_cluster_size );

        MemberIdSetBuilder memberSetBuilder = new MemberIdSetBuilder();

        SendToMyself leaderOnlyReplicator =
                new SendToMyself( myself, loggingOutbound );

        raftMembershipManager = new RaftMembershipManager( leaderOnlyReplicator, memberSetBuilder, raftLog, logProvider,
               expectedClusterSize, electionTimeout1, systemUTC(),
               config.get( CoreEdgeClusterSettings.join_catch_up_timeout ), raftMembershipStorage
        );

        life.add( raftMembershipManager );

        RaftLogShippingManager logShipping =
                new RaftLogShippingManager( loggingOutbound, logProvider, raftLog, systemUTC(),
                        myself, raftMembershipManager, electionTimeout1,
                        config.get( CoreEdgeClusterSettings.catchup_batch_size ),
                        config.get( CoreEdgeClusterSettings.log_shipping_max_lag ), inFlightMap );

        raftTimeoutService = new DelayedRenewableTimeoutService( systemUTC(), logProvider );

        raftInstance =
                new RaftInstance( myself, termState, voteState, raftLog, electionTimeout1,
                        heartbeatInterval, raftTimeoutService, loggingOutbound, logProvider, raftMembershipManager,
                        logShipping, inFlightMap, platformModule.monitors );

        life.add( new RaftDiscoveryServiceConnector( discoveryService, raftInstance ) );

        life.add(logShipping);
    }

    private RaftLog createRaftLog( Config config, LifeSupport life, FileSystemAbstraction fileSystem,
                                   File clusterStateDirectory, CoreReplicatedContentMarshal marshal, LogProvider logProvider )
    {
        EnterpriseCoreEditionModule.RaftLogImplementation raftLogImplementation =
                EnterpriseCoreEditionModule.RaftLogImplementation.valueOf( config.get( CoreEdgeClusterSettings.raft_log_implementation ) );
        switch ( raftLogImplementation )
        {
            case IN_MEMORY:
            {
                return new InMemoryRaftLog();
            }

            case SEGMENTED:
            {
                long rotateAtSize = config.get( CoreEdgeClusterSettings.raft_log_rotation_size );
                int readerPoolSize = config.get( CoreEdgeClusterSettings.raft_log_reader_pool_size );

                String pruningStrategyConfig = config.get( CoreEdgeClusterSettings.raft_log_pruning_strategy );

                return life.add( new SegmentedRaftLog(
                        fileSystem,
                        new File( clusterStateDirectory, RaftLog.PHYSICAL_LOG_DIRECTORY_NAME ),
                        rotateAtSize,
                        marshal,
                        logProvider,
                        pruningStrategyConfig,
                        readerPoolSize, systemUTC() ) );
            }
            default:
                throw new IllegalStateException( "Unknown raft log implementation: " + raftLogImplementation );
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

    public RaftLog raftLog()
    {
        return raftLog;
    }

    public RaftInstance raftInstance()
    {
        return raftInstance;
    }

    public Lifecycle raftTimeoutService()
    {
        return raftTimeoutService;
    }

    public RaftMembershipManager raftMembershipManager()
    {
        return raftMembershipManager;
    }
}
