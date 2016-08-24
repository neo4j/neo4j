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
package org.neo4j.coreedge.core.consensus;

import java.io.File;
import java.io.IOException;

import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.EnterpriseCoreEditionModule;
import org.neo4j.coreedge.core.consensus.log.InMemoryRaftLog;
import org.neo4j.coreedge.core.consensus.log.MonitoredRaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;
import org.neo4j.coreedge.core.consensus.log.segmented.InFlightMap;
import org.neo4j.coreedge.core.consensus.log.segmented.SegmentedRaftLog;
import org.neo4j.coreedge.core.consensus.membership.MemberIdSetBuilder;
import org.neo4j.coreedge.core.consensus.membership.RaftMembershipManager;
import org.neo4j.coreedge.core.consensus.membership.RaftMembershipState;
import org.neo4j.coreedge.core.consensus.schedule.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.core.consensus.shipping.RaftLogShippingManager;
import org.neo4j.coreedge.core.consensus.term.MonitoredTermStateStorage;
import org.neo4j.coreedge.core.consensus.term.TermState;
import org.neo4j.coreedge.core.consensus.vote.VoteState;
import org.neo4j.coreedge.core.replication.SendToMyself;
import org.neo4j.coreedge.core.state.storage.DurableStateStorage;
import org.neo4j.coreedge.core.state.storage.StateStorage;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.RaftDiscoveryServiceConnector;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.messaging.Outbound;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.catchup_batch_size;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.join_catch_up_timeout;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.log_shipping_max_lag;
import static org.neo4j.coreedge.core.consensus.log.RaftLog.PHYSICAL_LOG_DIRECTORY_NAME;

public class ConsensusModule
{
    public static final String RAFT_MEMBERSHIP_NAME = "membership";
    public static final String RAFT_TERM_NAME = "term";
    public static final String RAFT_VOTE_NAME = "vote";

    private final MonitoredRaftLog raftLog;
    private final RaftMachine raftMachine;
    private final DelayedRenewableTimeoutService raftTimeoutService;
    private final RaftMembershipManager raftMembershipManager;
    private final InFlightMap<Long,RaftLogEntry> inFlightMap = new InFlightMap<>();

    public ConsensusModule( MemberId myself, final PlatformModule platformModule, Outbound<MemberId,RaftMessages.RaftMessage> outbound,
            File clusterStateDirectory, CoreTopologyService discoveryService )
    {
        final Config config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final LifeSupport life = platformModule.life;

        LogProvider logProvider = logging.getInternalLogProvider();

        final CoreReplicatedContentMarshal marshal = new CoreReplicatedContentMarshal();

        RaftLog underlyingLog = createRaftLog( config, life, fileSystem, clusterStateDirectory, marshal, logProvider, platformModule.jobScheduler );

        raftLog = new MonitoredRaftLog( underlyingLog, platformModule.monitors );

        StateStorage<TermState> termState;
        StateStorage<VoteState> voteState;
        StateStorage<RaftMembershipState> raftMembershipStorage;

        try
        {
            StateStorage<TermState> durableTermState = life.add(
                    new DurableStateStorage<>( fileSystem, clusterStateDirectory, RAFT_TERM_NAME,
                            new TermState.Marshal(), config.get( CoreEdgeClusterSettings.term_state_size ), logProvider ) );

            termState = new MonitoredTermStateStorage( durableTermState, platformModule.monitors );

            voteState = life.add(
                    new DurableStateStorage<>( fileSystem, clusterStateDirectory, RAFT_VOTE_NAME,
                            new VoteState.Marshal( new MemberId.Marshal() ),
                            config.get( CoreEdgeClusterSettings.vote_state_size ), logProvider ) );

            raftMembershipStorage = life.add(
                    new DurableStateStorage<>( fileSystem, clusterStateDirectory, RAFT_MEMBERSHIP_NAME,
                            new RaftMembershipState.Marshal(), config.get( CoreEdgeClusterSettings.raft_membership_state_size ),
                            logProvider ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        long electionTimeout = config.get( CoreEdgeClusterSettings.leader_election_timeout );
        long heartbeatInterval = electionTimeout / 3;

        Integer expectedClusterSize = config.get( CoreEdgeClusterSettings.expected_core_cluster_size );

        MemberIdSetBuilder memberSetBuilder = new MemberIdSetBuilder();

        SendToMyself leaderOnlyReplicator = new SendToMyself( myself, outbound );

        raftMembershipManager = new RaftMembershipManager( leaderOnlyReplicator, memberSetBuilder, raftLog, logProvider,
                expectedClusterSize, electionTimeout, Clocks.systemClock(),
                config.get( join_catch_up_timeout ), raftMembershipStorage
        );

        life.add( raftMembershipManager );

        RaftLogShippingManager logShipping =
                new RaftLogShippingManager( outbound, logProvider, raftLog, Clocks.systemClock(),
                        myself, raftMembershipManager, electionTimeout,
                        config.get( catchup_batch_size ),
                        config.get( log_shipping_max_lag ), inFlightMap );

        raftTimeoutService = new DelayedRenewableTimeoutService( Clocks.systemClock(), logProvider );

        raftMachine =
                new RaftMachine( myself, termState, voteState, raftLog, electionTimeout,
                        heartbeatInterval, raftTimeoutService, outbound, logProvider, raftMembershipManager,
                        logShipping, inFlightMap, platformModule.monitors );

        life.add( new RaftDiscoveryServiceConnector( discoveryService, raftMachine ) );

        life.add(logShipping);
    }

    private RaftLog createRaftLog( Config config, LifeSupport life, FileSystemAbstraction fileSystem,
            File clusterStateDirectory, CoreReplicatedContentMarshal marshal, LogProvider logProvider, JobScheduler scheduler )
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
                        new File( clusterStateDirectory, PHYSICAL_LOG_DIRECTORY_NAME ),
                        rotateAtSize,
                        marshal,
                        logProvider,
                        pruningStrategyConfig,
                        readerPoolSize, Clocks.systemClock(), scheduler ) );
            }
            default:
                throw new IllegalStateException( "Unknown raft log implementation: " + raftLogImplementation );
        }
    }

    public RaftLog raftLog()
    {
        return raftLog;
    }

    public RaftMachine raftMachine()
    {
        return raftMachine;
    }

    public Lifecycle raftTimeoutService()
    {
        return raftTimeoutService;
    }

    public RaftMembershipManager raftMembershipManager()
    {
        return raftMembershipManager;
    }

    public InFlightMap<Long,RaftLogEntry> inFlightMap()
    {
        return inFlightMap;
    }
}
