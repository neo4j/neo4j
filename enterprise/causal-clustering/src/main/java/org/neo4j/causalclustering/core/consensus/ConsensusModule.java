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
package org.neo4j.causalclustering.core.consensus;

import java.io.File;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.EnterpriseCoreEditionModule;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.MonitoredRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCacheFactory;
import org.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategy;
import org.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategyFactory;
import org.neo4j.causalclustering.core.consensus.log.segmented.SegmentedRaftLog;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSetBuilder;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import org.neo4j.causalclustering.core.consensus.schedule.DelayedRenewableTimeoutService;
import org.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import org.neo4j.causalclustering.core.consensus.term.MonitoredTermStateStorage;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.core.replication.SendToMyself;
import org.neo4j.causalclustering.core.state.RefuseToBeLeaderStrategy;
import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.RaftCoreTopologyConnector;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.catchup_batch_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.join_catch_up_timeout;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.log_shipping_max_lag;
import static org.neo4j.causalclustering.core.consensus.log.RaftLog.RAFT_LOG_DIRECTORY_NAME;
import static org.neo4j.time.Clocks.systemClock;

public class ConsensusModule
{
    public static final String RAFT_MEMBERSHIP_NAME = "membership";
    public static final String RAFT_TERM_NAME = "term";
    public static final String RAFT_VOTE_NAME = "vote";

    private final MonitoredRaftLog raftLog;
    private final RaftMachine raftMachine;
    private final DelayedRenewableTimeoutService raftTimeoutService;
    private final RaftMembershipManager raftMembershipManager;
    private final InFlightCache inFlightCache;

    public ConsensusModule( MemberId myself, final PlatformModule platformModule,
            Outbound<MemberId,RaftMessages.RaftMessage> outbound, File clusterStateDirectory,
            CoreTopologyService coreTopologyService )
    {
        final Config config = platformModule.config;
        final LogService logging = platformModule.logging;
        final FileSystemAbstraction fileSystem = platformModule.fileSystem;
        final LifeSupport life = platformModule.life;

        LogProvider logProvider = logging.getInternalLogProvider();

        final CoreReplicatedContentMarshal marshal = new CoreReplicatedContentMarshal();

        RaftLog underlyingLog = createRaftLog( config, life, fileSystem, clusterStateDirectory, marshal, logProvider,
                platformModule.jobScheduler );

        raftLog = new MonitoredRaftLog( underlyingLog, platformModule.monitors );

        StateStorage<TermState> termState;
        StateStorage<VoteState> voteState;
        StateStorage<RaftMembershipState> raftMembershipStorage;

        StateStorage<TermState> durableTermState = life.add(
                new DurableStateStorage<>( fileSystem, clusterStateDirectory, RAFT_TERM_NAME, new TermState.Marshal(),
                        config.get( CausalClusteringSettings.term_state_size ), logProvider ) );

        termState = new MonitoredTermStateStorage( durableTermState, platformModule.monitors );

        voteState = life.add( new DurableStateStorage<>( fileSystem, clusterStateDirectory, RAFT_VOTE_NAME,
                new VoteState.Marshal( new MemberId.Marshal() ), config.get( CausalClusteringSettings.vote_state_size ),
                logProvider ) );

        raftMembershipStorage = life.add(
                new DurableStateStorage<>( fileSystem, clusterStateDirectory, RAFT_MEMBERSHIP_NAME,
                        new RaftMembershipState.Marshal(),
                        config.get( CausalClusteringSettings.raft_membership_state_size ), logProvider ) );

        long electionTimeout = config.get( CausalClusteringSettings.leader_election_timeout ).toMillis();
        long heartbeatInterval = electionTimeout / 3;

        Integer expectedClusterSize = config.get( CausalClusteringSettings.expected_core_cluster_size );

        MemberIdSetBuilder memberSetBuilder = new MemberIdSetBuilder();

        SendToMyself leaderOnlyReplicator = new SendToMyself( myself, outbound );

        raftMembershipManager = new RaftMembershipManager( leaderOnlyReplicator, memberSetBuilder, raftLog, logProvider,
                expectedClusterSize, electionTimeout, systemClock(), config.get( join_catch_up_timeout ).toMillis(),
                raftMembershipStorage );

        life.add( raftMembershipManager );

        inFlightCache = InFlightCacheFactory.create( config, platformModule.monitors );

        RaftLogShippingManager logShipping =
                new RaftLogShippingManager( outbound, logProvider, raftLog, systemClock(), myself,
                        raftMembershipManager, electionTimeout, config.get( catchup_batch_size ),
                        config.get( log_shipping_max_lag ), inFlightCache );

        raftTimeoutService = new DelayedRenewableTimeoutService( systemClock(), logProvider );

        raftMachine = new RaftMachine( myself, termState, voteState, raftLog, electionTimeout, heartbeatInterval,
                raftTimeoutService, outbound, logProvider, raftMembershipManager, logShipping, inFlightCache,
                RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config, logProvider.getLog( getClass() ) ), platformModule.monitors, systemClock() );

        life.add( new RaftCoreTopologyConnector( coreTopologyService, raftMachine ) );

        life.add( logShipping );
    }

    private RaftLog createRaftLog( Config config, LifeSupport life, FileSystemAbstraction fileSystem,
            File clusterStateDirectory, CoreReplicatedContentMarshal marshal, LogProvider logProvider,
            JobScheduler scheduler )
    {
        EnterpriseCoreEditionModule.RaftLogImplementation raftLogImplementation =
                EnterpriseCoreEditionModule.RaftLogImplementation
                        .valueOf( config.get( CausalClusteringSettings.raft_log_implementation ) );
        switch ( raftLogImplementation )
        {
        case IN_MEMORY:
        {
            return new InMemoryRaftLog();
        }

        case SEGMENTED:
        {
            long rotateAtSize = config.get( CausalClusteringSettings.raft_log_rotation_size );
            int readerPoolSize = config.get( CausalClusteringSettings.raft_log_reader_pool_size );

            CoreLogPruningStrategy pruningStrategy =
                    new CoreLogPruningStrategyFactory( config.get( CausalClusteringSettings.raft_log_pruning_strategy ),
                            logProvider ).newInstance();
            File directory = new File( clusterStateDirectory, RAFT_LOG_DIRECTORY_NAME );
            return life.add( new SegmentedRaftLog( fileSystem, directory, rotateAtSize, marshal, logProvider,
                    readerPoolSize, systemClock(), scheduler, pruningStrategy ) );
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

    public InFlightCache inFlightCache()
    {
        return inFlightCache;
    }
}
