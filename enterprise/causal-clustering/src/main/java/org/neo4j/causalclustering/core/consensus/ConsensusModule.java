/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus;

import java.io.File;
import java.time.Duration;

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
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import org.neo4j.causalclustering.core.consensus.term.MonitoredTermStateStorage;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.core.replication.SendToMyself;
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
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.catchup_batch_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.join_catch_up_timeout;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.log_shipping_max_lag;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.refuse_to_be_leader;
import static org.neo4j.causalclustering.core.consensus.log.RaftLog.RAFT_LOG_DIRECTORY_NAME;
import static org.neo4j.time.Clocks.systemClock;

public class ConsensusModule
{
    public static final String RAFT_MEMBERSHIP_NAME = "membership";
    public static final String RAFT_TERM_NAME = "term";
    public static final String RAFT_VOTE_NAME = "vote";

    private final MonitoredRaftLog raftLog;
    private final RaftMachine raftMachine;
    private final RaftMembershipManager raftMembershipManager;
    private final InFlightCache inFlightCache;

    private final LeaderAvailabilityTimers leaderAvailabilityTimers;

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

        TimerService timerService = new TimerService( platformModule.jobScheduler, logProvider );

        leaderAvailabilityTimers = createElectionTiming( config, timerService, logProvider );

        Integer minimumConsensusGroupSize = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_runtime );

        MemberIdSetBuilder memberSetBuilder = new MemberIdSetBuilder();

        SendToMyself leaderOnlyReplicator = new SendToMyself( myself, outbound );

        raftMembershipManager = new RaftMembershipManager( leaderOnlyReplicator, memberSetBuilder, raftLog, logProvider,
                minimumConsensusGroupSize, leaderAvailabilityTimers.getElectionTimeout(), systemClock(), config.get( join_catch_up_timeout ).toMillis(),
                raftMembershipStorage );

        life.add( raftMembershipManager );

        inFlightCache = InFlightCacheFactory.create( config, platformModule.monitors );

        RaftLogShippingManager logShipping =
                new RaftLogShippingManager( outbound, logProvider, raftLog, timerService, systemClock(), myself,
                        raftMembershipManager, leaderAvailabilityTimers.getElectionTimeout(), config.get( catchup_batch_size ),
                        config.get( log_shipping_max_lag ), inFlightCache );

        boolean supportsPreVoting = config.get( CausalClusteringSettings.enable_pre_voting );

        raftMachine = new RaftMachine( myself, termState, voteState, raftLog, leaderAvailabilityTimers,
                outbound, logProvider, raftMembershipManager, logShipping, inFlightCache,
                config.get( refuse_to_be_leader ),
                supportsPreVoting, platformModule.monitors );

        String dbName = config.get( CausalClusteringSettings.database );

        life.add( new RaftCoreTopologyConnector( coreTopologyService, raftMachine, dbName ) );

        life.add( logShipping );
    }

    private LeaderAvailabilityTimers createElectionTiming( Config config, TimerService timerService, LogProvider logProvider )
    {
        Duration electionTimeout = config.get( CausalClusteringSettings.leader_election_timeout );
        return new LeaderAvailabilityTimers( electionTimeout, electionTimeout.dividedBy( 3 ), systemClock(), timerService, logProvider );
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

    public RaftMembershipManager raftMembershipManager()
    {
        return raftMembershipManager;
    }

    public InFlightCache inFlightCache()
    {
        return inFlightCache;
    }

    public LeaderAvailabilityTimers getLeaderAvailabilityTimers()
    {
        return leaderAvailabilityTimers;
    }
}
