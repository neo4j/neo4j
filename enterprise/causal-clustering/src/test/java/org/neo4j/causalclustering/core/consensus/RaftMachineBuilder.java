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

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;

import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.membership.RaftGroup;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import org.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.core.replication.SendToMyself;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

public class RaftMachineBuilder
{
    private final MemberId member;

    private int expectedClusterSize;
    private RaftGroup.Builder memberSetBuilder;

    private TermState termState = new TermState();
    private StateStorage<TermState> termStateStorage = new InMemoryStateStorage<>( termState );
    private StateStorage<VoteState> voteStateStorage = new InMemoryStateStorage<>( new VoteState() );
    private RaftLog raftLog = new InMemoryRaftLog();
    private TimerService timerService;

    private Inbound<RaftMessages.RaftMessage> inbound = handler -> {};
    private Outbound<MemberId, RaftMessages.RaftMessage> outbound = ( to, message, block ) -> {};

    private LogProvider logProvider = NullLogProvider.getInstance();
    private Clock clock = Clocks.systemClock();

    private long term = termState.currentTerm();

    private long electionTimeout = 500;
    private long heartbeatInterval = 150;

    private long catchupTimeout = 30000;
    private long retryTimeMillis = electionTimeout / 2;
    private int catchupBatchSize = 64;
    private int maxAllowedShippingLag = 256;
    private StateStorage<RaftMembershipState> raftMembership =
            new InMemoryStateStorage<>( new RaftMembershipState() );
    private Monitors monitors = new Monitors();
    private CommitListener commitListener = commitIndex -> {};
    private InFlightCache inFlightCache = new ConsecutiveInFlightCache();

    public RaftMachineBuilder( MemberId member, int expectedClusterSize, RaftGroup.Builder memberSetBuilder )
    {
        this.member = member;
        this.expectedClusterSize = expectedClusterSize;
        this.memberSetBuilder = memberSetBuilder;
    }

    public RaftMachine build()
    {
        termState.update( term );
        LeaderAvailabilityTimers
                leaderAvailabilityTimers = new LeaderAvailabilityTimers( Duration.ofMillis( electionTimeout ), Duration.ofMillis( heartbeatInterval ), clock,
                timerService, logProvider );
        SendToMyself leaderOnlyReplicator = new SendToMyself( member, outbound );
        RaftMembershipManager membershipManager = new RaftMembershipManager( leaderOnlyReplicator,
                memberSetBuilder, raftLog, logProvider, expectedClusterSize, leaderAvailabilityTimers.getElectionTimeout(), clock, catchupTimeout,
                raftMembership );
        membershipManager.setRecoverFromIndexSupplier( () -> 0 );
        RaftLogShippingManager logShipping =
                new RaftLogShippingManager( outbound, logProvider, raftLog, timerService, clock, member, membershipManager,
                        retryTimeMillis, catchupBatchSize, maxAllowedShippingLag, inFlightCache );
        RaftMachine raft = new RaftMachine( member, termStateStorage, voteStateStorage, raftLog, leaderAvailabilityTimers, outbound, logProvider,
                membershipManager, logShipping, inFlightCache, false, false, monitors );
        inbound.registerHandler( incomingMessage ->
        {
            try
            {
                ConsensusOutcome outcome = raft.handle( incomingMessage );
                commitListener.notifyCommitted( outcome.getCommitIndex() );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        } );

        try
        {
            membershipManager.start();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return raft;
    }

    public RaftMachineBuilder electionTimeout( long electionTimeout )
    {
        this.electionTimeout = electionTimeout;
        return this;
    }

    public RaftMachineBuilder heartbeatInterval( long heartbeatInterval )
    {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public RaftMachineBuilder timerService( TimerService timerService )
    {
        this.timerService = timerService;
        return this;
    }

    public RaftMachineBuilder outbound( Outbound<MemberId, RaftMessages.RaftMessage> outbound )
    {
        this.outbound = outbound;
        return this;
    }

    public RaftMachineBuilder inbound( Inbound<RaftMessages.RaftMessage> inbound )
    {
        this.inbound = inbound;
        return this;
    }

    public RaftMachineBuilder raftLog( RaftLog raftLog )
    {
        this.raftLog = raftLog;
        return this;
    }

    public RaftMachineBuilder inFlightCache( InFlightCache inFlightCache )
    {
        this.inFlightCache = inFlightCache;
        return this;
    }

    public RaftMachineBuilder clock( Clock clock )
    {
        this.clock = clock;
        return this;
    }

    public RaftMachineBuilder commitListener( CommitListener commitListener )
    {
        this.commitListener = commitListener;
        return this;
    }

    RaftMachineBuilder monitors( Monitors monitors )
    {
        this.monitors = monitors;
        return this;
    }

    public RaftMachineBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public interface CommitListener
    {
        /**
         * Called when the highest committed index increases.
         */
        void notifyCommitted( long commitIndex );
    }
}
