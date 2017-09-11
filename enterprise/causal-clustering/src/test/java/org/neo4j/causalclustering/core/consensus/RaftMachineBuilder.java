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

import java.io.IOException;
import java.time.Clock;

import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.segmented.InFlightMap;
import org.neo4j.causalclustering.core.consensus.membership.RaftGroup;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import org.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import org.neo4j.causalclustering.core.consensus.schedule.DelayedRenewableTimeoutService;
import org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService;
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

import static org.neo4j.logging.NullLogProvider.getInstance;

public class RaftMachineBuilder
{
    private final MemberId member;

    private int expectedClusterSize;
    private RaftGroup.Builder memberSetBuilder;

    private StateStorage<TermState> termState = new InMemoryStateStorage<>( new TermState() );
    private StateStorage<VoteState> voteState = new InMemoryStateStorage<>( new VoteState() );
    private RaftLog raftLog = new InMemoryRaftLog();
    private RenewableTimeoutService renewableTimeoutService = new DelayedRenewableTimeoutService( Clocks.systemClock(),
            getInstance() );

    private Inbound<RaftMessages.RaftMessage> inbound = handler -> {};
    private Outbound<MemberId, RaftMessages.RaftMessage> outbound = ( to, message, block ) -> {};

    private LogProvider logProvider = NullLogProvider.getInstance();
    private Clock clock = Clocks.systemClock();
    private Clock shippingClock = Clocks.systemClock();

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
    private InFlightMap<RaftLogEntry> inFlightMap = new InFlightMap<>();

    public RaftMachineBuilder( MemberId member, int expectedClusterSize, RaftGroup.Builder memberSetBuilder )
    {
        this.member = member;
        this.expectedClusterSize = expectedClusterSize;
        this.memberSetBuilder = memberSetBuilder;
    }

    public RaftMachine build()
    {
        SendToMyself leaderOnlyReplicator = new SendToMyself( member, outbound );
        RaftMembershipManager membershipManager = new RaftMembershipManager( leaderOnlyReplicator,
                memberSetBuilder, raftLog, logProvider, expectedClusterSize, electionTimeout, clock, catchupTimeout,
                raftMembership );
        membershipManager.setRecoverFromIndexSupplier( () -> 0 );
        RaftLogShippingManager logShipping =
                new RaftLogShippingManager( outbound, logProvider, raftLog, shippingClock, member, membershipManager,
                        retryTimeMillis, catchupBatchSize, maxAllowedShippingLag, inFlightMap );
        RaftMachine raft = new RaftMachine( member, termState, voteState, raftLog, electionTimeout,
                heartbeatInterval, renewableTimeoutService, outbound, logProvider,
                membershipManager, logShipping, inFlightMap, false, monitors, clock );
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

    public RaftMachineBuilder timeoutService( RenewableTimeoutService renewableTimeoutService )
    {
        this.renewableTimeoutService = renewableTimeoutService;
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

    public RaftMachineBuilder inFlightMap( InFlightMap<RaftLogEntry> inFlightMap )
    {
        this.inFlightMap = inFlightMap;
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

    public interface CommitListener
    {
        /**
         * Called when the highest committed index increases.
         */
        void notifyCommitted( long commitIndex );
    }
}
