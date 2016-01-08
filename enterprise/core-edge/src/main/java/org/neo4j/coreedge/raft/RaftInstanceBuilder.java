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

import java.util.function.Supplier;

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.membership.RaftGroup;
import org.neo4j.coreedge.raft.membership.RaftMembershipManager;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.replication.LeaderOnlyReplicator;
import org.neo4j.coreedge.raft.replication.shipping.RaftLogShippingManager;
import org.neo4j.coreedge.raft.state.membership.InMemoryRaftMembershipState;
import org.neo4j.coreedge.raft.state.term.InMemoryTermState;
import org.neo4j.coreedge.raft.state.term.TermState;
import org.neo4j.coreedge.raft.state.vote.InMemoryVoteState;
import org.neo4j.coreedge.raft.state.vote.VoteState;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public class RaftInstanceBuilder<MEMBER>
{
    private final MEMBER member;

    private int expectedClusterSize;
    private RaftGroup.Builder<MEMBER> memberSetBuilder;

    private TermState termState = new InMemoryTermState();
    private VoteState<MEMBER> voteState = new InMemoryVoteState<>();
    private RaftLog raftLog = new InMemoryRaftLog();
    private RenewableTimeoutService renewableTimeoutService = new DelayedRenewableTimeoutService( Clock.SYSTEM_CLOCK,
            NullLogProvider.getInstance() );

    private Inbound inbound = handler -> {
    };
    private Outbound<MEMBER> outbound = ( advertisedSocketAddress, messages ) -> {
    };

    private LogProvider logProvider = NullLogProvider.getInstance();
    private Clock clock = Clock.SYSTEM_CLOCK;

    private long electionTimeout = 500;
    private long heartbeatInterval = 150;
    private long leaderWaitTimeout = 10000;
    private long catchupTimeout = 30000;
    private long retryTimeMillis = electionTimeout / 2;
    private int catchupBatchSize = 64;
    private int maxAllowedShippingLag = 256;
    private Supplier<DatabaseHealth> databaseHealthSupplier;
    private InMemoryRaftMembershipState<MEMBER> raftMembership = new InMemoryRaftMembershipState<>();

    public RaftInstanceBuilder( MEMBER member, int expectedClusterSize, RaftGroup.Builder<MEMBER> memberSetBuilder )
    {
        this.member = member;
        this.expectedClusterSize = expectedClusterSize;
        this.memberSetBuilder = memberSetBuilder;
    }

    public RaftInstance<MEMBER> build()
    {
        LeaderOnlyReplicator<MEMBER, MEMBER> leaderOnlyReplicator = new LeaderOnlyReplicator<>( member, member,
                outbound );
        RaftMembershipManager<MEMBER> membershipManager = new RaftMembershipManager<>( leaderOnlyReplicator,
                memberSetBuilder, raftLog, logProvider, expectedClusterSize, electionTimeout, clock, catchupTimeout,
                raftMembership );
        RaftLogShippingManager<MEMBER> logShipping = new RaftLogShippingManager<>( outbound, logProvider, raftLog,
                clock, member, membershipManager, retryTimeMillis, catchupBatchSize, maxAllowedShippingLag );

        return new RaftInstance<>( member, termState, voteState, raftLog, electionTimeout, heartbeatInterval,
                renewableTimeoutService, inbound, outbound, leaderWaitTimeout, logProvider, membershipManager,
                logShipping, databaseHealthSupplier, clock );
    }

    public RaftInstanceBuilder<MEMBER> leaderWaitTimeout( long leaderWaitTimeout )
    {
        this.leaderWaitTimeout = leaderWaitTimeout;
        return this;
    }

    public RaftInstanceBuilder<MEMBER> timeoutService( RenewableTimeoutService renewableTimeoutService )
    {
        this.renewableTimeoutService = renewableTimeoutService;
        return this;
    }

    public RaftInstanceBuilder<MEMBER> outbound( Outbound<MEMBER> outbound )
    {
        this.outbound = outbound;
        return this;
    }

    public RaftInstanceBuilder<MEMBER> inbound( Inbound inbound )
    {
        this.inbound = inbound;
        return this;
    }

    public RaftInstanceBuilder<MEMBER> raftLog( RaftLog raftLog )
    {
        this.raftLog = raftLog;
        return this;
    }

    public RaftInstanceBuilder<MEMBER> databaseHealth( final DatabaseHealth databaseHealth)
    {
        this.databaseHealthSupplier = () -> databaseHealth;
        return this;
    }

    public RaftInstanceBuilder<MEMBER> clock( Clock clock )
    {
        this.clock = clock;
        return this;
    }
}
