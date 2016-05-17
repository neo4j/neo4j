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

import java.time.Clock;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.membership.RaftGroup;
import org.neo4j.coreedge.raft.membership.RaftMembershipManager;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.replication.LeaderOnlyReplicator;
import org.neo4j.coreedge.raft.replication.shipping.RaftLogShippingManager;
import org.neo4j.coreedge.raft.state.InMemoryStateStorage;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.raft.state.membership.RaftMembershipState;
import org.neo4j.coreedge.raft.state.term.TermState;
import org.neo4j.coreedge.raft.state.vote.VoteState;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public class RaftInstanceBuilder<MEMBER>
{
    private final MEMBER member;

    private int expectedClusterSize;
    private RaftGroup.Builder<MEMBER> memberSetBuilder;

    private StateStorage<TermState> termState = new InMemoryStateStorage<>( new TermState() );
    private StateStorage<VoteState<MEMBER>> voteState = new InMemoryStateStorage<>( new VoteState<>() );
    private RaftLog raftLog = new InMemoryRaftLog();
    private RenewableTimeoutService renewableTimeoutService = new DelayedRenewableTimeoutService( Clock.systemUTC(),
            NullLogProvider.getInstance() );

    private Inbound<RaftMessages.RaftMessage<MEMBER>> inbound = handler -> {};
    private Outbound<MEMBER> outbound = ( advertisedSocketAddress, messages ) -> {};

    private LogProvider logProvider = NullLogProvider.getInstance();
    private Clock clock = Clock.systemUTC();

    private long electionTimeout = 500;
    private long heartbeatInterval = 150;
    private long catchupTimeout = 30000;
    private long retryTimeMillis = electionTimeout / 2;
    private int catchupBatchSize = 64;
    private int maxAllowedShippingLag = 256;
    private Supplier<DatabaseHealth> databaseHealthSupplier;
    private StateStorage<RaftMembershipState<MEMBER>> raftMembership =
            new InMemoryStateStorage<>( new RaftMembershipState<>() );
    private Monitors monitors = new Monitors();
    private RaftStateMachine raftStateMachine = new RaftStateMachine(){};
    private final InFlightMap<Long,RaftLogEntry> inFlightMap;

    public RaftInstanceBuilder( MEMBER member, int expectedClusterSize, RaftGroup.Builder<MEMBER> memberSetBuilder )
    {
        this.member = member;
        this.expectedClusterSize = expectedClusterSize;
        this.memberSetBuilder = memberSetBuilder;
        inFlightMap = new InFlightMap<>();
    }

    public RaftInstance<MEMBER> build()
    {
        LeaderOnlyReplicator<MEMBER, MEMBER> leaderOnlyReplicator = new LeaderOnlyReplicator<>( member, member,
                outbound );
        RaftMembershipManager<MEMBER> membershipManager = new RaftMembershipManager<>( leaderOnlyReplicator,
                memberSetBuilder, raftLog, logProvider, expectedClusterSize, electionTimeout, clock, catchupTimeout,
                raftMembership );
        RaftLogShippingManager<MEMBER> logShipping =
                new RaftLogShippingManager<>( outbound, logProvider, raftLog, clock, member, membershipManager,
                        retryTimeMillis, catchupBatchSize, maxAllowedShippingLag, inFlightMap );

        RaftInstance<MEMBER> raft = new RaftInstance<>( member, termState, voteState, raftLog, raftStateMachine, electionTimeout,
                heartbeatInterval, renewableTimeoutService, outbound, logProvider,
                membershipManager, logShipping, databaseHealthSupplier, monitors );
        inbound.registerHandler( raft );
        return raft;
    }

    public RaftInstanceBuilder<MEMBER> electionTimeout( long electionTimeout )
    {
        this.electionTimeout = electionTimeout;
        return this;
    }

    public RaftInstanceBuilder<MEMBER> heartbeatInterval( long heartbeatInterval )
    {
        this.heartbeatInterval = heartbeatInterval;
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

    public RaftInstanceBuilder<MEMBER> stateMachine( RaftStateMachine raftStateMachine )
    {
        this.raftStateMachine = raftStateMachine;
        return this;
    }

    RaftInstanceBuilder<MEMBER> databaseHealth( final DatabaseHealth databaseHealth )
    {
        this.databaseHealthSupplier = () -> databaseHealth;
        return this;
    }

    RaftInstanceBuilder<MEMBER> monitors( Monitors monitors )
    {
        this.monitors = monitors;
        return this;
    }
}
