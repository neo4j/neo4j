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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCacheMonitor;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.consensus.schedule.ControlledRenewableTimeoutService;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.core.consensus.RaftMachine.Timeouts.ELECTION;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteRequest;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteResponse;
import static org.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterables.last;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class RaftMachineTest
{
    private MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );
    private MemberId member3 = member( 3 );
    private MemberId member4 = member( 4 );

    private ReplicatedInteger data1 = ReplicatedInteger.valueOf( 1 );
    private ReplicatedInteger data2 = ReplicatedInteger.valueOf( 2 );

    private RaftLog raftLog = new InMemoryRaftLog();

    @Test
    public void shouldAlwaysStartAsFollower() throws Exception
    {
        // when
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .build();

        // then
        assertEquals( FOLLOWER, raft.currentRole() );
    }

    @Test
    public void shouldRequestVotesOnElectionTimeout() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        OutboundMessageCollector messages = new OutboundMessageCollector();

        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .clock( fakeClock )
                .outbound( messages )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        // When
        timeouts.invokeTimeout( ELECTION );

        // Then
        assertThat( messages.sentTo( myself ).size(), equalTo( 0 ) );

        assertThat( messages.sentTo( member1 ).size(), equalTo( 1 ) );
        assertThat( messages.sentTo( member1 ).get( 0 ), instanceOf( RaftMessages.Vote.Request.class ) );

        assertThat( messages.sentTo( member2 ).size(), equalTo( 1 ) );
        assertThat( messages.sentTo( member2 ).get( 0 ), instanceOf( RaftMessages.Vote.Request.class ) );
    }

    @Test
    public void shouldBecomeLeaderInMajorityOf3() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timeouts.invokeTimeout( ELECTION );
        assertThat( raft.isLeader(), is( false ) );

        // When
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // Then
        assertThat( raft.isLeader(), is( true ) );
    }

    @Test
    public void shouldBecomeLeaderInMajorityOf5() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState(
                new MembershipEntry( 0, asSet( myself, member1, member2, member3, member4 )  ) ) );
        raft.postRecoveryActions();

        timeouts.invokeTimeout( ELECTION );

        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );
        assertThat( raft.isLeader(), is( false ) );

        // When
        raft.handle( voteResponse().from( member2 ).term( 1 ).grant().build() );

        // Then
        assertThat( raft.isLeader(), is( true ) );
    }

    @Test
    public void shouldNotBecomeLeaderOnMultipleVotesFromSameMember() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState(
                new MembershipEntry( 0, asSet( myself, member1, member2, member3, member4 )  ) ) );
        raft.postRecoveryActions();

        timeouts.invokeTimeout( ELECTION );

        // When
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // Then
        assertThat( raft.isLeader(), is( false ) );
    }

    @Test
    public void shouldNotBecomeLeaderWhenVotingOnItself() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timeouts.invokeTimeout( ELECTION );

        // When
        raft.handle( voteResponse().from( myself ).term( 1 ).grant().build() );

        // Then
        assertThat( raft.isLeader(), is( false ) );
    }

    @Test
    public void shouldNotBecomeLeaderWhenMembersVoteNo() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timeouts.invokeTimeout( ELECTION );

        // When
        raft.handle( voteResponse().from( member1 ).term( 1 ).deny().build() );
        raft.handle( voteResponse().from( member2 ).term( 1 ).deny().build() );

        // Then
        assertThat( raft.isLeader(), is( false ) );
    }

    @Test
    public void shouldNotBecomeLeaderByVotesFromOldTerm() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timeouts.invokeTimeout( ELECTION );
        // When
        raft.handle( voteResponse().from( member1 ).term( 0 ).grant().build() );
        raft.handle( voteResponse().from( member2 ).term( 0 ).grant().build() );

        // Then
        assertThat( raft.isLeader(), is( false ) );
    }

    @Test
    public void shouldVoteFalseForCandidateInOldTerm() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        OutboundMessageCollector messages = new OutboundMessageCollector();

        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .clock( fakeClock )
                .outbound( messages )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        // When
        raft.handle( voteRequest().from( member1 ).term( -1 ).candidate( member1 )
                .lastLogIndex( 0 ).lastLogTerm( -1 ).build() );

        // Then
        assertThat( messages.sentTo( member1 ).size(), equalTo( 1 ) );
        assertThat( messages.sentTo( member1 ), hasItem( voteResponse().from( myself ).term( 0 ).deny().build() ) );
    }

    @Test
    public void shouldNotBecomeLeaderByVotesFromFutureTerm() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timeouts.invokeTimeout( ELECTION );

        // When
        raft.handle( voteResponse().from( member1 ).term( 2 ).grant().build() );
        raft.handle( voteResponse().from( member2 ).term( 2 ).grant().build() );

        assertThat( raft.isLeader(), is( false ) );
        assertEquals( raft.term(), 2L );
    }

    @Test
    public void shouldAppendNewLeaderBarrierAfterBecomingLeader() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        OutboundMessageCollector messages = new OutboundMessageCollector();

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .clock( fakeClock )
                .outbound( messages )
                .raftLog( raftLog )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        // When
        timeouts.invokeTimeout( ELECTION );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // Then
        assertEquals( new NewLeaderBarrier(), readLogEntry( raftLog, raftLog.appendIndex() ).content() );
    }

    @Test
    public void leaderShouldSendHeartBeatsOnHeartbeatTimeout() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        OutboundMessageCollector messages = new OutboundMessageCollector();

        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .outbound( messages )
                .clock( fakeClock )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timeouts.invokeTimeout( ELECTION );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // When
        timeouts.invokeTimeout( RaftMachine.Timeouts.HEARTBEAT );

        // Then
        assertTrue( last( messages.sentTo( member1 ) ) instanceof RaftMessages.Heartbeat );
        assertTrue( last( messages.sentTo( member2 ) ) instanceof RaftMessages.Heartbeat );
    }

    @Test
    public void shouldThrowExceptionIfReceivesClientRequestWithNoLeaderElected() throws Exception
    {
        // Given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );

        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).clock( fakeClock ).build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        try
        {
            // When
            // There is no leader
            raft.getLeader();
            fail( "Should have thrown exception" );
        }
        // Then
        catch ( NoLeaderFoundException e )
        {
            // expected
        }
    }

    @Test
    public void shouldPersistAtSpecifiedLogIndex() throws Exception
    {
        // given
        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .clock( fakeClock )
                .raftLog( raftLog )
                .build();

        raftLog.append( new RaftLogEntry(0, new MemberIdSet(asSet( myself, member1, member2 ))) );

        // when
        raft.handle(
                appendEntriesRequest().from( member1 ).prevLogIndex( 0 ).prevLogTerm( 0 ).leaderTerm( 0 )
                        .logEntry( new RaftLogEntry( 0, data1 ) ).build());
        // then
        assertEquals( 1, raftLog.appendIndex() );
        assertEquals( data1, readLogEntry( raftLog, 1 ).content() );
    }

    @Test
    public void newMembersShouldBeIncludedInHeartbeatMessages() throws Exception
    {
        // Given
        DirectNetworking network = new DirectNetworking();
        final MemberId newMember = member( 99 );
        DirectNetworking.Inbound<RaftMessages.RaftMessage> newMemberInbound = network.new Inbound<>( newMember );
        final OutboundMessageCollector messages = new OutboundMessageCollector();
        newMemberInbound.registerHandler( (Inbound.MessageHandler<RaftMessages.RaftMessage>) message -> messages.send( newMember, message ) );

        FakeClock fakeClock = Clocks.fakeClock();
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .outbound( messages )
                .clock( fakeClock )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        // We make ourselves the leader
        timeouts.invokeTimeout( ELECTION );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // When
        raft.setTargetMembershipSet( asSet( myself, member1, member2, newMember ) );
        network.processMessages();

        timeouts.invokeTimeout( RaftMachine.Timeouts.HEARTBEAT );
        network.processMessages();

        // Then
        assertEquals( RaftMessages.AppendEntries.Request.class, messages.sentTo( newMember ).get( 0 ).getClass() );
    }

    @Test
    public void shouldMonitorLeaderNotFound() throws Exception
    {
        // Given
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();

        Monitors monitors = new Monitors();
        LeaderNotFoundMonitor leaderNotFoundMonitor = new StubLeaderNotFoundMonitor();
        monitors.addMonitorListener( leaderNotFoundMonitor );

        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .monitors(monitors)
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );

        try
        {
            // When
            // There is no leader
            raft.getLeader();
            fail( "Should have thrown exception" );
        }
        // Then
        catch ( NoLeaderFoundException e )
        {
            // expected
            assertEquals(1, leaderNotFoundMonitor.leaderNotFoundExceptions());
        }
    }

    @Test
    public void shouldNotCacheInFlightEntriesUntilAfterRecovery() throws Exception
    {
        // given
        FakeClock fakeClock = Clocks.fakeClock();
        InFlightCache inFlightCache = new ConsecutiveInFlightCache( 10, 10000, InFlightCacheMonitor.VOID, false );
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .clock( fakeClock )
                .raftLog( raftLog )
                .inFlightCache( inFlightCache )
                .build();

        raftLog.append( new RaftLogEntry(0, new MemberIdSet(asSet( myself, member1, member2 ))) );

        // when
        raft.handle( appendEntriesRequest().from( member1 ).prevLogIndex( 0 ).prevLogTerm( 0 ).leaderTerm( 0 )
                .logEntry( new RaftLogEntry( 0, data1 ) ).build() );

        // then
        assertEquals( data1, readLogEntry( raftLog, 1 ).content() );
        assertNull( inFlightCache.get( 1L ) );

        // when
        raft.postRecoveryActions();
        raft.handle( appendEntriesRequest().from( member1 ).prevLogIndex( 1 ).prevLogTerm( 0 ).leaderTerm( 0 )
                .logEntry( new RaftLogEntry( 0, data2 ) ).build() );

        // then
        assertEquals( data2, readLogEntry( raftLog, 2 ).content() );
        assertEquals( data2, inFlightCache.get( 2L ).content() );
    }

    private static class ExplodingRaftLog implements RaftLog
    {
        private boolean startExploding = false;

        @Override
        public long append( RaftLogEntry... entries ) throws IOException
        {
            if ( startExploding )
            {
                throw new IOException( "Boom! append" );
            }
            else
            {
                return 0;
            }
        }

        @Override
        public void truncate( long fromIndex ) throws IOException
        {
            throw new IOException( "Boom! truncate" );
        }

        @Override
        public long prune( long safeIndex )
        {
            return -1;
        }

        @Override
        public long appendIndex()
        {
            return -1;
        }

        @Override
        public long prevIndex()
        {
            return -1;
        }

        @Override
        public long readEntryTerm( long logIndex ) throws IOException
        {
            return -1;
        }

        @Override
        public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
        {
            if ( startExploding )
            {
                throw new IOException( "Boom! entry cursor" );
            }
            else
            {
                return RaftLogCursor.empty();
            }
        }

        @Override
        public long skip( long index, long term )
        {
            return -1;
        }

        public void startExploding()
        {
            startExploding = true;
        }
    }

    private class StubLeaderNotFoundMonitor implements LeaderNotFoundMonitor
    {
        long count = 0;

        @Override
        public long leaderNotFoundExceptions()
        {
            return count;
        }

        @Override
        public void increment()
        {
            count++;
        }
    }
}
