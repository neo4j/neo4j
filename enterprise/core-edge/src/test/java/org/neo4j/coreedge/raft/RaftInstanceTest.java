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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.membership.RaftTestGroup;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.RaftTestMemberSetBuilder;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLog;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.coreedge.raft.RaftInstance.Timeouts.ELECTION;
import static org.neo4j.coreedge.raft.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.coreedge.raft.TestMessageBuilders.voteRequest;
import static org.neo4j.coreedge.raft.TestMessageBuilders.voteResponse;
import static org.neo4j.coreedge.raft.roles.Role.FOLLOWER;
import static org.neo4j.coreedge.server.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterables.last;
import static org.neo4j.helpers.collection.Iterators.asSet;

@RunWith(MockitoJUnitRunner.class)
public class RaftInstanceTest
{
    private RaftTestMember myself = member( 0 );

    /* A few members that we use at will in tests. */
    private RaftTestMember member1 = member( 1 );
    private RaftTestMember member2 = member( 2 );
    private RaftTestMember member3 = member( 3 );
    private RaftTestMember member4 = member( 4 );

    private ReplicatedInteger data1 = ReplicatedInteger.valueOf( 1 );

    private RaftLog raftLog = new InMemoryRaftLog();

    @Test
    public void shouldAlwaysStartAsFollower() throws Exception
    {
        // when
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .build();

        // then
        assertEquals( FOLLOWER, raft.currentRole() );
    }

    @Test
    public void shouldRequestVotesOnElectionTimeout() throws Exception
    {
        // Given
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        OutboundMessageCollector messages = new OutboundMessageCollector();

        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .outbound( messages )
                .build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) );

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
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) ); // @logIndex=0

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
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2, member3, member4 ) ) );
        // @logIndex=0

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
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2, member3, member4 ) ) );
        // @logIndex=0

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
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) ); // @logIndex=0

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
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) ); // @logIndex=0

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
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) ); // @logIndex=0

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
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        OutboundMessageCollector messages = new OutboundMessageCollector();

        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .outbound( messages )
                .build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) ); // @logIndex=0

        // When
        raft.handle( voteRequest().from( member1 ).term( -1 ).candidate( member1 ).lastLogIndex( 0 ).lastLogTerm( -1
        ).build() );

        // Then
        assertThat( messages.sentTo( member1 ).size(), equalTo( 1 ) );
        assertThat( messages.sentTo( member1 ), hasItem( voteResponse().from( myself ).term( 0 ).deny().build() ) );
    }

    @Test
    public void shouldNotBecomeLeaderByVotesFromFutureTerm() throws Exception
    {
        // Given
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) ); // @logIndex=0

        timeouts.invokeTimeout( ELECTION );

        // When
        raft.handle( voteResponse().from( member1 ).term( 2 ).grant().build() );
        raft.handle( voteResponse().from( member2 ).term( 2 ).grant().build() );

        assertThat( raft.isLeader(), is( false ) );
        assertEquals( raft.term(), 2l );
    }

    @Test
    public void shouldAppendNewLeaderBarrierAfterBecomingLeader() throws Exception
    {
        // Given
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        OutboundMessageCollector messages = new OutboundMessageCollector();

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .outbound( messages )
                .raftLog( raftLog )
                .build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) );

        // When
        timeouts.invokeTimeout( ELECTION );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // Then
        assertEquals( new NewLeaderBarrier(), raftLog.readLogEntry( raftLog.appendIndex() ).content() );
    }

    @Test
    public void leaderShouldSendHeartBeatsOnHeartbeatTimeout() throws Exception
    {
        // Given
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        OutboundMessageCollector messages = new OutboundMessageCollector();

        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .outbound( messages )
                .build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) );

        timeouts.invokeTimeout( ELECTION );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // When
        timeouts.invokeTimeout( RaftInstance.Timeouts.HEARTBEAT );

        // Then
        assertTrue( last( messages.sentTo( member1 ) ) instanceof RaftMessages.Heartbeat );
        assertTrue( last( messages.sentTo( member2 ) ) instanceof RaftMessages.Heartbeat );
    }

    @Test
    public void shouldThrowExceptionIfReceivesClientRequestWithNoLeaderElected() throws Exception
    {
        // Given
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();

        int leaderWaitTimeout = 10;
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts ).leaderWaitTimeout( leaderWaitTimeout ).build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) ); // @logIndex=0

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
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .raftLog( raftLog )
                .build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) ); // @logIndex=0

        // when
        raft.handle(
                appendEntriesRequest().from( member1 ).leaderTerm( 0 ).leader( myself ).prevLogIndex( 0 )
                        .prevLogTerm( 0 ).logEntry( new RaftLogEntry( 0, data1 ) ).leaderCommit( -1 ).build() );

        // then
        assertEquals( 1, raftLog.appendIndex() );
        assertEquals( data1, raftLog.readLogEntry( 1 ).content() );
    }

    @Test
    public void newMembersShouldBeIncludedInHeartbeatMessages() throws Exception
    {
        // Given
        DirectNetworking network = new DirectNetworking();
        final RaftTestMember newMember = member( 99 );
        DirectNetworking.Inbound newMemberInbound = network.new Inbound( 99 );
        final OutboundMessageCollector messages = new OutboundMessageCollector();
        newMemberInbound.registerHandler( message -> messages.send( newMember, message ) );

        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();

        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .outbound( messages )
                .build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) );

        // We make ourselves the leader
        timeouts.invokeTimeout( ELECTION );
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );

        // When
        raft.setTargetMembershipSet( asSet( myself, member1, member2, newMember ) );
        network.processMessages();

        timeouts.invokeTimeout( RaftInstance.Timeouts.HEARTBEAT );
        network.processMessages();

        // Then
        assertEquals( RaftMessages.AppendEntries.Request.class, messages.sentTo( newMember ).get( 0 ).getClass() );
    }

    @Test
    public void shouldPanicWhenFailingToHandleMessageAtBootstrapTime() throws Throwable
    {
        // given
        TestDatabaseHealth databaseHealth = new TestDatabaseHealth();

        ExplodingRaftLog explodingLog = new ExplodingRaftLog();
        explodingLog.startExploding();
        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .raftLog( explodingLog )
                .databaseHealth( databaseHealth )
                .build();
        try
        {
            // when
            raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) );
            fail( "Contract expects exception so that others can take remedial action" );
        }
        catch ( RaftInstance.BootstrapException e )
        {
            // then
            assertTrue( databaseHealth.hasPanicked() );
        }
    }

    @Test
    public void shouldPanicWhenFailingToHandleMessageUnderNormalConditions() throws Throwable
    {
        // given
        TestDatabaseHealth databaseHealth = new TestDatabaseHealth();

        ExplodingRaftLog explodingLog = new ExplodingRaftLog();

        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .raftLog( explodingLog )
                .databaseHealth( databaseHealth )
                .build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) );
        explodingLog.startExploding();

        // when
        raft.handle( new RaftMessages.AppendEntries.Request<>( member1, 0, -1, -1,
                new RaftLogEntry[]{new RaftLogEntry( 0, new ReplicatedString( "hello" ) )}, 0 ) );

        // then
        assertTrue( databaseHealth.hasPanicked() );
    }

    @Test
    public void shouldMonitorLeaderNotFound() throws Exception
    {
        // Given
        ControlledRenewableTimeoutService timeouts = new ControlledRenewableTimeoutService();

        int leaderWaitTimeout = 10;

        Monitors monitors = new Monitors();
        LeaderNotFoundMonitor leaderNotFoundMonitor = new StubLeaderNotFoundMonitor();
        monitors.addMonitorListener( leaderNotFoundMonitor );


        RaftInstance<RaftTestMember> raft = new RaftInstanceBuilder<>( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .timeoutService( timeouts )
                .leaderWaitTimeout( leaderWaitTimeout )
                .monitors(monitors)
                .build();

        raft.bootstrapWithInitialMembers( new RaftTestGroup( asSet( myself, member1, member2 ) ) ); // @logIndex=0

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

    private static class ExplodingRaftLog implements RaftLog
    {
        private boolean startExploding = false;

        @Override
        public long append( RaftLogEntry entry ) throws IOException
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
        public void commit( long commitIndex ) throws IOException
        {
            if ( startExploding )
            {
                throw new IOException( "Boom! commit" );
            }
        }

        @Override
        public long appendIndex()
        {
            return -1;
        }

        @Override
        public long commitIndex()
        {
            return -1;
        }

        @Override
        public RaftLogEntry readLogEntry( long logIndex ) throws IOException
        {
            throw new IOException( "Boom! readLogEntry" );
        }

        @Override
        public long readEntryTerm( long logIndex ) throws IOException
        {
            return -1;
        }

        @Override
        public boolean entryExists( long logIndex )
        {
            return false;
        }

        @Override
        public IOCursor<RaftLogEntry> getEntryCursor( long fromIndex ) throws IOException
        {
            if ( startExploding )
            {
                throw new IOException( "Boom! entry cursor" );
            }
            else
            {
                return IOCursor.getEmpty();
            }
        }

        public void startExploding()
        {
            startExploding = true;
        }
    }

    private static class TestDatabaseHealth extends DatabaseHealth
    {

        private boolean hasPanicked = false;

        public TestDatabaseHealth()
        {
            super( new DatabasePanicEventGenerator( new KernelEventHandlers( NullLog.getInstance() ) ),
                    NullLog.getInstance() );
        }

        @Override
        public void panic( Throwable cause )
        {
            this.hasPanicked = true;
        }

        public boolean hasPanicked()
        {
            return hasPanicked;
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
