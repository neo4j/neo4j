/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.roles;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.RaftMessages.Message;
import org.neo4j.coreedge.raft.RaftMessages.Timeout.Election;
import org.neo4j.coreedge.raft.RaftMessages.Vote;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.outcome.CommitCommand;
import org.neo4j.coreedge.raft.outcome.LogCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.RaftState;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.coreedge.raft.MessageUtils.messageFor;
import static org.neo4j.coreedge.raft.TestMessageBuilders.appendEntriesResponse;
import static org.neo4j.coreedge.server.RaftTestMember.member;
import static org.neo4j.coreedge.raft.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.coreedge.raft.TestMessageBuilders.heartbeat;
import static org.neo4j.coreedge.raft.TestMessageBuilders.voteRequest;
import static org.neo4j.coreedge.raft.RaftMessages.AppendEntries;
import static org.neo4j.coreedge.raft.roles.Role.CANDIDATE;
import static org.neo4j.coreedge.raft.roles.Role.FOLLOWER;
import static org.neo4j.coreedge.raft.state.RaftStateBuilder.raftState;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.Iterator;


@RunWith(MockitoJUnitRunner.class)
public class FollowerTest
{
    private RaftTestMember myself = member( 0 );

    /* A few members that we use at will in tests. */
    private RaftTestMember member1 = member( 1 );
    private RaftTestMember member2 = member( 2 );
    private RaftTestMember leader = member( 3 );

    @Mock
    private Inbound inbound;

    private LogProvider logProvider = NullLogProvider.getInstance();

    private static final int HIGHEST_TERM = 99;

    @Test
    public void followerShouldUpdateTermToCurrentMessage() throws Exception
    {
        // Given
        RaftState<RaftTestMember> state = raftState().build();


        Follower follower = new Follower();

        // When
        Outcome<RaftTestMember> outcome = follower.handle( voteRequest().from( member1 ).term( HIGHEST_TERM )
                .lastLogIndex( 0 ).lastLogTerm( -1 )
                .build(), state, log() );

        // Then
        assertEquals( HIGHEST_TERM, outcome.getTerm() );
    }

    @Test
    public void shouldVoteOnceOnlyPerTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Follower follower = new Follower();

        // when
        Outcome<RaftTestMember> outcome1 = follower.handle( voteRequest().from( member1 ).term( 1 ).build(), state,
                log() );
        state.update( outcome1 );
        Outcome<RaftTestMember> outcome2 = follower.handle( voteRequest().from( member2 ).term( 1 ).build(), state,
                log() );

        // then
        assertEquals( new Vote.Response<>( myself, 1, true ), messageFor( outcome1, member1 ) );
        assertEquals( new Vote.Response<>( myself, 1, false ), messageFor( outcome2,  member2 ) );

    }

    @Test
    public void followersShouldRejectAnyMessageWithOldTermAndStayAFollower() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )

                .build();

        Follower follower = new Follower();

        // when
        Outcome<RaftTestMember> outcome = follower.handle( voteRequest().from( member1 ).term( state.term() - 1 )
                .lastLogIndex( 0 ).lastLogTerm( -1 ).build(), state, log() );

        // then
        assertEquals( new Vote.Response<>( myself, state.term(), false ), messageFor( outcome, member1 ) );
        assertEquals( FOLLOWER, outcome.getNewRole() );
    }

    @Test
    public void followerShouldTransitToCandidateAndInstigateAnElectionAfterTimeout() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();


        // when
        Outcome<RaftTestMember> outcome = new Follower().handle( new Election<>( myself ), state,
                log() );

        state.update( outcome );

        // then
        assertEquals( CANDIDATE, outcome.getNewRole() );

        assertNotNull( messageFor( outcome, member1 ) );
        assertNotNull( messageFor( outcome, member2 ) );
    }

    @Test
    public void followerShouldVoteForOnlyOneCandidatePerElection() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState().build();

        Follower follower = new Follower();

        // when
        Outcome<RaftTestMember> outcome1 = follower.handle( voteRequest().from( member1 ).term( state.term() )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );
        state.update( outcome1 );
        Outcome<RaftTestMember> outcome2 = follower.handle( voteRequest().from( member2 ).term( state.term() )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertThat( messageFor( outcome1,  member1 ), instanceOf( Vote.Response.class ) );
        assertFalse( ((Vote.Response) messageFor( outcome2, member2 )).voteGranted() );
    }

    @Test
    public void shouldBecomeCandidateOnReceivingElectionTimeoutMessage() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Follower follower = new Follower();

        // when
        Outcome outcome = follower.handle( new Election<>( myself ), state, log() );

        // then
        assertEquals( CANDIDATE, outcome.getNewRole() );
    }

    @Test
    public void followerShouldRejectEntriesForWhichItDoesNotHavePrecedentInItsLog() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Follower follower = new Follower();

        // when
        Outcome<RaftTestMember> outcome = follower.handle( new RaftMessages.AppendEntries.Request<>( myself, 99,
                99, 0, new RaftLogEntry[] { new RaftLogEntry( 99, ContentGenerator.content() ) }, 99 ), state, log() );

        // then
        Message<RaftTestMember> response = messageFor( outcome,  myself );
        assertThat( response, instanceOf( AppendEntries.Response.class ) );
        assertFalse( ((AppendEntries.Response) response).success() );
    }

    @Test
    public void followerShouldAcceptEntriesForWhichItHasPrecedentInItsLog() throws Exception
    {
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .build();

        Follower follower = new Follower();

        state.update( follower.handle( new RaftMessages.AppendEntries.Request<>( myself, 0, -1, -1,
                new RaftLogEntry[] { new RaftLogEntry( 0, ContentGenerator.content() ) }, 0 ), state, log() ) );

        // when
        Outcome<RaftTestMember> outcome = follower.handle( new RaftMessages.AppendEntries.Request<>( myself, 0,
                0, 0, new RaftLogEntry[] { new RaftLogEntry( 0, ContentGenerator.content() ) }, 1 ), state, log() );

        state.update( outcome );

        // then
        Message<RaftTestMember> response = messageFor( outcome, myself );
        assertThat( response, instanceOf( AppendEntries.Response.class ) );
        assertTrue( ((AppendEntries.Response) response).success() );
    }

    @Test
    public void followerShouldOverwriteSomeAppendedEntriesOnReceiptOfConflictingCommittedEntries() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .build();

        Follower follower = new Follower();

        appendSomeEntriesToLog( state, follower, 9, 0 );

        // when
        Outcome<RaftTestMember> outcome = follower.handle( new AppendEntries.Request<>( myself, 1, -1, -1,
                new RaftLogEntry[] { new RaftLogEntry( 1, new ReplicatedString( "commit this!" ) ) }, 0 ), state, log() );
        state.update( outcome );

        // then
        assertEquals( 0, state.entryLog().commitIndex() );
    }

    @Test
    public void followerWithEmptyLogShouldCommitEntriesWithHigherTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .build();

        Follower follower = new Follower();

        // when
        Outcome<RaftTestMember> outcome = follower.handle( new AppendEntries.Request<>( myself, 1, -1, -1,
                new RaftLogEntry[] { new RaftLogEntry( 1, new ReplicatedString( "commit this!" ) ) }, 0 ), state, log() );
        state.update( outcome );

        // then
        assertEquals( 0, state.entryLog().commitIndex() );
        assertEquals( 1, state.term() );
    }

    @Test
    public void followerWithNonEmptyLogShouldOverwriteAppendedEntriesOnReceiptOfCommittedEntryWithHigherTerm()
            throws Exception
    {
        // given
        int term = 1;
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( term )
                .build();

        Follower follower = new Follower();

        appendSomeEntriesToLog( state, follower, 9, term );

        // when leader says it agrees with some of the existing log
        Outcome<RaftTestMember> outcome = follower.handle( new AppendEntries.Request<>( member1, 2, 3, 1,
                new RaftLogEntry[] { new RaftLogEntry( 2, new ReplicatedString( "commit this!" ) ) }, 4 ), state, log() );
        state.update( outcome );

        // then
        assertEquals( 4, state.entryLog().commitIndex() );
        assertEquals( 2, state.term() );
    }
    
    @Test
    public void followerReceivingHeartbeatIndicatingClusterIsAheadShouldElicitAppendResponse() throws Exception
    {
        // given
        int term = 1;
        int followerAppendIndex = 9;
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( term )
                .build();

        Follower follower = new Follower();
        appendSomeEntriesToLog( state, follower, followerAppendIndex, term );

        AppendEntries.Request<RaftTestMember> heartbeat = appendEntriesRequest().from( member1 )
                .leader( member1 )
                .leaderTerm( term )
                .prevLogIndex( followerAppendIndex + 2 ) // leader has appended 2 ahead from this follower
                .prevLogTerm( term ) // in the same term
                .build(); // no entries, this is a heartbeat

        Outcome<RaftTestMember> outcome = follower.handle( heartbeat, state, log() );

        assertEquals( 1, outcome.getOutgoingMessages().size() );
        Message<RaftTestMember> outgoing = outcome.getOutgoingMessages().iterator().next().message();
        assertEquals( RaftMessages.Type.APPEND_ENTRIES_RESPONSE, outgoing.type() );
        RaftMessages.AppendEntries.Response response = (AppendEntries.Response) outgoing;
        assertFalse( response.success() );
    }

    @Test
    public void heartbeatShouldNotResultInCommitIfReferringToFutureEntries() throws Exception
    {
        int term = 1;
        int followerAppendIndex = 9;

        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( term )
                .build();


        Follower follower = new Follower();
        appendSomeEntriesToLog( state, follower, followerAppendIndex, term );

        RaftMessages.Heartbeat<RaftTestMember> heartbeat = heartbeat().from( member1 )
                .commitIndex( followerAppendIndex + 2 ) // The leader is talking about committing stuff we don't know about
                .commitIndexTerm( term ) // And is in the same term
                .leaderTerm( term + 2 )
                .build();

        Outcome<RaftTestMember> outcome = follower.handle( heartbeat, state, log() );

        // Then there should be no actions taken against the log
        assertFalse( outcome.getLogCommands().iterator().hasNext() );
    }

    @Test
    public void heartbeatShouldNotResultInCommitIfHistoryMismatches() throws Exception
    {
        int term = 1;
        int followerAppendIndex = 9;

        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( term )
                .build();


        Follower follower = new Follower();
        appendSomeEntriesToLog( state, follower, followerAppendIndex, term );

        RaftMessages.Heartbeat<RaftTestMember> heartbeat = heartbeat().from( member1 )
                .commitIndex( followerAppendIndex ) // The leader suggests that we commit stuff we have appended
                .commitIndexTerm( term + 2 ) // but in a different term
                .leaderTerm( term + 2 ) // And is in a term that is further in the future
                .build();

        Outcome<RaftTestMember> outcome = follower.handle( heartbeat, state, log() );

        assertFalse( outcome.getLogCommands().iterator().hasNext() );
    }

    @Test
    public void historyShouldResultInCommitIfHistoryMatches() throws Exception
    {
        int term = 1;
        int followerAppendIndex = 9;

        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( term )
                .build();

        Follower follower = new Follower();
        appendSomeEntriesToLog( state, follower, followerAppendIndex, term );

        RaftMessages.Heartbeat<RaftTestMember> heartbeat = heartbeat().from( member1 )
                .commitIndex( followerAppendIndex - 1) // The leader suggests that we commit stuff we have appended
                .commitIndexTerm( term ) // in the same term
                .leaderTerm( term + 2 ) // with the leader in the future
                .build();

        Outcome<RaftTestMember> outcome = follower.handle( heartbeat, state, log() );

        Iterator<LogCommand> iterator = outcome.getLogCommands().iterator();
        assertTrue( iterator.hasNext() );
        LogCommand logCommand = iterator.next();
        assertFalse( iterator.hasNext() );
        assertThat( logCommand, instanceOf( CommitCommand.class ) );
        CommitCommand commit = (CommitCommand) logCommand;
        CapturingRaftLog capture = new CapturingRaftLog();
        commit.applyTo( capture );
        assertEquals( followerAppendIndex - 1, capture.commitIndex() );
    }

    @Test
    public void shouldAppendMultipleEntries() throws Exception
    {
        // given
        int term = 1;
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( term )
                .build();

        Follower follower = new Follower();

        RaftLogEntry[] entries = {
                new RaftLogEntry( 1, new ReplicatedString( "commit this!" ) ),
                new RaftLogEntry( 1, new ReplicatedString( "commit this as well!" ) ),
                new RaftLogEntry( 1, new ReplicatedString( "commit this too!" ) )
        };

        Outcome<RaftTestMember> outcome = follower.handle(
                new AppendEntries.Request<>( member1, 1, -1, -1, entries, -1 ), state, log() );
        state.update( outcome );

        // then
        assertEquals( 2, state.entryLog().appendIndex() );
    }

    @Test
    public void shouldTruncateIfTermDoesNotMatch() throws Exception
    {
        // given
        int term = 1;
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( term )
                .build();

        Follower follower = new Follower();

        state.update( follower.handle( new AppendEntries.Request<>( member1, 1, -1, -1,
                new RaftLogEntry[] {
                        new RaftLogEntry( 2, ContentGenerator.content() ),
                },
                -1 ), state, log() ) );


        RaftLogEntry[] entries = {
                new RaftLogEntry( 1, new ReplicatedString( "commit this!" ) ),
        };

        Outcome<RaftTestMember> outcome = follower.handle(
                new AppendEntries.Request<>( member1, 1, -1, -1, entries, -1 ), state, log() );
        state.update( outcome );

        // then
        assertEquals( 0, state.entryLog().appendIndex() );
        assertEquals( 1, state.entryLog().readEntryTerm( 0 ) );
    }

    @Test
    public void followerLearningAboutHigherCommitCausesValuesTobeAppliedToItsLog() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .build();

        Follower follower = new Follower();

        appendSomeEntriesToLog( state, follower, 3, 0 );

        // when receiving AppEntries with high leader commit (3)
        Outcome<RaftTestMember> outcome = follower.handle( new AppendEntries.Request<>( myself, 0, 2, 0,
                new RaftLogEntry[] { new RaftLogEntry( 0, ContentGenerator.content() ) }, 3 ), state, log() );

        state.update( outcome );

        // then
        assertEquals( 3, state.entryLog().commitIndex() );
    }

    @Test
    public void shouldRenewElectionTimeoutOnReceiptOfHeartbeatInCurrentOrHigherTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term(0)
                .build();

        Follower follower = new Follower();

        Outcome<RaftTestMember> outcome = follower.handle( new RaftMessages.Heartbeat<>( myself, 1, 1, 1 ),
                state, log() );

        // then
        assertTrue( outcome.electionTimeoutRenewed() );
    }

    @Test
    public void shouldNotRenewElectionTimeoutOnReceiptOfHeartbeatInLowerTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( 2 )
                .build();

        Follower follower = new Follower();

        Outcome<RaftTestMember> outcome = follower.handle( new RaftMessages.Heartbeat<>( myself, 1, 1, 1 ),
                state, log() );

        // then
        assertFalse( outcome.electionTimeoutRenewed() );
    }

    @Test
    public void shouldNotCommitAheadOfMatchingHistory() throws Exception
    {
        // given
        int LEADER_COMMIT = 10;
        int LEADER1_TERM = 1;
        int LEADER2_TERM = 2;

        InMemoryRaftLog raftLog = new InMemoryRaftLog();

        raftLog.append( new RaftLogEntry( LEADER1_TERM, ReplicatedString.valueOf( "gryffindor" ) ) ); // (0) we committed this far already
        raftLog.commit( 0 );
        raftLog.append( new RaftLogEntry( LEADER1_TERM, ReplicatedString.valueOf( "hufflepuff" ) ) ); // (1) we should only commit up to this, because this is how far we match
        raftLog.append( new RaftLogEntry( LEADER1_TERM, ReplicatedString.valueOf( "ravenclaw" ) ) ); // (2) leader will have committed including this and more

        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .entryLog( raftLog )
                .term( 1 )
                .build();

        Follower follower = new Follower();
        Outcome<RaftTestMember> outcome;

        // when - append request matching history and truncating
        RaftLogEntry[] newEntries = {
                new RaftLogEntry( LEADER2_TERM, new ReplicatedString( "slytherin" ) ), // (1 - overwrite and truncate)
        };

        outcome = follower.handle( new RaftMessages.AppendEntries.Request<>( myself, LEADER2_TERM, 0, LEADER1_TERM, newEntries, LEADER_COMMIT ), state, log() );

        // then
        assertThat( outcome.getLogCommands(), hasItem( new CommitCommand( 1 ) ) );
    }

    @Test
    public void shouldIncludeLatestAppendedInResponse() throws Exception
    {
        // given: just a single appended entry at follower
        RaftLogEntry entryA = new RaftLogEntry( 1, ReplicatedString.valueOf( "A" ) );

        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( entryA );

        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .entryLog( raftLog )
                .term( 1 )
                .build();

        Follower follower = new Follower();

        RaftLogEntry entryB = new RaftLogEntry( 1, ReplicatedString.valueOf( "B" ) );

        // when: append request for item way forward (index=10, term=2)
        Outcome<RaftTestMember> outcome = follower.handle(
                new RaftMessages.AppendEntries.Request<>( leader, 2, 10, 2,
                        new RaftLogEntry[]{entryB}, 10 ), state, log() );

        // then: respond with false and how far ahead we are
        assertThat( single( outcome.getOutgoingMessages()).message(), equalTo(
                appendEntriesResponse().from( myself ).term( 2 ).appendIndex( 0 ).matchIndex( -1 ).failure()
                        .build() ) );
    }

    private void appendSomeEntriesToLog( RaftState<RaftTestMember> raft, Follower follower, int numberOfEntriesToAppend, int
            term ) throws RaftStorageException
    {
        for ( int i = 0; i < numberOfEntriesToAppend; i++ )
        {
            if ( i == 0 )
            {
                raft.update( follower.handle( new AppendEntries.Request<>( myself, term, i - 1, -1,
                        new RaftLogEntry[] { new RaftLogEntry( term, ContentGenerator.content() ) }, -1 ), raft, log() ) );
            }
            else
            {
                raft.update( follower.handle( new AppendEntries.Request<>( myself, term, i - 1, term,
                        new RaftLogEntry[]{new RaftLogEntry( term, ContentGenerator.content() )}, -1 ), raft, log() ) );
            }
        }
    }

    private static class ContentGenerator
    {
        private static int count = 0;

        public static ReplicatedString content()
        {
            return new ReplicatedString( String.format( "content#%d", count++ ) );
        }
    }

    private Log log()
    {
        return logProvider.getLog( getClass() );
    }

    private static final class CapturingRaftLog implements RaftLog
    {

        private long commitIndex;

        @Override
        public void replay() throws Throwable
        {

        }

        @Override
        public void registerListener( Listener consumer )
        {

        }

        @Override
        public long append( RaftLogEntry entry ) throws RaftStorageException
        {
            return 0;
        }

        @Override
        public void truncate( long fromIndex ) throws RaftStorageException
        {

        }

        @Override
        public void commit( long commitIndex ) throws RaftStorageException
        {
            this.commitIndex = commitIndex;
        }

        @Override
        public long appendIndex()
        {
            return 0;
        }

        @Override
        public long commitIndex()
        {
            return commitIndex;
        }

        @Override
        public RaftLogEntry readLogEntry( long logIndex ) throws RaftStorageException
        {
            return null;
        }

        @Override
        public ReplicatedContent readEntryContent( long logIndex ) throws RaftStorageException
        {
            return null;
        }

        @Override
        public long readEntryTerm( long logIndex ) throws RaftStorageException
        {
            return 0;
        }

        @Override
        public boolean entryExists( long logIndex )
        {
            return false;
        }
    }
}
