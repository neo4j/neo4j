/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.roles;

import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import org.neo4j.causalclustering.core.consensus.RaftMessages.Timeout.Election;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.membership.RaftTestGroup;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class FollowerTest
{
    private MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );
    private MemberId member3 = member( 3 );
    private MemberId member4 = member( 4 );

    @Test
    public void shouldInstigateAnElectionAfterTimeout() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        assertEquals( RaftMessages.Type.VOTE_REQUEST, messageFor( outcome, member1 ).type() );
        assertEquals( RaftMessages.Type.VOTE_REQUEST, messageFor( outcome, member2 ).type() );
    }

    @Test
    public void shouldBecomeCandidateOnReceivingElectionTimeoutMessage() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        assertEquals( CANDIDATE, outcome.getRole() );
    }

    @Test
    public void shouldNotInstigateElectionOnElectionTimeoutIfRefusingToBeLeaderAndPreVoteNotSupported() throws Throwable
    {
        // given
        RaftState state = raftState()
                .setRefusesToBeLeader( true )
                .supportsPreVoting( false )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        assertThat( outcome.getOutgoingMessages(), empty() );
    }

    @Test
    public void shouldIgnoreAnElectionTimeoutIfRefusingToBeLeaderAndPreVoteNotSupported() throws Throwable
    {
        // given
        RaftState state = raftState()
                .setRefusesToBeLeader( true )
                .supportsPreVoting( false )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        assertEquals( new Outcome( Role.FOLLOWER, state ), outcome );
    }

    @Test
    public void shouldSetPreElectionOnTimeoutIfSupportedAndIAmVoterAndIRefuseToLead() throws Throwable
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .votingMembers( myself, member1, member2 )
                .setRefusesToBeLeader( true )
                .supportsPreVoting( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        assertTrue( outcome.isPreElection() );
    }

    @Test
    public void shouldNotSetPreElectionOnTimeoutIfSupportedAndIAmNotVoterAndIRefuseToLead() throws Throwable
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .votingMembers( member1, member2, member3 )
                .setRefusesToBeLeader( true )
                .supportsPreVoting( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        assertFalse( outcome.isPreElection() );
    }

    @Test
    public void shouldNotSolicitPreVotesOnTimeoutEvenIfSupportedIfRefuseToLead() throws Throwable
    {
        // given
        RaftState state = raftState()
                .setRefusesToBeLeader( true )
                .supportsPreVoting( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), state, log() );

        // then
        assertThat( outcome.getOutgoingMessages(), empty() );
    }

    @Test
    public void followerReceivingHeartbeatIndicatingClusterIsAheadShouldElicitAppendResponse() throws Exception
    {
        // given
        int term = 1;
        int followerAppendIndex = 9;
        RaftLog entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 0, new RaftTestGroup( 0 ) ) );
        RaftState state = raftState()
                .myself( myself )
                .term( term )
                .build();

        Follower follower = new Follower();
        appendSomeEntriesToLog( state, follower, followerAppendIndex - 1, term, 1 );

        AppendEntries.Request heartbeat = appendEntriesRequest().from( member1 )
                .leaderTerm( term )
                .prevLogIndex( followerAppendIndex + 2 ) // leader has appended 2 ahead from this follower
                .prevLogTerm( term ) // in the same term
                .build(); // no entries, this is a heartbeat

        Outcome outcome = follower.handle( heartbeat, state, log() );

        assertEquals( 1, outcome.getOutgoingMessages().size() );
        RaftMessage outgoing = outcome.getOutgoingMessages().iterator().next().message();
        assertEquals( RaftMessages.Type.APPEND_ENTRIES_RESPONSE, outgoing.type() );
        RaftMessages.AppendEntries.Response response = (AppendEntries.Response) outgoing;
        assertFalse( response.success() );
    }

    @Test
    public void shouldTruncateIfTermDoesNotMatch() throws Exception
    {
        // given
        RaftLog entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 0, new RaftTestGroup( 0 ) ) );
        int term = 1;
        RaftState state = raftState()
                .myself( myself )
                .entryLog( entryLog )
                .term( term )
                .build();

        Follower follower = new Follower();

        state.update( follower.handle( new AppendEntries.Request( member1, 1, 0, 0,
                new RaftLogEntry[]{
                        new RaftLogEntry( 2, ContentGenerator.content() ),
                },
                0 ), state, log() ) );

        RaftLogEntry[] entries = {
                new RaftLogEntry( 1, new ReplicatedString( "commit this!" ) ),
        };

        Outcome outcome = follower.handle(
                new AppendEntries.Request( member1, 1, 0, 0, entries, 0 ), state, log() );
        state.update( outcome );

        // then
        assertEquals( 1, state.entryLog().appendIndex() );
        assertEquals( 1, state.entryLog().readEntryTerm( 1 ) );
    }

    // TODO move this to outcome tests
    @Test
    public void followerLearningAboutHigherCommitCausesValuesTobeAppliedToItsLog() throws Exception
    {
        // given
        RaftLog entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 0, new RaftTestGroup( 0 ) ) );
        RaftState state = raftState()
                .myself( myself )
                .entryLog( entryLog )
                .build();

        Follower follower = new Follower();

        appendSomeEntriesToLog( state, follower, 3, 0, 1 );

        // when receiving AppEntries with high leader commit (4)
        Outcome outcome = follower.handle( new AppendEntries.Request( myself, 0, 3, 0,
                new RaftLogEntry[] { new RaftLogEntry( 0, ContentGenerator.content() ) }, 4 ), state, log() );

        state.update( outcome );

        // then
        assertEquals( 4, state.commitIndex() );
    }

    @Test
    public void shouldUpdateCommitIndexIfNecessary() throws Exception
    {
        //  If leaderCommit > commitIndex, set commitIndex = min( leaderCommit, index of last new entry )

        // given
        RaftLog entryLog = new InMemoryRaftLog();
        entryLog.append( new RaftLogEntry( 0, new RaftTestGroup( 0 ) ) );
        RaftState state = raftState()
                .myself( myself )
                .entryLog( entryLog )
                .build();

        Follower follower = new Follower();

        int localAppendIndex = 3;
        int localCommitIndex =  localAppendIndex - 1;
        int term = 0;
        appendSomeEntriesToLog( state, follower, localAppendIndex, term, 1 ); // append index is 0 based

        // the next when-then simply verifies that the test is setup properly, with commit and append index as expected
        // when
        Outcome raftTestMemberOutcome = new Outcome( FOLLOWER, state );
        raftTestMemberOutcome.setCommitIndex( localCommitIndex );
        state.update( raftTestMemberOutcome );

        // then
        assertEquals( localAppendIndex, state.entryLog().appendIndex() );
        assertEquals( localCommitIndex, state.commitIndex() );

        // when
        // an append req comes in with leader commit index > localAppendIndex but localCommitIndex < localAppendIndex
        Outcome outcome = follower.handle( appendEntriesRequest()
                .leaderTerm( term ).prevLogIndex( 3 )
                .prevLogTerm( term ).leaderCommit( localCommitIndex + 4 )
                .build(), state, log() );

        state.update( outcome );

        // then
        // The local commit index must be brought as far along as possible
        assertEquals( 3, state.commitIndex() );
    }

    @Test
    public void shouldNotRenewElectionTimeoutOnReceiptOfHeartbeatInLowerTerm() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .term( 2 )
                .build();

        Follower follower = new Follower();

        Outcome outcome = follower.handle( new RaftMessages.Heartbeat( myself, 1, 1, 1 ),
                state, log() );

        // then
        assertFalse( outcome.electionTimeoutRenewed() );
    }

    @Test
    public void shouldAcknowledgeHeartbeats() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .term( 2 )
                .build();

        Follower follower = new Follower();

        Outcome outcome = follower.handle( new RaftMessages.Heartbeat( state.leader(), 2, 2, 2 ),
                state, log() );

        // then
        Collection<RaftMessages.Directed> outgoingMessages = outcome.getOutgoingMessages();
        assertTrue( outgoingMessages.contains( new RaftMessages.Directed( state.leader(),
                new RaftMessages.HeartbeatResponse( myself ) ) ) );
    }

    @Test
    public void shouldRespondPositivelyToPreVoteRequestsIfWouldVoteForCandidate() throws Exception
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( true )  );
    }

    @Test
    public void shouldRespondPositivelyToPreVoteRequestsEvenIfAlreadyVotedInRealElection() throws Exception
    {
        // given
        RaftState raftState = preElectionActive();
        raftState.update( new Follower().handle( new RaftMessages.Vote.Request( member1, 0, member1, 0, 0 ), raftState, log() ) );

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member2, 0, member2, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member2 );
        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( true )  );
    }

    @Test
    public void shouldRespondNegativelyToPreVoteRequestsIfNotInPreVoteMyself() throws Exception
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setPreElection( false )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( false )  );
    }

    @Test
    public void shouldRespondNegativelyToPreVoteRequestsIfWouldNotVoteForCandidate() throws Exception
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .term( 1 )
                .setPreElection( true )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( false )  );
    }

    @Test
    public void shouldRespondPositivelyToPreVoteRequestsToMultipleMembersIfWouldVoteForAny() throws Exception
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome1 = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );
        raftState.update( outcome1 );
        Outcome outcome2 = new Follower().handle( new RaftMessages.PreVote.Request( member2, 0, member2, 0, 0 ), raftState, log() );
        raftState.update( outcome2 );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome2, member2 );

        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( true )  );
    }

    @Test
    public void shouldUseTermFromPreVoteRequestIfHigherThanOwn() throws Exception
    {
        // given
        RaftState raftState = preElectionActive();
        long newTerm = raftState.term() + 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, newTerm, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );

        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).term() , equalTo( newTerm )  );
    }

    @Test
    public void shouldUpdateOutcomeWithTermFromPreVoteRequestOfLaterTermIfInPreVoteState() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();
        long newTerm = raftState.term() + 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, newTerm, member1, 0, 0 ), raftState, log() );

        // then
        assertEquals( newTerm, outcome.getTerm() );
    }

    @Test
    public void shouldUpdateOutcomeWithTermFromPreVoteRequestOfLaterTermIfNotInPreVoteState() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .setPreElection( false )
                .build();
        long newTerm = raftState.term() + 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, newTerm, member1, 0, 0 ), raftState, log() );

        // then

        assertEquals( newTerm, outcome.getTerm() );
    }

    @Test
    public void shouldInstigatePreElectionIfSupportedAndNotActiveAndReceiveTimeout() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .supportsPreVoting( true )
                .setPreElection( false )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), raftState, log() );

        // then
        assertEquals( RaftMessages.Type.PRE_VOTE_REQUEST, messageFor( outcome, member1 ).type() );
        assertEquals( RaftMessages.Type.PRE_VOTE_REQUEST, messageFor( outcome, member2 ).type() );
    }

    @Test
    public void shouldSetPreElectionActiveWhenReceiveTimeout() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .votingMembers( asSet( myself, member1, member2 ) )
                .supportsPreVoting( true )
                .setPreElection( false )
                .build();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), raftState, log() );

        // then
        assertEquals( true, outcome.isPreElection() );
    }

    @Test
    public void shouldInstigatePreElectionIfSupportedAndActiveAndReceiveTimeout() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), raftState, log() );

        // then
        assertEquals( RaftMessages.Type.PRE_VOTE_REQUEST, messageFor( outcome, member1 ).type() );
        assertEquals( RaftMessages.Type.PRE_VOTE_REQUEST, messageFor( outcome, member2 ).type() );
        assertEquals( RaftMessages.Type.PRE_VOTE_REQUEST, messageFor( outcome, member3 ).type() );
    }

    @Test
    public void shouldKeepPreElectionActiveWhenReceiveTimeout() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new Election( myself ), raftState, log() );

        // then
        assertEquals( true, outcome.isPreElection() );
    }

    @Test
    public void shouldAbortPreElectionIfReceivePreVoteResponseFromNewerTerm() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();
        long newTerm = raftState.term() + 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, newTerm, false ), raftState, log() );

        // then
        assertEquals( newTerm, outcome.getTerm() );
        assertEquals( false, outcome.isPreElection() );
    }

    @Test
    public void shouldIgnoreVotesFromEarlierTerms() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();
        long oldTerm = raftState.term() - 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, oldTerm, true ), raftState, log() );

        // then
        assertEquals( new Outcome( Role.FOLLOWER, raftState ), outcome );
    }

    @Test
    public void shouldIgnoreVotesDeclining() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), false ), raftState, log() );

        // then
        assertEquals( new Outcome( Role.FOLLOWER, raftState ), outcome );
    }

    @Test
    public void shouldAddVoteFromADifferentMember() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        // then
        assertThat( outcome.getPreVotesForMe(), contains( member1 ) );
    }

    @Test
    public void shouldNotAddVoteFromMyself() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( myself, raftState.term(), true ), raftState, log() );

        // then
        assertThat( outcome.getPreVotesForMe(), not( contains( member1 ) ) );
    }

    @Test
    public void shouldNotStartElectionIfHaveNotReachedQuorum() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        // then
        assertEquals( Role.FOLLOWER, outcome.getRole() );
    }

    @Test
    public void shouldTransitionToCandidateIfHaveReachedQuorum() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome1 = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );
        raftState.update( outcome1 );
        Outcome outcome2 = new Follower().handle( new RaftMessages.PreVote.Response( member2, raftState.term(), true ), raftState, log() );

        // then
        assertEquals( Role.CANDIDATE, outcome2.getRole() );
    }

    @Test
    public void shouldInstigateElectionIfHaveReachedQuorum() throws Throwable
    {
        // given
        RaftState raftState = preElectionActive();

        // when
        Outcome outcome1 = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );
        raftState.update( outcome1 );
        Outcome outcome2 = new Follower().handle( new RaftMessages.PreVote.Response( member2, raftState.term(), true ), raftState, log() );

        // then
        assertEquals( RaftMessages.Type.VOTE_REQUEST, messageFor( outcome2, member1 ).type() );
        assertEquals( RaftMessages.Type.VOTE_REQUEST, messageFor( outcome2, member2 ).type() );
        assertEquals( RaftMessages.Type.VOTE_REQUEST, messageFor( outcome2, member3 ).type() );
    }

    @Test
    public void shouldIgnorePreVoteResponsesIfPreVoteInactive() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .setPreElection( false )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        assertEquals( new Outcome( Role.FOLLOWER, raftState ), outcome );
    }

    @Test
    public void shouldIgnorePreVoteRequestsIfPreVoteUnsupported() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .supportsPreVoting( false )
                .setPreElection( false )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, raftState.term(), member1, 0, 0 ), raftState, log() );

        assertEquals( new Outcome( Role.FOLLOWER, raftState ), outcome );
    }

    @Test
    public void shouldIgnorePreVoteResponsesIfPreVoteUnsupported() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .supportsPreVoting( false )
                .setPreElection( false )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        assertEquals( new Outcome( Role.FOLLOWER, raftState ), outcome );
    }

    @Test
    public void shouldIgnorePreVoteResponseWhenPreElectionFalseRefuseToBeLeader() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .setPreElection( false )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setRefusesToBeLeader( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        // then
        assertEquals( new Outcome( Role.FOLLOWER, raftState ), outcome );
    }

    @Test
    public void shouldIgnorePreVoteResponseWhenPreElectionTrueAndRefuseLeader() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .setPreElection( true )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setRefusesToBeLeader( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        // then
        assertEquals( new Outcome( Role.FOLLOWER, raftState ), outcome );
    }

    @Test
    public void shouldNotInstigateElectionOnPreVoteResponseWhenPreElectionTrueAndRefuseLeader() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .setPreElection( true )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setRefusesToBeLeader( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Response( member1, raftState.term(), true ), raftState, log() );

        // then
        assertThat( outcome.getOutgoingMessages(), empty() );
    }

    @Test
    public void shouldDeclinePreVoteRequestsIfPreElectionNotActiveAndRefusesToLead() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .setPreElection( false )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setRefusesToBeLeader( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, raftState.term(), member1, 0, 0 ), raftState, log() );

        // then
        assertEquals( false, ( (RaftMessages.PreVote.Response) messageFor( outcome, member1 ) ).voteGranted() );
    }

    @Test
    public void shouldApprovePreVoteRequestIfPreElectionActiveAndRefusesToLead() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .setPreElection( true )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setRefusesToBeLeader( true )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, raftState.term(), member1, 0, 0 ), raftState, log() );

        // then
        assertEquals( true, ( (RaftMessages.PreVote.Response) messageFor( outcome, member1 ) ).voteGranted() );
    }

    @Test
    public void shouldSetPreElectionOnElectionTimeout() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome );

        // then
        assertThat( outcome.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome.isPreElection(), equalTo( true ) );
    }

    @Test
    public void shouldSendPreVoteRequestsOnElectionTimeout() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome );

        // then
        assertThat( messageFor( outcome, member1 ).type(), equalTo( RaftMessages.Type.PRE_VOTE_REQUEST ) );
        assertThat( messageFor( outcome, member2 ).type(), equalTo( RaftMessages.Type.PRE_VOTE_REQUEST ) );
    }

    @Test
    public void shouldProceedToRealElectionIfReceiveQuorumOfPositivePreVoteResponses() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome2.getRole(), equalTo( CANDIDATE ) );
        assertThat( outcome2.isPreElection(), equalTo( false ) );
        assertThat( outcome2.getPreVotesForMe(), contains( member1 ) );
    }

    @Test
    public void shouldIgnorePreVotePositiveResponsesFromOlderTerm() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .term( 1 )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome2.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome2.isPreElection(), equalTo( true ) );
        assertThat( outcome2.getPreVotesForMe(), empty() );
    }

    @Test
    public void shouldIgnorePositivePreVoteResponsesIfNotInPreVotingStage() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        // when
        Outcome outcome = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome.isPreElection(), equalTo( false ) );
        assertThat( outcome.getPreVotesForMe(), empty() );
    }

    @Test
    public void shouldNotMoveToRealElectionWithoutPreVoteQuorum() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2, member3, member4 ) )
                .build();

        Follower underTest = new Follower();
        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome2.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome2.isPreElection(), equalTo( true ) );
        assertThat( outcome2.getPreVotesForMe(), contains( member1 ) );
    }

    @Test
    public void shouldMoveToRealElectionWithPreVoteQuorumOf5() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2, member3, member4 ) )
                .build();

        Follower underTest = new Follower();
        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member2, 0L, true ), state, log() );

        // then
        assertThat( outcome3.getRole(), equalTo( CANDIDATE ) );
        assertThat( outcome3.isPreElection(), equalTo( false ) );
    }

    @Test
    public void shouldNotCountPreVotesVotesFromSameMemberTwice() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2, member3, member4 ) )
                .build();

        Follower underTest = new Follower();
        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome3.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome3.isPreElection(), equalTo( true ) );
    }

    @Test
    public void shouldResetPreVotesWhenMovingBackToFollower() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Outcome outcome1 = new Follower().handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );
        Outcome outcome2 = new Follower().handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );
        assertThat( CANDIDATE, equalTo( outcome2.getRole() ) );
        assertThat( outcome2.getPreVotesForMe(), contains( member1 ) );

        // when
        Outcome outcome3 = new Candidate().handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // then
        assertThat( outcome3.getPreVotesForMe(), empty() );
    }

    @Test
    public void shouldSendRealVoteRequestsIfReceivePositivePreVoteResponses() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( messageFor( outcome2, member1 ).type(), equalTo( RaftMessages.Type.VOTE_REQUEST ) );
        assertThat( messageFor( outcome2, member2 ).type(), equalTo( RaftMessages.Type.VOTE_REQUEST ) );
    }

    @Test
    public void shouldNotProceedToRealElectionIfReceiveNegativePreVoteResponses() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, false ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member2, 0L, false ), state, log() );

        // then
        assertThat( outcome3.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome3.isPreElection(), equalTo( true ) );
        assertThat( outcome3.getPreVotesForMe(), empty() );
    }

    @Test
    public void shouldNotSendRealVoteRequestsIfReceiveNegativePreVoteResponses() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, false ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member2, 0L, false ), state, log() );

        // then
        assertThat( outcome2.getOutgoingMessages(), empty() );
        assertThat( outcome3.getOutgoingMessages(), empty() );
    }

    @Test
    public void shouldResetPreVoteIfReceiveHeartbeatFromLeader() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.Heartbeat( member1, 0L, 0L, 0L ), state, log() );

        // then
        assertThat( outcome2.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome2.isPreElection(), equalTo( false ) );
        assertThat( outcome2.getPreVotesForMe(), empty() );
    }

    @Test
    public void shouldResetPreVoteIfReceiveAppendEntriesRequestFromLeader() throws Exception
    {
        // given
        RaftState state = preElectionSupported();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle(
                appendEntriesRequest().leaderTerm( state.term() ).prevLogTerm( state.term() ).prevLogIndex( 0 ).build(),
                state, log() );

        // then
        assertThat( outcome2.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome2.isPreElection(), equalTo( false ) );
        assertThat( outcome2.getPreVotesForMe(), empty() );
    }

    private RaftState preElectionActive() throws IOException
    {
        return raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .setPreElection( true )
                .votingMembers( asSet( myself, member1, member2, member3 ) )
                .build();
    }

    private RaftState preElectionSupported() throws IOException
    {
        return raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();
    }

    private void appendSomeEntriesToLog( RaftState raft, Follower follower, int numberOfEntriesToAppend, int term,
            int firstIndex ) throws IOException
    {
        for ( int i = 0; i < numberOfEntriesToAppend; i++ )
        {
            int prevLogIndex = (firstIndex + i) - 1;
            raft.update( follower.handle( new AppendEntries.Request( myself, term, prevLogIndex, term,
                    new RaftLogEntry[]{new RaftLogEntry( term, ContentGenerator.content() )}, -1 ), raft, log() ) );
        }
    }

    private static class ContentGenerator
    {
        private static int count;

        public static ReplicatedString content()
        {
            return new ReplicatedString( String.format( "content#%d", count++ ) );
        }
    }

    private Log log()
    {
        return NullLog.getInstance();
    }
}
