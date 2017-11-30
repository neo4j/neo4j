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
package org.neo4j.causalclustering.core.consensus.roles;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;

public class Voting
{

    private Voting()
    {
    }

    static void handleVoteRequest( ReadableRaftState state, Outcome outcome,
            RaftMessages.Vote.Request voteRequest, Log log ) throws IOException
    {
        if ( voteRequest.term() > state.term() )
        {
            outcome.setNextTerm( voteRequest.term() );
            outcome.setVotedFor( null );
        }

        boolean willVoteForCandidate = shouldVoteFor( voteRequest.candidate(), outcome.getTerm(), voteRequest.term(),
                state.entryLog().readEntryTerm( state.entryLog().appendIndex() ), voteRequest.lastLogTerm(),
                state.entryLog().appendIndex(), voteRequest.lastLogIndex(),
                Optional.ofNullable( outcome.getVotedFor() ), log );

        if ( willVoteForCandidate )
        {
            outcome.setVotedFor( voteRequest.from() );
            outcome.renewElectionTimeout();
        }

        outcome.addOutgoingMessage( new RaftMessages.Directed( voteRequest.from(), new RaftMessages.Vote.Response(
                state.myself(), outcome.getTerm(),
                willVoteForCandidate ) ) );
    }

    static void handlePreVoteRequest( ReadableRaftState state, Outcome outcome,
            RaftMessages.PreVote.Request voteRequest, Log log ) throws IOException
    {
        if ( voteRequest.term() > state.term() )
        {
            outcome.setNextTerm( voteRequest.term() );
        }

        boolean willVoteForCandidate = shouldVoteFor( voteRequest.candidate(), outcome.getTerm(), voteRequest.term(),
                state.entryLog().readEntryTerm( state.entryLog().appendIndex() ), voteRequest.lastLogTerm(),
                state.entryLog().appendIndex(), voteRequest.lastLogIndex(),
                Optional.empty(), log );

        outcome.addOutgoingMessage( new RaftMessages.Directed( voteRequest.from(), new RaftMessages.PreVote.Response(
                state.myself(), outcome.getTerm(),
                willVoteForCandidate ) ) );
    }

    public static boolean shouldVoteFor( MemberId candidate, long contextTerm, long requestTerm,
                                         long contextLastLogTerm, long requestLastLogTerm,
                                         long contextLastAppended, long requestLastLogIndex,
                                         Optional<MemberId> votedFor, Log log )
    {
        if ( requestTerm < contextTerm )
        {
            log.debug( "Will not vote for %s as vote request term %s was earlier than my term %s", candidate, requestTerm, contextTerm );
            return false;
        }

        boolean requestLogEndsAtHigherTerm = requestLastLogTerm > contextLastLogTerm;
        boolean logsEndAtSameTerm = requestLastLogTerm == contextLastLogTerm;
        boolean requestLogAtLeastAsLongAsMyLog = requestLastLogIndex >= contextLastAppended;

        boolean requesterLogUpToDate = requestLogEndsAtHigherTerm ||
                (logsEndAtSameTerm && requestLogAtLeastAsLongAsMyLog);

        boolean votedForOtherInSameTerm = requestTerm == contextTerm &&
                votedFor.map( member -> !member.equals( candidate ) ).orElse( false );

        boolean shouldVoteFor = requesterLogUpToDate && !votedForOtherInSameTerm;

        log.debug( "Should vote for raft candidate %s: " +
                        "requester log up to date: %s " +
                        "(request last log term: %s, context last log term: %s, request last log index: %s, context last append: %s) " +
                        "voted for other in same term: %s " +
                        "(request term: %s, context term: %s, votedFor: %s)",
                shouldVoteFor,
                requesterLogUpToDate, requestLastLogTerm, contextLastLogTerm, requestLastLogIndex, contextLastAppended,
                votedForOtherInSameTerm, requestTerm, contextTerm, votedFor );

        return shouldVoteFor;
    }
}
