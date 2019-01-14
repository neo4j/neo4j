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
package org.neo4j.causalclustering.core.consensus.roles;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.function.ThrowingBooleanSupplier;
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

        boolean votedForAnother = outcome.getVotedFor() != null && !outcome.getVotedFor().equals( voteRequest.candidate() );
        boolean willVoteForCandidate = shouldVoteFor( state, outcome, voteRequest, votedForAnother, log );

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
        ThrowingBooleanSupplier<IOException> willVoteForCandidate =
                () -> shouldVoteFor( state, outcome, voteRequest, false, log );
        respondToPreVoteRequest( state, outcome, voteRequest, willVoteForCandidate );
    }

    static void declinePreVoteRequest( ReadableRaftState state, Outcome outcome,
            RaftMessages.PreVote.Request voteRequest ) throws IOException
    {
        respondToPreVoteRequest( state, outcome, voteRequest, () -> false );
    }

    private static void respondToPreVoteRequest( ReadableRaftState state, Outcome outcome,
            RaftMessages.PreVote.Request voteRequest, ThrowingBooleanSupplier<IOException> willVoteFor ) throws IOException
    {
        if ( voteRequest.term() > state.term() )
        {
            outcome.setNextTerm( voteRequest.term() );
        }

        outcome.addOutgoingMessage( new RaftMessages.Directed( voteRequest.from(), new RaftMessages.PreVote.Response(
                state.myself(), outcome.getTerm(),
                willVoteFor.getAsBoolean() ) ) );
    }

    private static boolean shouldVoteFor( ReadableRaftState state, Outcome outcome, RaftMessages.AnyVote.Request voteRequest,
            boolean committedToVotingForAnother, Log log )
            throws IOException
    {
        long requestTerm = voteRequest.term();
        MemberId candidate = voteRequest.candidate();
        long requestLastLogTerm = voteRequest.lastLogTerm();
        long requestLastLogIndex = voteRequest.lastLogIndex();
        long contextTerm = outcome.getTerm();
        long contextLastAppended = state.entryLog().appendIndex();
        long contextLastLogTerm = state.entryLog().readEntryTerm( contextLastAppended );

        return shouldVoteFor(
                candidate,
                contextTerm,
                requestTerm,
                contextLastLogTerm,
                requestLastLogTerm,
                contextLastAppended,
                requestLastLogIndex,
                committedToVotingForAnother,
                log
        );
    }

    public static boolean shouldVoteFor( MemberId candidate, long contextTerm, long requestTerm,
                                         long contextLastLogTerm, long requestLastLogTerm,
                                         long contextLastAppended, long requestLastLogIndex,
                                         boolean committedToVotingForAnother, Log log )
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

        boolean votedForOtherInSameTerm = requestTerm == contextTerm && committedToVotingForAnother;

        boolean shouldVoteFor = requesterLogUpToDate && !votedForOtherInSameTerm;

        log.debug( "Should vote for raft candidate %s: " +
                        "requester log up to date: %s " +
                        "(request last log term: %s, context last log term: %s, request last log index: %s, context last append: %s) " +
                        "voted for other in same term: %s " +
                        "(request term: %s, context term: %s, voted for another: %s)",
                shouldVoteFor,
                requesterLogUpToDate, requestLastLogTerm, contextLastLogTerm, requestLastLogIndex, contextLastAppended,
                votedForOtherInSameTerm, requestTerm, contextTerm, committedToVotingForAnother );

        return shouldVoteFor;
    }
}
