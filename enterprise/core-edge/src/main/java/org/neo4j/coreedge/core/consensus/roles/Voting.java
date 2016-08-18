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
package org.neo4j.coreedge.core.consensus.roles;

import java.io.IOException;

import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.outcome.Outcome;
import org.neo4j.coreedge.core.consensus.state.ReadableRaftState;
import org.neo4j.coreedge.identity.MemberId;

public class Voting
{
    static  void handleVoteRequest( ReadableRaftState state, Outcome outcome,
            RaftMessages.Vote.Request voteRequest ) throws IOException
    {
        if ( voteRequest.term() > state.term() )
        {
            outcome.setNextTerm( voteRequest.term() );
            outcome.setVotedFor( null );
        }

        boolean willVoteForCandidate = shouldVoteFor( voteRequest.candidate(), outcome.getTerm(), voteRequest.term(),
                state.entryLog().readEntryTerm( state.entryLog().appendIndex() ), voteRequest.lastLogTerm(),
                state.entryLog().appendIndex(), voteRequest.lastLogIndex(),
                outcome.getVotedFor() );

        if ( willVoteForCandidate )
        {
            outcome.setVotedFor( voteRequest.from() );
            outcome.renewElectionTimeout();
        }

        outcome.addOutgoingMessage( new RaftMessages.Directed( voteRequest.from(), new RaftMessages.Vote.Response(
                state.myself(), outcome.getTerm(),
                willVoteForCandidate ) ) );
    }

    public static boolean shouldVoteFor( MemberId candidate, long contextTerm, long requestTerm,
                                         long contextLastLogTerm, long requestLastLogTerm,
                                         long contextLastAppended, long requestLastLogIndex,
                                         MemberId votedFor )
    {
        if ( requestTerm < contextTerm )
        {
            return false;
        }

        boolean requestLogEndsAtHigherTerm = requestLastLogTerm > contextLastLogTerm;
        boolean logsEndAtSameTerm = requestLastLogTerm == contextLastLogTerm;
        boolean requestLogAtLeastAsLongAsMyLog = requestLastLogIndex >= contextLastAppended;
        boolean requesterLogUpToDate = requestLogEndsAtHigherTerm ||
                (logsEndAtSameTerm && requestLogAtLeastAsLongAsMyLog);

        boolean votedForOtherInSameTerm = requestTerm == contextTerm &&
                votedFor != null && !votedFor.equals( candidate );

        return requesterLogUpToDate && !votedForOtherInSameTerm;
    }
}
