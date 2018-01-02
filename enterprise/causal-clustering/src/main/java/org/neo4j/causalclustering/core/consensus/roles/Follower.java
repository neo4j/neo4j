/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.causalclustering.core.consensus.RaftMessageHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import org.neo4j.causalclustering.core.consensus.RaftMessages.Heartbeat;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.logging.Log;

import static java.lang.Long.min;
import static org.neo4j.causalclustering.core.consensus.MajorityIncludingSelfQuorum.isQuorum;
import static org.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;

class Follower implements RaftMessageHandler
{
    static boolean logHistoryMatches( ReadableRaftState ctx, long leaderSegmentPrevIndex, long leaderSegmentPrevTerm )
            throws IOException
    {
        // NOTE: A prevLogIndex before or at our log's prevIndex means that we
        //       already have all history (in a compacted form), so we report that history matches

        // NOTE: The entry term for a non existing log index is defined as -1,
        //       so the history for a non existing log entry never matches.

        long localLogPrevIndex = ctx.entryLog().prevIndex();
        long localSegmentPrevTerm = ctx.entryLog().readEntryTerm( leaderSegmentPrevIndex );

        return leaderSegmentPrevIndex > -1 &&
        (leaderSegmentPrevIndex <= localLogPrevIndex || localSegmentPrevTerm == leaderSegmentPrevTerm);
    }

    static void commitToLogOnUpdate( ReadableRaftState ctx, long indexOfLastNewEntry, long leaderCommit, Outcome outcome )
    {
        long newCommitIndex = min( leaderCommit, indexOfLastNewEntry );

        if ( newCommitIndex > ctx.commitIndex() )
        {
            outcome.setCommitIndex( newCommitIndex );
        }
    }

    private static void handleLeaderLogCompaction( ReadableRaftState ctx, Outcome outcome, RaftMessages.LogCompactionInfo compactionInfo )
    {
        if ( compactionInfo.leaderTerm() < ctx.term() )
        {
            return;
        }

        if ( ctx.entryLog().appendIndex() <= -1 || compactionInfo.prevIndex() > ctx.entryLog().appendIndex() )
        {
            outcome.markNeedForFreshSnapshot();
        }
    }

    @Override
    public Outcome handle( RaftMessages.RaftMessage message, ReadableRaftState ctx, Log log ) throws IOException
    {
        return message.dispatch( visitor( ctx, log ) );
    }

    private abstract static class Handler implements RaftMessages.Handler<Outcome, IOException>
    {
        protected final ReadableRaftState ctx;
        protected final Log log;
        protected final Outcome outcome;

        Handler( ReadableRaftState ctx, Log log )
        {
            this.ctx = ctx;
            this.log = log;
            this.outcome = new Outcome( FOLLOWER, ctx );
        }

        @Override
        public Outcome handle( Heartbeat heartbeat ) throws IOException
        {
            Heart.beat( ctx, outcome, heartbeat, log );
            return outcome;
        }

        @Override
        public Outcome handle( AppendEntries.Request request ) throws IOException
        {
            Appending.handleAppendEntriesRequest( ctx, outcome, request, log );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Vote.Request request ) throws IOException
        {
            Voting.handleVoteRequest( ctx, outcome, request, log );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.LogCompactionInfo logCompactionInfo ) throws IOException
        {
            handleLeaderLogCompaction( ctx, outcome, logCompactionInfo );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Vote.Response response ) throws IOException
        {
            log.info( "Late vote response: %s", response );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.PruneRequest pruneRequest ) throws IOException
        {
            Pruning.handlePruneRequest( outcome, pruneRequest );
            return outcome;
        }

        @Override
        public Outcome handle( AppendEntries.Response response ) throws IOException
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.HeartbeatResponse heartbeatResponse ) throws IOException
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Timeout.Heartbeat heartbeat ) throws IOException
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.NewEntry.Request request ) throws IOException
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.NewEntry.BatchRequest batchRequest ) throws IOException
        {
            return outcome;
        }
    }

    abstract class PreVoteSupportedHandler extends Handler
    {

        PreVoteSupportedHandler( ReadableRaftState ctx, Log log )
        {
            super( ctx, log );
        }

        @Override
        public Outcome handle( RaftMessages.Timeout.Election election ) throws IOException
        {
            log.info( "Election timeout triggered" );
            if ( Election.startPreElection( ctx, outcome, log ) )
            {
                outcome.setPreElection( true );
            }
            return outcome;
        }
    }

    class PreVoteActiveHandler extends PreVoteSupportedHandler
    {

        PreVoteActiveHandler( ReadableRaftState ctx, Log log )
        {
            super( ctx, log );
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Request request ) throws IOException
        {
            Voting.handlePreVoteRequest( ctx, outcome, request, log );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Response res ) throws IOException
        {
            if ( res.term() > ctx.term() )
            {
                outcome.setNextTerm( res.term() );
                log.info( "Aborting pre-election after receiving pre-vote response from %s at term %d (I am at %d)",
                        res.from(), res.term(), ctx.term() );
                return outcome;
            }
            else if ( res.term() < ctx.term() || !res.voteGranted() )
            {
                return outcome;
            }

            if ( !res.from().equals( ctx.myself() ) )
            {
                outcome.addPreVoteForMe( res.from() );
            }

            if ( isQuorum( ctx.votingMembers(), outcome.getPreVotesForMe() ) )
            {
                outcome.renewElectionTimeout();
                outcome.setPreElection( false );
                if ( Election.startRealElection( ctx, outcome, log ) )
                {
                    outcome.setNextRole( CANDIDATE );
                    log.info( "Moving to CANDIDATE state after successful pre-election stage" );
                }
            }
            return outcome;
        }
    }

    class PreVoteInactiveHandler extends PreVoteSupportedHandler
    {

        PreVoteInactiveHandler( ReadableRaftState ctx, Log log )
        {
            super( ctx, log );
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Response response ) throws IOException
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Request request ) throws IOException
        {
            outcome.addOutgoingMessage( new RaftMessages.Directed(
                    request.from(),
                    new RaftMessages.PreVote.Response( ctx.myself(), outcome.getTerm(), false )
            ) );
            return outcome;
        }
    }

    class PreVoteUnsupportedHandler extends Handler
    {

        PreVoteUnsupportedHandler( ReadableRaftState ctx, Log log )
        {
            super( ctx, log );
        }

        @Override
        public Outcome handle( RaftMessages.Timeout.Election election ) throws IOException
        {
            log.info( "Election timeout triggered" );
            if ( Election.startRealElection( ctx, outcome, log ) )
            {
                outcome.setNextRole( CANDIDATE );
                log.info( "Moving to CANDIDATE state after successfully starting election" );
            }
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Response response ) throws IOException
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Request request ) throws IOException
        {
            return outcome;
        }
    }

    private Handler visitor( ReadableRaftState ctx, Log log )
    {
        if ( ctx.supportPreVoting() )
        {
            if ( ctx.isPreElection() )
            {
                return new PreVoteActiveHandler( ctx, log );
            }
            else
            {
                return new PreVoteInactiveHandler( ctx, log );
            }
        }
        else
        {
            return new PreVoteUnsupportedHandler( ctx, log );
        }
    }
}
