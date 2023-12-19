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

import java.io.IOException;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.RaftMessageHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import org.neo4j.causalclustering.core.consensus.RaftMessages.Heartbeat;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;

import static java.lang.Long.min;
import static org.neo4j.causalclustering.core.consensus.MajorityIncludingSelfQuorum.isQuorum;
import static org.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;

class Follower implements RaftMessageHandler
{
    static boolean logHistoryMatches( ReadableRaftState ctx, long leaderSegmentPrevIndex, long leaderSegmentPrevTerm ) throws IOException
    {
        // NOTE: A prevLogIndex before or at our log's prevIndex means that we
        //       already have all history (in a compacted form), so we report that history matches

        // NOTE: The entry term for a non existing log index is defined as -1,
        //       so the history for a non existing log entry never matches.

        long localLogPrevIndex = ctx.entryLog().prevIndex();
        long localSegmentPrevTerm = ctx.entryLog().readEntryTerm( leaderSegmentPrevIndex );

        return leaderSegmentPrevIndex > -1 && (leaderSegmentPrevIndex <= localLogPrevIndex || localSegmentPrevTerm == leaderSegmentPrevTerm);
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

    private static class Handler implements RaftMessages.Handler<Outcome,IOException>
    {
        protected final ReadableRaftState ctx;
        protected final Log log;
        protected final Outcome outcome;
        private final PreVoteRequestHandler preVoteRequestHandler;
        private final PreVoteResponseHandler preVoteResponseHandler;
        private final ElectionTimeoutHandler electionTimeoutHandler;

        Handler( PreVoteRequestHandler preVoteRequestHandler, PreVoteResponseHandler preVoteResponseHandler,
                ElectionTimeoutHandler electionTimeoutHandler, ReadableRaftState ctx, Log log )
        {
            this.ctx = ctx;
            this.log = log;
            this.outcome = new Outcome( FOLLOWER, ctx );
            this.preVoteRequestHandler = preVoteRequestHandler;
            this.preVoteResponseHandler = preVoteResponseHandler;
            this.electionTimeoutHandler = electionTimeoutHandler;
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
        public Outcome handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            handleLeaderLogCompaction( ctx, outcome, logCompactionInfo );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Vote.Response response )
        {
            log.info( "Late vote response: %s", response );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Request request ) throws IOException
        {
            return preVoteRequestHandler.handle( request, outcome, ctx, log );
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Response response ) throws IOException
        {
            return preVoteResponseHandler.handle( response, outcome, ctx, log );
        }

        @Override
        public Outcome handle( RaftMessages.PruneRequest pruneRequest )
        {
            Pruning.handlePruneRequest( outcome, pruneRequest );
            return outcome;
        }

        @Override
        public Outcome handle( AppendEntries.Response response )
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Timeout.Election election ) throws IOException
        {
            return electionTimeoutHandler.handle( election, outcome, ctx, log );
        }

        @Override
        public Outcome handle( RaftMessages.Timeout.Heartbeat heartbeat )
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.NewEntry.Request request )
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.NewEntry.BatchRequest batchRequest )
        {
            return outcome;
        }
    }

    private interface ElectionTimeoutHandler
    {
        Outcome handle( RaftMessages.Timeout.Election election, Outcome outcome, ReadableRaftState ctx, Log log ) throws IOException;
    }

    private interface PreVoteRequestHandler
    {
        Outcome handle( RaftMessages.PreVote.Request request, Outcome outcome, ReadableRaftState ctx, Log log ) throws IOException;

    }
    private interface PreVoteResponseHandler
    {
        Outcome handle( RaftMessages.PreVote.Response response, Outcome outcome, ReadableRaftState ctx, Log log ) throws IOException;
    }

    private static class PreVoteSupportedHandler implements ElectionTimeoutHandler
    {
        public Outcome handle( RaftMessages.Timeout.Election election, Outcome outcome, ReadableRaftState ctx, Log log ) throws IOException
        {
            log.info( "Election timeout triggered" );
            if ( Election.startPreElection( ctx, outcome, log ) )
            {
                outcome.setPreElection( true );
            }
            return outcome;
        }

        private static ElectionTimeoutHandler instance = new PreVoteSupportedHandler();
    }

    private static class PreVoteUnsupportedHandler implements ElectionTimeoutHandler
    {
        @Override
        public Outcome handle( RaftMessages.Timeout.Election election, Outcome outcome, ReadableRaftState ctx, Log log ) throws IOException
        {
            log.info( "Election timeout triggered" );
            if ( Election.startRealElection( ctx, outcome, log ) )
            {
                outcome.setNextRole( CANDIDATE );
                log.info( "Moving to CANDIDATE state after successfully starting election" );
            }
            return outcome;
        }

        private static ElectionTimeoutHandler instance = new PreVoteUnsupportedHandler();
    }

    private static class PreVoteUnsupportedRefusesToLead implements ElectionTimeoutHandler
    {
        @Override
        public Outcome handle( RaftMessages.Timeout.Election election, Outcome outcome, ReadableRaftState ctx, Log log )
        {
            log.info( "Election timeout triggered but refusing to be leader" );
            return outcome;
        }

        private static ElectionTimeoutHandler instance = new PreVoteUnsupportedRefusesToLead();
    }

    private static class PreVoteSupportedRefusesToLeadHandler implements ElectionTimeoutHandler
    {
        @Override
        public Outcome handle( RaftMessages.Timeout.Election election, Outcome outcome, ReadableRaftState ctx, Log log )
        {
            log.info( "Election timeout triggered but refusing to be leader" );
            Set<MemberId> memberIds = ctx.votingMembers();
            if ( memberIds != null && memberIds.contains( ctx.myself() ) )
            {
                outcome.setPreElection( true );
            }
            return outcome;
        }

        private static ElectionTimeoutHandler instance = new PreVoteSupportedRefusesToLeadHandler();
    }

    private static class PreVoteRequestVotingHandler implements PreVoteRequestHandler
    {
        @Override
        public Outcome handle( RaftMessages.PreVote.Request request, Outcome outcome, ReadableRaftState ctx, Log log ) throws IOException
        {
            Voting.handlePreVoteRequest( ctx, outcome, request, log );
            return outcome;
        }

        private static PreVoteRequestHandler instance = new PreVoteRequestVotingHandler();
    }

    private static class PreVoteRequestDecliningHandler implements PreVoteRequestHandler
    {
        @Override
        public Outcome handle( RaftMessages.PreVote.Request request, Outcome outcome, ReadableRaftState ctx, Log log ) throws IOException
        {
            Voting.declinePreVoteRequest( ctx, outcome, request );
            return outcome;
        }

        private static PreVoteRequestHandler instance = new PreVoteRequestDecliningHandler();
    }

    private static class PreVoteRequestNoOpHandler implements PreVoteRequestHandler
    {
        @Override
        public Outcome handle( RaftMessages.PreVote.Request request, Outcome outcome, ReadableRaftState ctx, Log log )
        {
            return outcome;
        }

        private static PreVoteRequestHandler instance = new PreVoteRequestNoOpHandler();
    }

    private static class PreVoteResponseSolicitingHandler implements PreVoteResponseHandler
    {
        @Override
        public Outcome handle( RaftMessages.PreVote.Response res, Outcome outcome, ReadableRaftState ctx, Log log ) throws IOException
        {
            if ( res.term() > ctx.term() )
            {
                outcome.setNextTerm( res.term() );
                outcome.setPreElection( false );
                log.info( "Aborting pre-election after receiving pre-vote response from %s at term %d (I am at %d)", res.from(), res.term(), ctx.term() );
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
        private static PreVoteResponseHandler instance = new PreVoteResponseSolicitingHandler();
    }

    private static class PreVoteResponseNoOpHandler implements PreVoteResponseHandler
    {
        @Override
        public Outcome handle( RaftMessages.PreVote.Response response, Outcome outcome, ReadableRaftState ctx, Log log )
        {
            return outcome;
        }

        private static PreVoteResponseHandler instance = new PreVoteResponseNoOpHandler();
    }

    private Handler visitor( ReadableRaftState ctx, Log log )
    {
        final ElectionTimeoutHandler electionTimeoutHandler;
        final PreVoteRequestHandler preVoteRequestHandler;
        final PreVoteResponseHandler preVoteResponseHandler;

        if ( ctx.refusesToBeLeader() )
        {
            preVoteResponseHandler = PreVoteResponseNoOpHandler.instance;
            if ( ctx.supportPreVoting() )
            {
                electionTimeoutHandler = PreVoteSupportedRefusesToLeadHandler.instance;
                if ( ctx.isPreElection() )
                {
                    preVoteRequestHandler = PreVoteRequestVotingHandler.instance;
                }
                else
                {
                    preVoteRequestHandler = PreVoteRequestDecliningHandler.instance;
                }
            }
            else
            {
                preVoteRequestHandler = PreVoteRequestNoOpHandler.instance;
                electionTimeoutHandler = PreVoteUnsupportedRefusesToLead.instance;
            }
        }
        else
        {
            if ( ctx.supportPreVoting() )
            {
                electionTimeoutHandler = PreVoteSupportedHandler.instance;
                if ( ctx.isPreElection() )
                {
                    preVoteRequestHandler = PreVoteRequestVotingHandler.instance;
                    preVoteResponseHandler = PreVoteResponseSolicitingHandler.instance;
                }
                else
                {
                    preVoteRequestHandler = PreVoteRequestDecliningHandler.instance;
                    preVoteResponseHandler = PreVoteResponseNoOpHandler.instance;
                }
            }
            else
            {
                preVoteRequestHandler = PreVoteRequestNoOpHandler.instance;
                preVoteResponseHandler = PreVoteResponseNoOpHandler.instance;
                electionTimeoutHandler = PreVoteUnsupportedHandler.instance;
            }
        }
        return new Handler( preVoteRequestHandler, preVoteResponseHandler, electionTimeoutHandler, ctx, log );
    }
}
