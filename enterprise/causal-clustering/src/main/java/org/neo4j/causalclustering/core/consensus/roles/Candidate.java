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

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.RaftMessageHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.logging.Log;

import static org.neo4j.causalclustering.core.consensus.MajorityIncludingSelfQuorum.isQuorum;
import static org.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static org.neo4j.causalclustering.core.consensus.roles.Role.LEADER;

class Candidate implements RaftMessageHandler
{
    @Override
    public Outcome handle( RaftMessages.RaftMessage message, ReadableRaftState ctx, Log log ) throws IOException
    {
        return message.dispatch( new Handler( ctx, log ) );
    }

    private static class Handler implements RaftMessages.Handler<Outcome, IOException>
    {
        private final ReadableRaftState ctx;
        private final Log log;
        private final Outcome outcome;

        private Handler( ReadableRaftState ctx, Log log )
        {
            this.ctx = ctx;
            this.log = log;
            this.outcome = new Outcome( CANDIDATE, ctx );
        }

        @Override
        public Outcome handle( RaftMessages.Heartbeat req ) throws IOException
        {
            if ( req.leaderTerm() < ctx.term() )
            {
                return outcome;
            }

            outcome.setNextRole( FOLLOWER );
            log.info( "Moving to FOLLOWER state after receiving heartbeat from %s at term %d (I am at %d)",
                    req.from(), req.leaderTerm(), ctx.term() );
            Heart.beat( ctx, outcome, req, log );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.AppendEntries.Request req ) throws IOException
        {
            if ( req.leaderTerm() < ctx.term() )
            {
                RaftMessages.AppendEntries.Response appendResponse =
                        new RaftMessages.AppendEntries.Response( ctx.myself(), ctx.term(), false,
                                req.prevLogIndex(), ctx.entryLog().appendIndex() );

                outcome.addOutgoingMessage( new RaftMessages.Directed( req.from(), appendResponse ) );
                return outcome;
            }

            outcome.setNextRole( FOLLOWER );
            log.info( "Moving to FOLLOWER state after receiving append entries request from %s at term %d (I am at %d)n",
                    req.from(), req.leaderTerm(), ctx.term() );
            Appending.handleAppendEntriesRequest( ctx, outcome, req, log );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Vote.Response res ) throws IOException
        {
            if ( res.term() > ctx.term() )
            {
                outcome.setNextTerm( res.term() );
                outcome.setNextRole( FOLLOWER );
                log.info( "Moving to FOLLOWER state after receiving vote response from %s at term %d (I am at %d)",
                        res.from(), res.term(), ctx.term() );
                return outcome;
            }
            else if ( res.term() < ctx.term() || !res.voteGranted() )
            {
                return outcome;
            }

            if ( !res.from().equals( ctx.myself() ) )
            {
                outcome.addVoteForMe( res.from() );
            }

            if ( isQuorum( ctx.votingMembers(), outcome.getVotesForMe() ) )
            {
                outcome.setLeader( ctx.myself() );
                Appending.appendNewEntry( ctx, outcome, new NewLeaderBarrier() );
                Leader.sendHeartbeats( ctx, outcome );

                outcome.setLastLogIndexBeforeWeBecameLeader( ctx.entryLog().appendIndex() );
                outcome.electedLeader();
                outcome.renewElectionTimeout();
                outcome.setNextRole( LEADER );
                log.info( "Moving to LEADER state at term %d (I am %s), voted for by %s",
                        ctx.term(), ctx.myself(), outcome.getVotesForMe() );
            }
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Vote.Request req ) throws IOException
        {
            if ( req.term() > ctx.term() )
            {
                outcome.getVotesForMe().clear();
                outcome.setNextRole( FOLLOWER );
                log.info( "Moving to FOLLOWER state after receiving vote request from %s at term %d (I am at %d)",
                        req.from(), req.term(), ctx.term() );
                Voting.handleVoteRequest( ctx, outcome, req, log );
                return outcome;
            }

            outcome.addOutgoingMessage( new RaftMessages.Directed( req.from(),
                    new RaftMessages.Vote.Response( ctx.myself(), outcome.getTerm(), false ) ) );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Timeout.Election election ) throws IOException
        {
            log.info( "Failed to get elected. Got votes from: %s", ctx.votesForMe() );
            if ( !Election.startRealElection( ctx, outcome, log ) )
            {
                log.info( "Moving to FOLLOWER state after failing to start election" );
                outcome.setNextRole( FOLLOWER );
            }
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Request req ) throws IOException
        {
            if ( ctx.supportPreVoting() )
            {
                if ( req.term() > ctx.term() )
                {
                    outcome.getVotesForMe().clear();
                    outcome.setNextRole( FOLLOWER );
                    log.info( "Moving to FOLLOWER state after receiving pre vote request from %s at term %d (I am at %d)",
                            req.from(), req.term(), ctx.term() );
                }
                Voting.declinePreVoteRequest( ctx, outcome, req );
            }
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Response response )
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.AppendEntries.Response response )
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            return outcome;
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

        @Override
        public Outcome handle( RaftMessages.PruneRequest pruneRequest )
        {
            Pruning.handlePruneRequest( outcome, pruneRequest );
            return outcome;
        }
    }
}
