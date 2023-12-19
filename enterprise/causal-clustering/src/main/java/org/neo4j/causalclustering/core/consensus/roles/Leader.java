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
import java.util.List;

import org.neo4j.causalclustering.core.consensus.Followers;
import org.neo4j.causalclustering.core.consensus.RaftMessageHandler;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftMessages.Heartbeat;
import org.neo4j.causalclustering.core.consensus.RaftMessages.LogCompactionInfo;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.outcome.ShipCommand;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.logging.Log;

import static java.lang.Math.max;
import static org.neo4j.causalclustering.core.consensus.MajorityIncludingSelfQuorum.isQuorum;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static org.neo4j.causalclustering.core.consensus.roles.Role.LEADER;

public class Leader implements RaftMessageHandler
{
    private static Iterable<MemberId> replicationTargets( final ReadableRaftState ctx )
    {
        return new FilteringIterable<>( ctx.replicationMembers(), member -> !member.equals( ctx.myself() ) );
    }

    static void sendHeartbeats( ReadableRaftState ctx, Outcome outcome ) throws IOException
    {
        long commitIndex = ctx.commitIndex();
        long commitIndexTerm = ctx.entryLog().readEntryTerm( commitIndex );
        Heartbeat heartbeat = new Heartbeat( ctx.myself(), ctx.term(), commitIndex, commitIndexTerm );
        for ( MemberId to : replicationTargets( ctx ) )
        {
            outcome.addOutgoingMessage( new RaftMessages.Directed( to, heartbeat ) );
        }
    }

    @Override
    public Outcome handle( RaftMessages.RaftMessage message, ReadableRaftState ctx, Log log ) throws IOException
    {
        return message.dispatch( new Handler( ctx, log ) );
    }

    private static class Handler implements RaftMessages.Handler<Outcome,IOException>
    {
        private final ReadableRaftState ctx;
        private final Log log;
        private final Outcome outcome;

        Handler( ReadableRaftState ctx, Log log )
        {
            this.ctx = ctx;
            this.log = log;
            this.outcome = new Outcome( LEADER, ctx );
        }

        @Override
        public Outcome handle( Heartbeat heartbeat ) throws IOException
        {
            if ( heartbeat.leaderTerm() < ctx.term() )
            {
                return outcome;
            }

            stepDownToFollower( outcome, ctx );
            log.info( "Moving to FOLLOWER state after receiving heartbeat at term %d (my term is " + "%d) from %s",
                    heartbeat.leaderTerm(), ctx.term(), heartbeat.from() );
            Heart.beat( ctx, outcome, heartbeat, log );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Timeout.Heartbeat heartbeat ) throws IOException
        {
            sendHeartbeats( ctx, outcome );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            outcome.addHeartbeatResponse( heartbeatResponse.from() );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Timeout.Election election )
        {
            if ( !isQuorum( ctx.votingMembers().size(), ctx.heartbeatResponses().size() ) )
            {
                stepDownToFollower( outcome, ctx );
                log.info( "Moving to FOLLOWER state after not receiving heartbeat responses in this election timeout " +
                        "period. Heartbeats received: %s", ctx.heartbeatResponses() );
            }

            outcome.getHeartbeatResponses().clear();
            return outcome;

        }

        @Override
        public Outcome handle( RaftMessages.AppendEntries.Request req ) throws IOException
        {
            if ( req.leaderTerm() < ctx.term() )
            {
                RaftMessages.AppendEntries.Response appendResponse =
                        new RaftMessages.AppendEntries.Response( ctx.myself(), ctx.term(), false, -1,
                                ctx.entryLog().appendIndex() );

                outcome.addOutgoingMessage( new RaftMessages.Directed( req.from(), appendResponse ) );
                return outcome;
            }
            else if ( req.leaderTerm() == ctx.term() )
            {
                throw new IllegalStateException( "Two leaders in the same term." );
            }
            else
            {
                // There is a new leader in a later term, we should revert to follower. (§5.1)
                stepDownToFollower( outcome, ctx );
                log.info( "Moving to FOLLOWER state after receiving append request at term %d (my term is " +
                        "%d) from %s", req.leaderTerm(), ctx.term(), req.from() );
                Appending.handleAppendEntriesRequest( ctx, outcome, req, log );
                return outcome;
            }

        }

        @Override
        public Outcome handle( RaftMessages.AppendEntries.Response response ) throws IOException
        {
            if ( response.term() < ctx.term() )
            {
                    /* Ignore responses from old terms! */
                return outcome;
            }
            else if ( response.term() > ctx.term() )
            {
                outcome.setNextTerm( response.term() );
                stepDownToFollower( outcome, ctx );
                log.info( "Moving to FOLLOWER state after receiving append response at term %d (my term is " +
                        "%d) from %s", response.term(), ctx.term(), response.from() );
                outcome.replaceFollowerStates( new FollowerStates<>() );
                return outcome;
            }

            FollowerState follower = ctx.followerStates().get( response.from() );

            if ( response.success() )
            {
                assert response.matchIndex() <= ctx.entryLog().appendIndex();

                boolean followerProgressed = response.matchIndex() > follower.getMatchIndex();

                outcome.replaceFollowerStates( outcome.getFollowerStates()
                        .onSuccessResponse( response.from(), max( response.matchIndex(), follower.getMatchIndex() ) ) );

                outcome.addShipCommand( new ShipCommand.Match( response.matchIndex(), response.from() ) );

                    /*
                     * Matches from older terms can in complicated leadership change / log truncation scenarios
                     * be overwritten, even if they were replicated to a majority of instances. Thus we must only
                     * consider matches from this leader's term when figuring out which have been safely replicated
                     * and are ready for commit.
                     * This is explained nicely in Figure 3.7 of the thesis
                     */
                boolean matchInCurrentTerm = ctx.entryLog().readEntryTerm( response.matchIndex() ) == ctx.term();

                    /*
                     * The quorum situation may have changed only if the follower actually progressed.
                     */
                if ( followerProgressed && matchInCurrentTerm )
                {
                    // TODO: Test that mismatch between voting and participating members affects commit outcome

                    long quorumAppendIndex =
                            Followers.quorumAppendIndex( ctx.votingMembers(), outcome.getFollowerStates() );
                    if ( quorumAppendIndex > ctx.commitIndex() )
                    {
                        outcome.setLeaderCommit( quorumAppendIndex );
                        outcome.setCommitIndex( quorumAppendIndex );
                        outcome.addShipCommand( new ShipCommand.CommitUpdate() );
                    }
                }
            }
            else // Response indicated failure.
            {
                if ( response.appendIndex() > -1 && response.appendIndex() >= ctx.entryLog().prevIndex() )
                {
                    // Signal a mismatch to the log shipper, which will serve an earlier entry.
                    outcome.addShipCommand( new ShipCommand.Mismatch( response.appendIndex(), response.from() ) );
                }
                else
                {
                    // There are no earlier entries, message the follower that we have compacted so that
                    // it can take appropriate action.
                    LogCompactionInfo compactionInfo =
                            new LogCompactionInfo( ctx.myself(), ctx.term(), ctx.entryLog().prevIndex() );
                    RaftMessages.Directed directedCompactionInfo =
                            new RaftMessages.Directed( response.from(), compactionInfo );

                    outcome.addOutgoingMessage( directedCompactionInfo );
                }
            }
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Vote.Request req ) throws IOException
        {
            if ( req.term() > ctx.term() )
            {
                stepDownToFollower( outcome, ctx );
                log.info(
                        "Moving to FOLLOWER state after receiving vote request at term %d (my term is " + "%d) from %s",
                        req.term(), ctx.term(), req.from() );

                Voting.handleVoteRequest( ctx, outcome, req, log );
                return outcome;
            }

            outcome.addOutgoingMessage( new RaftMessages.Directed( req.from(),
                    new RaftMessages.Vote.Response( ctx.myself(), ctx.term(), false ) ) );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.NewEntry.Request req ) throws IOException
        {
            ReplicatedContent content = req.content();
            Appending.appendNewEntry( ctx, outcome, content );
            return outcome;

        }

        @Override
        public Outcome handle( RaftMessages.NewEntry.BatchRequest req ) throws IOException
        {
            List<ReplicatedContent> contents = req.contents();
            Appending.appendNewEntries( ctx, outcome, contents );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.PruneRequest pruneRequest )
        {
            Pruning.handlePruneRequest( outcome, pruneRequest );
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.Vote.Response response )
        {
            return outcome;
        }

        @Override
        public Outcome handle( RaftMessages.PreVote.Request req ) throws IOException
        {
            if ( ctx.supportPreVoting() )
            {
                if ( req.term() > ctx.term() )
                {
                    stepDownToFollower( outcome, ctx );
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
        public Outcome handle( LogCompactionInfo logCompactionInfo )
        {
            return outcome;
        }

        private void stepDownToFollower( Outcome outcome, ReadableRaftState raftState )
        {
            outcome.steppingDown( raftState.term() );
            outcome.setNextRole( FOLLOWER );
            outcome.setLeader( null );
        }
    }
}
