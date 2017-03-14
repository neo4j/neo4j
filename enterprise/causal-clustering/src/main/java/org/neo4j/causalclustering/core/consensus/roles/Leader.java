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
        Outcome outcome = new Outcome( LEADER, ctx );

        switch ( message.type() )
        {
        case HEARTBEAT:
        {
            Heartbeat req = (Heartbeat) message;

            if ( req.leaderTerm() < ctx.term() )
            {
                break;
            }

            stepDownToFollower( outcome );
            log.info( "Moving to FOLLOWER state after receiving heartbeat at term %d (my term is " + "%d) from %s",
                    req.leaderTerm(), ctx.term(), req.from() );
            Heart.beat( ctx, outcome, (Heartbeat) message, log );
            break;
        }

        case HEARTBEAT_TIMEOUT:
        {
            sendHeartbeats( ctx, outcome );
            break;
        }

        case HEARTBEAT_RESPONSE:
        {
            outcome.addHeartbeatResponse( message.from() );
            break;
        }

        case ELECTION_TIMEOUT:
        {
            if ( !isQuorum( ctx.votingMembers().size(), ctx.heartbeatResponses().size() ) )
            {
                stepDownToFollower( outcome );
                log.info( "Moving to FOLLOWER state after not receiving heartbeat responses in this election timeout " +
                        "period. Heartbeats received: %s", ctx.heartbeatResponses() );
            }

            outcome.getHeartbeatResponses().clear();
            break;
        }

        case APPEND_ENTRIES_REQUEST:
        {
            RaftMessages.AppendEntries.Request req = (RaftMessages.AppendEntries.Request) message;

            if ( req.leaderTerm() < ctx.term() )
            {
                RaftMessages.AppendEntries.Response appendResponse =
                        new RaftMessages.AppendEntries.Response( ctx.myself(), ctx.term(), false, -1,
                                ctx.entryLog().appendIndex() );

                outcome.addOutgoingMessage( new RaftMessages.Directed( req.from(), appendResponse ) );
                break;
            }
            else if ( req.leaderTerm() == ctx.term() )
            {
                throw new IllegalStateException( "Two leaders in the same term." );
            }
            else
            {
                // There is a new leader in a later term, we should revert to follower. (§5.1)
                stepDownToFollower( outcome );
                log.info( "Moving to FOLLOWER state after receiving append request at term %d (my term is " +
                        "%d) from %s", req.leaderTerm(), ctx.term(), req.from() );
                Appending.handleAppendEntriesRequest( ctx, outcome, req, log );
                break;
            }
        }

        case APPEND_ENTRIES_RESPONSE:
        {
            RaftMessages.AppendEntries.Response response = (RaftMessages.AppendEntries.Response) message;

            if ( response.term() < ctx.term() )
            {
                    /* Ignore responses from old terms! */
                break;
            }
            else if ( response.term() > ctx.term() )
            {
                outcome.setNextTerm( response.term() );
                stepDownToFollower( outcome );
                log.info( "Moving to FOLLOWER state after receiving append response at term %d (my term is " +
                        "%d) from %s", response.term(), ctx.term(), response.from() );
                outcome.replaceFollowerStates( new FollowerStates<>() );
                break;
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
            break;
        }

        case VOTE_REQUEST:
        {
            RaftMessages.Vote.Request req = (RaftMessages.Vote.Request) message;

            if ( req.term() > ctx.term() )
            {
                stepDownToFollower( outcome );
                log.info(
                        "Moving to FOLLOWER state after receiving vote request at term %d (my term is " + "%d) from %s",
                        req.term(), ctx.term(), req.from() );

                Voting.handleVoteRequest( ctx, outcome, req );
                break;
            }

            outcome.addOutgoingMessage( new RaftMessages.Directed( req.from(),
                    new RaftMessages.Vote.Response( ctx.myself(), ctx.term(), false ) ) );
            break;
        }

        case NEW_ENTRY_REQUEST:
        {
            RaftMessages.NewEntry.Request req = (RaftMessages.NewEntry.Request) message;
            ReplicatedContent content = req.content();
            Appending.appendNewEntry( ctx, outcome, content );
            break;
        }

        case NEW_BATCH_REQUEST:
        {
            RaftMessages.NewEntry.BatchRequest req = (RaftMessages.NewEntry.BatchRequest) message;
            List<ReplicatedContent> contents = req.contents();
            Appending.appendNewEntries( ctx, outcome, contents );
            break;
        }

        case PRUNE_REQUEST:
        {
            Pruning.handlePruneRequest( outcome, (RaftMessages.PruneRequest) message );
            break;
        }

        default:
            break;
        }

        return outcome;
    }

    private void stepDownToFollower( Outcome outcome )
    {
        outcome.steppingDown();
        outcome.setNextRole( FOLLOWER );
        outcome.setLeader( null );
    }
}
