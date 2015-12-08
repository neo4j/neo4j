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

import org.neo4j.coreedge.raft.RaftMessageHandler;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.NewLeaderBarrier;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.logging.Log;

import static org.neo4j.coreedge.raft.MajorityIncludingSelfQuorum.isQuorum;
import static org.neo4j.coreedge.raft.roles.Role.CANDIDATE;
import static org.neo4j.coreedge.raft.roles.Role.FOLLOWER;
import static org.neo4j.coreedge.raft.roles.Role.LEADER;

public class Candidate implements RaftMessageHandler
{
    @Override
    public <MEMBER> Outcome<MEMBER> handle( RaftMessages.Message<MEMBER> message,
                                            ReadableRaftState<MEMBER> ctx, Log log ) throws RaftStorageException
    {
        Outcome<MEMBER> outcome = new Outcome<>( CANDIDATE, ctx );

        switch ( message.type() )
        {
            case HEARTBEAT:
            {
                RaftMessages.Heartbeat<MEMBER> req = (RaftMessages.Heartbeat<MEMBER>) message;

                if ( req.leaderTerm() < ctx.term() )
                {
                    break;
                }

                outcome.setNextRole( FOLLOWER );
                outcome.addOutgoingMessage( new RaftMessages.Directed<>( ctx.myself(), message ) );
                break;
            }

            case APPEND_ENTRIES_REQUEST:
            {
                RaftMessages.AppendEntries.Request<MEMBER> req = (RaftMessages.AppendEntries.Request<MEMBER>) message;

                if ( req.leaderTerm() < ctx.term() )
                {
                    RaftMessages.AppendEntries.Response<MEMBER> appendResponse =
                            new RaftMessages.AppendEntries.Response<>( ctx.myself(), ctx.term(), false,
                                    req.prevLogIndex(), ctx.entryLog().appendIndex() );

                    outcome.addOutgoingMessage( new RaftMessages.Directed<>( req.from(), appendResponse ) );
                    break;
                }

                outcome.setNextRole( FOLLOWER );
                outcome.addOutgoingMessage( new RaftMessages.Directed<>( ctx.myself(), req ) );
                break;
            }

            case VOTE_RESPONSE:
            {
                RaftMessages.Vote.Response<MEMBER> res = (RaftMessages.Vote.Response<MEMBER>) message;

                if ( res.term() > ctx.term() )
                {
                    outcome.setNextTerm( res.term() );
                    outcome.setNextRole( FOLLOWER );
                    break;
                }
                else if ( res.term() < ctx.term() || !res.voteGranted() )
                {
                    break;
                }

                if ( !res.from().equals( ctx.myself() ) )
                {
                    outcome.addVoteForMe( res.from() );
                }

                if ( isQuorum( ctx.votingMembers().size(), outcome.getVotesForMe().size() ) )
                {
                    log.info( "In term %d %s ELECTED AS LEADER voted for by %s%n",
                            ctx.term(), ctx.myself(), outcome.getVotesForMe() );

                    outcome.setLeader( ctx.myself() );
                    Leader.appendNewEntry( ctx, outcome, new NewLeaderBarrier() );

                    outcome.setLastLogIndexBeforeWeBecameLeader( ctx.entryLog().appendIndex() );
                    outcome.setNextRole( LEADER );
                }
                break;
            }

            case VOTE_REQUEST:
            {
                RaftMessages.Vote.Request<MEMBER> req = (RaftMessages.Vote.Request<MEMBER>) message;

                if ( req.term() > ctx.term() )
                {
                    outcome.setNextTerm( req.term() );
                    outcome.getVotesForMe().clear();

                    outcome.setNextRole( FOLLOWER );
                    outcome.addOutgoingMessage( new RaftMessages.Directed<>( ctx.myself(), req ) );
                    break;
                }

                outcome.addOutgoingMessage( new RaftMessages.Directed<>( req.from(), new RaftMessages.Vote.Response<>( ctx.myself(), outcome.getTerm(), false ) ) );
                break;
            }

            case ELECTION_TIMEOUT:
            {
                outcome.setNextRole( FOLLOWER );
                break;
            }
        }

        return outcome;
    }
}
