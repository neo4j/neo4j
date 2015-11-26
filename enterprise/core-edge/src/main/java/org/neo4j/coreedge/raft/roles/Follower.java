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

import java.util.Set;

import org.neo4j.coreedge.raft.RaftMessageHandler;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.RaftMessages.AppendEntries;
import org.neo4j.coreedge.raft.RaftMessages.AppendEntries.Response;
import org.neo4j.coreedge.raft.RaftMessages.Heartbeat;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.outcome.BatchAppendLogEntries;
import org.neo4j.coreedge.raft.outcome.CommitCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.outcome.TruncateLogCommand;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.logging.Log;

import static java.lang.Long.min;
import static org.neo4j.coreedge.raft.Ballot.shouldVoteFor;
import static org.neo4j.coreedge.raft.roles.Role.CANDIDATE;
import static org.neo4j.coreedge.raft.roles.Role.FOLLOWER;

public class Follower implements RaftMessageHandler
{
    private static <MEMBER> boolean logHistoryMatches( ReadableRaftState<MEMBER> ctx, AppendEntries
            .Request<MEMBER> req ) throws RaftStorageException
    {
        // note that the entry term for a non existing log index is defined as -1.
        // Therefore, the history for a non existing log entry never matches.
        return req.prevLogIndex() == -1 || ctx.entryLog().readEntryTerm( req.prevLogIndex() ) == req.prevLogTerm();
    }

    private static <MEMBER> boolean logHistoryMatches( ReadableRaftState<MEMBER> ctx, Heartbeat<MEMBER> req )
            throws RaftStorageException
    {
        return req.commitIndex() == -1 || ctx.entryLog().readEntryTerm( req.commitIndex() ) == req.commitIndexTerm();
    }

    private static <MEMBER> boolean generateCommitIfNecessary( ReadableRaftState<MEMBER> ctx, AppendEntries
            .Request<MEMBER> req, Outcome<MEMBER> outcome )
    {
        long localCommit = ctx.entryLog().commitIndex();
        long newCommitIndex = min( req.leaderCommit(), req.prevLogIndex() + req.entries().length );
        if ( newCommitIndex > localCommit )
        {
            outcome.addLogCommand( new CommitCommand( newCommitIndex ) );
            return true;
        }
        return false;
    }

    @Override
    public <MEMBER> Outcome<MEMBER> handle( RaftMessages.Message<MEMBER> message, ReadableRaftState<MEMBER> ctx, Log log )
            throws RaftStorageException
    {
        Outcome<MEMBER> outcome = new Outcome<>( FOLLOWER, ctx );

        switch ( message.type() )
        {
            case HEARTBEAT:
            {
                Heartbeat<MEMBER> req = (Heartbeat<MEMBER>) message;

                if ( req.leaderTerm() < ctx.term() )
                {
                    break;
                }

                if ( logHistoryMatches( ctx, req ) )
                {
                    outcome.addLogCommand( new CommitCommand( req.commitIndex() ) );
                }

                outcome.renewElectionTimeout();
                break;
            }

            case APPEND_ENTRIES_REQUEST:
            {
                AppendEntries.Request<MEMBER> req = (AppendEntries.Request<MEMBER>) message;

                if ( req.leaderTerm() < ctx.term() )
                {
                    Response<MEMBER> appendResponse =
                            new Response<>( ctx.myself(), ctx.term(), false, req.prevLogIndex() );

                    outcome.addOutgoingMessage( new RaftMessages.Directed<>( req.from(), appendResponse ) );
                    break;
                }

                outcome.renewElectionTimeout();
                outcome.setNextTerm( req.leaderTerm() );
                outcome.setLeader( req.from() );
                outcome.setLeaderCommit( req.leaderCommit() );

                if ( !logHistoryMatches( ctx, req ) )
                {
                    assert req.prevLogIndex() > -1 && req.prevLogTerm() > -1;
                    Response<MEMBER> appendResponse = new Response<>( ctx.myself(), req.leaderTerm(), false, -1 );
                    outcome.addOutgoingMessage( new RaftMessages.Directed<>( req.from(), appendResponse ) );
                    break;
                }

                long baseIndex = req.prevLogIndex() + 1;
                int offset;

                /* Find possible truncation point. */
                for ( offset = 0; offset < req.entries().length; offset++ )
                {
                    long logTerm = ctx.entryLog().readEntryTerm( baseIndex + offset );

                    if( baseIndex + offset > ctx.entryLog().appendIndex() )
                    {
                        /* entry doesn't exist */
                        break;
                    }
                    else if ( logTerm != req.entries()[offset].term() )
                    {
                        outcome.addLogCommand( new TruncateLogCommand( baseIndex + offset ) );
                        break;
                    }
                }

                if( offset < req.entries().length )
                {
                    outcome.addLogCommand( new BatchAppendLogEntries( baseIndex, offset, req.entries() ) );
                }
                generateCommitIfNecessary( ctx, req, outcome );

                long endMatchIndex = req.prevLogIndex() + req.entries().length; // this is the index of the last incoming entry
                if ( endMatchIndex >= 0 )
                {
                    Response<MEMBER> appendResponse = new Response<>( ctx.myself(), req.leaderTerm(), true, endMatchIndex );
                    outcome.addOutgoingMessage( new RaftMessages.Directed<>( req.from(), appendResponse ) );
                }
                break;
            }

            case VOTE_REQUEST:
            {
                RaftMessages.Vote.Request<MEMBER> req = (RaftMessages.Vote.Request<MEMBER>) message;

                if ( req.term() > ctx.term() )
                {
                    outcome.setNextTerm( req.term() );
                    outcome.setVotedFor( null );
                }

                boolean willVoteForCandidate = shouldVoteFor( req.candidate(), req.term(), outcome.term, ctx
                                .entryLog().appendIndex(),
                        req.lastLogIndex(), ctx.entryLog().readEntryTerm( ctx.entryLog().appendIndex() ),
                        req.lastLogTerm(), outcome.votedFor );

                if ( willVoteForCandidate )
                {
                    outcome.setVotedFor( req.from() );
                    outcome.renewElectionTimeout();
                }

                outcome.addOutgoingMessage( new RaftMessages.Directed<>( req.from(), new RaftMessages.Vote.Response<>(
                        ctx.myself(), outcome.term,
                        willVoteForCandidate ) ) );
                break;
            }

            case ELECTION_TIMEOUT:
            {
                Set<MEMBER> currentMembers = ctx.votingMembers();
                if ( currentMembers == null || !currentMembers.contains( ctx.myself() ) )
                {
                    break;
                }

                outcome.setNextTerm( ctx.term() + 1 );

                RaftMessages.Vote.Request<MEMBER> voteForMe =
                        new RaftMessages.Vote.Request<>( ctx.myself(), outcome.term, ctx.myself(), ctx.entryLog()
                                .appendIndex(), ctx.entryLog().readEntryTerm( ctx.entryLog().appendIndex() ) );

                for ( MEMBER member : currentMembers )
                {
                    if ( !member.equals( ctx.myself() ) )
                    {
                        outcome.addOutgoingMessage( new RaftMessages.Directed<>( member, voteForMe ) );
                    }
                }

                outcome.setVotedFor( ctx.myself() );
                outcome.setNextRole( CANDIDATE );
                break;
            }
        }

        return outcome;
    }
}
