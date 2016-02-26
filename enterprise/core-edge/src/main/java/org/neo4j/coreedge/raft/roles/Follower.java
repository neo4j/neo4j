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
package org.neo4j.coreedge.raft.roles;

import org.neo4j.coreedge.raft.RaftMessageHandler;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.RaftMessages.AppendEntries;
import org.neo4j.coreedge.raft.RaftMessages.Heartbeat;
import org.neo4j.coreedge.raft.outcome.CommitCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.logging.Log;

import static java.lang.Long.min;
import static org.neo4j.coreedge.raft.roles.Role.CANDIDATE;
import static org.neo4j.coreedge.raft.roles.Role.FOLLOWER;

import java.io.IOException;

public class Follower implements RaftMessageHandler
{
    public static <MEMBER> boolean logHistoryMatches( ReadableRaftState<MEMBER> ctx, long prevLogIndex, long prevLogTerm )
            throws IOException
    {
        // NOTE: A previous log index of -1 means no history,
        //       so it always matches.

        // NOTE: The entry term for a non existing log index is defined as -1,
        //       so the history for a non existing log entry never matches.
        return prevLogIndex == -1 || ctx.entryLog().readEntryTerm( prevLogIndex ) == prevLogTerm;
    }

    public static <MEMBER> boolean commitToLogOnUpdate( ReadableRaftState<MEMBER> ctx, long indexOfLastNewEntry,
            long leaderCommit, Outcome<MEMBER> outcome )
    {
        long newCommitIndex = min( leaderCommit, indexOfLastNewEntry );

        if ( newCommitIndex > ctx.entryLog().commitIndex() )
        {
            outcome.addLogCommand( new CommitCommand( newCommitIndex ) );
            return true;
        }
        return false;
    }

    @Override
    public <MEMBER> Outcome<MEMBER> handle( RaftMessages.RaftMessage<MEMBER> message, ReadableRaftState<MEMBER> ctx, Log log )
            throws IOException
    {
        Outcome<MEMBER> outcome = new Outcome<>( FOLLOWER, ctx );

        switch ( message.type() )
        {
            case HEARTBEAT:
            {
                Heart.beat( ctx, outcome, (Heartbeat<MEMBER>) message );
                break;
            }

            case APPEND_ENTRIES_REQUEST:
            {
                Appending.handleAppendEntriesRequest( ctx, outcome, (AppendEntries.Request<MEMBER>) message );
                break;
            }

            case VOTE_REQUEST:
            {
                Voting.handleVoteRequest( ctx, outcome, (RaftMessages.Vote.Request<MEMBER>) message );
                break;
            }

            case ELECTION_TIMEOUT:
            {
                if ( Election.start( ctx, outcome ) )
                {
                    outcome.setNextRole( CANDIDATE );
                }
                break;
            }
        }

        return outcome;
    }

}
