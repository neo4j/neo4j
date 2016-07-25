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

import java.io.IOException;

import org.neo4j.coreedge.raft.RaftMessageHandler;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.RaftMessages.AppendEntries;
import org.neo4j.coreedge.raft.RaftMessages.Heartbeat;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.logging.Log;

import static java.lang.Long.min;
import static org.neo4j.coreedge.raft.roles.Role.CANDIDATE;
import static org.neo4j.coreedge.raft.roles.Role.FOLLOWER;

class Follower implements RaftMessageHandler
{
    static boolean logHistoryMatches( ReadableRaftState ctx, long prevLogIndex, long prevLogTerm, Log log )
            throws IOException
    {
        // NOTE: A prevLogIndex before or at our log's prevIndex means that we
        //       already have all history (in a compacted form), so we report that history matches

        // NOTE: The entry term for a non existing log index is defined as -1,
        //       so the history for a non existing log entry never matches.

        long logPrevIndex = ctx.entryLog().prevIndex();
        long logPrevTerm = ctx.entryLog().readEntryTerm( prevLogIndex );

        boolean logHistoryMatches = prevLogIndex <= logPrevIndex || logPrevTerm == prevLogTerm;

        if ( !logHistoryMatches )
        {
            log.info( "Log history mismatch: index:[%s, %s], term:[%s, %s]",
                    logPrevIndex, prevLogIndex, logPrevTerm, prevLogTerm );
        }

        return logHistoryMatches;
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

        if ( compactionInfo.prevIndex() > ctx.entryLog().appendIndex() )
        {
            outcome.markNeedForFreshSnapshot();
        }
    }

    @Override
    public Outcome handle( RaftMessages.RaftMessage message, ReadableRaftState ctx, Log log ) throws IOException
    {
        Outcome outcome = new Outcome( FOLLOWER, ctx );

        switch ( message.type() )
        {
            case HEARTBEAT:
            {
                Heart.beat( ctx, outcome, (Heartbeat) message, log );
                break;
            }

            case APPEND_ENTRIES_REQUEST:
            {
                Appending.handleAppendEntriesRequest( ctx, outcome, (AppendEntries.Request) message, log );
                break;
            }

            case VOTE_REQUEST:
            {
                Voting.handleVoteRequest( ctx, outcome, (RaftMessages.Vote.Request) message );
                break;
            }

            case LOG_COMPACTION_INFO:
            {
                handleLeaderLogCompaction( ctx, outcome, (RaftMessages.LogCompactionInfo) message );
                break;
            }

            case ELECTION_TIMEOUT:
            {
                if ( Election.start( ctx, outcome, log ) )
                {
                    outcome.setNextRole( CANDIDATE );
                    log.info( "Moving to CANDIDATE state after successfully starting election" );
                }
                break;
            }

            default:
                break;
        }

        return outcome;
    }

}
