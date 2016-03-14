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

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.RaftLogCompactedException;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.outcome.AppendLogEntry;
import org.neo4j.coreedge.raft.outcome.BatchAppendLogEntries;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.outcome.ShipCommand;
import org.neo4j.coreedge.raft.outcome.TruncateLogCommand;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ReadableRaftState;

public class Appending
{
    public static <MEMBER> void handleAppendEntriesRequest(
            ReadableRaftState<MEMBER> state, Outcome<MEMBER> outcome, RaftMessages.AppendEntries.Request<MEMBER> request )
            throws IOException, RaftLogCompactedException
    {
        if ( request.leaderTerm() < state.term() )
        {
            RaftMessages.AppendEntries.Response<MEMBER> appendResponse = new RaftMessages.AppendEntries.Response<>(
                    state.myself(), state.term(), false, -1, state.entryLog().appendIndex() );

            outcome.addOutgoingMessage( new RaftMessages.Directed<>( request.from(), appendResponse ) );
            return;
        }

        outcome.renewElectionTimeout();
        outcome.setNextTerm( request.leaderTerm() );
        outcome.setLeader( request.from() );
        outcome.setLeaderCommit( request.leaderCommit() );

        if ( !Follower.logHistoryMatches( state, request.prevLogIndex(), request.prevLogTerm() ) )
        {
            assert request.prevLogIndex() > -1 && request.prevLogTerm() > -1;
            RaftMessages.AppendEntries.Response<MEMBER> appendResponse = new RaftMessages.AppendEntries.Response<>(
                    state.myself(), request.leaderTerm(), false, -1, state.entryLog().appendIndex() );

            outcome.addOutgoingMessage( new RaftMessages.Directed<>( request.from(), appendResponse ) );
            return;
        }

        long baseIndex = request.prevLogIndex() + 1;
        int offset;

        /* Find possible truncation point. */
        for ( offset = 0; offset < request.entries().length; offset++ )
        {
            long logTerm = state.entryLog().readEntryTerm( baseIndex + offset );

            if( baseIndex + offset > state.entryLog().appendIndex() )
            {
                /* entry doesn't exist */
                break;
            }
            else if ( logTerm != request.entries()[offset].term() )
            {
                outcome.addLogCommand( new TruncateLogCommand( baseIndex + offset ) );
                break;
            }
        }

        if( offset < request.entries().length )
        {
            outcome.addLogCommand( new BatchAppendLogEntries( baseIndex, offset, request.entries() ) );
        }

        Follower.commitToLogOnUpdate(
                state, request.prevLogIndex() + request.entries().length, request.leaderCommit(), outcome );

        long endMatchIndex = request.prevLogIndex() + request.entries().length; // this is the index of the last incoming entry
        if ( endMatchIndex >= 0 )
        {
            RaftMessages.AppendEntries.Response<MEMBER> appendResponse = new RaftMessages.AppendEntries.Response<>( state.myself(), request.leaderTerm(), true, endMatchIndex, endMatchIndex );
            outcome.addOutgoingMessage( new RaftMessages.Directed<>( request.from(), appendResponse ) );
        }
    }

    public static <MEMBER> void appendNewEntry( ReadableRaftState<MEMBER> ctx, Outcome<MEMBER> outcome, ReplicatedContent
            content ) throws IOException, RaftLogCompactedException
    {
        long prevLogIndex = ctx.entryLog().appendIndex();
        long prevLogTerm = prevLogIndex == -1 ? -1 :
                prevLogIndex > ctx.lastLogIndexBeforeWeBecameLeader() ?
                        ctx.term() :
                        ctx.entryLog().readEntryTerm( prevLogIndex );

        RaftLogEntry newLogEntry = new RaftLogEntry( ctx.term(), content );

        outcome.addShipCommand( new ShipCommand.NewEntry( prevLogIndex, prevLogTerm, newLogEntry ) );
        outcome.addLogCommand( new AppendLogEntry( prevLogIndex + 1, newLogEntry ) );
    }
}
