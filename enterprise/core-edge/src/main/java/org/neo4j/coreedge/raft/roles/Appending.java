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
import java.util.List;

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.outcome.AppendLogEntry;
import org.neo4j.coreedge.raft.outcome.BatchAppendLogEntries;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.outcome.ShipCommand;
import org.neo4j.coreedge.raft.outcome.TruncateLogCommand;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.kernel.impl.store.StoreId;

public class Appending
{
    public static <MEMBER> void handleAppendEntriesRequest(
            ReadableRaftState<MEMBER> state, Outcome<MEMBER> outcome, RaftMessages.AppendEntries.Request<MEMBER> request, StoreId localStoreId )
            throws IOException
    {
        if ( request.leaderTerm() < state.term() )
        {
            RaftMessages.AppendEntries.Response<MEMBER> appendResponse = new RaftMessages.AppendEntries.Response<>(
                    state.myself(), state.term(), false, -1, state.entryLog().appendIndex(),localStoreId );

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
                    state.myself(), request.leaderTerm(), false, -1, state.entryLog().appendIndex(), localStoreId );

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
                // entry doesn't exist because it's beyond the current log end, so we can go ahead and append
                break;
            }
            else if ( baseIndex + offset < state.entryLog().prevIndex() )
            {
                // entry doesn't exist because it's before the earliest known entry, so continue with the next one
                continue;
            }
            else if ( logTerm != request.entries()[offset].term() )
            {
                /*
                 * the entry's index falls within our current range and the term doesn't match what we know. We must
                 * truncate.
                 */
                if ( baseIndex + offset <= state.commitIndex() ) // first, assert that we haven't committed what we are about to truncate
                {
                    throw new IllegalStateException( "Cannot truncate at index " + (baseIndex + offset) + " when commit index is at " + state.commitIndex() );
                }
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
            RaftMessages.AppendEntries.Response<MEMBER> appendResponse = new RaftMessages.AppendEntries.Response<>(
                    state.myself(), request.leaderTerm(), true, endMatchIndex, endMatchIndex, localStoreId );
            outcome.addOutgoingMessage( new RaftMessages.Directed<>( request.from(), appendResponse ) );
        }
    }

    public static <MEMBER> void appendNewEntry( ReadableRaftState<MEMBER> ctx, Outcome<MEMBER> outcome, ReplicatedContent
            content ) throws IOException
    {
        long prevLogIndex = ctx.entryLog().appendIndex();
        long prevLogTerm = prevLogIndex == -1 ? -1 :
                prevLogIndex > ctx.lastLogIndexBeforeWeBecameLeader() ?
                        ctx.term() :
                        ctx.entryLog().readEntryTerm( prevLogIndex );

        RaftLogEntry newLogEntry = new RaftLogEntry( ctx.term(), content );

        outcome.addShipCommand( new ShipCommand.NewEntries( prevLogIndex, prevLogTerm, new RaftLogEntry[]{ newLogEntry } ) );
        outcome.addLogCommand( new AppendLogEntry( prevLogIndex + 1, newLogEntry ) );
    }

    public static <MEMBER> void appendNewEntries( ReadableRaftState<MEMBER> ctx, Outcome<MEMBER> outcome,
            List<ReplicatedContent> contents ) throws IOException
    {
        long prevLogIndex = ctx.entryLog().appendIndex();
        long prevLogTerm = prevLogIndex == -1 ? -1 :
                prevLogIndex > ctx.lastLogIndexBeforeWeBecameLeader() ?
                        ctx.term() :
                        ctx.entryLog().readEntryTerm( prevLogIndex );

        RaftLogEntry[] raftLogEntries = contents.stream().map( content -> new RaftLogEntry( ctx.term(), content ) )
                .toArray( RaftLogEntry[]::new );

        outcome.addShipCommand( new ShipCommand.NewEntries( prevLogIndex, prevLogTerm, raftLogEntries ) );
        outcome.addLogCommand( new BatchAppendLogEntries( prevLogIndex + 1, 0, raftLogEntries ) );
    }
}
