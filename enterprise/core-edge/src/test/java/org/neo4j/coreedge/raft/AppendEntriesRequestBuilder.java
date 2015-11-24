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
package org.neo4j.coreedge.raft;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.neo4j.coreedge.raft.log.RaftLogEntry;

public class AppendEntriesRequestBuilder<MEMBER>
{
    private List<RaftLogEntry> logEntries = new LinkedList<>();
    private long leaderCommit = -1;
    private long prevLogTerm = -1;
    private long prevLogIndex = -1;
    private MEMBER leader = null;
    private long leaderTerm = -1;
    private MEMBER from = null;
    private UUID correlationId = new UUID( 0, 0 );

    public RaftMessages.AppendEntries.Request<MEMBER> build()
    {
        return new RaftMessages.AppendEntries.Request<>( from, leaderTerm, prevLogIndex, prevLogTerm,
                logEntries.toArray( new RaftLogEntry[logEntries.size()] ), leaderCommit );
    }

    public AppendEntriesRequestBuilder<MEMBER> from( MEMBER from )
    {
        this.from = from;
        return this;
    }

    public AppendEntriesRequestBuilder<MEMBER> leaderTerm( long leaderTerm )
    {
        this.leaderTerm = leaderTerm;
        return this;
    }

    public AppendEntriesRequestBuilder<MEMBER> prevLogIndex( long prevLogIndex )
    {
        this.prevLogIndex = prevLogIndex;
        return this;
    }

    public AppendEntriesRequestBuilder<MEMBER> prevLogTerm( long prevLogTerm )
    {
        this.prevLogTerm = prevLogTerm;
        return this;
    }

    public AppendEntriesRequestBuilder<MEMBER> logEntry( RaftLogEntry logEntry )
    {
        logEntries.add( logEntry );
        return this;
    }

    public AppendEntriesRequestBuilder<MEMBER> leaderCommit( long leaderCommit )
    {
        this.leaderCommit = leaderCommit;
        return this;
    }

    public AppendEntriesRequestBuilder<MEMBER> leader( MEMBER leader )
    {
        this.leader = leader;
        return this;
    }

    public AppendEntriesRequestBuilder<MEMBER> correlationId( UUID correlationId )
    {
        this.correlationId = correlationId;
        return this;
    }
}
