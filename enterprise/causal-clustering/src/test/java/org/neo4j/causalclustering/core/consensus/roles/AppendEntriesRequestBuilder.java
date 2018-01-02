/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.LinkedList;
import java.util.List;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.identity.MemberId;

public class AppendEntriesRequestBuilder
{
    private List<RaftLogEntry> logEntries = new LinkedList<>();
    private long leaderCommit = -1;
    private long prevLogTerm = -1;
    private long prevLogIndex = -1;
    private long leaderTerm = -1;
    private MemberId from;

    public RaftMessages.AppendEntries.Request build()
    {
        return new RaftMessages.AppendEntries.Request( from, leaderTerm, prevLogIndex, prevLogTerm,
                logEntries.toArray( new RaftLogEntry[logEntries.size()] ), leaderCommit );
    }

    public AppendEntriesRequestBuilder from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public AppendEntriesRequestBuilder leaderTerm( long leaderTerm )
    {
        this.leaderTerm = leaderTerm;
        return this;
    }

    public AppendEntriesRequestBuilder prevLogIndex( long prevLogIndex )
    {
        this.prevLogIndex = prevLogIndex;
        return this;
    }

    public AppendEntriesRequestBuilder prevLogTerm( long prevLogTerm )
    {
        this.prevLogTerm = prevLogTerm;
        return this;
    }

    public AppendEntriesRequestBuilder logEntry( RaftLogEntry logEntry )
    {
        logEntries.add( logEntry );
        return this;
    }

    public AppendEntriesRequestBuilder leaderCommit( long leaderCommit )
    {
        this.leaderCommit = leaderCommit;
        return this;
    }
}
