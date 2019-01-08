/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
