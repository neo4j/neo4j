/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.roles;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.identity.MemberId;

public class AppendEntriesResponseBuilder
{
    private boolean success;
    private long term = -1;
    private MemberId from;
    private long matchIndex = -1;
    private long appendIndex = -1;

    public RaftMessages.AppendEntries.Response build()
    {
        // a response of false should always have a match index of -1
        assert success || matchIndex == -1;
        return new RaftMessages.AppendEntries.Response( from, term, success, matchIndex, appendIndex );
    }

    public AppendEntriesResponseBuilder from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public AppendEntriesResponseBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public AppendEntriesResponseBuilder matchIndex( long matchIndex )
    {
        this.matchIndex = matchIndex;
        return this;
    }

    public AppendEntriesResponseBuilder appendIndex( long appendIndex )
    {
        this.appendIndex = appendIndex;
        return this;
    }

    public AppendEntriesResponseBuilder success()
    {
        this.success = true;
        return this;
    }

    public AppendEntriesResponseBuilder failure()
    {
        this.success = false;
        return this;
    }
}
