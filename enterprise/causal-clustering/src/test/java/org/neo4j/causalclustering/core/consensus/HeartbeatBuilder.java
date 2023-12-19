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
package org.neo4j.causalclustering.core.consensus;

import org.neo4j.causalclustering.identity.MemberId;

public class HeartbeatBuilder
{
    private long commitIndex = -1;
    private long leaderTerm = -1;
    private long commitIndexTerm = -1;
    private MemberId from;

    public RaftMessages.Heartbeat build()
    {
        return new RaftMessages.Heartbeat( from, leaderTerm, commitIndex, commitIndexTerm );
    }

    public HeartbeatBuilder from( MemberId from )
    {
        this.from = from;
        return this;
    }

    public HeartbeatBuilder leaderTerm( long leaderTerm )
    {
        this.leaderTerm = leaderTerm;
        return this;
    }

    public HeartbeatBuilder commitIndex( long commitIndex )
    {
        this.commitIndex = commitIndex;
        return this;
    }

    public HeartbeatBuilder commitIndexTerm( long commitIndexTerm )
    {
        this.commitIndexTerm = commitIndexTerm;
        return this;
    }
}
