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
package org.neo4j.causalclustering.core.consensus.roles.follower;

import static java.lang.String.format;

/**
 * Things the leader thinks it knows about a follower.
 */
public class FollowerState
{
    // We know that the follower agrees with our (leader) log up until this index. Only updated by the leader when:
    // * increased when it receives a successful AppendEntries.Response
    private final long matchIndex;

    public FollowerState()
    {
        this( -1 );
    }

    private FollowerState( long matchIndex )
    {
        assert matchIndex >= -1 : format( "Match index can never be less than -1. Was %d", matchIndex );
        this.matchIndex = matchIndex;
    }

    public long getMatchIndex()
    {
        return matchIndex;
    }

    public FollowerState onSuccessResponse( long newMatchIndex )
    {
        return new FollowerState( newMatchIndex );
    }

    @Override
    public String toString()
    {
        return format( "State{matchIndex=%d}", matchIndex );
    }
}
