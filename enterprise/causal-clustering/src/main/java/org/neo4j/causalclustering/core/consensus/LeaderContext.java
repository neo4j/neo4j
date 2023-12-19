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

import static java.lang.String.format;

/**
 * Consistent leader state at a point in time.
 */
public class LeaderContext
{
    public final long term;
    public final long commitIndex;

    public LeaderContext( long term, long commitIndex )
    {
        this.term = term;
        this.commitIndex = commitIndex;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        LeaderContext that = (LeaderContext) o;

        if ( term != that.term )
        {
            return false;
        }
        return commitIndex == that.commitIndex;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return format( "LeaderContext{term=%d, commitIndex=%d}", term, commitIndex );
    }
}
