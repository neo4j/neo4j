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
