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
package org.neo4j.coreedge.raft.log.physical;

import static java.lang.String.format;

/**
 * Instances of this class represent a pair of a log version and the range of index entries it contains. The starting
 * index is set in creation and cannot be changed, but the end index is initially assumed to be unbounded (practically
 * it is represented as {@link Long#MAX_VALUE} until it is capped by calling {@link #endAt(long)}. This corresponds
 * to the way logs are structured, since until rotation happens the end index is unknown.
 */
public class VersionIndexRange
{
    public final long version;
    public final long prevIndex;
    private long lastIndex = Long.MAX_VALUE;

    public VersionIndexRange( long version, long prevIndex )
    {
        this.version = version;
        this.prevIndex = prevIndex;
    }

    public boolean includes( long index )
    {
        return index <= lastIndex && index > prevIndex;
    }

    void endAt( long lastIndex )
    {
        if ( lastIndex < prevIndex )
        {
            throw new IllegalArgumentException( format( "A range cannot have the upper bound set to a value " +
                    "(%d) less than the lower bound (%d)", lastIndex, prevIndex ) );
        }
        this.lastIndex = lastIndex;
    }

    @Override
    public String toString()
    {
        return format( "%d: %d < index <= %d", version, prevIndex, lastIndex );
    }

    public static final VersionIndexRange OUT_OF_RANGE = new VersionIndexRange( -1, -1 ) {

        @Override
        public boolean includes( long index )
        {
            return false;
        }

        @Override
        void endAt( long lastIndex )
        {
            throw new UnsupportedOperationException( "Immutable" );
        }

        @Override
        public String toString()
        {
            return "OUT_OF_RANGE";
        }
    };
}
