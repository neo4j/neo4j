/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.helpers.collection;

import static java.lang.String.format;

public final class LongRange
{
    public static LongRange range( long from, long to )
    {
        return new LongRange( from, to );
    }

    public static void assertIsRange( long from, long to )
    {
        if ( from < 0 )
        {
            throw new IllegalArgumentException( "Range cannot start from negative value. Got: " + from );
        }
        if ( to < from )
        {
            throw new IllegalArgumentException( format( "Not a valid range. RequiredTxId[%d] must be higher or equal to startTxId[%d].", to, from ) );
        }
    }

    private final long from;

    private final long to;

    private LongRange( long from, long to )
    {
        assertIsRange( from, to );
        this.from = from;
        this.to = to;
    }

    /**
     * @param val value to compare whether or not it's within this range.
     * @return {@code true} if {@code from <= val <= to}, i.e. inclusive from and inclusive to.
     */
    public boolean isWithinRange( long val )
    {
        return val >= from && val <= to;
    }

    /**
     * @param val value to compare whether or not it's within this range.
     * @return {@code true} if {@code from <= val < to}, i.e. inclusive from and exclusive to.
     */
    public boolean isWithinRangeExclusiveTo( long val )
    {
        return val >= from && val < to;
    }

    @Override
    public String toString()
    {
        return "LongRange{" + "from=" + from + ", to=" + to + '}';
    }

    public long from()
    {
        return from;
    }

    public long to()
    {
        return to;
    }
}
