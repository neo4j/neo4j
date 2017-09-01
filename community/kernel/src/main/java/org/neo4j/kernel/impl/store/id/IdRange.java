/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.store.id;

import java.util.Arrays;
import java.util.Objects;

public class IdRange
{
    private final long[] defragIds;
    private final long rangeStart;
    private final int rangeLength;

    public IdRange( long[] defragIds, long rangeStart, int rangeLength )
    {
        this.defragIds = defragIds;
        this.rangeStart = rangeStart;
        this.rangeLength = rangeLength;
    }

    public long[] getDefragIds()
    {
        return defragIds;
    }

    public long getRangeStart()
    {
        return rangeStart;
    }

    public int getRangeLength()
    {
        return rangeLength;
    }

    @Override
    public String toString()
    {
        return "IdRange[" + rangeStart + "-" + (rangeStart + rangeLength - 1) + ", defrag " +
                Arrays.toString( defragIds ) + "]";
    }

    public int totalSize()
    {
        return defragIds.length + rangeLength;
    }

    public IdRangeIterator iterator()
    {
        return new IdRangeIterator( this );
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
        IdRange idRange = (IdRange) o;
        return rangeStart == idRange.rangeStart &&
                rangeLength == idRange.rangeLength &&
                Arrays.equals( defragIds, idRange.defragIds );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( defragIds, rangeStart, rangeLength );
    }
}
