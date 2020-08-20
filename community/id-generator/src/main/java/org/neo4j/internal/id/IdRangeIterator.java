/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.id;

import java.util.Arrays;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

public class IdRangeIterator implements IdSequence
{
    public static final long VALUE_REPRESENTING_NULL = -1;
    private int position;
    private final long[] defrag;
    private final long start;
    private final int length;

    public IdRangeIterator( IdRange idRange )
    {
        this.defrag = idRange.getDefragIds();
        this.start = idRange.getRangeStart();
        this.length = idRange.getRangeLength();
    }

    @Override
    public long nextId( PageCursorTracer ignored )
    {
        try
        {
            if ( position < defrag.length )
            {
                return defrag[position];
            }

            long candidate = nextRangeCandidate();
            if ( IdValidator.isReservedId( candidate ) )
            {
                position++;
                candidate = nextRangeCandidate();
            }
            return candidate;
        }
        finally
        {
            ++position;
        }
    }

    private long nextRangeCandidate()
    {
        int offset = currentRangeOffset();
        return (offset < length) ? (start + offset) : VALUE_REPRESENTING_NULL;
    }

    private int currentRangeOffset()
    {
        return position - defrag.length;
    }

    @Override
    public String toString()
    {
        return "IdRangeIterator[start:" + start + ", length:" + length + ", position:" + position + ", defrag:" +
                Arrays.toString( defrag ) + "]";
    }
}
