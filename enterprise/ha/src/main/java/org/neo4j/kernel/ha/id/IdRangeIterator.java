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
package org.neo4j.kernel.ha.id;

import java.util.Arrays;

import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.id.IdRange;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

public class IdRangeIterator
{
    public static IdRangeIterator EMPTY_ID_RANGE_ITERATOR =
            new IdRangeIterator( new IdRange( EMPTY_LONG_ARRAY, 0, 0 ) )
            {
                @Override
                public long next()
                {
                    return VALUE_REPRESENTING_NULL;
                }
            };

    public static final long VALUE_REPRESENTING_NULL = -1;
    private int position = 0;
    private final long[] defrag;
    private final long start;
    private final int length;

    public IdRangeIterator( IdRange idRange )
    {
        this.defrag = idRange.getDefragIds();
        this.start = idRange.getRangeStart();
        this.length = idRange.getRangeLength();
    }

    public long next()
    {
        try
        {
            if ( position < defrag.length )
            {
                return defrag[position];
            }

            long candidate = nextRangeCandidate();
            if ( candidate == IdGeneratorImpl.INTEGER_MINUS_ONE )
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
        int offset = position - defrag.length;
        return (offset < length) ? (start + offset) : VALUE_REPRESENTING_NULL;
    }

    @Override
    public String toString()
    {
        return "IdRangeIterator[start:" + start + ", length:" + length + ", position:" + position + ", defrag:" +
                Arrays.toString( defrag ) + "]";
    }
}
