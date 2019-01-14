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
package org.neo4j.unsafe.impl.batchimport;

import java.util.Iterator;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.unsafe.impl.batchimport.staging.TicketedProcessing;

import static java.lang.Long.min;

/**
 * Useful utility in conjunction with {@link TicketedProcessing} where an id range is divided up
 * in chunks and given as input to be processed.
 */
public class IdRangeInput extends PrefetchingIterator<IdRangeInput.Range>
{
    private final long max;
    private final int batchSize;
    private long start;

    public IdRangeInput( long max, int batchSize )
    {
        this.max = max;
        this.batchSize = batchSize;
    }

    @Override
    protected Range fetchNextOrNull()
    {
        int count = (int) min( batchSize, max - start );
        if ( count == 0 )
        {
            return null;
        }

        try
        {
            return new Range( start, count );
        }
        finally
        {
            start += count;
        }
    }

    public static Iterator<Range> idRangeInput( long max, int batchSize )
    {
        return new IdRangeInput( max, batchSize );
    }

    public static class Range
    {
        private final long start;
        private final int size;

        Range( long start, int size )
        {
            this.start = start;
            this.size = size;
        }

        public long getStart()
        {
            return start;
        }

        public int getSize()
        {
            return size;
        }
    }
}
