/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.collection;

import org.eclipse.collections.api.iterator.LongIterator;

import java.nio.LongBuffer;
import java.util.NoSuchElementException;

public class RangeLongIterator implements LongIterator
{
    private final LongBuffer buffer;
    private final int stop;
    private int currentPosition;

    public RangeLongIterator( LongBuffer buffer, int start, int stop )
    {
        assertRange( buffer, start, stop );
        this.buffer = buffer;
        this.currentPosition = start;
        this.stop = stop;
    }

    @Override
    public long next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        return buffer.get( currentPosition++ );
    }

    private void assertRange( LongBuffer buffer, int start, int stop )
    {
        int range = stop - start;
        int limit = buffer.limit();
        if ( start < 0 || stop < 0 || range < 0 || range > buffer.remaining() ||
             (range != 0 && (start >= limit || stop > limit)) )
        {
            throw new IllegalArgumentException(
                    String.format( "Invalid range, capacity=%d, start=%d, stop=%d", buffer.remaining(), start, stop ) );
        }
    }

    @Override
    public boolean hasNext()
    {
        return currentPosition < stop;
    }
}
