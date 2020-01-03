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
package org.neo4j.collection;

import org.eclipse.collections.api.iterator.LongIterator;

import java.util.NoSuchElementException;

import static org.neo4j.util.Preconditions.requireBetween;

public class RangeLongIterator implements LongIterator
{
    private final long[] array;
    private final int stopIndex;
    private int currentIndex;

    public RangeLongIterator( long[] array, int start, int size )
    {
        requireBetween( start, 0, array.length );
        requireBetween( start + size, 0, array.length + 1);
        this.array = array;
        this.currentIndex = start;
        this.stopIndex = start + size;
    }

    @Override
    public long next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        return array[currentIndex++];
    }

    @Override
    public boolean hasNext()
    {
        return currentIndex < stopIndex;
    }
}
