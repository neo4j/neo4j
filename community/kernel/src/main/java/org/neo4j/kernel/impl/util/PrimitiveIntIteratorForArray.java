/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.NoSuchElementException;

import static java.util.Arrays.copyOf;

public class PrimitiveIntIteratorForArray implements PrimitiveIntIterator
{
    private final int[] values;

    int i = 0;

    public PrimitiveIntIteratorForArray( int... values )
    {
        this.values = values;
    }

    @Override
    public boolean hasNext()
    {
        return i < values.length;
    }

    @Override
    public int next()
    {
        if ( hasNext() )
        {
            return values[i++];
        }
        throw new NoSuchElementException( );
    }

    public static final int[] EMPTY_INT_ARRAY = new int[0];

    public static int[] primitiveIntIteratorToIntArray( PrimitiveIntIterator iterator )
    {
        if ( !iterator.hasNext() )
        {
            return EMPTY_INT_ARRAY;
        }

        int[] result = new int[5]; // arbitrary initial size
        int cursor = 0;
        while ( iterator.hasNext() )
        {
            if ( cursor >= result.length )
            {
                result = copyOf( result, result.length*2 );
            }
            result[cursor++] = iterator.next();
        }
        // shrink if needed
        return cursor == result.length ? result : copyOf( result, cursor );
    }
}
