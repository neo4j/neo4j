/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.util.Arrays;

import static org.neo4j.unsafe.impl.batchimport.Utils.safeCastLongToInt;

/**
 * A {@code long[]} on heap, abstracted into a {@link IntArray}.
 */
public class HeapIntArray extends HeapNumberArray implements IntArray
{
    private final int[] array;
    private final int defaultValue;

    public HeapIntArray( int length, int defaultValue )
    {
        super( 4 );
        this.defaultValue = defaultValue;
        this.array = new int[length];
        clear();
    }

    @Override
    public long length()
    {
        return array.length;
    }

    @Override
    public int get( long index )
    {
        return array[safeCastLongToInt( index )];
    }

    @Override
    public void set( long index, int value )
    {
        array[safeCastLongToInt( index )] = value;
    }

    @Override
    public void clear()
    {
        Arrays.fill( array, defaultValue );
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        for ( int i = 0; i < numberOfEntries; i++ )
        {
            int fromValue = get( fromIndex+i );
            set( fromIndex+i, get( toIndex+i ) );
            set( toIndex+i, fromValue );
        }
    }

    @Override
    public IntArray fixate()
    {
        return this;
    }
}
