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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.util.Arrays;

/**
 * A {@code long[]} on heap, abstracted into a {@link LongArray}.
 */
public class HeapLongArray extends HeapNumberArray<LongArray> implements LongArray
{
    private final long[] array;
    private final long defaultValue;

    public HeapLongArray( int length, long defaultValue, long base )
    {
        super( 8, base );
        this.defaultValue = defaultValue;
        this.array = new long[length];
        clear();
    }

    @Override
    public long length()
    {
        return array.length;
    }

    @Override
    public long get( long index )
    {
        return array[index( index )];
    }

    @Override
    public void set( long index, long value )
    {
        array[index( index )] = value;
    }

    @Override
    public void clear()
    {
        Arrays.fill( array, defaultValue );
    }
}
