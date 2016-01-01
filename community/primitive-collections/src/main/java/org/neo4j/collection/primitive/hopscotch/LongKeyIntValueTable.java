/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.collection.primitive.hopscotch;

import static java.util.Arrays.fill;

public class LongKeyIntValueTable extends LongKeyTable<int[]>
{
    public static final int NULL = -1;
    private final int[] transport;
    private int[] values;

    public LongKeyIntValueTable( int capacity )
    {
        super( capacity, new int[] { NULL } );
        this.transport = new int[1];
    }

    @Override
    protected void initializeTable()
    {
        super.initializeTable();
        values = new int[capacity];
    }

    @Override
    protected void clearTable()
    {
        super.clearTable();
        fill( values, NULL );
    }

    @Override
    public int[] value( int index )
    {
        int value = values[index];
        return value == NULL ? null : pack( value );
    }

    @Override
    public void put( int index, long key, int[] value )
    {
        values[index] = unpack( value );
        super.put( index, key, value );
    }

    @Override
    public int[] putValue( int index, int[] value )
    {
        int previous = values[index];
        values[index] = unpack( value );
        super.putValue( index, value );
        return pack( previous );
    }

    @Override
    public int[] remove( int index )
    {
        int[] result = pack( values[index] );
        values[index] = NULL;
        super.remove( index );
        return result;
    }

    @Override
    public long move( int fromIndex, int toIndex )
    {
        values[toIndex] = values[fromIndex];
        values[fromIndex] = NULL;
        return super.move( fromIndex, toIndex );
    }

    @Override
    protected LongKeyTable<int[]> newInstance( int newCapacity )
    {
        return new LongKeyIntValueTable( newCapacity );
    }

    private int unpack( int[] value )
    {
        return value[0];
    }

    private int[] pack( int value )
    {
        transport[0] = value;
        return transport;
    }
}
