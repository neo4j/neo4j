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
package org.neo4j.collection.primitive.hopscotch;

public class LongKeyIntValueTable extends IntArrayBasedKeyTable<int[]>
{
    public static final int NULL = -1;

    public LongKeyIntValueTable( int capacity )
    {
        super( capacity, 3 + 1, 32, new int[] { NULL } );
    }

    @Override
    public long key( int index )
    {
        return getLong( index( index ) );
    }

    @Override
    protected void internalPut( int actualIndex, long key, int[] value )
    {
        putLong( actualIndex, key );
        table[actualIndex+2] = value[0];
    }

    @Override
    public int[] putValue( int index, int[] value )
    {
        int actualIndex = index( index )+2;
        int previous = table[actualIndex];
        table[actualIndex] = value[0];
        return pack( previous );
    }

    @Override
    public int[] value( int index )
    {
        return pack( table[index( index )+2] );
    }

    @Override
    protected Table<int[]> newInstance( int newCapacity )
    {
        return new LongKeyIntValueTable( newCapacity );
    }

    private int[] pack( int value )
    {
        singleValue[0] = value;
        return singleValue;
    }
}
