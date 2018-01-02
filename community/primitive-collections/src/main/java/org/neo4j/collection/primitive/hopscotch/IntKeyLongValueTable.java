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

public class IntKeyLongValueTable extends IntArrayBasedKeyTable<long[]>
{
    public static final long NULL = -1L;

    public IntKeyLongValueTable( int capacity )
    {
        super( capacity, 3 + 1, 32, new long[] { NULL } );
    }

    @Override
    public long key( int index )
    {
        return table[index( index )];
    }

    @Override
    protected void internalPut( int actualIndex, long key, long[] valueHolder )
    {
        table[actualIndex] = (int)key; // we know that key is an int
        putLong( actualIndex+1, valueHolder[0] );
    }

    @Override
    public long[] value( int index )
    {
        singleValue[0] = getLong( index( index )+1 );
        return singleValue;
    }

    @Override
    public long[] putValue( int index, long[] value )
    {
        singleValue[0] = putLong( index( index )+1, value[0] );
        return singleValue;
    }

    @Override
    protected Table<long[]> newInstance( int newCapacity )
    {
        return new IntKeyLongValueTable( newCapacity );
    }
}
