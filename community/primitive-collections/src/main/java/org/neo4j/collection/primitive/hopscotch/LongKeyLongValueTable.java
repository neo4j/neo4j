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
package org.neo4j.collection.primitive.hopscotch;

public class LongKeyLongValueTable
        extends IntArrayBasedKeyTable<long[]>
{
    public static final long NULL = -1;

    public LongKeyLongValueTable( int capacity )
    {
        super( capacity, 5, 32, new long[]{ NULL } );
    }

    @Override
    public long key( int index )
    {
        return getLong( address( index ) );
    }

    @Override
    protected void internalPut( int actualIndex, long key, long[] value )
    {
        putLong( actualIndex, key );
        putLong( actualIndex + 2, value[0] );
    }

    @Override
    public long[] putValue( int index, long[] value )
    {
        int actualValueIndex = address( index ) + 2;
        long previous = getLong( actualValueIndex );
        putLong( actualValueIndex, value[0] );
        return pack( previous );
    }

    @Override
    public long[] value( int index )
    {
        return pack( getLong( address( index ) + 2 ) );
    }

    @Override
    protected LongKeyLongValueTable newInstance( int newCapacity )
    {
        return new LongKeyLongValueTable( newCapacity );
    }

    private long[] pack( long value )
    {
        singleValue[0] = value;
        return singleValue;
    }
}
