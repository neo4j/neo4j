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

public class LongKeyLongValueUnsafeTable extends UnsafeTable<long[]>
{
    public LongKeyLongValueUnsafeTable( int capacity )
    {
        super( capacity, 16, new long[1] );
    }

    @Override
    protected long internalKey( long keyAddress )
    {
        return unsafe.getLong( keyAddress );
    }

    @Override
    protected void internalPut( long keyAddress, long key, long[] value )
    {
        unsafe.putLong( keyAddress, key );
        unsafe.putLong( keyAddress+8, value[0] );
    }

    @Override
    protected long[] internalRemove( long keyAddress )
    {
        valueMarker[0] = unsafe.getLong( keyAddress+8 );
        unsafe.putLong( keyAddress, -1 );
        return valueMarker;
    }

    @Override
    public long[] putValue( int index, long[] value )
    {
        long valueAddress = keyAddress( index )+8;
        long oldValue = unsafe.getLong( valueAddress );
        unsafe.putLong( valueAddress, value[0] );
        valueMarker[0] = oldValue;
        return valueMarker;
    }

    @Override
    protected Table<long[]> newInstance( int newCapacity )
    {
        return new LongKeyLongValueUnsafeTable( newCapacity );
    }
}
