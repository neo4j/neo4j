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

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public class LongKeyLongValueUnsafeTable extends UnsafeTable<long[]>
{
    public LongKeyLongValueUnsafeTable( int capacity )
    {
        super( capacity, 16, new long[1] );
    }

    @Override
    protected long internalKey( long keyAddress )
    {
        return UnsafeUtil.getLong( keyAddress );
    }

    @Override
    protected void internalPut( long keyAddress, long key, long[] value )
    {
        UnsafeUtil.putLong( keyAddress, key );
        UnsafeUtil.putLong( keyAddress+8, value[0] );
    }

    @Override
    protected long[] internalRemove( long keyAddress )
    {
        valueMarker[0] = UnsafeUtil.getLong( keyAddress+8 );
        UnsafeUtil.putLong( keyAddress, -1 );
        return valueMarker;
    }

    @Override
    public long[] putValue( int index, long[] value )
    {
        long valueAddress = valueAddress( index );
        long oldValue = UnsafeUtil.getLong( valueAddress );
        UnsafeUtil.putLong( valueAddress, value[0] );
        return pack( oldValue );
    }

    private long[] pack( long value )
    {
        valueMarker[0] = value;
        return valueMarker;
    }

    private long valueAddress( int index )
    {
        return keyAddress( index )+8;
    }

    @Override
    public long[] value( int index )
    {
        long value = UnsafeUtil.getLong( valueAddress( index ) );
        return pack( value );
    }

    @Override
    protected Table<long[]> newInstance( int newCapacity )
    {
        return new LongKeyLongValueUnsafeTable( newCapacity );
    }
}
