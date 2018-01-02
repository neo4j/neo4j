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
package org.neo4j.consistency.checking.cache;

import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongBitsManipulator;

import static org.neo4j.consistency.checking.cache.CacheSlots.ID_SLOT_SIZE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;

/**
 * Simply combining a {@link LongArray} with {@link LongBitsManipulator}, so that each long can be split up into
 * slots, i.e. holding multiple values per long for space efficiency.
 */
public class PackedMultiFieldCache
{
    private final LongArray array;
    private LongBitsManipulator slots;
    private long[] initValues;

    public PackedMultiFieldCache( int... slotSizes )
    {
        this( AUTO.newDynamicLongArray( 1_000_000, 0 ), slotSizes );
    }

    public PackedMultiFieldCache( LongArray array, int... slotSizes )
    {
        this.array = array;
        setSlotSizes( slotSizes );
    }

    public void put( long index, long... values )
    {
        long field = 0;
        for ( int i = 0; i < values.length; i++ )
        {
            field = slots.set( field, i, values[i] );
        }
        array.set( index, field );
    }

    public void put( long index, int slot, long value )
    {
        long field = array.get( index );
        field = slots.set( field, slot, value );
        array.set( index, field );
    }

    public long get( long index, int slot )
    {
        return slots.get( array.get( index ), slot );
    }

    public void setSlotSizes( int... slotSizes )
    {
        this.slots = new LongBitsManipulator( slotSizes );
        this.initValues = getInitVals( slotSizes );
    }

    public void clear()
    {
        long length = array.length();
        for ( long i = 0; i < length; i++ )
        {
            clear( i );
        }
    }

    public void clear( long index )
    {
        put( index, initValues );
    }

    private static long[] getInitVals( int[] slotSizes )
    {
        long[] initVals = new long[slotSizes.length];
        for ( int i = 0; i < initVals.length; i++ )
        {
            initVals[i] = isId( slotSizes, i ) ? Record.NO_NEXT_RELATIONSHIP.intValue() : 0;
        }
        return initVals;
    }

    private static boolean isId( int[] slotSizes, int i )
    {
        return slotSizes[i] >= ID_SLOT_SIZE;
    }
}
