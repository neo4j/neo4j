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

import org.neo4j.kernel.impl.util.Bits;

/**
 * Turns a long into 64 bits of memory where variables can be allocated in, for example:
 * <pre>
 * [eeee,eeee][dddd,dddd][dddd,dddd][dddd,cccc][bbbb,bbbb][bbbb,bbbb][bbaa,aaaa][aaaa,aaaa]
 * </pre>
 * Which has the variables a (14 bits), b (18 bits), c (4 bits), d (20 bits) and e (8 bits)
 */
public class LongBitsManipulator
{
    private static class Slot
    {
        private final long mask;
        private final long maxValue;
        private final int bitOffset;

        public Slot( int bits, long mask, int bitOffset )
        {
            this.mask = mask;
            this.bitOffset = bitOffset;
            this.maxValue = (1L << bits) - 1;
        }

        public long get( long field )
        {
            long raw = field & mask;
            return raw == mask ? -1 : raw >>> bitOffset;
        }

        public long set( long field, long value )
        {
            if ( value < -1 || value > maxValue )
            {
                throw new IllegalStateException( "Invalid value " + value + ", max is " + maxValue );
            }

            long otherBits = field & ~mask;
            return ((value << bitOffset)&mask) | otherBits;
        }

        public long clear( long field, boolean trueForAllOnes )
        {
            long otherBits = field & ~mask;
            return trueForAllOnes ?
                    // all bits in this slot as 1
                    otherBits | mask :

                    // all bits in this slot as 0
                    otherBits;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[" + Bits.numbersToBitString( new long[] {maxValue << bitOffset} ) + "]";
        }
    }

    private final Slot[] slots;

    public LongBitsManipulator( int... slotsAndTheirBitCounts )
    {
        slots = intoSlots( slotsAndTheirBitCounts );
    }

    private Slot[] intoSlots( int[] slotsAndTheirSizes )
    {
        Slot[] slots = new Slot[slotsAndTheirSizes.length];
        int bitCursor = 0;
        for ( int i = 0; i < slotsAndTheirSizes.length; i++ )
        {
            int bits = slotsAndTheirSizes[i];
            long mask = (1L << bits) - 1;
            mask <<= bitCursor;
            slots[i] = new Slot( bits, mask, bitCursor );
            bitCursor += bits;
        }
        return slots;
    }

    public int slots()
    {
        return slots.length;
    }

    public long set( long field, int slotIndex, long value )
    {
        return slot( slotIndex ).set( field, value );
    }

    public long get( long field, int slotIndex )
    {
        return slot( slotIndex ).get( field );
    }

    public long clear( long field, int slotIndex, boolean trueForAllOnes )
    {
        return slot( slotIndex ).clear( field, trueForAllOnes );
    }

    public long template( boolean... trueForOnes )
    {
        if ( trueForOnes.length != slots.length )
        {
            throw new IllegalArgumentException( "Invalid boolean arguments, expected " + slots.length );
        }

        long field = 0;
        for ( int i = 0; i < trueForOnes.length; i++ )
        {
            field = slots[i].clear( field, trueForOnes[i] );
        }
        return field;
    }

    private Slot slot( int slotIndex )
    {
        if ( slotIndex < 0 || slotIndex >= slots.length )
        {
            throw new IllegalArgumentException( "Invalid slot " + slotIndex + ", I've got " + this );
        }
        return slots[slotIndex];
    }

    @Override
    public String toString()
    {
        return Arrays.toString( slots );
    }
}
