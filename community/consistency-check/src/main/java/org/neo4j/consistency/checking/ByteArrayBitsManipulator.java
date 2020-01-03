/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.checking;

import java.util.Arrays;

import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;

/**
 * Uses a {@link ByteArray} and can conveniently split up an index into slots, not only per byte, but arbitrary bit-sizes,
 * e.g. two 40-bit fields and eight 1-bit fields. To favor simplicity there are two types of fields: booleans and long fields.
 * Boolean fields are 1-bit fields that can be anywhere in the byte[] index, but multi-bit fields must start at the beginning of
 * a byte in the index.
 */
public class ByteArrayBitsManipulator
{
    public static final int MAX_BYTES = 11;
    private static final int MAX_BITS = MAX_BYTES * Byte.SIZE;
    public static final int MAX_SLOT_BITS = 5 * Byte.SIZE;
    static final long MAX_SLOT_VALUE = (1L << MAX_SLOT_BITS) - 1;

    private static class Slot
    {
        private final int byteOffset;
        private final int bitOffset;
        private final int bitCount;
        private final long mask;
        private final int fbMask;

        Slot( int bits, int absoluteBitOffset )
        {
            this.byteOffset = absoluteBitOffset / Byte.SIZE;
            this.bitOffset = absoluteBitOffset % Byte.SIZE;
            this.bitCount = bits;
            this.mask = (1L << bits) - 1;
            this.fbMask = 1 << bitOffset;
        }

        public long get( ByteArray array, long index )
        {
            // Basically two modes: boolean or 5B field, right?
            if ( bitCount == 1 )
            {
                int field = array.getByte( index, byteOffset ) & 0xFF;
                boolean bitIsSet = (field & fbMask) != 0;
                return bitIsSet ? -1 : 0; // the -1 here is a bit weird, but for the time being this is what the rest of the code expects
            }
            else // we know that this larger field starts at the beginning of a byte
            {
                long field = array.get5ByteLong( index, byteOffset );
                long raw = field & mask;
                return raw == mask ? -1 : raw;
            }
        }

        public void set( ByteArray array, long index, long value )
        {
            if ( value < -1 || value > mask )
            {
                throw new IllegalStateException( "Invalid value " + value + ", max is " + mask );
            }

            if ( bitCount == 1 )
            {
                int field = array.getByte( index, byteOffset ) & 0xFF;
                int otherBits = field & ~fbMask;
                array.setByte( index, byteOffset, (byte) (otherBits | (value << bitOffset)) );
            }
            else
            {
                long field = array.get5ByteLong( index, byteOffset );
                long otherBits = field & ~mask;
                array.set5ByteLong( index, byteOffset, value | otherBits );
            }
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[" + Bits.numbersToBitString( new long[] {mask << bitOffset} ) + "]";
        }
    }

    private final Slot[] slots;

    public ByteArrayBitsManipulator( int... slotsAndTheirBitCounts )
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
            if ( bits > 1 && bitCursor % Byte.SIZE != 0 )
            {
                throw new IllegalArgumentException( "Larger slots, i.e. size > 1 needs to be placed at the beginning of a byte" );
            }
            if ( bits > MAX_SLOT_BITS )
            {
                throw new IllegalArgumentException( "Too large slot " + bits + ", biggest allowed " + MAX_SLOT_BITS );
            }
            slots[i] = new Slot( bits, bitCursor );
            bitCursor += bits;
        }
        if ( bitCursor > MAX_BITS )
        {
            throw new IllegalArgumentException( "Max number of bits is " + MAX_BITS );
        }
        return slots;
    }

    public void set( ByteArray array, long index, int slotIndex, long value )
    {
        slot( slotIndex ).set( array, index, value );
    }

    public long get( ByteArray array, long index, int slotIndex )
    {
        return slot( slotIndex ).get( array, index );
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
