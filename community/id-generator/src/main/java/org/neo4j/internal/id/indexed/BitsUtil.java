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
package org.neo4j.internal.id.indexed;

import static java.lang.Integer.min;

/**
 * Various utilities for manipulating and understanding bit-sets.
 */
final class BitsUtil
{
    private BitsUtil()
    {
        // util
    }

    static long bitsInSingleLong( int start, int slots )
    {
        long bits = (start + slots == Long.SIZE) ? -1 : (1L << (start + slots)) - 1;
        return bits & -(1L << start);
    }

    static void setBits( long[] bits, int start, int slots, int bitsArraySlot )
    {
        int firstArraySlot = bitsArraySlot + start / Long.SIZE;
        start %= Long.SIZE;
        for ( int arraySlot = firstArraySlot; slots > 0; arraySlot++ )
        {
            int slotsInThisLong = min( slots, Long.SIZE - start );
            long bitsInThisSlot = bitsInSingleLong( start, slotsInThisLong );
            assert bits[arraySlot] == 0;
            bits[arraySlot] = bitsInThisSlot;
            slots -= slotsInThisLong;
            start = 0;
        }
    }

    private static String internalBitsToString( long bits )
    {
        char[] chars = new char[Long.SIZE + Long.SIZE / 8 - 1];
        for ( int i = 0, ci = 0; i < Long.SIZE; i++, ci++ )
        {
            if ( i > 0 && i % 8 == 0 )
            {
                chars[ci++] = ' ';
            }
            long mask = 1L << (Long.SIZE - i - 1);
            chars[ci] = (bits & mask) == 0 ? '0' : '1';
        }
        return String.valueOf( chars );
    }

    static String bitsToString( long[] bits )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = bits.length - 1; i >= 0; i-- )
        {
            if ( i < bits.length - 1 )
            {
                builder.append( " , " );
            }
            builder.append( internalBitsToString( bits[i] ) );
        }
        return builder.toString();
    }
}
