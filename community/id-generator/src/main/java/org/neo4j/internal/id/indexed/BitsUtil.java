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

    /**
     * Constructs and returns a {@code long} with a range of bits set.
     *
     * @param start offset of starting bit to set.
     * @param slots number of bits to set.
     * @return a {@code long} with the desired bits set.
     */
    static long bitsInSingleLong( int start, int slots )
    {
        long bits = (start + slots == Long.SIZE) ? -1 : (1L << (start + slots)) - 1;
        return bits & -(1L << start);
    }

    /**
     * Sets a range of bits in one of the longs in the provided array.
     *
     * @param bits array of longs where one of them, decided by {@code bitsArraySlot} will have some bits set.
     * @param start offset of starting bit to set.
     * @param slots number of bits to set.
     * @param bitsArraySlot the index into {@code bits} which will have the bits set.
     */
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
}
