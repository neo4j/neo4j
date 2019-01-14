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
package org.neo4j.kernel.impl.index.labelscan;

/**
 * A small bit set of maximum 64 bits. Used in {@link LabelScanLayout}.
 */
class LabelScanValue
{
    static final int RANGE_SIZE = Long.SIZE;
    static final int RANGE_SIZE_BYTES = Long.BYTES;

    /**
     * Small bit set.
     */
    long bits;

    /**
     * Sets bit at given {@code index}, where {@code index=0} is the lowest index, {@code index=63} the highest.
     *
     * @param index index into the bit set of the bit to set.
     */
    LabelScanValue set( int index )
    {
        long mask = 1L << index;
        bits |= mask;
        return this;
    }

    /**
     * Adds all bits from {@code other} to this bit set.
     * Result is a union of the two bit sets. {@code other} is kept intact.
     *
     * @param other value containing bits to add.
     * @return this instance, now with added bits from {@code other}.
     */
    LabelScanValue add( LabelScanValue other )
    {
        bits |= other.bits;
        return this;
    }

    /**
     * Removes all bits in {@code other} from this bit set.
     * Result is bits in this set before the call with all bits from {@code other} removed.
     * {@code other} is kept intact.
     *
     * @param other value containing bits to remove.
     * @return this instance, now with removed bits from {@code other}.
     */
    LabelScanValue remove( LabelScanValue other )
    {
        bits &= ~other.bits;
        return this;
    }

    /**
     * Clears all bits in this bit set.
     */
    void clear()
    {
        bits = 0;
    }

    @Override
    public String toString()
    {
        return String.valueOf( bits );
    }
}
