/*
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
package org.neo4j.kernel.impl.index.labelscan;

public class LabelScanValue
{
    public long bits;

    /**
     * Every set bit means a bit which should be set in our bits.
     *
     * @param other value containing bits to add.
     * @return this instance, now with added bits from {@code other}.
     */
    public LabelScanValue add( LabelScanValue other )
    {
        bits |= other.bits;
        return this;
    }

    /**
     * Every set bit means a bit which should be cleared in our bits.
     *
     * @param other value containing bits to remove.
     * @return this instance, now with removed bits from {@code other}.
     */
    public LabelScanValue remove( LabelScanValue other )
    {
        bits &= ~other.bits;
        return this;
    }

    public void reset()
    {
        bits = 0;
    }

    @Override
    public String toString()
    {
        return String.valueOf( bits );
    }

    public void set( int index )
    {
        long mask = 1L << index;
        bits |= mask;
    }
}
