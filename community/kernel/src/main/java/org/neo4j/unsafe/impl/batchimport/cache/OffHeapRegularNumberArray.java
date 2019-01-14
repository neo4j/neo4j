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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.neo4j.memory.MemoryAllocationTracker;

/**
 * Base class for common functionality for any {@link NumberArray} where the data lives off-heap.
 */
abstract class OffHeapRegularNumberArray<N extends NumberArray<N>> extends OffHeapNumberArray<N>
{
    protected final int shift;

    protected OffHeapRegularNumberArray( long length, int shift, long base, MemoryAllocationTracker allocationTracker )
    {
        super( length, 1 << shift, base, allocationTracker );
        this.shift = shift;
    }

    protected long addressOf( long index )
    {
        index = rebase( index );
        if ( index < 0 || index >= length )
        {
            throw new ArrayIndexOutOfBoundsException( "Requested index " + index + ", but length is " + length );
        }
        return address + (index << shift);
    }

    protected boolean isByteUniform( long value )
    {
        byte any = (byte)value;
        for ( int i = 1; i < itemSize; i++ )
        {
            byte test = (byte)(value >>> (i << 3));
            if ( test != any )
            {
                return false;
            }
        }
        return true;
    }
}
