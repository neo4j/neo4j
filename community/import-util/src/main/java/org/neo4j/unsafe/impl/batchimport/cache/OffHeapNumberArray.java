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
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public abstract class OffHeapNumberArray<N extends NumberArray<N>> extends BaseNumberArray<N>
{
    private final long allocatedAddress;
    protected final long address;
    protected final long length;
    protected final MemoryAllocationTracker allocationTracker;
    private final long allocatedBytes;
    private boolean closed;

    protected OffHeapNumberArray( long length, int itemSize, long base, MemoryAllocationTracker allocationTracker )
    {
        super( itemSize, base );
        UnsafeUtil.assertHasUnsafe();
        this.length = length;
        this.allocationTracker = allocationTracker;

        long dataSize = length * itemSize;
        boolean itemSizeIsPowerOfTwo = Integer.bitCount( itemSize ) == 1;
        if ( UnsafeUtil.allowUnalignedMemoryAccess || !itemSizeIsPowerOfTwo )
        {
            // we can end up here even if we require aligned memory access. Reason is that item size
            // isn't power of two anyway and so we have to fallback to safer means of accessing the memory,
            // i.e. byte for byte.
            allocatedBytes = dataSize;
            this.allocatedAddress = this.address = UnsafeUtil.allocateMemory( allocatedBytes, allocationTracker );
        }
        else
        {
            // the item size is a power of two and we're required to access memory aligned
            // so we can allocate a bit more to ensure we can get an aligned memory address to start from.
            allocatedBytes = dataSize + itemSize - 1;
            this.allocatedAddress = UnsafeUtil.allocateMemory( allocatedBytes, allocationTracker );
            this.address = UnsafeUtil.alignedMemory( allocatedAddress, itemSize );
        }
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        visitor.offHeapUsage( allocatedBytes );
    }

    @Override
    public void close()
    {
        if ( !closed )
        {
            if ( length > 0 )
            {
                // Allocating 0 bytes actually returns address 0
                UnsafeUtil.free( allocatedAddress, allocatedBytes, allocationTracker );
            }
            closed = true;
        }
    }
}
