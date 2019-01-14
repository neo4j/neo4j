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
package org.neo4j.kernel.impl.util.collection;

import org.neo4j.memory.MemoryAllocationTracker;

public interface OffHeapBlockAllocator
{
    /**
     * @return memory block of requested size; there's no guarantee whether allocated memory is zero-filled or dirty
     */
    MemoryBlock allocate( long size, MemoryAllocationTracker tracker );

    void free( MemoryBlock block, MemoryAllocationTracker tracker );

    void release();

    class MemoryBlock
    {
        final long addr;
        final long size;
        final long unalignedAddr;
        final long unalignedSize;

        MemoryBlock( long addr, long size, long unalignedAddr, long unalignedSize )
        {
            this.size = size;
            this.addr = addr;
            this.unalignedSize = unalignedSize;
            this.unalignedAddr = unalignedAddr;
        }
    }
}
