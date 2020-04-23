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
package org.neo4j.internal.batchimport.cache;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.MemoryTracker;

public abstract class OffHeapNumberArray<N extends NumberArray<N>> extends BaseNumberArray<N>
{
    protected final long address;
    protected final long length;
    private final long allocatedBytes;
    protected final MemoryTracker memoryTracker;
    private boolean closed;

    protected OffHeapNumberArray( long length, int itemSize, long base, MemoryTracker memoryTracker )
    {
        super( itemSize, base );
        UnsafeUtil.assertHasUnsafe();
        this.memoryTracker = memoryTracker;
        this.length = length;
        this.allocatedBytes = length * itemSize;
        this.address = UnsafeUtil.allocateMemory( allocatedBytes, memoryTracker );
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
                UnsafeUtil.free( address, allocatedBytes, memoryTracker );
            }
            closed = true;
        }
    }
}
