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
package org.neo4j.internal.unsafe;

import java.util.concurrent.atomic.LongAdder;

import org.neo4j.memory.MemoryTracker;

/**
 * Global memory tracker that can be used in a global multi threaded context to record
 * allocation and de-allocation of native memory.
 * @see MemoryTracker
 */
public final class GlobalMemoryTracker implements MemoryTracker
{
    public static final GlobalMemoryTracker INSTANCE = new GlobalMemoryTracker();

    private final LongAdder allocatedBytesDirect = new LongAdder();
    private final LongAdder allocatedBytesHeap = new LongAdder();

    private GlobalMemoryTracker()
    {
    }

    @Override
    public long usedDirectMemory()
    {
        return allocatedBytesDirect.sum();
    }

    @Override
    public long estimatedHeapMemory()
    {
        return allocatedBytesHeap.sum();
    }

    @Override
    public void allocateDirect( long bytes )
    {
        allocatedBytesDirect.add( bytes );
    }

    @Override
    public void releaseDirect( long bytes )
    {
        allocatedBytesDirect.add( -bytes );
    }

    @Override
    public void allocateHeap( long bytes )
    {
        allocatedBytesHeap.add( bytes );
    }

    @Override
    public void releaseHeap( long bytes )
    {
        allocatedBytesHeap.add( -bytes );
    }

    @Override
    public long heapHighWaterMark()
    {
        throw new UnsupportedOperationException( "Global memory tracker does not support high water mark." );
    }

    @Override
    public void reset()
    {
    }
}
