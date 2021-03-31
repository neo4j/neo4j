/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.io.bufferpool.impl;

import java.util.concurrent.atomic.LongAdder;

import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.memory.MemoryGroup.CENTRAL_BYTE_BUFFER_MANAGER;

class MemoryMonitor extends GlobalMemoryGroupTracker
{
    private final LongAdder usedNativeMemory = new LongAdder();
    private final LongAdder usedHeapMemory = new LongAdder();
    private final MemoryTracker memoryTracker = new MemoryTrackerImpl();

    MemoryMonitor( MemoryPools memoryPools )
    {
        super( memoryPools, CENTRAL_BYTE_BUFFER_MANAGER, 0, false, true, null );
        memoryPools.registerPool( this );
    }

    MemoryTracker getMemoryTracker()
    {
        return memoryTracker;
    }

    @Override
    public void reserveHeap( long bytes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reserveNative( long bytes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseHeap( long bytes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseNative( long bytes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long totalSize()
    {
        return usedHeap() + usedNative();
    }

    @Override
    public long usedHeap()
    {
        return usedHeapMemory.longValue();
    }

    @Override
    public long usedNative()
    {
        return usedNativeMemory.longValue();
    }

    @Override
    public long free()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public void setSize( long size )
    {
        throw new UnsupportedOperationException();
    }

    private class MemoryTrackerImpl implements MemoryTracker
    {

        @Override
        public long usedNativeMemory()
        {
            return  usedNativeMemory.longValue();
        }

        @Override
        public long estimatedHeapMemory()
        {
            return usedHeapMemory.longValue();
        }

        @Override
        public void allocateNative( long bytes )
        {
            usedNativeMemory.add( bytes );
        }

        @Override
        public void releaseNative( long bytes )
        {
            usedNativeMemory.add( -bytes );
        }

        @Override
        public void allocateHeap( long bytes )
        {
            usedHeapMemory.add( bytes );
        }

        @Override
        public void releaseHeap( long bytes )
        {
            usedHeapMemory.add( -bytes );
        }

        @Override
        public long heapHighWaterMark()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryTracker getScopedMemoryTracker()
        {
            throw new UnsupportedOperationException();
        }
    }
}
