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
package org.neo4j.memory;

import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.util.Preconditions.requirePositive;

/**
 * A pool of memory that can be limited.
 * The implementation is thread-safe.
 */
abstract class MemoryPoolImpl implements MemoryPool
{
    final AtomicLong usedHeapBytes = new AtomicLong();
    final AtomicLong usedNativeBytes = new AtomicLong();

    @Override
    public long usedHeap()
    {
        return usedHeapBytes.get();
    }

    @Override
    public long usedNative()
    {
        return usedNativeBytes.get();
    }

    @Override
    public long totalUsed()
    {
        return usedHeap() + usedNative();
    }

    @Override
    public long free()
    {
        return totalSize() - totalUsed();
    }

    @Override
    public void releaseHeap( long bytes )
    {
        usedHeapBytes.addAndGet( -bytes );
    }

    @Override
    public void releaseNative( long bytes )
    {
        usedNativeBytes.addAndGet( -bytes );
    }

    static class BoundedMemoryPool extends MemoryPoolImpl
    {
        private final long maxMemory;

        BoundedMemoryPool( long maxMemory )
        {
            this.maxMemory = requirePositive( maxMemory );
        }

        @Override
        public void reserveHeap( long bytes )
        {
            reserveMemory( usedHeapBytes, bytes );
        }

        @Override
        public void reserveNative( long bytes )
        {
            reserveMemory( usedNativeBytes, bytes );
        }

        @Override
        public long totalSize()
        {
            return maxMemory;
        }

        private void reserveMemory( AtomicLong counter, long bytes )
        {
            long usedMemoryBefore;
            do
            {
                usedMemoryBefore = counter.get();
                long totalUsedMemory = totalUsed();
                long totalUsedMemoryAfter = totalUsedMemory + bytes;
                if ( totalUsedMemoryAfter > maxMemory )
                {
                    throw new HeapMemoryLimitExceeded( bytes, maxMemory, totalUsedMemory );
                }
            }
            while ( !counter.weakCompareAndSetVolatile( usedMemoryBefore, usedMemoryBefore + bytes ) );
        }
    }

    static class UnboundedMemoryPool extends MemoryPoolImpl
    {
        @Override
        public void reserveHeap( long bytes )
        {
            usedHeapBytes.addAndGet( bytes );
        }

        @Override
        public void reserveNative( long bytes )
        {
            usedNativeBytes.addAndGet( bytes );
        }

        @Override
        public long totalSize()
        {
            return Long.MAX_VALUE;
        }
    }
}
