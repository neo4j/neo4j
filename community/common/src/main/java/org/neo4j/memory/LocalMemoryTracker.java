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

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static org.neo4j.memory.MemoryPools.NO_TRACKING;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requireNonNegative;
import static org.neo4j.util.Preconditions.requirePositive;

/**
 * Memory allocation tracker that can be used in local context that required
 * tracking of memory that is independent from global. You can impose a limit
 * on the total number of allocated bytes.
 * <p>
 * To reduce contention on the parent tracker, locally reserved bytes are batched
 * from the parent to a local pool. Once the pool is used up, new bytes will be
 * reserved. Calling {@link #reset()} will give back all the reserved bytes to
 * the parent. Forgetting to call this will "leak" bytes and starve the database
 * of allocations.
 */
public class LocalMemoryTracker implements MemoryTracker
{
    private static final long NO_LIMIT = Long.MAX_VALUE;
    private static final long DEFAULT_RESERVE = 1024;

    /**
     * Imposes limits on a {@link MemoryGroup} level, e.g. global maximum transactions size
     */
    private final MemoryPool memoryGroupPool;

    /**
     * A per tracker limit.
     */
    private final long localHeapBytesLimit;

    /**
     * Number of bytes we are allowed to use on the heap. If this run out, we need to reserve more from the parent.
     */
    private long localHeapPool;

    /**
     * The current size of the tracked heap
     */
    private long allocatedBytesHeap;

    /**
     * The currently allocated off heap
     */
    private long allocatedBytesDirect;

    /**
     * The heap high water mark, i.e. the maximum observed alloced heap bytes
     */
    private long heapHighWaterMark;

    public LocalMemoryTracker()
    {
        this( NO_TRACKING, NO_LIMIT, DEFAULT_RESERVE );
    }

    public LocalMemoryTracker( MemoryPool memoryGroupPool )
    {
        this( memoryGroupPool, NO_LIMIT, DEFAULT_RESERVE );
    }

    public LocalMemoryTracker( MemoryPool memoryGroupPool, long localHeapBytesLimit, long initialReservedBytes )
    {
        this.memoryGroupPool = requireNonNull( memoryGroupPool );
        this.localHeapBytesLimit = localHeapBytesLimit == 0 ? NO_LIMIT : requireNonNegative( localHeapBytesLimit );
        reserveHeap( initialReservedBytes );
    }

    @Override
    public void allocateDirect( long bytes )
    {
        this.allocatedBytesDirect += bytes;
    }

    @Override
    public void releaseDirect( long bytes )
    {
        this.allocatedBytesDirect -= bytes;
    }

    @Override
    public void allocateHeap( long bytes )
    {
        if ( bytes == 0 )
        {
            return;
        }
        requirePositive( bytes );

        allocatedBytesHeap += bytes;

        if ( allocatedBytesHeap > localHeapBytesLimit )
        {
            throw new HeapMemoryLimitExceeded( bytes, localHeapBytesLimit, allocatedBytesHeap - bytes );
        }

        if ( allocatedBytesHeap > heapHighWaterMark )
        {
            heapHighWaterMark = allocatedBytesHeap;
        }

        if ( allocatedBytesHeap > localHeapPool )
        {
            long grab = max( bytes, localHeapPool ); // TODO: try different strategies, e.g. grow factor, static increment, etc... For now we double
            reserveHeap( grab );
        }
    }

    @Override
    public void releaseHeap( long bytes )
    {
        requireNonNegative( bytes );
        allocatedBytesHeap -= bytes;
    }

    @Override
    public long heapHighWaterMark()
    {
        return heapHighWaterMark;
    }

    /**
     * @return number of used bytes.
     */
    @Override
    public long usedDirectMemory()
    {
        return allocatedBytesDirect;
    }

    @Override
    public long estimatedHeapMemory()
    {
        return allocatedBytesHeap;
    }

    @Override
    public void reset()
    {
        checkState( allocatedBytesDirect == 0, "Potential direct memory leak" );
        memoryGroupPool.release( localHeapPool );
        localHeapPool = 0;
        allocatedBytesHeap = 0;
        heapHighWaterMark = 0;
    }

    /**
     * Will reserve heap on the parent tracker.
     *
     * @param size heap space to reserve for the local pool
     * @throws HeapMemoryLimitExceeded if not enough free memory
     */
    private void reserveHeap( long size )
    {
        memoryGroupPool.reserve( size );
        localHeapPool += size;
    }
}
