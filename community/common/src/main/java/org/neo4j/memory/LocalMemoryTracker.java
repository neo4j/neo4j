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
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requireNonNegative;
import static org.neo4j.util.Preconditions.requirePositive;

/**
 * Memory allocation tracker that can be used in local context that required
 * tracking of memory that is independent from global.
 */
public class LocalMemoryTracker implements MemoryTracker
{
    private static final long NO_LIMIT = Long.MAX_VALUE;
    private static final long DEFAULT_RESERVE = 1024;

    private final MemoryTracker parent;

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

    public LocalMemoryTracker()
    {
        this( EmptyMemoryTracker.INSTANCE, NO_LIMIT, DEFAULT_RESERVE );
    }

    public LocalMemoryTracker( MemoryTracker parent )
    {
        this( parent, NO_LIMIT, DEFAULT_RESERVE );
    }

    LocalMemoryTracker( MemoryTracker parent, long localHeapBytesLimit, long initialReservedBytes )
    {
        this.parent = requireNonNull( parent );
        this.localHeapBytesLimit = requirePositive( localHeapBytesLimit );
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
        parent.releaseHeap( localHeapPool );
        localHeapPool = 0; // TODO: reset to initial allocation?
        allocatedBytesHeap = 0;
    }

    /**
     * Will reserve heap on the parent tracker.
     *
     * @param size heap space to reserve for the local pool
     * @throws RuntimeException TODO: which exception?
     */
    private void reserveHeap( long size )
    {
        parent.allocateHeap( size );
        localHeapPool += size;
    }
}
