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

/**
 * Wrap a memory tracker so every tracked sub-allocation can be closed in a single call.
 * Can be useful for collections when the items are tracked, to avoid iterating over all
 * of the elements and releasing them individual.
 */
public class ScopedMemoryTracker implements MemoryTracker
{
    private final MemoryTracker delegate;
    private long trackedNative;
    private long trackedHeap;
    private boolean isClosed;

    public ScopedMemoryTracker( MemoryTracker delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public long usedNativeMemory()
    {
        return trackedNative;
    }

    @Override
    public long estimatedHeapMemory()
    {
        return trackedHeap;
    }

    @Override
    public void allocateNative( long bytes )
    {
        throwIfClosed();
        delegate.allocateNative( bytes );
        trackedNative += bytes;
    }

    @Override
    public void releaseNative( long bytes )
    {
        throwIfClosed();
        delegate.releaseNative( bytes );
        trackedNative -= bytes;
    }

    @Override
    public void allocateHeap( long bytes )
    {
        throwIfClosed();
        delegate.allocateHeap( bytes );
        trackedHeap += bytes;
    }

    @Override
    public void releaseHeap( long bytes )
    {
        throwIfClosed();
        delegate.releaseHeap( bytes );
        trackedHeap -= bytes;
    }

    private void throwIfClosed()
    {
        if ( isClosed )
        {
            throw new IllegalStateException( "Should not use a closed ScopedMemoryTracker" );
        }
    }

    @Override
    public long heapHighWaterMark()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset()
    {
        delegate.releaseNative( trackedNative );
        delegate.releaseHeap( trackedHeap );
        trackedNative = 0;
        trackedHeap = 0;
    }

    @Override
    public void close()
    {
        // On a parent ScopedMemoryTracker, only release memory if that parent was not already closed.
        if ( !(delegate instanceof ScopedMemoryTracker) || !((ScopedMemoryTracker) delegate).isClosed )
        {
            delegate.releaseNative( trackedNative );
            delegate.releaseHeap( trackedHeap );
        }
        trackedNative = 0;
        trackedHeap = 0;
        isClosed = true;
    }

    @Override
    public MemoryTracker getScopedMemoryTracker()
    {
        return new ScopedMemoryTracker( this );
    }
}
