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

import org.jctools.queues.QueueFactory;
import org.jctools.queues.spec.ConcurrentQueueSpec;

import java.util.Queue;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.internal.helpers.Numbers.isPowerOfTwo;
import static org.neo4j.internal.helpers.Numbers.log2floor;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requirePositive;
import static org.neo4j.util.Preconditions.requirePowerOfTwo;

/**
 * Block allocator that caches freed blocks matching following criteria:
 * <ul>
 *     <li>Size must be power of 2
 *     <li>Size must be less or equal to {@link #maxCacheableBlockSize}
 * </ul>
 * <p>
 * This class is thread safe.
 */
public class CachingOffHeapBlockAllocator implements OffHeapBlockAllocator
{
    /**
     * Max size of cached blocks, a power of 2
     */
    private final long maxCacheableBlockSize;
    private volatile boolean released;
    private final Queue<MemoryBlock>[] caches;

    @VisibleForTesting
    public CachingOffHeapBlockAllocator()
    {
        this( ByteUnit.kibiBytes( 512 ), 128 );
    }

    /**
     * @param maxCacheableBlockSize Max size of cached blocks including alignment padding, must be a power of 2
     * @param maxCachedBlocks Max number of blocks of each size to store
     */
    public CachingOffHeapBlockAllocator( long maxCacheableBlockSize, int maxCachedBlocks )
    {
        requirePositive( maxCachedBlocks );
        this.maxCacheableBlockSize = requirePowerOfTwo( maxCacheableBlockSize );

        final int numOfCaches = log2floor( maxCacheableBlockSize ) + 1;
        ConcurrentQueueSpec spec = ConcurrentQueueSpec.createBoundedMpmc( maxCachedBlocks );
        //noinspection unchecked
        this.caches = new Queue[numOfCaches];
        for ( int i = 0; i < caches.length; i++ )
        {
            caches[i] = QueueFactory.newQueue( spec );
        }
    }

    @Override
    public MemoryBlock allocate( long size, MemoryAllocationTracker tracker )
    {
        requirePositive( size );
        checkState( !released, "Allocator is already released" );
        if ( notCacheable( size ) )
        {
            return allocateNew( size, tracker );
        }

        final Queue<MemoryBlock> cache = caches[log2floor( size )];
        MemoryBlock block = cache.poll();
        if ( block == null )
        {
            block = allocateNew( size, tracker );
        }
        else
        {
            tracker.allocated( block.unalignedSize );
        }
        return block;
    }

    @Override
    public void free( MemoryBlock block, MemoryAllocationTracker tracker )
    {
        if ( released || notCacheable( block.size ) )
        {
            doFree( block, tracker );
            return;
        }

        final Queue<MemoryBlock> cache = caches[log2floor( block.size )];
        if ( !cache.offer( block ) )
        {
            doFree( block, tracker );
            return;
        }

        // it is possible that allocator is released just before we put the block into queue;
        // in such case case we need to free memory right away, since release() will never be called again
        if ( released && cache.remove( block ) )
        {
            doFree( block, tracker );
            return;
        }

        tracker.deallocated( block.unalignedSize );
    }

    @Override
    public void release()
    {
        released = true;
        for ( Queue<MemoryBlock> cache : caches )
        {
            MemoryBlock block;
            while ( (block = cache.poll()) != null )
            {
                UnsafeUtil.free( block.unalignedAddr, block.unalignedSize );
            }
        }
    }

    @VisibleForTesting
    void doFree( MemoryBlock block, MemoryAllocationTracker tracker )
    {
        UnsafeUtil.free( block.unalignedAddr, block.unalignedSize, tracker );
    }

    @VisibleForTesting
    MemoryBlock allocateNew( long size, MemoryAllocationTracker tracker )
    {
        final long unalignedSize = requirePositive( size ) + Long.BYTES - 1;
        final long unalignedAddr = UnsafeUtil.allocateMemory( unalignedSize, tracker );
        final long addr = UnsafeUtil.alignedMemory( unalignedAddr, Long.BYTES );
        return new MemoryBlock( addr, size, unalignedAddr, unalignedSize );
    }

    private boolean notCacheable( long size )
    {
        return !isPowerOfTwo( size ) || size > maxCacheableBlockSize;
    }
}
