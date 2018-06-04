/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.SynchronizedLongObjectMap;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.neo4j.io.ByteUnit;
import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.neo4j.util.Preconditions.requirePositive;

/**
 * Block allocator that caches freed blocks matching following criteria:
 * <ul>
 *     <li>Aligned size (without alignment padding) must be power of 2
 *     <li>Unaligned size (with alignment padding) must be less or equal to {@link #MAX_CACHEABLE_SIZE}
 * </ul>
 *
 * This class can store at most {@link #MAX_CACHED_BLOCKS} blocks of each cacheable size.
 *
 * This class is thread safe.
 */
public class CachingOffHeapBlockAllocator implements OffHeapBlockAllocator
{
    /**
     * Max size of cached blocks including alignment padding.
     */
    private static final long MAX_CACHEABLE_SIZE = ByteUnit.kibiBytes( 512 ) + Long.BYTES - 1;

    /**
     * Max number of blocks of each size to store.
     */
    private static final int MAX_CACHED_BLOCKS = 128;

    private final SynchronizedLongObjectMap<BlockingQueue<MemoryBlock>> pool = new SynchronizedLongObjectMap<>( new LongObjectHashMap<>() );
    private volatile boolean released;

    @Override
    public MemoryBlock allocate( long size, MemoryAllocationTracker tracker )
    {
        requirePositive( size );
        if ( size > MAX_CACHEABLE_SIZE || Long.bitCount( size ) > 1 )
        {
            return allocateNew( size, tracker );
        }

        final BlockingQueue<MemoryBlock> cached = pool.getIfAbsentPut( size, () -> new ArrayBlockingQueue<>( MAX_CACHED_BLOCKS ) );

        MemoryBlock block = cached.poll();
        if ( block == null )
        {
            block = allocateNew( size, tracker );
        }
        else
        {
            tracker.allocated( block.unalignedSize );
        }
        UnsafeUtil.setMemory( block.unalignedAddr, block.unalignedSize, (byte) 0 );
        return block;
    }

    @Override
    public void free( MemoryBlock block, MemoryAllocationTracker tracker )
    {
        if ( released || block.size > MAX_CACHEABLE_SIZE || Long.bitCount( block.size ) > 1 )
        {
            UnsafeUtil.free( block.unalignedAddr, block.unalignedSize, tracker );
            return;
        }

        final BlockingQueue<MemoryBlock> cached = pool.getIfAbsentPut( block.size, () -> new ArrayBlockingQueue<>( MAX_CACHED_BLOCKS ) );
        if ( cached.offer( block ) )
        {
            tracker.deallocated( block.unalignedSize );
        }
        else
        {
            UnsafeUtil.free( block.unalignedAddr, block.unalignedSize, tracker );
        }
    }

    @Override
    public void release()
    {
        released = true;
        pool.forEach( cached ->
        {
            cached.forEach( block -> UnsafeUtil.free( block.unalignedAddr, block.unalignedSize ) );
        } );
    }

    private static MemoryBlock allocateNew( long size, MemoryAllocationTracker tracker )
    {
        final long unalignedSize = requirePositive( size ) + Long.BYTES - 1;
        final long unalignedAddr = UnsafeUtil.allocateMemory( unalignedSize, tracker );
        final long addr = UnsafeUtil.alignedMemory( unalignedAddr, Long.BYTES );
        UnsafeUtil.setMemory( unalignedAddr, unalignedSize, (byte) 0 );
        return new MemoryBlock( addr, size, unalignedAddr, unalignedSize );
    }
}
