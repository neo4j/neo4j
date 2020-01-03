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
package org.neo4j.kernel.impl.util.collection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.util.collection.OffHeapBlockAllocator.MemoryBlock;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryAllocationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CachingOffHeapBlockAllocatorTest
{
    private static final int CACHE_SIZE = 4;
    private static final int MAX_CACHEABLE_BLOCK_SIZE = 128;

    private final MemoryAllocationTracker memoryTracker = new LocalMemoryTracker();
    private final CachingOffHeapBlockAllocator allocator = spy( new CachingOffHeapBlockAllocator( MAX_CACHEABLE_BLOCK_SIZE, CACHE_SIZE ) );

    @AfterEach
    void afterEach()
    {
        allocator.release();
        assertEquals( 0, memoryTracker.usedDirectMemory(), "Native memory is leaking" );
    }

    @Test
    void allocateAfterRelease()
    {
        allocator.release();
        assertThrows( IllegalStateException.class, () -> allocator.allocate( 128, memoryTracker ) );
    }

    @Test
    void freeAfterRelease()
    {
        final MemoryBlock block = allocator.allocate( 128, memoryTracker );
        allocator.release();
        allocator.free( block, memoryTracker );
        verify( allocator ).doFree( eq( block ), any() );
    }

    @Test
    void allocateAndFree()
    {
        final MemoryBlock block1 = allocator.allocate( 128, memoryTracker );
        assertEquals( block1.size, 128 );
        assertEquals( 128 + Long.BYTES - 1, block1.unalignedSize );
        assertEquals( block1.unalignedSize, memoryTracker.usedDirectMemory() );

        final MemoryBlock block2 = allocator.allocate( 256, memoryTracker );
        assertEquals( block2.size, 256 );
        assertEquals( 256 + Long.BYTES - 1, block2.unalignedSize );
        assertEquals( block1.unalignedSize + block2.unalignedSize, memoryTracker.usedDirectMemory() );

        allocator.free( block1, memoryTracker );
        allocator.free( block2, memoryTracker );
        assertEquals( 0, memoryTracker.usedDirectMemory() );
    }

    @ParameterizedTest
    @ValueSource( longs = {10, 100, 256} )
    void allocateNonCacheableSize( long bytes )
    {
        final MemoryBlock block1 = allocator.allocate( bytes, memoryTracker );
        allocator.free( block1, memoryTracker );

        final MemoryBlock block2 = allocator.allocate( bytes, memoryTracker );
        allocator.free( block2, memoryTracker );

        verify( allocator, times( 2 ) ).allocateNew( eq( bytes ), any() );
        verify( allocator ).doFree( eq( block1 ), any() );
        verify( allocator ).doFree( eq( block2 ), any() );
        assertEquals( 0, memoryTracker.usedDirectMemory() );
    }

    @ParameterizedTest
    @ValueSource( longs = {8, 64, 128} )
    void allocateCacheableSize( long bytes )
    {
        final MemoryBlock block1 = allocator.allocate( bytes, memoryTracker );
        allocator.free( block1, memoryTracker );

        final MemoryBlock block2 = allocator.allocate( bytes, memoryTracker );
        allocator.free( block2, memoryTracker );

        verify( allocator ).allocateNew( eq( bytes ), any() );
        verify( allocator, never() ).doFree( any(), any() );
        assertEquals( 0, memoryTracker.usedDirectMemory() );
    }

    @Test
    void cacheCapacityPerBlockSize()
    {
        final int EXTRA = 3;
        final List<MemoryBlock> blocks64 = new ArrayList<>();
        final List<MemoryBlock> blocks128 = new ArrayList<>();
        for ( int i = 0; i < CACHE_SIZE + EXTRA; i++ )
        {
            blocks64.add( allocator.allocate( 64, memoryTracker ) );
            blocks128.add( allocator.allocate( 128, memoryTracker ) );
        }

        verify( allocator, times( CACHE_SIZE + EXTRA ) ).allocateNew( eq( 64L ), any() );
        verify( allocator, times( CACHE_SIZE + EXTRA ) ).allocateNew( eq( 128L ), any() );
        assertEquals( (CACHE_SIZE + EXTRA) * (64 + 128 + 2 * (Long.BYTES - 1)), memoryTracker.usedDirectMemory() );

        blocks64.forEach( it -> allocator.free( it, memoryTracker ) );
        assertEquals( (CACHE_SIZE + EXTRA) * (128 + Long.BYTES - 1), memoryTracker.usedDirectMemory() );

        blocks128.forEach( it -> allocator.free( it, memoryTracker ) );
        assertEquals( 0, memoryTracker.usedDirectMemory() );

        verify( allocator, times( EXTRA * 2 ) ).doFree( any(), any() );
    }
}
