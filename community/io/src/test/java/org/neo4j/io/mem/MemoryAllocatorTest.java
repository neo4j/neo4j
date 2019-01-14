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
package org.neo4j.io.mem;

import org.junit.Test;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MemoryAllocatorTest
{
    protected static final String ONE_PAGE = PageCache.PAGE_SIZE + "";
    protected static final String EIGHT_PAGES = (8 * PageCache.PAGE_SIZE) + "";

    protected MemoryAllocator createAllocator( String expectedMaxMemory )
    {
        return MemoryAllocator.createAllocator( expectedMaxMemory, new LocalMemoryTracker() );
    }

    @Test
    public void allocatedPointerMustNotBeNull()
    {
        MemoryAllocator mman = createAllocator( EIGHT_PAGES );
        long address = mman.allocateAligned( PageCache.PAGE_SIZE, 8 );
        assertThat( address, is( not( 0L ) ) );
    }

    @Test
    public void allocatedPointerMustBePageAligned()
    {
        MemoryAllocator mman = createAllocator( EIGHT_PAGES );
        long address = mman.allocateAligned( PageCache.PAGE_SIZE, UnsafeUtil.pageSize() );
        assertThat( address % UnsafeUtil.pageSize(), is( 0L ) );
    }

    @Test
    public void allocatedPointerMustBeAlignedToArbitraryByte()
    {
        int pageSize = UnsafeUtil.pageSize();
        for ( int initialOffset = 0; initialOffset < 8; initialOffset++ )
        {
            for ( int i = 0; i < pageSize - 1; i++ )
            {
                MemoryAllocator mman = createAllocator( ONE_PAGE );
                mman.allocateAligned( initialOffset, 1 );
                long alignment = 1 + i;
                long address = mman.allocateAligned( PageCache.PAGE_SIZE, alignment );
                assertThat( "With initial offset " + initialOffset +
                            ", iteration " + i +
                            ", aligning to " + alignment +
                            " and got address " + address,
                        address % alignment, is( 0L ) );
            }
        }
    }

    @Test
    public void mustBeAbleToAllocatePastMemoryLimit()
    {
        MemoryAllocator mman = createAllocator( ONE_PAGE );
        for ( int i = 0; i < 4100; i++ )
        {
            assertThat( mman.allocateAligned( 1, 2 ) % 2, is( 0L ) );
        }
        // Also asserts that no OutOfMemoryError is thrown.
    }

    @Test
    public void allocatedPointersMustBeAlignedPastMemoryLimit()
    {
        MemoryAllocator mman = createAllocator( ONE_PAGE );
        for ( int i = 0; i < 4100; i++ )
        {
            assertThat( mman.allocateAligned( 1, 2 ) % 2, is( 0L ) );
        }

        int pageSize = UnsafeUtil.pageSize();
        for ( int i = 0; i < pageSize - 1; i++ )
        {
            int alignment = pageSize - i;
            long address = mman.allocateAligned( PageCache.PAGE_SIZE, alignment );
            assertThat( "iteration " + i + ", aligning to " + alignment,  address % alignment, is( 0L ) );
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void alignmentCannotBeZero()
    {
        createAllocator( ONE_PAGE ).allocateAligned( 8, 0 );
    }

    @Test
    public void mustBeAbleToAllocateSlabsLargerThanGrabSize()
    {
        MemoryAllocator mman = createAllocator( "2 MiB" );
        long page1 = mman.allocateAligned( UnsafeUtil.pageSize(), 1 );
        long largeBlock = mman.allocateAligned( 1024 * 1024, 1 ); // 1 MiB
        long page2 = mman.allocateAligned( UnsafeUtil.pageSize(), 1 );
        assertThat( page1, is( not( 0L ) ) );
        assertThat( largeBlock, is( not( 0L ) ) );
        assertThat( page2, is( not( 0L ) ) );
    }

    @Test
    public void allocatingMustIncreaseMemoryUsedAndDecreaseAvailableMemory()
    {
        MemoryAllocator mman = createAllocator( ONE_PAGE );
        // We haven't allocated anything, so usedMemory should be zero, and the available memory should be the
        // initial capacity.
        assertThat( mman.usedMemory(), is( 0L ) );
        assertThat( mman.availableMemory(), is( (long) PageCache.PAGE_SIZE ) );

        // Allocate 32 bytes of unaligned memory. Ideally there would be no memory wasted on this allocation,
        // but in principle we cannot rule it out.
        mman.allocateAligned( 32, 1 );
        assertThat( mman.usedMemory(), is( greaterThanOrEqualTo( 32L ) ) );
        assertThat( mman.availableMemory(), is( lessThanOrEqualTo( PageCache.PAGE_SIZE - 32L ) ) );

        // Allocate another 32 bytes of unaligned memory.
        mman.allocateAligned( 32, 1 );
        assertThat( mman.usedMemory(), is( greaterThanOrEqualTo( 64L ) ) );
        assertThat( mman.availableMemory(), is( lessThanOrEqualTo( PageCache.PAGE_SIZE - 64L ) ) );

        // Allocate 1 byte to throw off any subsequent accidental alignment.
        mman.allocateAligned( 1, 1 );

        // Allocate 32 bytes memory, but this time it is aligned to a 16 byte boundary.
        mman.allocateAligned( 32, 16 );
        // Don't count the 16 byte alignment in our assertions since we might already be accidentally aligned.
        assertThat( mman.usedMemory(), is( greaterThanOrEqualTo( 97L ) ) );
        assertThat( mman.availableMemory(), is( lessThanOrEqualTo( PageCache.PAGE_SIZE - 97L ) ) );
    }

    @Test
    public void trackMemoryAllocations() throws Throwable
    {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        GrabAllocator allocator = (GrabAllocator) MemoryAllocator.createAllocator( "2m", memoryTracker );

        assertEquals( 0, memoryTracker.usedDirectMemory() );

        long pointer = allocator.allocateAligned( ByteUnit.mebiBytes( 1 ), 1 );

        assertEquals( ByteUnit.mebiBytes( 1 ), memoryTracker.usedDirectMemory() );

        //noinspection FinalizeCalledExplicitly
        allocator.finalize();
        assertEquals( 0, memoryTracker.usedDirectMemory() );
    }
}
