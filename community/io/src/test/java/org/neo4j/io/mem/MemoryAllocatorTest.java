/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class MemoryAllocatorTest
{
    protected MemoryAllocator createAllocator( long expectedMaxMemory, long alignment )
    {
        return MemoryAllocator.createAllocator( expectedMaxMemory, alignment );
    }

    @Test
    public void allocatedPointerMustNotBeNull() throws Exception
    {
        MemoryAllocator mman = createAllocator( 8 * PageCache.PAGE_SIZE, 8 );
        long address = mman.allocateAligned( PageCache.PAGE_SIZE );
        assertThat( address, is( not( 0L ) ) );
    }

    @Test
    public void allocatedPointerMustBePageAligned() throws Exception
    {
        MemoryAllocator mman = createAllocator( 8 * PageCache.PAGE_SIZE, UnsafeUtil.pageSize() );
        long address = mman.allocateAligned( PageCache.PAGE_SIZE );
        assertThat( address % UnsafeUtil.pageSize(), is( 0L ) );
    }

    @Test
    public void mustBeAbleToAllocatePastMemoryLimit() throws Exception
    {
        MemoryAllocator mman = createAllocator( PageCache.PAGE_SIZE, 2 );
        for ( int i = 0; i < 4100; i++ )
        {
            assertThat( mman.allocateAligned( 1 ) % 2, is( 0L ) );
        }
        // Also asserts that no OutOfMemoryError is thrown.
    }

    @Test( expected = IllegalArgumentException.class )
    public void alignmentCannotBeZero() throws Exception
    {
        createAllocator( PageCache.PAGE_SIZE, 0 );
    }

    @Test
    public void mustBeAbleToAllocateSlabsLargerThanGrabSize() throws Exception
    {
        MemoryAllocator mman = createAllocator( 32 * 1024 * 1024, 1 );
        long page1 = mman.allocateAligned( UnsafeUtil.pageSize() );
        long largeBlock = mman.allocateAligned( 1024 * 1024 ); // 1 MiB
        long page2 = mman.allocateAligned( UnsafeUtil.pageSize() );
        assertThat( page1, is( not( 0L ) ) );
        assertThat( largeBlock, is( not( 0L ) ) );
        assertThat( page2, is( not( 0L ) ) );
    }

    @Test
    public void allocatingMustIncreaseMemoryUsedAndDecreaseAvailableMemory() throws Exception
    {
        MemoryAllocator mman = createAllocator( PageCache.PAGE_SIZE, 1 );
        assertThat( mman.usedMemory(), is( 0L ) );
        assertThat( mman.availableMemory(), is( (long) PageCache.PAGE_SIZE ) );
        assertThat( mman.usedMemory() + mman.availableMemory(), is( (long) PageCache.PAGE_SIZE ) );

        mman.allocateAligned( 32 );
        assertThat( mman.usedMemory(), is( greaterThanOrEqualTo( 32L ) ) );
        assertThat( mman.availableMemory(), is( lessThanOrEqualTo( PageCache.PAGE_SIZE - 32L ) ) );
        assertThat( mman.usedMemory() + mman.availableMemory(), is( (long) PageCache.PAGE_SIZE ) );

        mman.allocateAligned( 32 );
        assertThat( mman.usedMemory(), is( greaterThanOrEqualTo( 64L ) ) );
        assertThat( mman.availableMemory(), is( lessThanOrEqualTo( PageCache.PAGE_SIZE - 32 - 32L ) ) );
        assertThat( mman.usedMemory() + mman.availableMemory(), is( (long) PageCache.PAGE_SIZE ) );
    }
}
