/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.internal.dragons;

/**
 * The memory manager is simple: it only allocates memory, until it itself is finalizable and frees it all in one go.
 *
 * The memory is allocated in large segments, and the memory returned by the memory manager is page aligned, and plays
 * well with transparent huge pages and other operating system optimisations.
 *
 * The memory manager assumes that the memory claimed from it is evenly divisible in units of pages.
 */
public final class MemoryManager
{
    /**
     * The amount of memory, in bytes, to grab in each Slab.
     */
    private static final long GRAB_SIZE = FeatureToggles.getInteger( MemoryManager.class, "GRAB_SIZE", 512 * 1024 ); // 512 KiB

    /**
     * The amount of memory that this memory manager can still allocate.
     */
    private long memoryReserve;
    private final long alignment;

    private Slab slabs;

    /**
     * Create a new MemoryManager that will allocate the given amount of memory, to pointers that are aligned to the
     * given alignment size.
     * @param expectedMaxMemory The maximum amount of memory that this memory manager is expected to allocate. The
     * actual amount of memory used can end up greater than this value, if some of it gets wasted on alignment padding.
     * @param alignment The byte multiple that the allocated pointers have to be aligned at.
     */
    public MemoryManager( long expectedMaxMemory, long alignment )
    {
        this.memoryReserve = expectedMaxMemory;
        this.alignment = alignment;
    }

    /**
     * Allocate a contiguous, aligned region of memory of the given size in bytes.
     * @param bytes the number of bytes to allocate.
     * @return A pointer to the allocated memory.
     */
    public synchronized long allocateAligned( long bytes )
    {
        if ( slabs == null || !slabs.canAllocate( bytes ) )
        {
            long slabGrab = Math.min( GRAB_SIZE, memoryReserve );
            if ( slabGrab < bytes )
            {
                slabGrab = bytes;
                Slab slab = new Slab( slabs, slabGrab, alignment );
                if ( slab.canAllocate( bytes ) )
                {
                    memoryReserve -= slabGrab;
                    slabs = slab;
                    return slabs.allocate( bytes );
                }
                slab.free();
                slabGrab = bytes + alignment;
            }
            memoryReserve -= slabGrab;
            slabs = new Slab( slabs, slabGrab, alignment );
        }
        return slabs.allocate( bytes );
    }

    @Override
    protected synchronized void finalize() throws Throwable
    {
        super.finalize();
        Slab current = slabs;

        while ( current != null )
        {
            current.free();
            current = current.next;
        }
    }

    private static class Slab
    {
        public final Slab next;
        private final long address;
        private final long limit;
        private final long alignMask;
        private long nextAlignedPointer;

        public Slab( Slab next, long size, long alignment )
        {
            this.next = next;
            this.address = UnsafeUtil.allocateMemory( size );
            this.limit = address + size;
            this.alignMask = alignment - 1;

            nextAlignedPointer = nextAligned( address );
        }

        private long nextAligned( long pointer )
        {
            if ( (pointer & ~alignMask) == pointer )
            {
                return pointer;
            }
            return (pointer + alignMask) & ~alignMask;
        }

        public long allocate( long bytes )
        {
            long allocation = nextAlignedPointer;
            nextAlignedPointer = nextAligned( nextAlignedPointer + bytes );
            return allocation;
        }

        public void free()
        {
            UnsafeUtil.free( address );
        }

        public boolean canAllocate( long bytes )
        {
            return nextAlignedPointer + bytes <= limit;
        }
    }
}
