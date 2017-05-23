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
package org.neo4j.unsafe.impl.internal.dragons;

/**
 * The memory manager is simple: it only allocates memory, until it itself is finalizable and frees it all in one go.
 *
 * The memory is allocated in large segments, called "grabs", and the memory returned by the memory manager is page
 * aligned, and plays well with transparent huge pages and other operating system optimisations.
 *
 * The memory manager assumes that the memory claimed from it is evenly divisible in units of pages.
 */
public final class MemoryManager
{
    /**
     * The amount of memory, in bytes, to grab in each Grab.
     */
    private static final long GRAB_SIZE = FeatureToggles.getInteger( MemoryManager.class, "GRAB_SIZE", 512 * 1024 ); // 512 KiB

    /**
     * The amount of memory that this memory manager can still allocate.
     */
    private long memoryReserve;
    private final long alignment;

    private Grab grabs;

    /**
     * Create a new MemoryManager that will allocate the given amount of memory, to pointers that are aligned to the
     * given alignment size.
     * @param expectedMaxMemory The maximum amount of memory that this memory manager is expected to allocate. The
     * actual amount of memory used can end up greater than this value, if some of it gets wasted on alignment padding.
     * @param alignment The byte multiple that the allocated pointers have to be aligned at.
     */
    public MemoryManager( long expectedMaxMemory, long alignment )
    {
        if ( alignment == 0 )
        {
            throw new IllegalArgumentException( "Alignment cannot be zero" );
        }
        this.memoryReserve = expectedMaxMemory;
        this.alignment = alignment;
    }

    public synchronized long sumUsedMemory()
    {
        long sum = 0;
        Grab grab = grabs;
        while ( grab != null )
        {
            sum += grab.nextAlignedPointer - grab.address;
            grab = grab.next;
        }
        return sum;
    }

    /**
     * Allocate a contiguous, aligned region of memory of the given size in bytes.
     * @param bytes the number of bytes to allocate.
     * @return A pointer to the allocated memory.
     */
    public synchronized long allocateAligned( long bytes )
    {
        if ( bytes > GRAB_SIZE )
        {
            // This is a huge allocation. Put it in its own grab and keep any existing grab at the head.
            Grab nextGrab = grabs == null ? null : grabs.next;
            Grab allocationGrab = new Grab( nextGrab, bytes, alignment );
            if ( !allocationGrab.canAllocate( bytes ) )
            {
                allocationGrab.free();
                allocationGrab = new Grab( nextGrab, bytes + alignment, alignment );
            }
            long allocation = allocationGrab.allocate( bytes );
            grabs = grabs == null ? allocationGrab : grabs.setNext( allocationGrab );
            memoryReserve -= bytes;
            return allocation;
        }

        if ( grabs == null || !grabs.canAllocate( bytes ) )
        {
            long desiredGrabSize = Math.min( GRAB_SIZE, memoryReserve );
            if ( desiredGrabSize < bytes )
            {
                desiredGrabSize = bytes;
                Grab grab = new Grab( grabs, desiredGrabSize, alignment );
                if ( grab.canAllocate( bytes ) )
                {
                    memoryReserve -= desiredGrabSize;
                    grabs = grab;
                    return grabs.allocate( bytes );
                }
                grab.free();
                desiredGrabSize = bytes + alignment;
            }
            memoryReserve -= desiredGrabSize;
            grabs = new Grab( grabs, desiredGrabSize, alignment );
        }
        return grabs.allocate( bytes );
    }

    @Override
    protected synchronized void finalize() throws Throwable
    {
        super.finalize();
        Grab current = grabs;

        while ( current != null )
        {
            current.free();
            current = current.next;
        }
    }

    private static class Grab
    {
        public final Grab next;
        private final long address;
        private final long limit;
        private final long alignMask;
        private long nextAlignedPointer;

        Grab( Grab next, long size, long alignment )
        {
            this.next = next;
            this.address = UnsafeUtil.allocateMemory( size );
            this.limit = address + size;
            this.alignMask = alignment - 1;

            nextAlignedPointer = nextAligned( address );
        }

        Grab( Grab next, long address, long limit, long alignMask, long nextAlignedPointer )
        {
            this.next = next;
            this.address = address;
            this.limit = limit;
            this.alignMask = alignMask;
            this.nextAlignedPointer = nextAlignedPointer;
        }

        private long nextAligned( long pointer )
        {
            if ( (pointer & ~alignMask) == pointer )
            {
                return pointer;
            }
            return (pointer + alignMask) & ~alignMask;
        }

        long allocate( long bytes )
        {
            long allocation = nextAlignedPointer;
            nextAlignedPointer = nextAligned( nextAlignedPointer + bytes );
            return allocation;
        }

        void free()
        {
            UnsafeUtil.free( address );
        }

        boolean canAllocate( long bytes )
        {
            return nextAlignedPointer + bytes <= limit;
        }

        Grab setNext( Grab grab )
        {
            return new Grab( grab, address, limit, alignMask, nextAlignedPointer );
        }

        @Override
        public String toString()
        {
            long size = limit - address;
            long reserve = nextAlignedPointer > limit ? 0 : limit - nextAlignedPointer;
            double use = (1.0 - reserve / ((double) size)) * 100.0;
            return String.format( "Grab[size = %d bytes, reserve = %d bytes, use = %5.2f %%]", size, reserve, use );
        }
    }
}
