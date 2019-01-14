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

import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.unsafe.impl.internal.dragons.NativeMemoryAllocationRefusedError;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.util.FeatureToggles.getInteger;

/**
 * This memory allocator is allocating memory in large segments, called "grabs", and the memory returned by the memory
 * manager is page aligned, and plays well with transparent huge pages and other operating system optimisations.
 */
public final class GrabAllocator implements MemoryAllocator
{
    /**
     * The amount of memory, in bytes, to grab in each Grab.
     */
    private static final long GRAB_SIZE = getInteger( GrabAllocator.class, "GRAB_SIZE", (int) kibiBytes( 512 ) );

    /**
     * The amount of memory that this memory manager can still allocate.
     */
    private long memoryReserve;
    private final MemoryAllocationTracker memoryTracker;

    private Grab grabs;

    /**
     * Create a new GrabAllocator that will allocate the given amount of memory, to pointers that are aligned to the
     * given alignment size.
     * @param expectedMaxMemory The maximum amount of memory that this memory manager is expected to allocate. The
     * actual amount of memory used can end up greater than this value, if some of it gets wasted on alignment padding.
     * @param memoryTracker memory usage tracker
     */
    GrabAllocator( long expectedMaxMemory, MemoryAllocationTracker memoryTracker )
    {
        this.memoryReserve = expectedMaxMemory;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public synchronized long usedMemory()
    {
        long sum = 0;
        Grab grab = grabs;
        while ( grab != null )
        {
            sum += grab.nextPointer - grab.address;
            grab = grab.next;
        }
        return sum;
    }

    @Override
    public synchronized long availableMemory()
    {
        Grab grab = grabs;
        long availableInCurrentGrab = 0;
        if ( grab != null )
        {
            availableInCurrentGrab = grab.limit - grab.nextPointer;
        }
        return Math.max( memoryReserve, 0L ) + availableInCurrentGrab;
    }

    @Override
    public synchronized long allocateAligned( long bytes, long alignment )
    {
        if ( alignment <= 0 )
        {
            throw new IllegalArgumentException( "Invalid alignment: " + alignment + ". Alignment must be positive." );
        }
        long grabSize = Math.min( GRAB_SIZE, memoryReserve );
        if ( bytes > GRAB_SIZE )
        {
            // This is a huge allocation. Put it in its own grab and keep any existing grab at the head.
            grabSize = bytes;
            Grab nextGrab = grabs == null ? null : grabs.next;
            Grab allocationGrab = new Grab( nextGrab, grabSize, memoryTracker );
            if ( !allocationGrab.canAllocate( bytes, alignment ) )
            {
                allocationGrab.free();
                grabSize = bytes + alignment;
                allocationGrab = new Grab( nextGrab, grabSize, memoryTracker );
            }
            long allocation = allocationGrab.allocate( bytes, alignment );
            grabs = grabs == null ? allocationGrab : grabs.setNext( allocationGrab );
            memoryReserve -= bytes;
            return allocation;
        }

        if ( grabs == null || !grabs.canAllocate( bytes, alignment ) )
        {
            if ( grabSize < bytes )
            {
                grabSize = bytes;
                Grab grab = new Grab( grabs, grabSize, memoryTracker );
                if ( grab.canAllocate( bytes, alignment ) )
                {
                    memoryReserve -= grabSize;
                    grabs = grab;
                    return grabs.allocate( bytes, alignment );
                }
                grab.free();
                grabSize = bytes + alignment;
            }
            grabs = new Grab( grabs, grabSize, memoryTracker );
            memoryReserve -= grabSize;
        }
        return grabs.allocate( bytes, alignment );
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
        private final MemoryAllocationTracker memoryTracker;
        private long nextPointer;

        Grab( Grab next, long size, MemoryAllocationTracker memoryTracker )
        {
            this.next = next;
            this.address = UnsafeUtil.allocateMemory( size, memoryTracker );
            this.limit = address + size;
            this.memoryTracker = memoryTracker;
            nextPointer = address;
        }

        Grab( Grab next, long address, long limit, long nextPointer, MemoryAllocationTracker memoryTracker )
        {
            this.next = next;
            this.address = address;
            this.limit = limit;
            this.nextPointer = nextPointer;
            this.memoryTracker = memoryTracker;
        }

        private long nextAligned( long pointer, long alignment )
        {
            if ( alignment == 1 )
            {
                return pointer;
            }
            long off = pointer % alignment;
            if ( off == 0 )
            {
                return pointer;
            }
            return pointer + (alignment - off);
        }

        long allocate( long bytes, long alignment )
        {
            long allocation = nextAligned( nextPointer, alignment );
            nextPointer = allocation + bytes;
            return allocation;
        }

        void free()
        {
            UnsafeUtil.free( address, limit - address, memoryTracker );
        }

        boolean canAllocate( long bytes, long alignment )
        {
            return nextAligned( nextPointer, alignment ) + bytes <= limit;
        }

        Grab setNext( Grab grab )
        {
            return new Grab( grab, address, limit, nextPointer, memoryTracker );
        }

        @Override
        public String toString()
        {
            long size = limit - address;
            long reserve = nextPointer > limit ? 0 : limit - nextPointer;
            double use = (1.0 - reserve / ((double) size)) * 100.0;
            return String.format( "Grab[size = %d bytes, reserve = %d bytes, use = %5.2f %%]", size, reserve, use );
        }
    }
}
