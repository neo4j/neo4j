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
package org.neo4j.io.mem;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Objects;

import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.util.FeatureToggles.getInteger;

/**
 * This memory allocator is allocating memory in large segments, called "grabs", and the memory returned by the memory
 * manager is page aligned, and plays well with transparent huge pages and other operating system optimisations.
 */
public final class GrabAllocator implements MemoryAllocator
{
    private static final Object globalCleanerInstance = globalCleaner();

    private final Grabs grabs;
    @SuppressWarnings( {"unused", "FieldCanBeLocal"} )
    private final Object cleaner;
    private final MethodHandle cleanHandle;

    /**
     * Create a new GrabAllocator that will allocate the given amount of memory, to pointers that are aligned to the
     * given alignment size.
     * @param expectedMaxMemory The maximum amount of memory that this memory manager is expected to allocate. The
     * actual amount of memory used can end up greater than this value, if some of it gets wasted on alignment padding.
     * @param memoryTracker memory usage tracker
     */
    GrabAllocator( long expectedMaxMemory, MemoryAllocationTracker memoryTracker )
    {
        this.grabs = new Grabs( expectedMaxMemory, memoryTracker );
        try
        {
            CleanerHandles handles = findCleanerHandles();
            this.cleaner = handles.creator.invoke( this, new GrabsDeallocator( grabs ) );
            this.cleanHandle = handles.cleaner;
        }
        catch ( Throwable throwable )
        {
            throw new LinkageError( "Unable to instantiate cleaner", throwable );
        }
    }

    @Override
    public synchronized long usedMemory()
    {
        return grabs.usedMemory();
    }

    @Override
    public synchronized long availableMemory()
    {
        return grabs.availableMemory();
    }

    @Override
    public synchronized long allocateAligned( long bytes, long alignment )
    {
        return grabs.allocateAligned( bytes, alignment );
    }

    @Override
    public void close()
    {
        try
        {
            cleanHandle.invoke( cleaner );
        }
        catch ( Throwable throwable )
        {
            throw new LinkageError( "Unable to clean cleaner.", throwable );
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

    private static final class Grabs
    {
        /**
         * The amount of memory, in bytes, to grab in each Grab.
         */
        private static final long GRAB_SIZE = getInteger( GrabAllocator.class, "GRAB_SIZE", (int) kibiBytes( 512 ) );

        private final MemoryAllocationTracker memoryTracker;
        private long expectedMaxMemory;
        private Grab head;

        Grabs( long expectedMaxMemory, MemoryAllocationTracker memoryTracker )
        {
            this.expectedMaxMemory = expectedMaxMemory;
            this.memoryTracker = memoryTracker;
        }

        long usedMemory()
        {
            long sum = 0;
            Grab grab = head;
            while ( grab != null )
            {
                sum += grab.nextPointer - grab.address;
                grab = grab.next;
            }
            return sum;
        }

        long availableMemory()
        {
            Grab grab = head;
            long availableInCurrentGrab = 0;
            if ( grab != null )
            {
                availableInCurrentGrab = grab.limit - grab.nextPointer;
            }
            return Math.max( expectedMaxMemory, 0L ) + availableInCurrentGrab;
        }

        public void close()
        {
            Grab current = head;

            while ( current != null )
            {
                current.free();
                current = current.next;
            }
            head = null;
        }

        long allocateAligned( long bytes, long alignment )
        {
            if ( alignment <= 0 )
            {
                throw new IllegalArgumentException( "Invalid alignment: " + alignment + ". Alignment must be positive." );
            }
            long grabSize = Math.min( GRAB_SIZE, expectedMaxMemory );
            if ( bytes + alignment - 1 > GRAB_SIZE )
            {
                // This is a huge allocation. Put it in its own grab and keep any existing grab at the head.
                grabSize = bytes;
                Grab nextGrab = head == null ? null : head.next;
                Grab allocationGrab = new Grab( nextGrab, grabSize, memoryTracker );
                if ( !allocationGrab.canAllocate( bytes, alignment ) )
                {
                    allocationGrab.free();
                    grabSize = bytes + alignment - 1;
                    allocationGrab = new Grab( nextGrab, grabSize, memoryTracker );
                }
                long allocation = allocationGrab.allocate( bytes, alignment );
                head = head == null ? allocationGrab : head.setNext( allocationGrab );
                expectedMaxMemory -= bytes;
                return allocation;
            }

            if ( head == null || !head.canAllocate( bytes, alignment ) )
            {
                if ( grabSize < bytes )
                {
                    grabSize = bytes;
                    Grab grab = new Grab( head, grabSize, memoryTracker );
                    if ( grab.canAllocate( bytes, alignment ) )
                    {
                        expectedMaxMemory -= grabSize;
                        head = grab;
                        return head.allocate( bytes, alignment );
                    }
                    grab.free();
                    grabSize = bytes + alignment - 1;
                }
                head = new Grab( head, grabSize, memoryTracker );
                expectedMaxMemory -= grabSize;
            }
            return head.allocate( bytes, alignment );
        }
    }

    private static Object globalCleaner()
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try
        {
            Class<?> newCleaner = Class.forName( "java.lang.ref.Cleaner" );
            MethodHandle createInstance = lookup.findStatic( newCleaner, "create", MethodType.methodType( newCleaner ) );
            return createInstance.invoke();
        }
        catch ( Throwable throwable )
        {
            return null;
        }
    }

    private static CleanerHandles findCleanerHandles()
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        return globalCleanerInstance == null ? findHandlesForOldCleaner( lookup ) : findHandlesForNewCleaner( lookup );
    }

    private static CleanerHandles findHandlesForNewCleaner( MethodHandles.Lookup lookup )
    {
        try
        {
            Objects.requireNonNull( globalCleanerInstance );
            Class<?> newCleaner = globalCleanerInstance.getClass();
            Class<?> newCleanable = Class.forName( "java.lang.ref.Cleaner$Cleanable" );
            MethodHandle registerHandle = findCreationMethod( "register", lookup, newCleaner );
            registerHandle = registerHandle.bindTo( globalCleanerInstance );
            return CleanerHandles.of( registerHandle, findCleanMethod( lookup, newCleanable ) );
        }
        catch ( ClassNotFoundException | NoSuchMethodException | IllegalAccessException newCleanerException )
        {
            throw new LinkageError( "Unable to find cleaner methods.", newCleanerException );
        }
    }

    private static CleanerHandles findHandlesForOldCleaner( MethodHandles.Lookup lookup )
    {
        try
        {
            Class<?> oldCleaner = Class.forName( "sun.misc.Cleaner" );
            return CleanerHandles.of( findCreationMethod( "create", lookup, oldCleaner ), findCleanMethod( lookup, oldCleaner ) );
        }
        catch ( ClassNotFoundException | NoSuchMethodException | IllegalAccessException oldCleanerException )
        {
            throw new LinkageError( "Unable to find cleaner methods.", oldCleanerException );
        }
    }

    private static MethodHandle findCleanMethod( MethodHandles.Lookup lookup, Class<?> cleaner ) throws IllegalAccessException, NoSuchMethodException
    {
        return lookup.unreflect( cleaner.getDeclaredMethod( "clean" ) );
    }

    private static MethodHandle findCreationMethod( String methodName, MethodHandles.Lookup lookup, Class<?> cleaner )
            throws IllegalAccessException, NoSuchMethodException
    {
        return lookup.unreflect( cleaner.getDeclaredMethod( methodName, Object.class, Runnable.class ) );
    }

    private static final class CleanerHandles
    {
        private final MethodHandle creator;
        private final MethodHandle cleaner;

        static CleanerHandles of( MethodHandle creator, MethodHandle cleaner )
        {
            return new CleanerHandles( creator, cleaner );
        }

        private CleanerHandles( MethodHandle creator, MethodHandle cleaner )
        {
            this.creator = creator;
            this.cleaner = cleaner;
        }
    }

    private static final class GrabsDeallocator implements Runnable
    {
        private final Grabs grabs;

        GrabsDeallocator( Grabs grabs )
        {
            this.grabs = grabs;
        }

        @Override
        public void run()
        {
            grabs.close();
        }
    }
}
