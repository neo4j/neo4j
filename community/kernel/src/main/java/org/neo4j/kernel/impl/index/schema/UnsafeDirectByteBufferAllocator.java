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
package org.neo4j.kernel.impl.index.schema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.unsafe.impl.internal.dragons.NativeMemoryAllocationRefusedError;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;
import org.neo4j.util.Preconditions;

/**
 * Allocates {@link ByteBuffer} instances using {@link UnsafeUtil#newDirectByteBuffer(long, int)}/{@link UnsafeUtil#initDirectByteBuffer(Object, long, int)}
 * and frees all allocated memory in {@link #close()}.
 */
public class UnsafeDirectByteBufferAllocator implements ByteBufferFactory.Allocator
{
    private final MemoryAllocationTracker memoryAllocationTracker;
    private final List<Allocation> allocations = new ArrayList<>();
    private boolean closed;

    public UnsafeDirectByteBufferAllocator( MemoryAllocationTracker memoryAllocationTracker )
    {
        this.memoryAllocationTracker = memoryAllocationTracker;
    }

    @Override
    public synchronized ByteBuffer allocate( int bufferSize )
    {
        assertOpen();
        try
        {
            long address = UnsafeUtil.allocateMemory( bufferSize, memoryAllocationTracker );
            try
            {
                ByteBuffer buffer = UnsafeUtil.newDirectByteBuffer( address, bufferSize );
                UnsafeUtil.initDirectByteBuffer( buffer, address, bufferSize );
                allocations.add( new Allocation( address, bufferSize ) );
                return buffer;
            }
            catch ( Exception e )
            {
                // What ever went wrong we can safely fall back to on-heap buffer. Free the allocated memory right away first.
                UnsafeUtil.free( address, bufferSize, memoryAllocationTracker );
                return allocateHeapBuffer( bufferSize );
            }
        }
        catch ( NativeMemoryAllocationRefusedError allocationRefusedError )
        {
            // What ever went wrong we can safely fall back to on-heap buffer.
            return allocateHeapBuffer( bufferSize );
        }
    }

    private ByteBuffer allocateHeapBuffer( int bufferSize )
    {
        return ByteBuffer.allocate( bufferSize );
    }

    @Override
    public synchronized void close()
    {
        // Idempotent close due to the way the population lifecycle works sometimes
        if ( !closed )
        {
            allocations.forEach( allocation -> UnsafeUtil.free( allocation.address, allocation.bytes, memoryAllocationTracker ) );
            closed = true;
        }
    }

    private void assertOpen()
    {
        Preconditions.checkState( !closed, "Already closed" );
    }

    private static class Allocation
    {
        private final long address;
        private final int bytes;

        Allocation( long address, int bytes )
        {
            this.address = address;
            this.bytes = bytes;
        }
    }
}
