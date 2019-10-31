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
package org.neo4j.kernel.impl.index.schema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.unsafe.NativeMemoryAllocationRefusedError;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.util.Preconditions;

/**
 * Allocates {@link ByteBuffer} instances using {@link UnsafeUtil#newDirectByteBuffer(long, int)}/{@link UnsafeUtil#initDirectByteBuffer(Object, long, int)}
 * and frees all allocated memory in {@link #close()}.
 */
public class UnsafeDirectByteBufferAllocator implements ByteBufferFactory.Allocator
{
    private final MemoryAllocationTracker memoryAllocationTracker;
    private final List<ByteBuffer> allocations = new ArrayList<>();
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
            var byteBuffer = ByteBuffers.allocateDirect( bufferSize );
            allocations.add( byteBuffer );
            memoryAllocationTracker.allocated( bufferSize );
            return byteBuffer;
        }
        catch ( NativeMemoryAllocationRefusedError allocationRefusedError )
        {
            // What ever went wrong fallback to on-heap buffer.
            return ByteBuffers.allocate( bufferSize );
        }
    }

    @Override
    public synchronized void close()
    {
        // Idempotent close due to the way the population lifecycle works sometimes
        if ( !closed )
        {
            allocations.forEach( buffer -> {
                int capacity = buffer.capacity();
                memoryAllocationTracker.deallocated( capacity );
                ByteBuffers.releaseBuffer( buffer );
            } );
            closed = true;
        }
    }

    private void assertOpen()
    {
        Preconditions.checkState( !closed, "Already closed" );
    }
}
