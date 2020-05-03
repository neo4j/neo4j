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
package org.neo4j.io.memory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.memory.MemoryTracker;

import static java.lang.Math.toIntExact;

public final class ByteBuffers
{
    private ByteBuffers()
    {
    }

    /**
     * Allocate on heap byte buffer with default byte order
     * @param capacity byte buffer capacity
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return byte buffer with requested size
     */
    public static ByteBuffer allocate( int capacity, MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( capacity );
        return ByteBuffer.allocate( capacity );
    }

    /**
     * Allocate on heap byte buffer with requested byte order
     * @param capacity byte buffer capacity
     * @param order byte buffer order
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return byte buffer with requested size
     */
    public static ByteBuffer allocate( int capacity, ByteOrder order, MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( capacity );
        return ByteBuffer.allocate( capacity ).order( order );
    }

    /**
     * Allocate on heap byte buffer with default byte order
     * @param capacity byte buffer capacity
     * @param capacityUnit byte buffer capacity unit
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return byte buffer with requested size
     */
    public static ByteBuffer allocate( int capacity, ByteUnit capacityUnit, MemoryTracker memoryTracker )
    {
        int bufferCapacity = toIntExact( capacityUnit.toBytes( capacity ) );
        memoryTracker.allocateHeap( bufferCapacity );
        return ByteBuffer.allocate( bufferCapacity );
    }

    /**
     * Allocate direct byte buffer with default byte order
     *
     * Allocated memory will be tracked by global memory allocator.
     * @param capacity byte buffer capacity
     * @return byte buffer with requested size
     */
    public static ByteBuffer allocateDirect( int capacity, MemoryTracker memoryTracker )
    {
        return UnsafeUtil.allocateByteBuffer( capacity, memoryTracker );
    }

    /**
     * Release all the memory that was allocated for the buffer in case its native.
     * Noop for on heap buffers
     * @param byteBuffer byte buffer to release
     */
    public static void releaseBuffer( ByteBuffer byteBuffer, MemoryTracker memoryTracker )
    {
        if ( !byteBuffer.isDirect() )
        {
            memoryTracker.releaseHeap( byteBuffer.capacity() );
            return;
        }
        UnsafeUtil.freeByteBuffer( byteBuffer, memoryTracker );
    }
}
