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

import org.neo4j.io.ByteUnit;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.io.memory.ByteBuffers.allocate;
import static org.neo4j.io.memory.ByteBuffers.releaseBuffer;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

/**
 * Autocloseable container for heap byte buffer that will release memory from memory tracker on close.
 */
public final class HeapScopedBuffer implements ScopedBuffer
{
    public static final HeapScopedBuffer EMPTY_BUFFER = new HeapScopedBuffer( allocate( 0, INSTANCE ).asReadOnlyBuffer(), INSTANCE );
    private final ByteBuffer buffer;
    private final MemoryTracker memoryTracker;
    private boolean closed;

    public HeapScopedBuffer( int capacity, MemoryTracker memoryTracker )
    {
        this( allocate( capacity, memoryTracker ), memoryTracker );
    }

    public HeapScopedBuffer( int capacity, ByteUnit byteUnit, MemoryTracker memoryTracker )
    {
        this( allocate( capacity, byteUnit, memoryTracker ), memoryTracker );
    }

    private HeapScopedBuffer( ByteBuffer buffer, MemoryTracker memoryTracker )
    {
        this.buffer = buffer;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public ByteBuffer getBuffer()
    {
        return buffer;
    }

    @Override
    public void close()
    {
        if ( !closed )
        {
            releaseBuffer( buffer, memoryTracker );
            closed = true;
        }
    }
}
