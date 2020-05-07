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

import org.neo4j.memory.MemoryTracker;

import static java.lang.Math.toIntExact;

/**
 * A life-time scope for the contained direct byte buffer.
 */
public final class NativeScopedBuffer implements ScopedBuffer
{
    private final ByteBuffer buffer;
    private final MemoryTracker memoryTracker;
    private boolean closed;

    public NativeScopedBuffer( long capacity, MemoryTracker memoryTracker )
    {
        this( toIntExact( capacity ), memoryTracker );
    }

    public NativeScopedBuffer( int capacity, MemoryTracker memoryTracker )
    {
        buffer = ByteBuffers.allocateDirect( capacity, memoryTracker );
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
            ByteBuffers.releaseBuffer( buffer, memoryTracker );
            closed = true;
        }
    }
}
