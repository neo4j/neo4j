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
package org.neo4j.kernel.impl.api.state;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.kernel.impl.util.collection.Memory;
import org.neo4j.kernel.impl.util.collection.MemoryAllocator;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryAllocationTracker;

import static java.lang.Math.toIntExact;

class TestMemoryAllocator implements MemoryAllocator
{
    final MemoryAllocationTracker tracker;

    TestMemoryAllocator()
    {
        this( new LocalMemoryTracker() );
    }

    TestMemoryAllocator( MemoryAllocationTracker tracker )
    {
        this.tracker = tracker;
    }

    @Override
    public Memory allocate( long size, boolean zeroed )
    {
        final ByteBuffer buf = ByteBuffer.allocate( toIntExact( size ) );
        if ( zeroed )
        {
            Arrays.fill( buf.array(), (byte) 0 );
        }
        return new MemoryImpl( buf );
    }

    class MemoryImpl implements Memory
    {
        final ByteBuffer buf;

        MemoryImpl( ByteBuffer buf )
        {
            this.buf = buf;
            tracker.allocated( buf.capacity() );
        }

        @Override
        public long readLong( long offset )
        {
            return buf.getLong( toIntExact( offset ) );
        }

        @Override
        public void writeLong( long offset, long value )
        {
            buf.putLong( toIntExact( offset ), value );
        }

        @Override
        public void clear()
        {
            Arrays.fill( buf.array(), (byte) 0 );
        }

        @Override
        public long size()
        {
            return buf.capacity();
        }

        @Override
        public void free()
        {
            tracker.deallocated( buf.capacity() );
        }

        @Override
        public Memory copy()
        {
            ByteBuffer copyBuf = ByteBuffer.wrap( Arrays.copyOf( buf.array(), buf.array().length ) );
            return new MemoryImpl( copyBuf );
        }

        @Override
        public ByteBuffer asByteBuffer()
        {
            return buf;
        }
    }
}
