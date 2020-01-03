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

import org.junit.jupiter.api.Test;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryAllocationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UnsafeDirectByteBufferFactoryTest
{
    @Test
    void shouldAllocateBuffer()
    {
        // given
        MemoryAllocationTracker tracker = new LocalMemoryTracker();
        try ( UnsafeDirectByteBufferAllocator factory = new UnsafeDirectByteBufferAllocator( tracker ) )
        {
            // when
            int bufferSize = 128;
            factory.allocate( bufferSize );

            // then
            assertEquals( bufferSize, tracker.usedDirectMemory() );
        }
    }

    @Test
    void shouldFreeOnClose()
    {
        // given
        MemoryAllocationTracker tracker = new LocalMemoryTracker();
        try ( UnsafeDirectByteBufferAllocator factory = new UnsafeDirectByteBufferAllocator( tracker ) )
        {
            // when
            factory.allocate( 256 );
        }

        // then
        assertEquals( 0, tracker.usedDirectMemory() );
    }

    @Test
    void shouldHandleMultipleClose()
    {
        // given
        MemoryAllocationTracker tracker = new LocalMemoryTracker();
        UnsafeDirectByteBufferAllocator factory = new UnsafeDirectByteBufferAllocator( tracker );

        // when
        factory.allocate( 256 );
        factory.close();

        // then
        assertEquals( 0, tracker.usedDirectMemory() );
        factory.close();
        assertEquals( 0, tracker.usedDirectMemory() );
    }

    @Test
    void shouldNotAllocateAfterClosed()
    {
        // given
        UnsafeDirectByteBufferAllocator factory = new UnsafeDirectByteBufferAllocator( new LocalMemoryTracker() );
        factory.close();

        // when
        try
        {
            factory.allocate( 8 );
        }
        catch ( IllegalStateException e )
        {
            // then good
        }
    }
}
