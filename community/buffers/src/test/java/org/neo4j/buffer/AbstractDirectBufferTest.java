/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.buffer;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.BeforeEach;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.io.bufferpool.ByteBufferManger;
import org.neo4j.io.bufferpool.impl.NeoByteBufferPool;
import org.neo4j.io.bufferpool.impl.NeoBufferPoolConfigOverride;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class AbstractDirectBufferTest
{
    NettyMemoryManagerWrapper nettyBufferAllocator;
    private TracingPoolWrapper tracingPoolWrapper;

    @BeforeEach
    void setUp()
    {
        var buckets = List.of("1K:1", "2K:1", "4K:1", "8K:1");
        var poolConfig = new NeoBufferPoolConfigOverride( Duration.ZERO, buckets );
        var bufferManger = new NeoByteBufferPool( poolConfig, new MemoryPools(), null );
        tracingPoolWrapper = new TracingPoolWrapper( bufferManger );
        nettyBufferAllocator = new NettyMemoryManagerWrapper( tracingPoolWrapper );
    }

    void write( ByteBuf buf, int size )
    {
        for ( var i = 0; i < size; i++ )
        {
            buf.writeByte( 1 );
        }
    }

    void assertAcquiredAndReleased( Integer... capacities )
    {
        assertAcquired( capacities );
        assertReleased( capacities );
    }

    private void assertAcquired( Integer... capacities )
    {
        assertEquals( Arrays.asList( capacities ), tracingPoolWrapper.acquired );
    }

    private void assertReleased( Integer... capacities )
    {
        assertEquals( Arrays.asList( capacities ), tracingPoolWrapper.released );
    }

    static class TracingPoolWrapper implements ByteBufferManger
    {
        private final List<Integer> acquired = new ArrayList<>();
        private final List<Integer> released = new ArrayList<>();

        private final ByteBufferManger wrappedPool;

        TracingPoolWrapper( ByteBufferManger wrappedPool )
        {
            this.wrappedPool = wrappedPool;
        }

        @Override
        public ByteBuffer acquire( int size )
        {
            var buffer = wrappedPool.acquire( size );
            acquired.add( buffer.capacity() );
            return buffer;
        }

        @Override
        public void release( ByteBuffer buffer )
        {
            released.add( buffer.capacity() );
            wrappedPool.release( buffer );
        }

        @Override
        public int recommendNewCapacity( int minNewCapacity, int maxCapacity )
        {
            return wrappedPool.recommendNewCapacity( minNewCapacity, maxCapacity );
        }

        @Override
        public MemoryTracker getHeapBufferMemoryTracker()
        {
            return wrappedPool.getHeapBufferMemoryTracker();
        }
    }
}
