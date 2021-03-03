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
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import org.neo4j.io.bufferpool.ByteBufferManger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class HeapBufferTest
{
    private final NettyMemoryManagerWrapper nettyBufferAllocator = new NettyMemoryManagerWrapper( new ThrowingBufferPool() );

    @Test
    void testBasicAllocation()
    {
        ByteBuf buf = nettyBufferAllocator.heapBuffer( 1500, 10_000 );

        assertEquals( 1500, buf.capacity() );
        assertEquals( 10_000, buf.maxCapacity() );
        assertFalse( buf.isDirect() );

        write( buf, 1000 );
        buf.release();
    }

    @Test
    void testBufferGrow()
    {
        ByteBuf buf = nettyBufferAllocator.heapBuffer( 1500, 20_000 );
        write( buf, 1000 );
        assertEquals( 1500, buf.capacity() );
        write( buf, 1000 );
        assertEquals( 2048, buf.capacity() );
        write( buf, 1000 );
        assertEquals( 4096, buf.capacity() );
        write( buf, 10_000 );
        assertEquals( 16_384, buf.capacity() );
        buf.release();
    }

    @Test
    void testDefaultCapacities()
    {
        ByteBuf buf = nettyBufferAllocator.heapBuffer();

        assertEquals( 256, buf.capacity() );
        assertEquals( Integer.MAX_VALUE, buf.maxCapacity() );
        buf.release();
    }

    @Test
    void testBasicCompositeBufferAllocation()
    {
        ByteBuf buf = nettyBufferAllocator.compositeHeapBuffer( 10 );

        assertEquals( 0, buf.capacity() );
        assertEquals( Integer.MAX_VALUE, buf.maxCapacity() );
        assertFalse( buf.isDirect() );

        write( buf, 1000 );

        assertEquals( 1024, buf.capacity() );

        buf.release();
    }

    @Test
    void testCompositeBufferGrow()
    {
        ByteBuf buf = nettyBufferAllocator.compositeHeapBuffer( 10 );
        write( buf, 1000 );
        assertEquals( 1024, buf.capacity() );
        write( buf, 1000 );
        assertEquals( 2048, buf.capacity() );
        write( buf, 1000 );
        assertEquals( 4096, buf.capacity() );
        write( buf, 10_000 );
        assertEquals( 16384, buf.capacity() );
        buf.release();
    }

    private void write( ByteBuf buf, int size )
    {
        for ( var i = 0; i < size; i++ )
        {
            buf.writeByte( 1 );
        }
    }

    private static class ThrowingBufferPool implements ByteBufferManger
    {

        @Override
        public ByteBuffer acquire( int size )
        {
            throw new IllegalStateException( "Trying to acquire ByteBuffer" );
        }

        @Override
        public void release( ByteBuffer buffer )
        {
            throw new IllegalStateException( "Trying to release ByteBuffer" );
        }

        @Override
        public int recommendNewCapacity( int minNewCapacity, int maxCapacity )
        {
            throw new IllegalStateException( "Trying to recommend new capacity" );
        }
    }
}
