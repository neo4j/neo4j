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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

abstract class DirectCompositeBufferTest extends AbstractDirectBufferTest
{
    protected abstract ByteBuf allocate();

    @Test
    void testBasicAllocation()
    {
        ByteBuf buf = allocate();

        assertEquals( 0, buf.capacity() );
        assertEquals( Integer.MAX_VALUE, buf.maxCapacity() );
        assertFalse( buf.isDirect() );

        write( buf, 1000 );
        assertEquals( 1024, buf.capacity() );

        buf.release();

        assertAcquiredAndReleased( 1024 );
    }

    @Test
    void testBufferGrow()
    {
        ByteBuf buf = allocate();

        write( buf, 1000 );
        assertEquals( 1024, buf.capacity() );
        write( buf, 1000 );
        assertEquals( 2048, buf.capacity() );
        write( buf, 1000 );
        assertEquals( 4096, buf.capacity() );
        write( buf, 10_000 );
        assertEquals( 16_384, buf.capacity() );
        write( buf, 10_000 );
        assertEquals( 32_768, buf.capacity() );

        buf.release();

        assertAcquiredAndReleased( 1024, 1024, 2048, 4096, 8192, 16384 );
    }

    static class DefaultBufferTest extends DirectCompositeBufferTest
    {

        @Override
        protected ByteBuf allocate()
        {
            return nettyBufferAllocator.compositeBuffer();
        }
    }

    static class DirectBufferTest extends DirectCompositeBufferTest
    {

        @Override
        protected ByteBuf allocate()
        {
            return nettyBufferAllocator.compositeDirectBuffer();
        }
    }
}
