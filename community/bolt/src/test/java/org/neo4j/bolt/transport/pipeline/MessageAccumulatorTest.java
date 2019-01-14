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
package org.neo4j.bolt.transport.pipeline;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.bolt.testing.BoltTestUtil.assertByteBufEquals;

public class MessageAccumulatorTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel( new MessageAccumulator() );

    @After
    public void cleanup()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldDecodeMessageWithSingleChunk()
    {
        assertFalse( channel.writeInbound( wrappedBuffer( new byte[]{1, 2, 3, 4, 5} ) ) );
        assertTrue( channel.writeInbound( wrappedBuffer( new byte[0] ) ) );
        assertTrue( channel.finish() );

        assertEquals( 1, channel.inboundMessages().size() );
        assertByteBufEquals( wrappedBuffer( new byte[]{1, 2, 3, 4, 5} ), channel.readInbound() );
    }

    @Test
    public void shouldDecodeMessageWithMultipleChunks()
    {
        assertFalse( channel.writeInbound( wrappedBuffer( new byte[]{1, 2, 3} ) ) );
        assertFalse( channel.writeInbound( wrappedBuffer( new byte[]{4, 5} ) ) );
        assertFalse( channel.writeInbound( wrappedBuffer( new byte[]{6, 7, 8} ) ) );
        assertTrue( channel.writeInbound( wrappedBuffer( new byte[0] ) ) );
        assertTrue( channel.finish() );

        assertEquals( 1, channel.inboundMessages().size() );
        assertByteBufEquals( wrappedBuffer( new byte[]{1, 2, 3, 4, 5, 6, 7, 8} ), channel.readInbound() );
    }

    @Test
    public void shouldDecodeMultipleConsecutiveMessages()
    {
        channel.writeInbound( wrappedBuffer( new byte[]{1, 2, 3} ) );
        channel.writeInbound( wrappedBuffer( new byte[0] ) );

        channel.writeInbound( wrappedBuffer( new byte[]{4, 5} ) );
        channel.writeInbound( wrappedBuffer( new byte[]{6} ) );
        channel.writeInbound( wrappedBuffer( new byte[0] ) );

        channel.writeInbound( wrappedBuffer( new byte[]{7, 8} ) );
        channel.writeInbound( wrappedBuffer( new byte[]{9, 10} ) );
        channel.writeInbound( wrappedBuffer( new byte[0] ) );

        assertEquals( 3, channel.inboundMessages().size() );
        assertByteBufEquals( wrappedBuffer( new byte[]{1, 2, 3} ), channel.readInbound() );
        assertByteBufEquals( wrappedBuffer( new byte[]{4, 5, 6} ), channel.readInbound() );
        assertByteBufEquals( wrappedBuffer( new byte[]{7, 8, 9, 10} ), channel.readInbound() );
    }

}
