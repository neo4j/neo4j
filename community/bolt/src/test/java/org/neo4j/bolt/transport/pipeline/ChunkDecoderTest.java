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

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.copyShort;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.bolt.testing.BoltTestUtil.assertByteBufEquals;

public class ChunkDecoderTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel( new ChunkDecoder() );

    @After
    public void cleanup()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldDecodeFullChunk()
    {
        // whole chunk with header and body arrives at once
        ByteBuf input = buffer();
        input.writeShort( 7 );
        input.writeByte( 1 );
        input.writeByte( 11 );
        input.writeByte( 2 );
        input.writeByte( 22 );
        input.writeByte( 3 );
        input.writeByte( 33 );
        input.writeByte( 4 );

        // after buffer is written there should be something to read on the other side
        assertTrue( channel.writeInbound( input ) );
        assertTrue( channel.finish() );

        // there should only be a single chunk available for reading
        assertEquals( 1, channel.inboundMessages().size() );
        // it should have no size header and expected body
        assertByteBufEquals( input.slice( 2, 7 ), channel.readInbound() );
    }

    @Test
    public void shouldDecodeSplitChunk()
    {
        // first part of the chunk contains size header and some bytes
        ByteBuf input1 = buffer();
        input1.writeShort( 9 );
        input1.writeByte( 1 );
        input1.writeByte( 11 );
        input1.writeByte( 2 );
        // nothing should be available for reading
        assertFalse( channel.writeInbound( input1 ) );

        // second part contains just a single byte
        ByteBuf input2 = buffer();
        input2.writeByte( 22 );
        // nothing should be available for reading
        assertFalse( channel.writeInbound( input2 ) );

        // third part contains couple more bytes
        ByteBuf input3 = buffer();
        input3.writeByte( 3 );
        input3.writeByte( 33 );
        input3.writeByte( 4 );
        // nothing should be available for reading
        assertFalse( channel.writeInbound( input3 ) );

        // fourth part contains couple more bytes, and the chunk is now complete
        ByteBuf input4 = buffer();
        input4.writeByte( 44 );
        input4.writeByte( 5 );
        // there should be something to read now
        assertTrue( channel.writeInbound( input4 ) );

        assertTrue( channel.finish() );

        // there should only be a single chunk available for reading
        assertEquals( 1, channel.inboundMessages().size() );
        // it should have no size header and expected body
        assertByteBufEquals( wrappedBuffer( new byte[]{1, 11, 2, 22, 3, 33, 4, 44, 5} ), channel.readInbound() );
    }

    @Test
    public void shouldDecodeEmptyChunk()
    {
        // chunk contains just the size header which is zero
        ByteBuf input = copyShort( 0 );
        assertTrue( channel.writeInbound( input ) );
        assertTrue( channel.finish() );

        // there should only be a single chunk available for reading
        assertEquals( 1, channel.inboundMessages().size() );
        // it should have no size header and empty body
        assertByteBufEquals( wrappedBuffer( new byte[0] ), channel.readInbound() );
    }

    @Test
    public void shouldDecodeMaxSizeChunk()
    {
        byte[] message = new byte[0xFFFF];

        ByteBuf input = buffer();
        input.writeShort( message.length );
        input.writeBytes( message );

        assertTrue( channel.writeInbound( input ) );
        assertTrue( channel.finish() );

        assertEquals( 1, channel.inboundMessages().size() );
        assertByteBufEquals( wrappedBuffer( message ), channel.readInbound() );
    }
}
