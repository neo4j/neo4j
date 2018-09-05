/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.causalclustering.helpers.Buffers;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ByteArrayChunkedEncoderTest
{
    @Rule
    public final Buffers buffers = new Buffers();
    @Test
    public void shouldWriteToBufferInChunks()
    {
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6};
        byte[] readData = new byte[6];
        ByteArrayChunkedEncoder byteArraySerializer = new ByteArrayChunkedEncoder( data, 5 );

        ByteBuf buffer = byteArraySerializer.readChunk( buffers );
        assertEquals( 6, buffer.readInt() );
        assertEquals( 1, buffer.readableBytes() );
        buffer.readBytes( readData, 0, 1 );

        buffer = byteArraySerializer.readChunk( buffers );
        buffer.readBytes( readData, 1, buffer.readableBytes() );
        assertArrayEquals( data, readData );
        assertEquals( 0, buffer.readableBytes() );

        assertNull( byteArraySerializer.readChunk( buffers ) );
    }

    @Test
    public void shouldHandleSmallByteBuf()
    {
        byte[] data = new byte[1];

        ByteBuf byteBuf = new ByteArrayChunkedEncoder( data ).readChunk( buffers );
        assertEquals( 1, byteBuf.readInt() );
        assertEquals( 1, byteBuf.readableBytes() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowOnToSmallChunk()
    {
        new ByteArrayChunkedEncoder( new byte[1], 0 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shoudThrowOnEmprtContent()
    {
        new ByteArrayChunkedEncoder( new byte[0] );
    }
}
