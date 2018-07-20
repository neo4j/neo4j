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
package org.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import io.netty.buffer.ByteBuf;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.helpers.Buffers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ChunkHandlerTest
{
    @Rule
    public Buffers buffers = new Buffers();

    @Test
    public void shouldReturnNullIfNotLastChunk()
    {
        ChunkHandler chunkHandler = new ChunkHandler();
        ByteBuf buffer = buffers.buffer( 9 );
        int length = buffer.writableBytes();
        buffer.writerIndex( buffer.writerIndex() + length );
        writeInitialMetaData( buffer, length );

        ChunkHandler.ComposedChunks composed = chunkHandler.handle( buffer );

        assertNull( composed );
    }

    @Test
    public void shouldHandleSingleChunkAndOnlyReadGivenLength()
    {
        ChunkHandler chunkHandler = new ChunkHandler();

        ByteBuf buffer = buffers.buffer( 16 );
        writeInitialMetaData( buffer, 4 );
        // set to last chunk
        buffer.setBoolean( 0, true );

        buffer.writeInt( 2 );

        int unusedBytes = buffer.writableBytes();
        buffer.writerIndex( buffer.capacity() );

        ChunkHandler.ComposedChunks composed = chunkHandler.handle( buffer );

        assertNotNull( composed );
        assertEquals( unusedBytes, buffer.readableBytes() );
    }

    @Test
    public void shouldHandleSeriesOfChunks()
    {
        ChunkHandler chunkHandler = new ChunkHandler();

        ByteBuf chunk1 = buffers.buffer( 10 );
        ByteBuf chunk2 = buffers.buffer( 9 );
        writeInitialMetaData( chunk1, 4 );
        writeEndMetaData( chunk2, 4 );

        chunk1.writeInt( 1 );
        chunk2.writeInt( 2 );

        chunkHandler.handle( chunk1 );
        ChunkHandler.ComposedChunks composed = chunkHandler.handle( chunk2 );

        assertEquals( 1, composed.content().readInt() );
        assertEquals( 2, composed.content().readInt() );
        assertEquals( 0, composed.content().readableBytes() );
    }

    private void writeEndMetaData( ByteBuf buffer, int length )
    {
        buffer.writeBoolean( true );
        buffer.writeInt( length );
    }

    private void writeInitialMetaData( ByteBuf buffer, int length )
    {
        buffer.writeBoolean( false );
        buffer.writeInt( length );
        buffer.writeByte( 0 );
    }
}
