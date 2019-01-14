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
package org.neo4j.bolt.v1.transport.socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.v1.transport.ChunkedOutput;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.v1.transport.ChunkedOutput.CHUNK_HEADER_SIZE;

/** Helper to chunk up serialized data for testing */
public class Chunker
{
    private Chunker()
    {
    }

    public static byte[] chunk( int maxChunkSize, byte[][] messages ) throws IOException
    {
        final ByteBuffer outputBuffer = ByteBuffer.allocate( 1024 * 8 );

        Channel ch = mock( Channel.class );
        when( ch.alloc() ).thenReturn( UnpooledByteBufAllocator.DEFAULT );
        when( ch.writeAndFlush( any(), isNull() ) ).then( inv ->
        {
            ByteBuf buf = inv.getArgument( 0 );
            outputBuffer.limit( outputBuffer.position() + buf.readableBytes() );
            buf.readBytes( outputBuffer );
            buf.release();
            return null;
        } );

        int maxBufferSize = maxChunkSize + CHUNK_HEADER_SIZE;
        ChunkedOutput out = new ChunkedOutput( ch, maxBufferSize, maxBufferSize, TransportThrottleGroup.NO_THROTTLE );

        for ( byte[] message : messages )
        {
            out.beginMessage();
            out.writeBytes( message, 0, message.length );
            out.messageSucceeded();
        }
        out.flush();
        out.close();

        byte[] bytes = new byte[outputBuffer.limit()];
        outputBuffer.position( 0 );
        outputBuffer.get( bytes );
        return bytes;
    }
}
