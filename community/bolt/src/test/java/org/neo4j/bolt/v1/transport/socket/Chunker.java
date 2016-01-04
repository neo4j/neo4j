/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import io.netty.channel.ChannelPromise;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.bolt.v1.transport.ChunkedOutput;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Helper to chunk up serialized data for testing */
public class Chunker
{
    public static byte[] chunk( int maxChunkSize, byte[][] messages ) throws IOException
    {
        final ByteBuffer outputBuffer = ByteBuffer.allocate( 1024 * 8 );

        Channel ch = mock( Channel.class );
        when( ch.alloc() ).thenReturn( UnpooledByteBufAllocator.DEFAULT );
        when( ch.writeAndFlush( any(), any( ChannelPromise.class ) ) ).then( new Answer<Object>()
        {
            @Override
            public Object answer( InvocationOnMock inv ) throws Throwable
            {
                ByteBuf buf = (ByteBuf) inv.getArguments()[0];
                outputBuffer.limit( outputBuffer.position() + buf.readableBytes() );
                buf.readBytes( outputBuffer );
                buf.release();
                return null;
            }
        } );

        ChunkedOutput out = new ChunkedOutput( ch, maxChunkSize + 2 /* for chunk header */ );

        for ( byte[] message : messages )
        {
            out.writeBytes( message, 0, message.length );
            out.onMessageComplete();
        }
        out.flush();
        out.close();

        byte[] bytes = new byte[outputBuffer.limit()];
        outputBuffer.position( 0 );
        outputBuffer.get( bytes );
        return bytes;
    }
}
