/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.helpers.TickingClock;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.com.Protocol.EMPTY_SERIALIZER;
import static org.neo4j.com.Protocol.VOID_DESERIALIZER;

public class ServerTest
{
    private final Protocol protocol = new Protocol214( 1024, (byte) 0, Server.INTERNAL_PROTOCOL_VERSION );
    private final TxChecksumVerifier checksumVerifier = mock( TxChecksumVerifier.class );
    private final RequestType reqType = mock( RequestType.class );
    private final RecordingChannel channel = new RecordingChannel();

    @Test
    public void shouldSendExceptionBackToClientOnInvalidChecksum() throws Exception
    {
        // Given
        Server<Object, Object> server = newServer( checksumVerifier );
        RequestContext ctx = new RequestContext( 0, 1, 0, 1, -1, 12 );

        doThrow(new IllegalStateException("123")).when(checksumVerifier).assertMatch( anyLong(), anyInt(), anyLong() );

        // When
        try
        {
            server.channelRead( channelCtx( channel ), message( reqType, ctx, EMPTY_SERIALIZER ) );
            fail("Should have failed.");
        }
        catch(IllegalStateException e)
        {
            // Expected
        }

        // Then
        try
        {
            protocol.deserializeResponse( channel.asBlockingReadHandler(), ByteBuffer.allocateDirect( 1024 ), 1,
                    VOID_DESERIALIZER, mock( ResourceReleaser.class ) );
            fail("Should have failed.");
        }
        catch(IllegalStateException e)
        {
            assertThat(e.getMessage(), equalTo("123"));
        }

    }

    private ByteBuf message( RequestType reqType, RequestContext ctx,
            Serializer payloadSerializer ) throws IOException
    {
        ByteBuffer backingBuffer = ByteBuffer.allocate( 1024 );

        protocol.serializeRequest( new RecordingChannel(), Unpooled.wrappedBuffer( backingBuffer ),
                reqType, ctx,
                payloadSerializer );

        return Unpooled.wrappedBuffer( backingBuffer );
    }

    private ChannelHandlerContext channelCtx( Channel channel )
    {
        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
        when(ctx.channel()).thenReturn( channel );
        return ctx;
    }

    private Server<Object, Object> newServer( final TxChecksumVerifier checksumVerifier )
    {
        return new Server<Object, Object>( null, mock( Server.Configuration.class ), new DevNullLoggingService(),
                Protocol.DEFAULT_FRAME_LENGTH,
                new ProtocolVersion( ((byte) 0), ProtocolVersion.INTERNAL_PROTOCOL_VERSION ),
                checksumVerifier, new TickingClock( 0, 1 ),
                mock( ByteCounterMonitor.class ), mock( RequestMonitor.class ) )
        {
            @Override
            protected RequestType<Object> getRequestContext( byte id )
            {
                return mock( RequestType.class );
            }

            @Override
            protected void finishOffChannel( Channel channel, RequestContext context )
            {
            }
        };
    }
}