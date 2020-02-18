/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.logging.AssertableLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

class TransportSelectionHandlerTest
{
    @Test
    void shouldLogOnUnexpectedExceptionsAndClosesContext() throws Throwable
    {
        // Given
        ChannelHandlerContext context = channelHandlerContextMock();
        AssertableLogProvider logging = new AssertableLogProvider();
        TransportSelectionHandler handler = new TransportSelectionHandler( null, null, false, false, logging, null );

        // When
        Throwable cause = new Throwable( "Oh no!" );
        handler.exceptionCaught( context, cause );

        // Then
        verify( context ).close();
        assertThat( logging ).forClass( TransportSelectionHandler.class ).forLevel( ERROR )
                .containsMessageWithException( "Fatal error occurred when initialising pipeline: " + context.channel(), cause );
    }

    @Test
    void shouldLogConnectionResetErrorsAtWarningLevelAndClosesContext() throws Exception
    {
        // Given
        ChannelHandlerContext context = channelHandlerContextMock();
        AssertableLogProvider logging = new AssertableLogProvider();
        TransportSelectionHandler handler = new TransportSelectionHandler( null, null, false, false, logging, null );

        IOException connResetError = new IOException( "Connection reset by peer" );

        // When
        handler.exceptionCaught( context, connResetError );

        // Then
        verify( context ).close();
        assertThat( logging ).forClass( TransportSelectionHandler.class ).forLevel( WARN )
                .containsMessageWithArguments( "Fatal error occurred when initialising pipeline, " +
                        "remote peer unexpectedly closed connection: %s", context.channel() );
    }

    @Test
    void shouldPreventMultipleLevelsOfSslEncryption() throws Exception
    {
        // Given
        ChannelHandlerContext context = channelHandlerContextMockSslAlreadyConfigured();
        AssertableLogProvider logging = new AssertableLogProvider();
        SslContext sslCtx = mock( SslContext.class );
        TransportSelectionHandler handler = new TransportSelectionHandler( null, sslCtx, false, false, logging, null );

        final ByteBuf payload = Unpooled.wrappedBuffer(new byte[] { 22, 3, 1, 0, 5 }); //encrypted

        // When
        handler.decode( context, payload, null );

        // Then
        verify( context ).close();
        assertThat( logging ).forClass( TransportSelectionHandler.class ).forLevel( ERROR )
                             .containsMessageWithArguments( "Fatal error: multiple levels of SSL encryption detected." +
                                                            " Terminating connection: %s", context.channel()  );
    }

    private static ChannelHandlerContext channelHandlerContextMock()
    {
        Channel channel = mock( Channel.class );
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        when( context.channel() ).thenReturn( channel );
        return context;
    }

    private static ChannelHandlerContext channelHandlerContextMockSslAlreadyConfigured()
    {
        Channel channel = mock( Channel.class );
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        ChannelPipeline pipeline = mock( ChannelPipeline.class );
        SslHandler sslHandler = mock( SslHandler.class );
        when( context.channel() ).thenReturn( channel );
        when( context.pipeline() ).thenReturn( pipeline );
        when( context.pipeline().get( SslHandler.class )).thenReturn( sslHandler );
        return context;
    }
}
