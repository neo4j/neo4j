/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.logging.BoltMessageLogging;
import org.neo4j.bolt.transport.BoltHandshakeProtocolHandler;
import org.neo4j.bolt.transport.BoltMessagingProtocolHandler;
import org.neo4j.bolt.transport.SocketTransportHandler;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.SynchronousBoltWorker;
import org.neo4j.bolt.v1.transport.BoltMessagingProtocolV1Handler;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class SocketTransportHandlerTest
{
    private static final LogProvider LOG_PROVIDER = NullLogProvider.getInstance();
    private static final BoltMessageLogging BOLT_LOGGING = BoltMessageLogging.none();

    @Test
    public void shouldCloseProtocolOnChannelInactive() throws Throwable
    {
        // Given
        BoltStateMachine machine = mock( BoltStateMachine.class );
        ChannelHandlerContext ctx = channelHandlerContextMock();

        SocketTransportHandler handler = newSocketTransportHandler( protocolChooser( machine ) );

        // And Given a session has been established
        handler.channelRead( ctx, handshake() );

        // When
        handler.channelInactive( ctx );

        // Then
        verify( machine ).close();
    }

    @Test
    public void shouldCloseContextWhenProtocolNotInitializedOnChannelInactive() throws Throwable
    {
        // Given
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        SocketTransportHandler handler = newSocketTransportHandler( mock( BoltHandshakeProtocolHandler.class ) );

        // When
        handler.channelInactive( context );

        // Then
        verify( context ).close();
    }

    @Test
    public void shouldCloseProtocolOnHandlerRemoved() throws Throwable
    {
        // Given
        BoltStateMachine machine = mock( BoltStateMachine.class );
        ChannelHandlerContext ctx = channelHandlerContextMock();

        SocketTransportHandler handler = newSocketTransportHandler( protocolChooser( machine ) );

        // And Given a session has been established
        handler.channelRead( ctx, handshake() );

        // When
        handler.handlerRemoved( ctx );

        // Then
        verify( machine ).close();
    }

    @Test
    public void shouldCloseContextWhenProtocolNotInitializedOnHandlerRemoved() throws Throwable
    {
        // Given
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        SocketTransportHandler handler = newSocketTransportHandler( mock( BoltHandshakeProtocolHandler.class ) );

        // When
        handler.handlerRemoved( context );

        // Then
        verify( context ).close();
    }

    @Test
    public void logsAndClosesProtocolOnUnexpectedExceptions() throws Throwable
    {
        // Given
        BoltStateMachine machine = mock( BoltStateMachine.class );
        ChannelHandlerContext ctx = channelHandlerContextMock();
        AssertableLogProvider logging = new AssertableLogProvider();

        BoltHandshakeProtocolHandler protocolChooser = protocolChooser( machine );
        SocketTransportHandler handler = new SocketTransportHandler( protocolChooser, logging, BOLT_LOGGING );

        // And Given a session has been established
        handler.channelRead( ctx, handshake() );

        // When
        Throwable cause = new Throwable( "Oh no!" );
        handler.exceptionCaught( ctx, cause );

        // Then
        verify( machine ).close();
        logging.assertExactly( inLog( SocketTransportHandler.class )
                .error( equalTo( "Fatal error occurred when handling a client connection: Oh no!" ), is( cause ) ) );
    }

    @Test
    public void logsAndClosesContextWhenProtocolNotInitializedOnUnexpectedExceptions() throws Throwable
    {
        // Given
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        AssertableLogProvider logging = new AssertableLogProvider();
        SocketTransportHandler handler = new SocketTransportHandler( mock( BoltHandshakeProtocolHandler.class ),
                logging, BOLT_LOGGING );

        // When
        Throwable cause = new Throwable( "Oh no!" );
        handler.exceptionCaught( context, cause );

        // Then
        verify( context ).close();
        logging.assertExactly( inLog( SocketTransportHandler.class )
                .error( equalTo( "Fatal error occurred when handling a client connection: Oh no!" ),
                        is( cause ) ) );
    }

    @Test
    public void shouldInitializeProtocolOnFirstMessage() throws Exception
    {
        BoltStateMachine machine = mock( BoltStateMachine.class );
        BoltHandshakeProtocolHandler chooser = protocolChooser( machine );
        ChannelHandlerContext context = channelHandlerContextMock();

        SocketTransportHandler handler = new SocketTransportHandler( chooser, LOG_PROVIDER, BOLT_LOGGING );

        handler.channelRead( context, handshake() );
        BoltMessagingProtocolHandler protocol1 = chooser.chosenProtocol();

        handler.channelRead( context, handshake() );
        BoltMessagingProtocolHandler protocol2 = chooser.chosenProtocol();

        assertSame( protocol1, protocol2 );
    }

    private static SocketTransportHandler newSocketTransportHandler( BoltHandshakeProtocolHandler handler )
    {
        return new SocketTransportHandler( handler, LOG_PROVIDER, BOLT_LOGGING );
    }

    private static ChannelHandlerContext channelHandlerContextMock()
    {
        Channel channel = mock( Channel.class );
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        when( context.channel() ).thenReturn( channel );

        when( channel.alloc() ).thenReturn( UnpooledByteBufAllocator.DEFAULT );
        when( context.alloc() ).thenReturn( UnpooledByteBufAllocator.DEFAULT );

        return context;
    }

    private BoltHandshakeProtocolHandler protocolChooser( final BoltStateMachine machine )
    {
        Map<Long,Function<BoltChannel, BoltMessagingProtocolHandler>> availableVersions = new HashMap<>();
        availableVersions.put( (long) BoltMessagingProtocolV1Handler.VERSION,
                boltChannel -> new BoltMessagingProtocolV1Handler( boltChannel, new SynchronousBoltWorker( machine ),
                        NullLogService.getInstance() )
        );

        return new BoltHandshakeProtocolHandler( availableVersions, false, true );
    }

    private ByteBuf handshake()
    {
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.buffer();
        buf.writeInt( 0x6060B017 );
        buf.writeInt( 1 );
        buf.writeInt( 0 );
        buf.writeInt( 0 );
        buf.writeInt( 0 );
        return buf;
    }

}
