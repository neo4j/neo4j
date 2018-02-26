/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import org.neo4j.bolt.transport.BoltProtocol;
import org.neo4j.bolt.transport.ProtocolChooser;
import org.neo4j.bolt.transport.SocketTransportHandler;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.SynchronousBoltWorker;
import org.neo4j.bolt.v1.transport.BoltProtocolV1;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.logging.AssertableLogProvider;
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
<<<<<<< HEAD
=======
    private static final String CONNECTOR = "default";
    private static final LogProvider LOG_PROVIDER = NullLogProvider.getInstance();
    private static final BoltMessageLogging BOLT_LOGGING = BoltMessageLogging.none();

    @Parameter
    public Neo4jPack neo4jPack;

    @Parameters( name = "{0}" )
    public static List<Neo4jPack> parameters()
    {
        return Arrays.asList( new Neo4jPackV1(), new Neo4jPackV2() );
    }

>>>>>>> 1ba1d2f8c3f... Make `BoltScheduler` configurable per bolt connector
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
        SocketTransportHandler handler = newSocketTransportHandler( mock( ProtocolChooser.class ) );

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
        SocketTransportHandler handler = newSocketTransportHandler( mock( ProtocolChooser.class ) );

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

<<<<<<< HEAD
        SocketTransportHandler handler = new SocketTransportHandler( protocolChooser( machine ), logging );
=======
        BoltHandshakeProtocolHandler handshakeHandler = newHandshakeHandler( machine );
        SocketTransportHandler handler = new SocketTransportHandler( CONNECTOR, handshakeHandler, logging, BOLT_LOGGING );
>>>>>>> 1ba1d2f8c3f... Make `BoltScheduler` configurable per bolt connector

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
<<<<<<< HEAD
        SocketTransportHandler handler = new SocketTransportHandler( mock( ProtocolChooser.class ), logging );
=======
        SocketTransportHandler handler = new SocketTransportHandler( CONNECTOR, mock( BoltHandshakeProtocolHandler.class ),
                logging, BOLT_LOGGING );
>>>>>>> 1ba1d2f8c3f... Make `BoltScheduler` configurable per bolt connector

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
        ProtocolChooser chooser = protocolChooser( machine );
        ChannelHandlerContext context = channelHandlerContextMock();

<<<<<<< HEAD
        SocketTransportHandler handler = new SocketTransportHandler( chooser, NullLogProvider.getInstance() );
=======
        SocketTransportHandler handler = new SocketTransportHandler( CONNECTOR, handshakeHandler, LOG_PROVIDER, BOLT_LOGGING );
>>>>>>> 1ba1d2f8c3f... Make `BoltScheduler` configurable per bolt connector

        handler.channelRead( context, handshake() );
        BoltProtocol protocol1 = chooser.chosenProtocol();

        handler.channelRead( context, handshake() );
        BoltProtocol protocol2 = chooser.chosenProtocol();

        assertSame( protocol1, protocol2 );
    }

    private static SocketTransportHandler newSocketTransportHandler( ProtocolChooser protocolChooser )
    {
<<<<<<< HEAD
        return new SocketTransportHandler( protocolChooser, NullLogProvider.getInstance() );
=======
        return new SocketTransportHandler( CONNECTOR, handler, LOG_PROVIDER, BOLT_LOGGING );
>>>>>>> 1ba1d2f8c3f... Make `BoltScheduler` configurable per bolt connector
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

    private ProtocolChooser protocolChooser( final BoltStateMachine machine )
    {
        Map<Long,BiFunction<Channel,Boolean,BoltProtocol>> availableVersions = new HashMap<>();
        availableVersions.put( (long) BoltProtocolV1.VERSION,
                ( channel, isSecure ) -> new BoltProtocolV1( new SynchronousBoltWorker( machine ), channel,
                        NullLogService.getInstance() )
        );

        return new ProtocolChooser( availableVersions, false, true );
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
