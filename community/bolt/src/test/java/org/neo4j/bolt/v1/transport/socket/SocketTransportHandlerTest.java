/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.bolt.transport.BoltProtocol;
import org.neo4j.bolt.transport.SocketTransportHandler;
import org.neo4j.bolt.v1.transport.BoltProtocolV1;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.function.Function;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.bolt.v1.runtime.Session;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.primitive.Primitive.longObjectMap;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class SocketTransportHandlerTest
{
    @Test
    public void shouldCloseSessionOnChannelClose() throws Throwable
    {
        // Given
        Session session = mock(Session.class);
        Channel ch = mock( Channel.class );
        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
        when(ctx.channel()).thenReturn( ch );

        when( ch.alloc() ).thenReturn( UnpooledByteBufAllocator.DEFAULT );
        when( ctx.alloc() ).thenReturn( UnpooledByteBufAllocator.DEFAULT );

        SocketTransportHandler handler = new SocketTransportHandler( protocolChooser( session ), NullLogProvider.getInstance() );

        // And Given a session has been established
        handler.channelRead( ctx, handshake() );

        // When
        handler.channelInactive( ctx );

        // Then
        verify(session).close();
    }

    @Test
    public void logsAndClosesConnectionOnUnexpectedExceptions() throws Throwable
    {
        // Given
        Session session = mock(Session.class);
        Channel ch = mock( Channel.class );
        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
        when(ctx.channel()).thenReturn( ch );

        when( ch.alloc() ).thenReturn( UnpooledByteBufAllocator.DEFAULT );
        when( ctx.alloc() ).thenReturn( UnpooledByteBufAllocator.DEFAULT );

        AssertableLogProvider logging = new AssertableLogProvider();

        SocketTransportHandler handler = new SocketTransportHandler( protocolChooser( session ), logging );

        // And Given a session has been established
        handler.channelRead( ctx, handshake() );

        // When
        Throwable cause = new Throwable( "Oh no!" );
        handler.exceptionCaught( ctx, cause );

        // Then
        verify(session).close();
        logging.assertExactly( inLog( SocketTransportHandler.class )
            .error( equalTo("Fatal error occurred when handling a client connection: Oh no!"), is(cause) ) );
    }

    private SocketTransportHandler.ProtocolChooser protocolChooser( final Session session )
    {
        PrimitiveLongObjectMap<Function<Channel,BoltProtocol>> availableVersions = longObjectMap();
        availableVersions.put( BoltProtocolV1.VERSION, new Function<Channel,BoltProtocol>()
        {
            @Override
            public BoltProtocol apply( Channel channel )
            {
                return new BoltProtocolV1( NullLogService.getInstance(), session, channel );
            }
        } );

        return new SocketTransportHandler.ProtocolChooser( availableVersions );
    }

    private ByteBuf handshake()
    {
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.buffer();
        buf.writeInt( 1 );
        buf.writeInt( 0 );
        buf.writeInt( 0 );
        buf.writeInt( 0 );
        return buf;
    }

}