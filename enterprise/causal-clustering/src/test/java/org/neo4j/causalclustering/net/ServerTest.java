/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.ports.allocation.PortAuthority;

import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.net.Server.QUIET_PERIOD_MILLIS;

public class ServerTest
{
    private final int BLOCKING_TIME_MILLIS = QUIET_PERIOD_MILLIS * 2;
    private final ChannelInboundHandlerAdapter EMPTY_HANDLER = new ChannelInboundHandlerAdapter();

    @Test
    public void shouldAwaitBlockingClientInStop() throws Exception
    {
        // given
        Semaphore clientPermit = new Semaphore( 0 );
        AtomicBoolean blockingOperationComplete = new AtomicBoolean();
        ListenSocketAddress listenAddress = new ListenSocketAddress( "localhost", PortAuthority.allocatePort() );
        ChildInitializer childInitializer = channel -> channel.pipeline().addLast( new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg ) throws InterruptedException
            {
                // this releases the client, since we are now in the server blocking
                clientPermit.release();
                // this is part of the assertion, blocking longer than the quiet period
                Thread.sleep( BLOCKING_TIME_MILLIS );
                blockingOperationComplete.set( true );
            }
        } );

        Server server = new Server( childInitializer, listenAddress, "test-server" );
        server.start();

        EventLoopGroup eventLoopGroup = new NioEventLoopGroup( 1, new NamedThreadFactory( "test-client" ) );

        Bootstrap bootstrap = new Bootstrap()
                .group( eventLoopGroup )
                .channel( NioSocketChannel.class )
                .handler( EMPTY_HANDLER );

        ChannelFuture fChannel = bootstrap.connect( server.address().socketAddress() );
        Channel client = fChannel.sync().channel();

        // when
        // writing a message
        client.writeAndFlush( ByteBufAllocator.DEFAULT.buffer( 1 ).writeByte( 1 ) );
        // await server to receive the message
        clientPermit.acquire();

        // close the connection, while the server is blocking
        client.close().sync();
        // stop should still block
        server.stop();

        // then
        // assert that we blocked in stop() until the operation completed
        assertTrue( blockingOperationComplete.get() );
    }
}
