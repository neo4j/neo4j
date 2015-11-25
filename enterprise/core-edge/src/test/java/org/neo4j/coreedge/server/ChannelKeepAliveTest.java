/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.junit.Test;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.ExpiryScheduler;
import org.neo4j.coreedge.server.Expiration;
import org.neo4j.coreedge.server.SenderService;
import org.neo4j.helpers.FakeClock;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;

import static java.util.concurrent.TimeUnit.MINUTES;
import static junit.framework.TestCase.assertEquals;
import static org.neo4j.coreedge.PortsForIntegrationTesting.findFreeAddress;

public class ChannelKeepAliveTest
{
    /** Server that sends "Hello, World!" on connect. */
    private Channel bootstrapHelloServer( InetSocketAddress serverAddress ) throws IOException
    {
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group( new NioEventLoopGroup() )
                .channel( NioServerSocketChannel.class )
                .localAddress( new InetSocketAddress( serverAddress.getPort() ) )
                .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast( new LengthFieldPrepender( 4 ) );

                        ByteBuf buffer = ch.alloc().buffer();
                        String message = "Hello, World!";
                        buffer.writeInt( message.getBytes().length );
                        buffer.writeBytes( message.getBytes() );
                        ch.write( buffer );
                        ch.flush();
                    }
                } );

        return bootstrap.bind().syncUninterruptibly().channel();
    }

    /** Client that just discards read data. */
    private ChannelInitializer<SocketChannel> discardClientInitializer()
    {
        ChannelInitializer<SocketChannel> channelInitializer = new ChannelInitializer<SocketChannel>()
        {
            @Override
            protected void initChannel( SocketChannel channel ) throws Exception
            {
                final SimpleChannelInboundHandler<ByteBuf> clientHandler = new SimpleChannelInboundHandler<ByteBuf>()
                {
                    @Override
                    protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg ) throws Exception
                    {
                        int size = msg.readInt();
                        byte[] bytes = new byte[size];
                        msg.readBytes( bytes );
                    }
                };

                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                pipeline.addLast( clientHandler );
            }
        };

        return channelInitializer;
    }

    @Test
    public void shouldReapChannelOnlyAfterItHasExpired() throws Throwable
    {
        // given
        InetSocketAddress serverAddress = findFreeAddress();
        Channel serverChannel = bootstrapHelloServer( serverAddress );

        final FakeClock fakeClock = new FakeClock();

        OnDemandJobScheduler onDemandJobScheduler = new OnDemandJobScheduler();
        SenderService senderService = new SenderService( new ExpiryScheduler( onDemandJobScheduler ),
                new Expiration( fakeClock, 2, MINUTES ), discardClientInitializer(), NullLogProvider.getInstance() );

        senderService.start();
        senderService.send( new AdvertisedSocketAddress( serverAddress ), "GO!" );

        // when
        fakeClock.forward( 1, TimeUnit.MINUTES ); // 1 minutes total < 2 minutes expiry time
        onDemandJobScheduler.runJob();

        //then
        assertEquals( 1, senderService.activeChannelCount() );

        // when
        fakeClock.forward( 2, TimeUnit.MINUTES ); // (1+2) minutes total > 2 minutes expiry time
        onDemandJobScheduler.runJob();

        //then
        assertEquals( 0, senderService.activeChannelCount() );

        senderService.stop();
        serverChannel.close();
    }
}
