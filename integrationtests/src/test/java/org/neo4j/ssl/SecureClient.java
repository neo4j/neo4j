/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.ssl;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;

import javax.net.ssl.SSLEngine;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class SecureClient
{
    private Bootstrap bootstrap;
    private ClientInitializer clientInitializer;
    private NioEventLoopGroup eventLoopGroup;
    private Channel channel;
    private Bucket bucket = new Bucket();

    public SecureClient( SslContext sslContext )
    {
        eventLoopGroup = new NioEventLoopGroup();
        clientInitializer = new ClientInitializer( sslContext, bucket );
        bootstrap = new Bootstrap()
                .group( eventLoopGroup )
                .channel( NioSocketChannel.class )
                .handler( clientInitializer );
    }

    public void connect( int port )
    {
        ChannelFuture channelFuture = bootstrap.connect( "localhost", port ).awaitUninterruptibly();
        channel = channelFuture.channel();
        if ( !channelFuture.isSuccess() )
        {
            throw new RuntimeException( "Failed to connect", channelFuture.cause() );
        }
    }

    void disconnect()
    {
        if ( channel != null )
        {
            channel.close().awaitUninterruptibly();
            eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
        }

        bucket.collectedData.release();
    }

    void assertResponse( ByteBuf expected ) throws InterruptedException
    {
        assertEventually( channel.toString(), () -> bucket.collectedData, equalTo( expected ), 5, SECONDS );
    }

    Channel channel()
    {
        return channel;
    }

    public Future<Channel> sslHandshakeFuture()
    {
        return clientInitializer.handshakeFuture;
    }

    public String ciphers()
    {
        return clientInitializer.sslEngine.getSession().getCipherSuite();
    }

    public String protocol()
    {
        return clientInitializer.sslEngine.getSession().getProtocol();
    }

    static class Bucket extends SimpleChannelInboundHandler<ByteBuf>
    {
        private final ByteBuf collectedData;

        Bucket()
        {
            collectedData = ByteBufAllocator.DEFAULT.buffer();
        }

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg ) throws Exception
        {
            collectedData.writeBytes( msg );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
        {
            //cause.printStackTrace(); // for debugging
        }
    }

    public static class ClientInitializer extends ChannelInitializer<SocketChannel>
    {
        private SslContext sslContext;
        private final Bucket bucket;
        private Future<Channel> handshakeFuture;
        private SSLEngine sslEngine;

        ClientInitializer( SslContext sslContext, Bucket bucket )
        {
            this.sslContext = sslContext;
            this.bucket = bucket;
        }

        @Override
        protected void initChannel( SocketChannel channel ) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            this.sslEngine = sslContext.newEngine( channel.alloc() );
            this.sslEngine.setUseClientMode( true );

            SslHandler sslHandler = new SslHandler( sslEngine );
            handshakeFuture = sslHandler.handshakeFuture();

            pipeline.addLast( sslHandler );
            pipeline.addLast( bucket );
        }
    }
}
