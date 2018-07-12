/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.assertion.Assert.assertObjectOrArrayEquals;

public class SecureClient
{
    private Bootstrap bootstrap;
    private ClientInitializer clientInitializer;
    private NioEventLoopGroup eventLoopGroup;
    private Channel channel;
    private Bucket bucket = new Bucket();
    private SslPolicy sslPolicy;

    public SecureClient( SslPolicy sslPolicy, LogProvider logProvider ) throws SSLException
    {
        eventLoopGroup = new NioEventLoopGroup();
        clientInitializer = new ClientInitializer( sslPolicy, bucket, logProvider );
        bootstrap = new Bootstrap().group( eventLoopGroup ).channel( NioSocketChannel.class ).handler( clientInitializer );
    }

    public Future<Channel> sslHandshakeFuture()
    {
        return clientInitializer.channelFuture;
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

    String ciphers()
    {
        return clientInitializer.getSslEngine().getSession().getCipherSuite();
    }

    String protocol()
    {
        return clientInitializer.getSslEngine().getSession().getProtocol();
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
        }
    }

    public static class ClientInitializer extends ChannelInitializer<SocketChannel>
    {
        private SslContext sslContext; // TODO
        private final Bucket bucket;
        private OnConnectSslHandlerInjectorHandler onConnectSslHandler;
        Future<Channel> channelFuture;
        private final LogProvider logProvider;
        private final SslPolicy sslPolicy;

        ClientInitializer( SslPolicy sslPolicy, Bucket bucket, LogProvider logProvider ) throws SSLException
        {
            this.sslContext = sslPolicy.nettyClientContext();
            this.bucket = bucket;
            this.logProvider = logProvider;
            this.sslPolicy = sslPolicy;
        }

        SSLEngine getSslEngine()
        {
            return onConnectSslHandler.getSslHandler().engine();
        }

        @Override
        protected void initChannel( SocketChannel channel ) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

//            String[] tlsVersions = null;

            onConnectSslHandler = (OnConnectSslHandlerInjectorHandler) sslPolicy.nettyClientHandler( channel, sslContext );
//                    new OnConnectSslHandlerInjectorHandler( channel, sslContext, true, verifyHostname, tlsVersions, logProvider );
            channelFuture = onConnectSslHandler.handshakeFuture();

            pipeline.addLast( onConnectSslHandler );
            pipeline.addLast( bucket );
        }
    }
}
