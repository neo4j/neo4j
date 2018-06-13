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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import javax.net.ssl.SSLEngine;

import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.lang.Enum.valueOf;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SecureServer
{
    public static final byte[] RESPONSE = {5, 6, 7, 8};

    private SslContext sslContext;
    private Channel channel;
    private NioEventLoopGroup eventLoopGroup;
    private final LogProvider logProvider;
    private final boolean verifyHostname;

    public SecureServer( SslContext sslContext, boolean verifyHostname )
    {
        this( sslContext, verifyHostname, NullLogProvider.getInstance() );
    }

    public SecureServer( SslContext sslContext, boolean verifyHostname, LogProvider logProvider )
    {
        this.sslContext = sslContext;
        this.logProvider = logProvider;
        this.verifyHostname = verifyHostname;
    }

    public void start()
    {
        eventLoopGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group( eventLoopGroup )
                .channel( NioServerSocketChannel.class )
                .option( ChannelOption.SO_REUSEADDR, true )
                .localAddress( 0 )
                .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {
                        ChannelPipeline pipeline = ch.pipeline();

                        OnConnectSslHandlerInjectorHandler sslHandler =
                                new OnConnectSslHandlerInjectorHandler( ch, sslContext, false, verifyHostname, logProvider );
                        pipeline.addLast( sslHandler );

                        pipeline.addLast( new Responder() );
                    }
                } );

        channel = bootstrap.bind().syncUninterruptibly().channel();
    }

    public void stop()
    {
        channel.close().awaitUninterruptibly();
        channel = null;
        eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
    }

    public int port()
    {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    static class Responder extends SimpleChannelInboundHandler<ByteBuf>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg ) throws Exception
        {
            ctx.channel().writeAndFlush( ctx.alloc().buffer().writeBytes( RESPONSE ) );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
        {
            //cause.printStackTrace(); // for debugging
        }
    }
}
