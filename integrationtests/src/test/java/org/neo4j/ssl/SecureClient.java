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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.net.ssl.SSLException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class SecureClient
{
    private Bootstrap bootstrap;
    private NioEventLoopGroup eventLoopGroup;
    private Channel channel;
    private Bucket bucket = new Bucket();

    private String protocol;
    private String ciphers;
    private SslHandshakeCompletionEvent handshakeEvent;
    private CompletableFuture<Channel> handshakeFuture = new CompletableFuture<>();

    public SecureClient( SslPolicy sslPolicy ) throws SSLException
    {
        eventLoopGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap().group( eventLoopGroup )
                .channel( NioSocketChannel.class )
                .handler( new ClientInitializer( sslPolicy, bucket ) );
    }

    public Future<Channel> sslHandshakeFuture()
    {
        return handshakeFuture;
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
        if ( ciphers == null )
        {
            throw new IllegalStateException( "Handshake must have been completed" );
        }
        return ciphers;
    }

    String protocol()
    {
        if ( protocol == null )
        {
            throw new IllegalStateException( "Handshake must have been completed" );
        }
        return protocol;
    }

    static class Bucket extends SimpleChannelInboundHandler<ByteBuf>
    {
        private final ByteBuf collectedData;

        Bucket()
        {
            collectedData = ByteBufAllocator.DEFAULT.buffer();
        }

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg )
        {
            collectedData.writeBytes( msg );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
        {
        }
    }

    public class ClientInitializer extends ChannelInitializer<SocketChannel>
    {
        private SslContext sslContext;
        private final Bucket bucket;
        private final SslPolicy sslPolicy;

        ClientInitializer( SslPolicy sslPolicy, Bucket bucket ) throws SSLException
        {
            this.sslContext = sslPolicy.nettyClientContext();
            this.bucket = bucket;
            this.sslPolicy = sslPolicy;
        }

        @Override
        protected void initChannel( SocketChannel channel )
        {
            ChannelPipeline pipeline = channel.pipeline();

            ChannelHandler clientOnConnectSslHandler = sslPolicy.nettyClientHandler( channel, sslContext );

            pipeline.addLast( clientOnConnectSslHandler );
            pipeline.addLast( new ChannelInboundHandlerAdapter()
                {
                    @Override
                    public void userEventTriggered( ChannelHandlerContext ctx, Object evt ) throws Exception
                    {
                        if ( evt instanceof SslHandlerDetailsRegisteredEvent )
                        {
                            SslHandlerDetailsRegisteredEvent sslHandlerDetailsRegisteredEvent = (SslHandlerDetailsRegisteredEvent) evt;
                            protocol = sslHandlerDetailsRegisteredEvent.protocol;
                            ciphers = sslHandlerDetailsRegisteredEvent.cipherSuite;
                            handshakeFuture.complete( ctx.channel() ); // We complete the handshake here since it will also signify that the correct
                            // information has been carried
                            return;
                        }
                        if ( evt instanceof SslHandshakeCompletionEvent )
                        {
                            handshakeEvent = (SslHandshakeCompletionEvent) evt;
                            if ( handshakeEvent.cause() != null )
                            {
                                handshakeFuture.completeExceptionally( handshakeEvent.cause() );
                            }
                            // We do not complete if no error, that will be handled by the funky SslHandlerReplacedEvent
                        }
                    }
                } );
            pipeline.addLast( bucket );
        }
    }
}
