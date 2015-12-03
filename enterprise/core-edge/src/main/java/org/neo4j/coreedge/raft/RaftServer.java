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
package org.neo4j.coreedge.raft;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.coreedge.server.logging.ExceptionLoggingHandler;
import org.neo4j.coreedge.raft.net.codecs.RaftMessageDecoder;
import org.neo4j.coreedge.raft.replication.ReplicatedContentMarshal;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftServer<MEMBER> extends LifecycleAdapter implements Inbound
{
    private final ListenSocketAddress listenAddress;
    private final Log log;
    private final ReplicatedContentMarshal<ByteBuf> serializer;
    private MessageHandler messageHandler;
    private EventLoopGroup workerGroup;
    private Channel channel;

    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "raft-server" );

    public RaftServer( ReplicatedContentMarshal<ByteBuf> serializer, ListenSocketAddress listenAddress, LogProvider logProvider )
    {
        this.serializer = serializer;
        this.listenAddress = listenAddress;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public synchronized void start() throws Throwable
    {
        startNettyServer();
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        try
        {
            channel.close().sync();
        }
        catch( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            log.warn( "Interrupted while closing channel." );
        }

        if ( workerGroup.shutdownGracefully( 2, 5, TimeUnit.SECONDS ).awaitUninterruptibly( 10, TimeUnit.SECONDS ) )
        {
            log.warn( "Worker group not shutdown within 10 seconds." );
        }
    }

    private void startNettyServer()
    {
        workerGroup = new NioEventLoopGroup( 0, threadFactory );

        log.info( "Starting server at: " + listenAddress );

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group( workerGroup )
                .channel( NioServerSocketChannel.class )
                .option( ChannelOption.SO_REUSEADDR, true )
                .localAddress( listenAddress.socketAddress() )
                .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                        pipeline.addLast( new LengthFieldPrepender( 4 ) );
                        pipeline.addLast( new RaftMessageDecoder( serializer ) );
                        pipeline.addLast( new RaftMessageHandler() );
                        pipeline.addLast( new ExceptionLoggingHandler( log ) );
                    }
                } );

        channel = bootstrap.bind().syncUninterruptibly().channel();
    }

    @Override
    public void registerHandler( Inbound.MessageHandler handler )
    {
        this.messageHandler = handler;
    }

    private class RaftMessageHandler extends SimpleChannelInboundHandler<RaftMessages.Message<MEMBER>>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext channelHandlerContext, RaftMessages.Message<MEMBER>
                message ) throws Exception
        {
            messageHandler.handle( message );
        }
    }
}
