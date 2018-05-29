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
package org.neo4j.causalclustering.messaging;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

class TestServer
{
    private final int port;
    private final ChildHandler childHandler;
    private EventLoopGroup elg;
    private ChannelFuture fServer;

    TestServer( int port )
    {
        this( port, null );
    }

    private TestServer( int port, ChannelInitializer<SocketChannel> channelInitializer )
    {
        this.port = port;
        this.childHandler = new ChildHandler( channelInitializer );
    }

    void start()
    {
        elg = new NioEventLoopGroup( 0 );
        ServerBootstrap boot = new ServerBootstrap()
                .channel( NioServerSocketChannel.class )
                .option( ChannelOption.SO_REUSEADDR, Boolean.TRUE )
                .childHandler( childHandler )
                .group( elg );
        fServer = boot.bind( port );
        fServer.syncUninterruptibly();
    }

    void stop()
    {
        if ( elg != null )
        {
            fServer.channel().close().syncUninterruptibly();
            childHandler.closeAll();
            elg.shutdownGracefully().syncUninterruptibly();
            elg = null;
        }
    }

    int childCount()
    {
        return childHandler.channels.size();
    }

    class ChildHandler extends ChannelInitializer<SocketChannel>
    {
        private final ChannelInitializer<SocketChannel> delegate;
        private final Set<Channel> channels = new ConcurrentSkipListSet<>();

        ChildHandler( ChannelInitializer<SocketChannel> delegate )
        {
            super();
            this.delegate = delegate;
        }

        @Override
        public void handlerAdded( ChannelHandlerContext ctx ) throws Exception
        {
            super.handlerAdded( ctx );
            if ( delegate != null )
            {
                delegate.handlerAdded( ctx );
            }
        }

        @Override
        protected void initChannel( SocketChannel ch ) throws Exception
        {
            channels.add( ch );
            ch.closeFuture().addListener( future -> channels.remove( ch ) );
        }

        void closeAll()
        {
            channels.forEach( channel -> channel.close().syncUninterruptibly() );
        }
    }
}
