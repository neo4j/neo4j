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
package org.neo4j.causalclustering.common;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.AbstractNioChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import org.mockito.stubbing.Stubber;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.causalclustering.common.server.ServerBindToChannel;
import org.neo4j.concurrent.Futures;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.ExternalResource;

import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NettyApplicationHelper extends ExternalResource
{
    private ArrayList<EventLoopGroup> realEventLoop;

    public <C extends AbstractNioChannel> EventLoopContext<C> createRealEventLoopContext( Class<C> clazz )
    {
        NioEventLoopGroup test = new NioEventLoopGroup( 0, new NamedThreadFactory( "test" ) );
        realEventLoop.add( test );
        return new EventLoopContext<>( test, clazz );
    }

    public Supplier<InetSocketAddress> createRealAddressSupplier()
    {
        return () -> new InetSocketAddress( PortAuthority.allocatePort() );
    }

    public ServerBootstrap createRealEmptyServerBootstrap( EventLoopContext<? extends ServerChannel> eventLoopContext )
    {
        return new ServerBootstrap()
                .group( eventLoopContext.eventExecutors() )
                .channel( eventLoopContext.channelClass() )
                .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {

                    }
                } );
    }

    public ServerBootstrap createBindFailingMockedServeBootstrapper( Throwable e )
    {
        ServerBootstrap bootstrap = mock( ServerBootstrap.class );
        ChannelFuture future = mock( ChannelFuture.class );
        doReturn( future ).when( bootstrap ).bind( any( InetSocketAddress.class ) );
        doReturn( false ).when( future ).isSuccess();
        doReturn( future ).when( future ).awaitUninterruptibly();
        doReturn( e ).when( future ).cause();
        return bootstrap;
    }

    public <C extends ServerChannel> ServerBindToChannel<C> createRealServerChannelService( LogProvider logProvider,
            LogProvider userLogProvider )
    {
        return new ServerBindToChannel<>( createRealAddressSupplier(), logProvider, userLogProvider,
                this::createRealEmptyServerBootstrap );
    }

    public EventLoopGroup createMockedEventExecutor( Stubber onGet )
    {
        try
        {
            EventLoopGroup eventExecutors = mock( EventLoopGroup.class );
            Future future = mock( Future.class );
            onGet.when( future ).get( anyLong(), any( TimeUnit.class ) );
            when( eventExecutors.shutdownGracefully( anyLong(), anyLong(), any( TimeUnit.class ) ) )
                    .thenReturn( future );
            return eventExecutors;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    protected void after( boolean successful ) throws Throwable
    {
        Futures.combine( realEventLoop.stream().map( EventExecutorGroup::shutdownGracefully ).collect( toList() ) )
                .get();
    }

    @Override
    protected void before() throws Throwable
    {
        realEventLoop = new ArrayList<>();
    }
}
