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
package org.neo4j.causalclustering.common.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketException;

import org.neo4j.causalclustering.common.EventLoopContext;
import org.neo4j.causalclustering.common.NettyApplicationHelper;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;

public class ServerBindToChannelTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    @Rule
    public final NettyApplicationHelper nettyApplicationHelper = new NettyApplicationHelper();

    @Test
    public void shouldThrowFailedFutureCauseAsBindExceptionAndGiveExtraLogging() throws Throwable
    {
        // given
        Exception exception = new BindException( "some exception" );
        AssertableLogProvider userLogProvider = new AssertableLogProvider();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ServerBootstrap bootstrap = nettyApplicationHelper.createBindFailingMockedServeBootstrapper( exception );

        ServerBindToChannel<ServerChannel> serverChannelService = new ServerBindToChannel<>(
                () -> new InetSocketAddress( 1 ), logProvider, userLogProvider,
                serverChannelEventLoopContext -> bootstrap );

        expectedException.expect( BindException.class );
        expectedException.expectMessage( "some exception" );

        // when
        serverChannelService.bootstrap( null );
        serverChannelService.start();

        //then
        logProvider.assertContainsMessageContaining( "Address is already bound for setting" );
        userLogProvider.assertContainsMessageContaining( "Address is already bound for setting" );
        // and expected exceptions
    }

    @Test
    public void shouldThrowFailedFutureCauseAsRuntimeException() throws Throwable
    {
        // given
        Exception exception = new SocketException( "some exception" );
        ServerBootstrap bootstrap =
                nettyApplicationHelper.createBindFailingMockedServeBootstrapper( exception );

        ServerBindToChannel<ServerChannel> serverChannelService = new ServerBindToChannel<>(
                () -> new InetSocketAddress( 1 ), NullLogProvider.getInstance(), NullLogProvider.getInstance(),
                serverChannelEventLoopContext -> bootstrap );

        expectedException.expect( RuntimeException.class );
        expectedException.expectMessage( "some exception" );

        // when
        serverChannelService.bootstrap( null );
        serverChannelService.start();

        // then expected exceptions
    }

    @Test
    public void shouldFailToStartIfNotBootstrapped() throws Throwable
    {
        // given
        ServerBindToChannel<ServerChannel> serverBindToChannel =
                new ServerBindToChannel<>( () -> mock( InetSocketAddress.class ), NullLogProvider
                        .getInstance(),
                        NullLogProvider.getInstance(),
                        eventLoopContext -> mock( ServerBootstrap.class ) );

        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage( "The Channel Manager has not been bootstrapped." );

        // when
        serverBindToChannel.start();

        //then expected exception
    }

    @Test
    public void shouldReleaseChannelWhenStopped() throws Throwable
    {
        //given
        EventLoopContext<NioServerSocketChannel> realEventLoopContext =
                nettyApplicationHelper.createRealEventLoopContext( NioServerSocketChannel.class );
        ServerBindToChannel<NioServerSocketChannel> realService = nettyApplicationHelper
                .createRealServerChannelService( NullLogProvider.getInstance(), NullLogProvider.getInstance() );

        // when
        realService.bootstrap( realEventLoopContext );
        realService.start();
        realService.closeChannels();
        realService.start();
        realService.closeChannels();

        //then should throw no exception
    }

    @Test
    public void shouldFailToBindIfAlreadyStarted() throws Throwable
    {
        //given
        EventLoopContext<NioServerSocketChannel> realEventLoopContext =
                nettyApplicationHelper.createRealEventLoopContext( NioServerSocketChannel.class );
        ServerBindToChannel<NioServerSocketChannel> realService = nettyApplicationHelper
                .createRealServerChannelService( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage( "Already running" );

        // when
        try
        {
            realService.bootstrap( realEventLoopContext );
            realService.start();
            realService.start();
        }
        finally
        {
            realService.closeChannels();
        }

        //then expected exceptions
    }
}
