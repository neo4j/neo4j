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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NettyApplicationTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    @Rule
    public final NettyApplicationHelper nettyApplicationHelper = new NettyApplicationHelper();

    @Test
    public void shouldTerminateEventLoopGroupOnShutdown() throws Throwable
    {
        // given
        EventLoopContext<NioServerSocketChannel> eventLoopContext =
                nettyApplicationHelper.createRealEventLoopContext( NioServerSocketChannel.class );
        NettyApplication<NioServerSocketChannel> applicationSubject = new NettyApplication<NioServerSocketChannel>(
                mock( ChannelService.class ), () -> eventLoopContext );
        EventLoopGroup eventExecutors = eventLoopContext.eventExecutors();
        assertFalse( eventExecutors.isTerminated() );

        // when
        applicationSubject.init();
        applicationSubject.shutdown();

        // then
        assertTrue( eventExecutors.isTerminated() );
    }

    @Test
    public void shouldHandleNullOnShutdown() throws Throwable
    {
        // given
        NettyApplication<NioServerSocketChannel> applicationSubject = new NettyApplication<NioServerSocketChannel>(
                mock( ChannelService.class ), () -> new EventLoopContext<>( null, NioServerSocketChannel.class ) );
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "EventLoopGroup cannot be null" );

        // when
        applicationSubject.shutdown();

        // then expected exceptions
    }

    @Test
    public void shouldThrowFailedFutureCauseWhenShuttingDown() throws Throwable
    {
        // given
        Exception exception = new IllegalArgumentException( "some exception" );
        EventLoopGroup mockedEventExecutor = nettyApplicationHelper.createMockedEventExecutor( doThrow( exception ) );
        NettyApplication<NioServerSocketChannel> applicationSubject = new NettyApplication<NioServerSocketChannel>(
                mock( ChannelService.class ),
                () -> new EventLoopContext<>( mockedEventExecutor, NioServerSocketChannel.class ) );
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "some exception" );

        // when
        applicationSubject.init();
        applicationSubject.shutdown();

        // then expected exceptions
    }

    @Test
    public void shouldCallChannelServiceOnStartAndStop() throws Throwable
    {
        // given
        EventLoopGroup eventExecutors = nettyApplicationHelper.createMockedEventExecutor( doReturn( null ) );
        ChannelService<ServerBootstrap,ServerChannel> channelService = mock( ChannelService.class );
        EventLoopContext<ServerChannel> context = new EventLoopContext<>( eventExecutors, ServerChannel.class );

        NettyApplication<ServerChannel> nettyApplication = new NettyApplication<>( channelService, () -> context );
        nettyApplication.init();

        // when
        nettyApplication.start();
        nettyApplication.stop();
        nettyApplication.start();
        nettyApplication.stop();

        //then
        verify( channelService, times( 2 ) ).start();
        verify( channelService, times( 2 ) ).closeChannels();
        verify( channelService, times( 1 ) ).bootstrap( context );
    }
}
