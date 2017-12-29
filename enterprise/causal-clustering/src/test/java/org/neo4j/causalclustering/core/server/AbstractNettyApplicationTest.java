/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractNettyApplicationTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldTerminateEventLoopGroupOnShutdown() throws Throwable
    {
        // given
        StubNettyApplication
                stubServer = StubNettyApplication.realEventExecutor();
        assertFalse( stubServer.getEventExecutors().isTerminated() );

        // when
        stubServer.shutdown();

        // then
        assertTrue( stubServer.getEventExecutors().isTerminated() );
    }

    @Test
    public void shouldHandleNullOnShutdown() throws Throwable
    {
        // given
        EventLoopGroup eventExecutors = null;
        StubNettyApplication stubServer = new StubNettyApplication( eventExecutors );
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "EventLoopGroup cannot be null" );

        // then
        stubServer.shutdown();
    }

    @Test
    public void shouldThrowFailedFutureCauseWhenShuttingDown() throws Throwable
    {
        // given
        EventLoopGroup eventExecutors = mock( EventLoopGroup.class );
        Future future = mock( Future.class );
        Exception exception = new IllegalArgumentException( "some exception" );
        doThrow( exception ).when( future ).get( anyLong(), any( TimeUnit.class ) );
        when( eventExecutors.shutdownGracefully( anyLong(), anyLong(), any( TimeUnit.class ) ) ).thenReturn( future );
        StubNettyApplication
                stubServer = new StubNettyApplication( eventExecutors );
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "some exception" );

        // when
        stubServer.shutdown();

    }

    @Test
    public void shouldThrowFailedFutureCauseAsBindException() throws Throwable
    {
        // given
        StubNettyApplication stubServer = new StubNettyApplication( new BindException( "some exception" ) );
        expectedException.expect( BindException.class );
        expectedException.expectMessage( "some exception" );

        // when
        stubServer.init();
        stubServer.start();
    }

    @Test
    public void shouldThrowFailedFutureCauseAsRuntimeException() throws Throwable
    {
        // given
        StubNettyApplication stubServer = new StubNettyApplication( new SocketException( "some exception" ) );
        expectedException.expect( RuntimeException.class );
        expectedException.expectMessage( "some exception" );

        // when
        stubServer.init();
        stubServer.start();
    }

    @Test
    public void shouldThrowIllegalArgumentIfCauseIsNull() throws Throwable
    {
        // given
        SocketException exception = null;
        StubNettyApplication stubServer = new StubNettyApplication( exception );
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Cause cannot be null" );

        // when
        stubServer.init();
        stubServer.start();
    }

    @Test
    public void shouldBeAbleToRestart() throws Throwable
    {
        // given
        StubNettyApplication stubServer = StubNettyApplication.mockedEventExecutor();
        stubServer.init();

        // when
        stubServer.start();
        stubServer.stop();
        stubServer.stop();
        stubServer.start();

        //then
        assertEquals( 2, stubServer.bootstrap().getBindCalls() );
    }

    @Test
    public void shouldNotRebindIfAlreadyRunning() throws Throwable
    {
        // given
        StubNettyApplication stubServer = StubNettyApplication.mockedEventExecutor();
        stubServer.init();

        // when
        stubServer.start();
        stubServer.start();

        //then
        assertEquals( 1, stubServer.bootstrap().getBindCalls() );
    }

    @Test
    public void shouldReleaseChannelWhenStopped() throws Throwable
    {
        RealNettyApplication realNettyApplication1 =
                new RealNettyApplication( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        RealNettyApplication realNettyApplication2 =
                new RealNettyApplication( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        try
        {
            realNettyApplication1.init();
            realNettyApplication2.init();
            realNettyApplication1.start();
            realNettyApplication1.stop();
            realNettyApplication2.start();
            realNettyApplication2.stop();
            realNettyApplication1.start();
            realNettyApplication1.stop();
        }
        finally
        {
            try
            {
                realNettyApplication1.shutdown();
            }
            finally
            {
                realNettyApplication2.shutdown();
            }

        }
    }

    private class RealNettyApplication extends AbstractNettyApplication<Bootstrap>
    {
        private final NioEventLoopGroup eventExecutors;

        RealNettyApplication( LogProvider logProvider,
                LogProvider userLogProvider )
        {
            super( logProvider, userLogProvider );
            eventExecutors = new NioEventLoopGroup( 0, new NamedThreadFactory( "test" ) );
        }

        @Override
        protected EventLoopGroup getEventLoopGroup()
        {
            return eventExecutors;
        }

        @Override
        protected Bootstrap bootstrap()
        {
            return new Bootstrap()
                    .group( eventExecutors )
                    .channel( NioSocketChannel.class ).handler( new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel( SocketChannel ch ) throws Exception
                        {

                        }
                    } );
        }

        @Override
        protected InetSocketAddress bindAddress()
        {
            return new InetSocketAddress( PortAuthority.allocatePort() );
        }
    }
}
