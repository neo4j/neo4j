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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StubNettyApplication extends AbstractNettyApplication<StubNettyApplication.CountingBindRequestBootstrap>
{
    private final EventLoopGroup eventExecutors;
    private final CountingBindRequestBootstrap bootstrap;

    static StubNettyApplication mockedEventExecutor() throws InterruptedException, ExecutionException, TimeoutException
    {
        return new StubNettyApplication( createMockedEventExecutor() );
    }

    static StubNettyApplication realEventExecutor() throws InterruptedException, ExecutionException, TimeoutException
    {
        return new StubNettyApplication();
    }

    private StubNettyApplication()
    {
        this( new NioEventLoopGroup( 0, new NamedThreadFactory( "test" ) ) );
    }

    StubNettyApplication( Exception bindFailure ) throws InterruptedException, ExecutionException, TimeoutException
    {
        super( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        this.eventExecutors = createMockedEventExecutor();
        this.bootstrap = new CountingBindRequestBootstrap( bindFailure );

    }

    StubNettyApplication( EventLoopGroup eventExecutors )
    {
        super( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        this.eventExecutors = eventExecutors;
        this.bootstrap = new CountingBindRequestBootstrap();

    }

    public EventLoopGroup getEventExecutors()
    {
        return eventExecutors;
    }

    @Override
    protected EventLoopGroup getEventLoopGroup()
    {
        return eventExecutors;
    }

    @Override
    protected CountingBindRequestBootstrap bootstrap()
    {
        return bootstrap;
    }

    @Override
    protected InetSocketAddress bindAddress()
    {
        return new InetSocketAddress( 1 );
    }

    public class CountingBindRequestBootstrap extends Bootstrap
    {
        private int bindCalls;
        private final Exception failure;
        private final boolean failed;

        CountingBindRequestBootstrap()
        {
            this.failure = null;
            this.failed = false;
        }

        private CountingBindRequestBootstrap( Exception failure )
        {
            this.failure = failure;
            this.failed = true;
        }

        @Override
        public ChannelFuture bind( SocketAddress address )
        {
            bindCalls++;
            Channel mockedChannel = mock( Channel.class );
            ChannelFuture mockedFuture = mock( ChannelFuture.class );
            when( mockedFuture.awaitUninterruptibly() ).thenReturn( mockedFuture );
            when( mockedFuture.channel() ).thenReturn( mockedChannel );
            when( mockedChannel.close() ).thenReturn( mockedFuture );
            if ( failed )
            {
                when( mockedFuture.isSuccess() ).thenReturn( false );
                when( mockedFuture.cause() ).thenReturn( failure );
            }
            else
            {
                when( mockedFuture.isSuccess() ).thenReturn( true );
            }

            return mockedFuture;
        }

        public int getBindCalls()
        {
            return bindCalls;
        }
    }

    private static EventLoopGroup createMockedEventExecutor()
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException
    {
        EventLoopGroup eventExecutors = mock( EventLoopGroup.class );
        Future future = mock( Future.class );
        doReturn( null ).when( future ).get( anyLong(), any( TimeUnit.class ) );
        when( eventExecutors.shutdownGracefully( anyLong(), anyLong(), any( TimeUnit.class ) ) ).thenReturn( future );
        return eventExecutors;
    }
}
