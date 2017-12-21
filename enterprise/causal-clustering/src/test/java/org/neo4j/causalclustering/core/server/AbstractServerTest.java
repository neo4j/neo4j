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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.NamedThreadFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractServerTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldTerminateEventLoopGroup() throws Throwable
    {
        // given
        StubServer stubServer = new StubServer();
        assertFalse( stubServer.getEventExecutors().isTerminated() );

        // when
        stubServer.shutdown();

        // then
        assertTrue( stubServer.getEventExecutors().isTerminated() );
    }

    @Test
    public void shouldHandleNull() throws Throwable
    {
        // given
        StubServer stubServer = new StubServer( null );
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "EventLoopGroup cannot be null" );

        // then
        stubServer.shutdown();
    }

    @Test
    public void shouldThrowFailedFutureCause() throws Throwable
    {
        // given
        EventLoopGroup eventExecutors = mock( EventLoopGroup.class );
        Future future = mock( Future.class );
        Exception exception = new RuntimeException( "some exception" );
        doThrow( exception ).when( future ).get( anyInt(), any( TimeUnit.class ) );
        when( eventExecutors.shutdownGracefully( anyInt(), anyInt(), any( TimeUnit.class ) ) ).thenReturn( future );
        StubServer stubServer = new StubServer( eventExecutors );
        expectedException.expect( RuntimeException.class );
        expectedException.expectMessage( "some exception" );

        // when
        stubServer.shutdown();
    }

    @Test
    public void shouldOnlyBootstrapServerIfNotRunning() throws Throwable
    {
        // given
        StubServer stubServer = new StubServer();

        // when
        stubServer.start();
        stubServer.start();

        //then
        assertEquals( 1, stubServer.bootstrapCalls() );
    }

    @Test
    public void shouldStopAndStart() throws Throwable
    {
        // given
        StubServer stubServer = new StubServer();

        // when
        stubServer.start();
        stubServer.stop();
        stubServer.stop();
        stubServer.start();

        //then
        assertEquals( 2, stubServer.bootstrapCalls() );
    }

    @Test
    public void shouldNotStartAfterShutdown() throws Throwable
    {
        // given
        StubServer stubServer = new StubServer();

        //then
        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage( "Cannot start server. Already shutdown" );

        // when
        stubServer.shutdown();
        stubServer.start();
    }

    @Test
    public void shouldNotInitAfterShutdown() throws Throwable
    {
        // given
        StubServer stubServer = new StubServer();

        //then
        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage( "Cannot initiate server. Already shutdown" );

        // when
        stubServer.shutdown();
        stubServer.init();
    }


    public class StubServer extends AbstractServer
    {
        private final EventLoopGroup eventExecutors;
        private int bootstrapCalls = 0;

        StubServer()
        {
            this( new NioEventLoopGroup( 0, new NamedThreadFactory( "test" ) ) );
        }

        StubServer( EventLoopGroup eventExecutors )
        {
            this.eventExecutors = eventExecutors;
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
        protected void bootstrapServer()
        {
            bootstrapCalls++;
        }

        public int bootstrapCalls()
        {
            return bootstrapCalls;
        }
    }
}
