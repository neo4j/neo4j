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
package org.neo4j.causalclustering.messaging;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.helpers.SocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class NonBlockingChannelTest
{
    private static final int PORT = 7000;
    private static final ChannelHandler VOID_HANDLER = new ChannelInitializer<SocketChannel>()
    {
        @Override
        protected void initChannel( SocketChannel ch ) throws Exception
        {
        }
    };
    private static final long DEFAULT_TIMEOUT_MS = 20_000;

    private final Log log = NullLogProvider.getInstance().getLog( getClass() );
    private final SocketAddress serverAddress = new SocketAddress( "localhost", PORT );
    private final TestServer server = new TestServer( PORT );

    private EventLoopGroup elg;
    private NonBlockingChannel channel;

    @Before
    public void before()
    {
        elg = new NioEventLoopGroup( 0 );
        Bootstrap bootstrap = new Bootstrap().channel( NioSocketChannel.class ).group( elg ).handler( VOID_HANDLER );
        channel = new NonBlockingChannel( bootstrap, elg.next(), serverAddress, log );
    }

    @After
    public void after()
    {
        elg.shutdownGracefully( 0, DEFAULT_TIMEOUT_MS, MILLISECONDS ).awaitUninterruptibly();
        server.stop();
    }

    @Test
    public void shouldBeAbleToSendMessage() throws Exception
    {
        // given
        server.start();

        // when
        channel.start();

        // when
        Future<Void> fSend = channel.send( emptyBuffer() );

        // then
        assertTrue( fSend.await( DEFAULT_TIMEOUT_MS ) );
        assertNull( fSend.cause() );
    }

    @Test
    public void shouldAllowDeferredSend() throws Exception
    {
        // given
        channel.start();
        server.start();

        // this is slightly racy, but generally we will send before the channel was connected
        // this is benign in the sense that the test will pass in the condition where it was already connected as well

        // when
        Future<Void> fSend = channel.send( emptyBuffer() );

        // then
        assertTrue( fSend.await( DEFAULT_TIMEOUT_MS ) );
        assertNull( fSend.cause() );
        assertTrue( fSend.isSuccess() );
    }

    @Test
    public void shouldFailSendWhenNoServer() throws Exception
    {
        // given
        channel.start();

        // when
        Future<Void> fSend = channel.send( emptyBuffer() );

        // then
        assertTrue( fSend.await( DEFAULT_TIMEOUT_MS ) );
        assertFalse( fSend.isSuccess() );
    }

    @Test
    public void shouldReconnectAfterServerComesBack() throws Exception
    {
        // given
        server.start();
        channel.start();

        // when
        Future<Void> fSend = channel.send( emptyBuffer() );

        // then
        assertTrue( fSend.awaitUninterruptibly( DEFAULT_TIMEOUT_MS ) );
        assertNull( fSend.cause() );

        // when
        server.stop();
        fSend = channel.send( emptyBuffer() );

        // then
        assertTrue( fSend.await( DEFAULT_TIMEOUT_MS ) );
        assertFalse( fSend.isSuccess() );

        // when
        server.start();
        fSend = channel.send( emptyBuffer() );

        // then
        assertTrue( fSend.await( DEFAULT_TIMEOUT_MS ) );
        assertTrue( fSend.isSuccess() );
    }

    @Test
    public void shouldNotAllowSendingOnDisposedChannel() throws Exception
    {
        // given
        server.start();
        channel.start();

        // ensure we are connected
        Future<Void> fSend = channel.send( emptyBuffer() );
        fSend.await( DEFAULT_TIMEOUT_MS );
        assertTrue( fSend.isSuccess() );
        assertEventually( "", server::childCount, equalTo( 1 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );

        // when
        channel.dispose();

        try
        {
            channel.send( emptyBuffer() );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }

        // then
        assertEventually( "", server::childCount, equalTo( 0 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
    }

    private ByteBuf emptyBuffer()
    {
        return ByteBufAllocator.DEFAULT.buffer();
    }
}
