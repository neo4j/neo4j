/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.causalclustering.net.Server;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ReconnectingChannelIT
{
    private static final int PORT = PortAuthority.allocatePort();
    private static final long DEFAULT_TIMEOUT_MS = 20_000;
    private final Log log = NullLogProvider.getInstance().getLog( getClass() );
    private final ListenSocketAddress listenAddress = new ListenSocketAddress( "localhost", PORT );
    private final Server server = new Server( channel -> {}, listenAddress, "test-server" );
    private EventLoopGroup elg;
    private ReconnectingChannel channel;
    private AtomicInteger childCount = new AtomicInteger();
    private final ChannelHandler childCounter = new ChannelInitializer<SocketChannel>()
    {
        @Override
        protected void initChannel( SocketChannel ch )
        {
            ch.pipeline().addLast( new ChannelInboundHandlerAdapter()
            {
                @Override
                public void channelActive( ChannelHandlerContext ctx )
                {
                    childCount.incrementAndGet();
                }

                @Override
                public void channelInactive( ChannelHandlerContext ctx )
                {
                    childCount.decrementAndGet();
                }
            } );
        }
    };

    @Before
    public void before()
    {
        elg = new NioEventLoopGroup( 0 );
        Bootstrap bootstrap = new Bootstrap().channel( NioSocketChannel.class ).group( elg ).handler( childCounter );
        channel = new ReconnectingChannel( bootstrap, elg.next(), listenAddress, log );
    }

    @After
    public void after() throws Throwable
    {
        elg.shutdownGracefully( 0, DEFAULT_TIMEOUT_MS, MILLISECONDS ).awaitUninterruptibly();
        server.stop();
    }

    @Test
    public void shouldBeAbleToSendMessage() throws Throwable
    {
        // given
        server.start();

        // when
        channel.start();

        // when
        Future<Void> fSend = channel.writeAndFlush( emptyBuffer() );

        // then will be successfully completed
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    @Test
    public void shouldAllowDeferredSend() throws Throwable
    {
        // given
        channel.start();
        server.start();

        // this is slightly racy, but generally we will send before the channel was connected
        // this is benign in the sense that the test will pass in the condition where it was already connected as well

        // when
        Future<Void> fSend = channel.writeAndFlush( emptyBuffer() );

        // then will be successfully completed
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    @Test( expected = ExecutionException.class )
    public void shouldFailSendWhenNoServer() throws Exception
    {
        // given
        channel.start();

        // when
        Future<Void> fSend = channel.writeAndFlush( emptyBuffer() );

        // then will throw
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    @Test
    public void shouldReconnectAfterServerComesBack() throws Throwable
    {
        // given
        server.start();
        channel.start();

        // when
        Future<Void> fSend = channel.writeAndFlush( emptyBuffer() );

        // then will not throw
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );

        // when
        server.stop();
        fSend = channel.writeAndFlush( emptyBuffer() );

        // then will throw
        try
        {
            fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
            fail( "Expected failure to send" );
        }
        catch ( ExecutionException ex )
        {
            // pass
        }

        // when
        server.start();
        fSend = channel.writeAndFlush( emptyBuffer() );

        // then will not throw
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    @Test
    public void shouldNotAllowSendingOnDisposedChannel() throws Throwable
    {
        // given
        server.start();
        channel.start();

        // ensure we are connected
        Future<Void> fSend = channel.writeAndFlush( emptyBuffer() );
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        assertEventually( childCount::get, equalTo( 1 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );

        // when
        channel.dispose();

        try
        {
            channel.writeAndFlush( emptyBuffer() );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }

        // then
        assertEventually( childCount::get, equalTo( 0 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
    }

    private ByteBuf emptyBuffer()
    {
        return ByteBufAllocator.DEFAULT.buffer();
    }
}
