/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.transport.pipeline;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.EventExecutor;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;

import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;

import static io.netty.buffer.ByteBufUtil.writeUtf8;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class HouseKeeperTest
{
    private EmbeddedChannel channel;

    @After
    public void cleanup()
    {
        if ( channel != null )
        {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldStopConnectionOnChannelInactive()
    {
        BoltConnection connection = mock( BoltConnection.class );
        channel = new EmbeddedChannel( new HouseKeeper( connection, NullLog.getInstance() ) );

        channel.pipeline().fireChannelInactive();

        verify( connection ).stop();
    }

    @Test
    public void shouldNotPropagateChannelInactive() throws Exception
    {
        ChannelInboundHandler next = mock( ChannelInboundHandler.class );
        BoltConnection connection = mock( BoltConnection.class );
        channel = new EmbeddedChannel( new HouseKeeper( connection, NullLog.getInstance() ), next );

        channel.pipeline().fireChannelInactive();

        verify( next, never() ).channelInactive( any() );
    }

    @Test
    public void shouldStopConnectionOnExceptionCaught()
    {
        BoltConnection connection = mock( BoltConnection.class );
        channel = new EmbeddedChannel( new HouseKeeper( connection, NullLog.getInstance() ) );

        channel.pipeline().fireExceptionCaught( new RuntimeException( "some exception" ) );

        verify( connection ).stop();
    }

    @Test
    public void shouldLogExceptionOnExceptionCaught()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        BoltConnection connection = mock( BoltConnection.class );
        channel = new EmbeddedChannel( new HouseKeeper( connection, logProvider.getLog( HouseKeeper.class ) ) );

        RuntimeException exception = new RuntimeException( "some exception" );
        channel.pipeline().fireExceptionCaught( exception );

        verify( connection ).stop();
        logProvider.assertExactly(
                inLog( HouseKeeper.class ).error( startsWith( "Fatal error occurred when handling a client connection" ), equalTo( exception ) ) );
    }

    @Test
    public void shouldNotPropagateExceptionCaught() throws Exception
    {
        ChannelInboundHandler next = mock( ChannelInboundHandler.class );
        BoltConnection connection = mock( BoltConnection.class );
        channel = new EmbeddedChannel( new HouseKeeper( connection, NullLog.getInstance() ), next );

        channel.pipeline().fireExceptionCaught( new RuntimeException( "some exception" ) );

        verify( next, never() ).exceptionCaught( any(), any() );
    }

    @Test
    public void shouldNotLogExceptionsWhenEvenLoopIsShuttingDown() throws Exception
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        BoltConnection connection = mock( BoltConnection.class );
        HouseKeeper houseKeeper = new HouseKeeper( connection, logProvider.getLog( HouseKeeper.class ) );
        Bootstrap bootstrap = newBootstrap( houseKeeper );

        try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
        {
            ChannelFuture future = bootstrap.connect( "localhost", serverSocket.getLocalPort() ).sync();
            Channel channel = future.channel();

            // write some messages without flushing
            for ( int i = 0; i < 100; i++ )
            {
                // use void promise which should redirect all write errors back to the pipeline and the HouseKeeper
                channel.write( writeUtf8( channel.alloc(), "Hello" ), channel.voidPromise() );
            }

            // stop the even loop to make all pending writes fail
            bootstrap.config().group().shutdownGracefully();
            // await for the channel to be closed by the HouseKeeper
            channel.closeFuture().sync();
        }
        finally
        {
            // make sure event loop group is always terminated
            bootstrap.config().group().shutdownGracefully().sync();
        }

        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void shouldLogOnlyTheFirstCaughtException() throws Exception
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        BoltConnection connection = mock( BoltConnection.class );
        HouseKeeper houseKeeper = new HouseKeeper( connection, logProvider.getLog( HouseKeeper.class ) );
        Bootstrap bootstrap = newBootstrap( houseKeeper );

        RuntimeException error1 = new RuntimeException( "error #1" );
        RuntimeException error2 = new RuntimeException( "error #2" );
        RuntimeException error3 = new RuntimeException( "error #3" );

        try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
        {
            ChannelFuture future = bootstrap.connect( "localhost", serverSocket.getLocalPort() ).sync();
            Channel channel = future.channel();

            // fire multiple errors
            channel.pipeline().fireExceptionCaught( error1 );
            channel.pipeline().fireExceptionCaught( error2 );
            channel.pipeline().fireExceptionCaught( error3 );

            // await for the channel to be closed by the HouseKeeper
            channel.closeFuture().sync();
        }
        finally
        {
            // make sure event loop group is always terminated
            bootstrap.config().group().shutdownGracefully().sync();
        }

        logProvider.assertExactly(
                inLog( HouseKeeper.class ).error( startsWith( "Fatal error occurred when handling a client connection" ), equalTo( error1 ) ) );
    }

    @Test
    public void shouldNotLogConnectionResetErrors() throws Exception
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        HouseKeeper keeper = new HouseKeeper( null, logProvider.getLog( HouseKeeper.class ) );
        Channel channel = mock( Channel.class );
        when( channel.toString() ).thenReturn( "[some channel info]" );
        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
        when( ctx.channel() ).thenReturn( channel );
        when( ctx.executor() ).thenReturn( mock( EventExecutor.class ) );
        IOException connResetError = new IOException( "Connection reset by peer" );

        // When
        keeper.exceptionCaught( ctx, connResetError );

        // Then
        logProvider.assertExactly( AssertableLogProvider.inLog( HouseKeeper.class ).warn(
                "Fatal error occurred when handling a client connection, " +
                        "remote peer unexpectedly closed connection: %s", channel ) );
    }

    private static Bootstrap newBootstrap( HouseKeeper houseKeeper )
    {
        return new Bootstrap()
                .group( new NioEventLoopGroup( 1 ) )
                .channel( NioSocketChannel.class )
                .handler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {
                        ch.pipeline().addLast( houseKeeper );
                    }
                } );
    }
}
