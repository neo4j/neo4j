/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.handler;

import static io.netty.buffer.ByteBufUtil.writeUtf8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.concurrent.EventExecutor;
import java.io.IOException;
import java.net.ServerSocket;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

public class HouseKeeperHandlerTest {

    @Test
    void shouldCloseChannelOnExceptionCaught() {
        var channel =
                ConnectionMockFactory.newFactory().createChannel(new HouseKeeperHandler(NullLogProvider.getInstance()));

        channel.pipeline().fireExceptionCaught(new RuntimeException("some exception"));

        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void shouldLogExceptionOnExceptionCaught() {
        var logProvider = new AssertableLogProvider();

        var channel = ConnectionMockFactory.newFactory().createChannel(new HouseKeeperHandler(logProvider));

        var ex = new RuntimeException("some exception");
        channel.pipeline().fireExceptionCaught(ex);

        assertThat(logProvider)
                .forClass(HouseKeeperHandler.class)
                .forLevel(ERROR)
                .containsMessageWithException("Fatal error occurred when handling a client connection", ex);
    }

    @Test
    void shouldNotPropagateExceptionCaught() throws Exception {
        var next = mock(ChannelInboundHandler.class);

        var channel = ConnectionMockFactory.newFactory()
                .createChannel(new HouseKeeperHandler(NullLogProvider.getInstance()), next);

        channel.pipeline().fireExceptionCaught(new RuntimeException("some exception"));

        verify(next, never()).exceptionCaught(any(), any());
    }

    @Test
    void shouldNotLogExceptionsWhenEvenLoopIsShuttingDown() throws Exception {
        var logProvider = new AssertableLogProvider();
        var connection = mock(Connection.class);
        var houseKeeperHandler = new HouseKeeperHandler(logProvider);

        var bootstrap = newBootstrap(connection, houseKeeperHandler);

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            var future =
                    bootstrap.connect("localhost", serverSocket.getLocalPort()).sync();
            var channel = future.channel();

            // write some messages without flushing
            for (int i = 0; i < 100; i++) {
                // use void promise which should redirect all write errors back to the pipeline and the HouseKeeper
                channel.write(writeUtf8(channel.alloc(), "Hello"), channel.voidPromise());
            }

            // stop the even loop to make all pending writes fail
            bootstrap.config().group().shutdownGracefully();
            // await for the channel to be closed by the HouseKeeper
            channel.closeFuture().sync();
        } finally {
            // make sure event loop group is always terminated
            bootstrap.config().group().shutdownGracefully().sync();
        }

        assertThat(logProvider).doesNotHaveAnyLogs();
    }

    @Test
    void shouldLogOnlyTheFirstCaughtException() throws Exception {
        var logProvider = new AssertableLogProvider();
        var connection = mock(Connection.class);
        var houseKeeperHandler = new HouseKeeperHandler(logProvider);

        var bootstrap = newBootstrap(connection, houseKeeperHandler);

        var error1 = new RuntimeException("error #1");
        var error2 = new RuntimeException("error #2");
        var error3 = new RuntimeException("error #3");

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            ChannelFuture future =
                    bootstrap.connect("localhost", serverSocket.getLocalPort()).sync();
            Channel channel = future.channel();

            // fire multiple errors
            channel.pipeline().fireExceptionCaught(error1);
            channel.pipeline().fireExceptionCaught(error2);
            channel.pipeline().fireExceptionCaught(error3);

            // await for the channel to be closed by the HouseKeeper
            channel.closeFuture().sync();
        } finally {
            // make sure event loop group is always terminated
            bootstrap.config().group().shutdownGracefully().sync();
        }

        assertThat(logProvider)
                .forClass(HouseKeeperHandler.class)
                .forLevel(ERROR)
                .containsMessageWithException("Fatal error occurred when handling a client connection", error1);
    }

    @Test
    void shouldNotLogConnectionResetErrors() {
        // Given
        var logProvider = new AssertableLogProvider();
        var keeper = new HouseKeeperHandler(logProvider);

        var channel = mock(Channel.class);
        when(channel.toString()).thenReturn("[some channel info]");

        var ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        when(ctx.executor()).thenReturn(mock(EventExecutor.class));

        var connResetError = new IOException("Connection reset by peer");

        // When
        keeper.exceptionCaught(ctx, connResetError);

        // Then
        assertThat(logProvider)
                .forClass(HouseKeeperHandler.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "Fatal error occurred when handling a client connection, "
                                + "remote peer unexpectedly closed connection: %s",
                        channel);
    }

    @Test
    void shouldHandleExceptionsWithNullMessages() {
        // Given
        var logProvider = new AssertableLogProvider();
        var keeper = new HouseKeeperHandler(logProvider);

        var channel = mock(Channel.class);
        when(channel.toString()).thenReturn("[some channel info]");

        var ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        when(ctx.executor()).thenReturn(mock(EventExecutor.class));

        // When
        keeper.exceptionCaught(ctx, ReadTimeoutException.INSTANCE);

        // Then
        assertThat(logProvider)
                .forClass(HouseKeeperHandler.class)
                .forLevel(ERROR)
                .containsMessageWithException(
                        "Fatal error occurred when handling a client connection: " + ctx.channel(),
                        ReadTimeoutException.INSTANCE);
    }

    private static Bootstrap newBootstrap(Connection connection, HouseKeeperHandler houseKeeperHandler) {
        return new Bootstrap()
                .group(new NioEventLoopGroup(1))
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        Connection.setAttribute(ch, connection);

                        ch.pipeline().addLast(houseKeeperHandler);
                    }
                });
    }
}
