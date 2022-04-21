/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.transport.pipeline.ProtocolHandshaker;
import org.neo4j.bolt.transport.pipeline.WebSocketFrameTranslator;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.MemoryTracker;

class TransportSelectionHandlerTest {
    @Test
    void shouldLogOnUnexpectedExceptionsAndClosesContext() throws Throwable {
        // Given
        ChannelHandlerContext context = channelHandlerContextMock();
        AssertableLogProvider logging = new AssertableLogProvider();
        var memoryTracker = mock(MemoryTracker.class);

        TransportSelectionHandler handler =
                new TransportSelectionHandler(null, null, false, false, logging, null, null, memoryTracker, null);

        // When
        Throwable cause = new Throwable("Oh no!");
        handler.exceptionCaught(context, cause);

        // Then
        verify(context).close();
        assertThat(logging)
                .forClass(TransportSelectionHandler.class)
                .forLevel(ERROR)
                .containsMessageWithException(
                        "Fatal error occurred when initialising pipeline: " + context.channel(), cause);
    }

    @Test
    void shouldLogConnectionResetErrorsAtWarningLevelAndClosesContext() throws Exception {
        // Given
        ChannelHandlerContext context = channelHandlerContextMock();
        AssertableLogProvider logging = new AssertableLogProvider();
        var memoryTracker = mock(MemoryTracker.class);

        TransportSelectionHandler handler =
                new TransportSelectionHandler(null, null, false, false, logging, null, null, memoryTracker, null);

        IOException connResetError = new IOException("Connection reset by peer");

        // When
        handler.exceptionCaught(context, connResetError);

        // Then
        verify(context).close();
        assertThat(logging)
                .forClass(TransportSelectionHandler.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "Fatal error occurred when initialising pipeline, "
                                + "remote peer unexpectedly closed connection: %s",
                        context.channel());
    }

    @Test
    void shouldPreventMultipleLevelsOfSslEncryption() throws Exception {
        // Given
        ChannelHandlerContext context = channelHandlerContextMockSslAlreadyConfigured();
        AssertableLogProvider logging = new AssertableLogProvider();
        SslContext sslCtx = mock(SslContext.class);
        var memoryTracker = mock(MemoryTracker.class);

        TransportSelectionHandler handler =
                new TransportSelectionHandler(null, sslCtx, false, false, logging, null, null, memoryTracker, null);

        final ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {22, 3, 1, 0, 5}); // encrypted

        // When
        handler.decode(context, payload, null);

        // Then
        verify(context).close();
        assertThat(logging)
                .forClass(TransportSelectionHandler.class)
                .forLevel(ERROR)
                .containsMessageWithArguments(
                        "Fatal error: multiple levels of SSL encryption detected." + " Terminating connection: %s",
                        context.channel());
    }

    @Test
    void shouldRemoveAllocationUponRemoval() {
        var ctx = channelHandlerContextMockSslAlreadyConfigured();
        var logging = new AssertableLogProvider();
        var memoryTracker = mock(MemoryTracker.class);

        var handler = new TransportSelectionHandler(null, null, false, false, logging, null, null, memoryTracker, null);

        handler.handlerRemoved0(ctx);

        verify(memoryTracker).releaseHeap(TransportSelectionHandler.SHALLOW_SIZE);
    }

    @Test
    void shouldAllocateUponSslHandshake() {
        var ctx = channelHandlerContextMockSslAlreadyConfigured();
        var sslCtx = mock(SslContext.class);
        var logging = new AssertableLogProvider();
        var memoryTracker = mock(MemoryTracker.class);

        var payload = Unpooled.wrappedBuffer(new byte[] {22, 3, 1, 0, 5});

        var handler =
                new TransportSelectionHandler(null, sslCtx, false, false, logging, null, null, memoryTracker, null);

        handler.decode(ctx, payload, new ArrayList<>());

        verify(memoryTracker)
                .allocateHeap(
                        TransportSelectionHandler.SHALLOW_SIZE + TransportSelectionHandler.SSL_HANDLER_SHALLOW_SIZE);
    }

    @Test
    void shouldAllocateUponWebsocketHandshake() {
        var ctx = channelHandlerContextMockSslAlreadyConfigured();
        var sslCtx = mock(SslContext.class);
        var logging = new AssertableLogProvider();
        var memoryTracker = mock(MemoryTracker.class);

        var payload = Unpooled.wrappedBuffer("GET /\r\n".getBytes(StandardCharsets.UTF_8));

        var channel = new EmbeddedChannel(
                new TransportSelectionHandler(null, sslCtx, false, false, logging, null, null, memoryTracker, null));
        channel.writeInbound(payload);

        verify(memoryTracker)
                .allocateHeap(TransportSelectionHandler.HTTP_SERVER_CODEC_SHALLOW_SIZE
                        + TransportSelectionHandler.HTTP_OBJECT_AGGREGATOR_SHALLOW_SIZE
                        + TransportSelectionHandler.WEB_SOCKET_SERVER_PROTOCOL_HANDLER_SHALLOW_SIZE
                        + TransportSelectionHandler.WEB_SOCKET_FRAME_AGGREGATOR_SHALLOW_SIZE
                        + WebSocketFrameTranslator.SHALLOW_SIZE
                        + ProtocolHandshaker.SHALLOW_SIZE);
        verify(memoryTracker).releaseHeap(TransportSelectionHandler.SHALLOW_SIZE);
    }

    private static ChannelHandlerContext channelHandlerContextMock() {
        Channel channel = mock(Channel.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        when(context.channel()).thenReturn(channel);
        return context;
    }

    private static ChannelHandlerContext channelHandlerContextMockSslAlreadyConfigured() {
        Channel channel = mock(Channel.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        ChannelPipeline pipeline = mock(ChannelPipeline.class);
        SslHandler sslHandler = mock(SslHandler.class);
        when(context.channel()).thenReturn(channel);
        when(context.pipeline()).thenReturn(pipeline);
        when(context.pipeline().get(SslHandler.class)).thenReturn(sslHandler);
        return context;
    }
}
