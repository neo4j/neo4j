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
package org.neo4j.bolt.protocol.common.handler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.net.ssl.SSLException;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.packstream.codec.transport.WebSocketFramePackingEncoder;
import org.neo4j.packstream.codec.transport.WebSocketFrameUnpackingDecoder;

class TransportSelectionHandlerTest {
    @Test
    void shouldLogOnUnexpectedExceptionsAndClosesContext() throws Throwable {
        // Given
        var logging = new AssertableLogProvider();

        var channel = ConnectionMockFactory.newFactory()
                .createChannel(new TransportSelectionHandler(Config.defaults(), null, logging));

        // When
        var ex = new Throwable("Oh no!");
        channel.pipeline().fireExceptionCaught(ex);

        // Then
        assertThat(channel.isOpen()).isFalse();

        assertThat(logging)
                .forClass(TransportSelectionHandler.class)
                .forLevel(ERROR)
                .containsMessageWithException("Fatal error occurred when initialising pipeline: ", ex);
    }

    @Test
    void shouldLogConnectionResetErrorsAtWarningLevelAndClosesContext() throws Exception {
        // Given
        var logging = new AssertableLogProvider();

        var channel = ConnectionMockFactory.newFactory()
                .createChannel(new TransportSelectionHandler(Config.defaults(), null, logging));

        // When
        var ex = new IOException("Connection reset by peer");
        channel.pipeline().fireExceptionCaught(ex);

        // Then
        assertThat(channel.isOpen()).isFalse();

        assertThat(logging)
                .forClass(TransportSelectionHandler.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "Fatal error occurred when initialising pipeline, "
                                + "remote peer unexpectedly closed connection: %s",
                        channel);
    }

    @Test
    void shouldPreventMultipleLevelsOfSslEncryption() throws SSLException {
        // Given
        var logging = new AssertableLogProvider();
        var sslCtx = SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();

        var channel = ConnectionMockFactory.newFactory()
                .createChannel(new TransportSelectionHandler(Config.defaults(), sslCtx, logging, true));

        // When
        channel.writeInbound(Unpooled.wrappedBuffer(new byte[] {22, 3, 1, 0, 5})); // encrypted

        // Then
        assertThat(channel.isOpen()).isFalse();

        assertThat(logging)
                .forClass(TransportSelectionHandler.class)
                .forLevel(ERROR)
                .containsMessageWithArguments(
                        "Fatal error: multiple levels of SSL encryption detected." + " Terminating connection: %s",
                        channel);
    }

    @Test
    void shouldRemoveAllocationUponRemoval() {
        var memoryTracker = mock(MemoryTracker.class);

        var channel = ConnectionMockFactory.newFactory()
                .withMemoryTracker(memoryTracker)
                .createChannel(new TransportSelectionHandler(Config.defaults(), null, NullLogProvider.getInstance()));

        channel.pipeline().remove(TransportSelectionHandler.class);

        verify(memoryTracker).releaseHeap(TransportSelectionHandler.SHALLOW_SIZE);
    }

    @Test
    void shouldAllocateUponSslHandshake() throws SSLException {
        // since we'll need the fake SSL packet to be consumed, we'll create a client SSL context instead of a server
        // context as this does not require any certificates to be present - there is no difference to the handler
        var sslCtx = SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();
        var memoryTracker = mock(MemoryTracker.class);

        var channel = ConnectionMockFactory.newFactory()
                .withMemoryTracker(memoryTracker)
                .createChannel(new TransportSelectionHandler(Config.defaults(), sslCtx, NullLogProvider.getInstance()));

        channel.writeInbound(Unpooled.wrappedBuffer(new byte[] {22, 3, 1, 0, 5}));

        verify(memoryTracker)
                .allocateHeap(
                        TransportSelectionHandler.SHALLOW_SIZE + TransportSelectionHandler.SSL_HANDLER_SHALLOW_SIZE);
    }

    @Test
    void shouldAllocateUponWebsocketHandshake() {
        var memoryTracker = mock(MemoryTracker.class);

        var channel = ConnectionMockFactory.newFactory()
                .withMemoryTracker(memoryTracker)
                .createChannel(new TransportSelectionHandler(Config.defaults(), null, NullLogProvider.getInstance()));

        channel.writeInbound(Unpooled.wrappedBuffer("GET /\r\n".getBytes(StandardCharsets.UTF_8)));

        verify(memoryTracker)
                .allocateHeap(TransportSelectionHandler.HTTP_SERVER_CODEC_SHALLOW_SIZE
                        + TransportSelectionHandler.HTTP_OBJECT_AGGREGATOR_SHALLOW_SIZE
                        + DiscoveryResponseHandler.SHALLOW_SIZE
                        + TransportSelectionHandler.WEB_SOCKET_SERVER_PROTOCOL_HANDLER_SHALLOW_SIZE
                        + TransportSelectionHandler.WEB_SOCKET_FRAME_AGGREGATOR_SHALLOW_SIZE
                        + WebSocketFramePackingEncoder.SHALLOW_SIZE
                        + WebSocketFrameUnpackingDecoder.SHALLOW_SIZE);

        verify(memoryTracker).releaseHeap(TransportSelectionHandler.SHALLOW_SIZE);
    }
}
