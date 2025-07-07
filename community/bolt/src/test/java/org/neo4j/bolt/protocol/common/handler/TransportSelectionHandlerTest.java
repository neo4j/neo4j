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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.channel.StrictBufferContext;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings.ProtocolLoggingMode;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.packstream.codec.transport.WebSocketFramePackingEncoder;
import org.neo4j.packstream.codec.transport.WebSocketFrameUnpackingDecoder;

@StrictBufferExtension
class TransportSelectionHandlerTest {

    @Test
    void shouldLogOnUnexpectedExceptionsAndClosesContext(StrictBufferContext ctx) {
        // Given
        var logging = new AssertableLogProvider();

        var channel = ctx.withConnection(new TransportSelectionHandler(logging));

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
    void shouldLogConnectionResetErrorsAtWarningLevelAndClosesContext(StrictBufferContext ctx) {
        // Given
        var logging = new AssertableLogProvider();

        var channel = ctx.withConnection(new TransportSelectionHandler(logging));

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
    void shouldPreventMultipleLevelsOfSslEncryption(StrictBufferContext ctx) {
        // Given
        var logging = new AssertableLogProvider();

        var channel = ctx.withConnection(new TransportSelectionHandler(true, logging));

        // When
        channel.writeInbound(ctx.buffer(new byte[] {22, 3, 1, 0, 5})); // encrypted

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
    void shouldRemoveAllocationUponRemoval(StrictBufferContext ctx) {
        var memoryTracker = mock(MemoryTracker.class);

        var channel = ctx.withConnection(
                conn -> conn.withMemoryTracker(memoryTracker),
                new TransportSelectionHandler(NullLogProvider.getInstance()));

        channel.pipeline().remove(TransportSelectionHandler.class);

        verify(memoryTracker).releaseHeap(TransportSelectionHandler.SHALLOW_SIZE);
    }

    @Test
    void shouldAllocateUponSslHandshake(StrictBufferContext ctx) {
        var memoryTracker = mock(MemoryTracker.class);

        var channel = ctx.withConnection(
                conn -> conn.withMemoryTracker(memoryTracker),
                new TransportSelectionHandler(NullLogProvider.getInstance()));

        // expect buffer to remain valid at the end of the test since it is a truncated TLS
        // handshake that will not be handled by SslHandler and thus remain inside its cumulation
        // buffer
        channel.writeInbound(ctx.buffer(new byte[] {22, 3, 1, 0, 5}, 1));

        verify(memoryTracker).allocateHeap(TransportSelectionHandler.SSL_HANDLER_SHALLOW_SIZE);
    }

    @Test
    void shouldAllocateUponWebsocketHandshake(StrictBufferContext ctx) {
        var memoryTracker = mock(MemoryTracker.class);

        var channel = ctx.withConnection(
                conn -> conn.withMemoryTracker(memoryTracker),
                new TransportSelectionHandler(NullLogProvider.getInstance()));

        channel.writeInbound(ctx.buffer("GET /\r\n"));

        verify(memoryTracker)
                .allocateHeap(TransportSelectionHandler.HTTP_SERVER_CODEC_SHALLOW_SIZE
                        + TransportSelectionHandler.HTTP_OBJECT_AGGREGATOR_SHALLOW_SIZE
                        + DiscoveryResponseHandler.SHALLOW_SIZE
                        + TransportSelectionHandler.WEB_SOCKET_SERVER_PROTOCOL_HANDLER_SHALLOW_SIZE
                        + TransportSelectionHandler.WEB_SOCKET_FRAME_AGGREGATOR_SHALLOW_SIZE
                        + WebSocketFramePackingEncoder.SHALLOW_SIZE
                        + WebSocketFrameUnpackingDecoder.SHALLOW_SIZE);

        verify(memoryTracker).releaseHeap(TransportSelectionHandler.SHALLOW_SIZE);

        // discard irrelevant outbound handshake data
        // FIXME: Reports refCnt of 2 at the end of the test despite buffers internally indicating one
        //        reference
        channel.releaseOutbound();
    }

    @Test
    void shouldInstallProtocolLoggingHandlers(StrictBufferContext ctx) {
        var memoryTracker = Mockito.mock(MemoryTracker.class);

        var channel = ctx.withConnection(
                conn -> conn.withConfiguration(config -> config.withProtocolLogging(ProtocolLoggingMode.BOTH))
                        .withMemoryTracker(memoryTracker),
                new TransportSelectionHandler(NullLogProvider.getInstance()));

        // generate an incomplete handshake which exceeds the 5-byte threshold of the selection
        // handler
        channel.writeInbound(ctx.buffer().writeInt(0x6060B017).writeInt(0x00090005));

        var handlers = channel.pipeline().names();

        Assertions.assertThat(handlers)
                .containsSequence(ProtocolLoggingHandler.RAW_NAME, "protocolNegotiationRequestEncoder")
                .containsSubsequence(
                        "protocolNegotiationRequestDecoder",
                        ProtocolLoggingHandler.DECODED_NAME,
                        "protocolHandshakeHandler");

        Mockito.verify(memoryTracker, Mockito.times(2)).allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);
    }

    @Test
    void shouldInstallRawProtocolLoggingHandlers(StrictBufferContext ctx) {
        var memoryTracker = Mockito.mock(MemoryTracker.class);

        var channel = ctx.withConnection(
                conn -> conn.withConfiguration(config -> config.withProtocolLogging(ProtocolLoggingMode.RAW))
                        .withMemoryTracker(memoryTracker),
                new TransportSelectionHandler(NullLogProvider.getInstance()));

        // generate an incomplete handshake which exceeds the 5-byte threshold of the selection
        // handler
        channel.writeInbound(ctx.buffer().writeInt(0x6060B017).writeInt(0x00090005));

        var handlers = channel.pipeline().names();

        Assertions.assertThat(handlers)
                .containsSequence(ProtocolLoggingHandler.RAW_NAME, "protocolNegotiationRequestEncoder")
                .doesNotContain(ProtocolLoggingHandler.DECODED_NAME);

        Mockito.verify(memoryTracker).allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);
    }

    @Test
    void shouldInstallDecodedProtocolLoggingHandlers(StrictBufferContext ctx) {
        var memoryTracker = Mockito.mock(MemoryTracker.class);

        var channel = ctx.withConnection(
                conn -> conn.withConfiguration(config -> config.withProtocolLogging(ProtocolLoggingMode.DECODED))
                        .withMemoryTracker(memoryTracker),
                new TransportSelectionHandler(NullLogProvider.getInstance()));

        // generate an incomplete handshake which exceeds the 5-byte threshold of the selection
        // handler
        channel.writeInbound(ctx.buffer().writeInt(0x6060B017).writeInt(0x00090005));

        var handlers = channel.pipeline().names();

        Assertions.assertThat(handlers)
                .containsSubsequence(
                        "protocolNegotiationRequestDecoder",
                        ProtocolLoggingHandler.DECODED_NAME,
                        "protocolHandshakeHandler")
                .doesNotContain(ProtocolLoggingHandler.RAW_NAME);

        Mockito.verify(memoryTracker).allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);
    }
}
