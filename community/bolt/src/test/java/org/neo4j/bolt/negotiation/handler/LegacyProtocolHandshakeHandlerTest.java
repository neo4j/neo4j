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
package org.neo4j.bolt.negotiation.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationRequestDecoder;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationResponseEncoder;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationRequest;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationResponse;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.common.handler.ProtocolLoggingHandler;
import org.neo4j.bolt.protocol.common.handler.RequestHandler;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.channel.StrictBufferContext;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings.ProtocolLoggingMode;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;

@StrictBufferExtension
class LegacyProtocolHandshakeHandlerTest extends AbstractProtocolHandshakeHandlerTest {

    @Test
    void shouldNegotiateProtocol(StrictBufferContext ctx) {
        // Given
        var version = new ProtocolVersion(2, 0);
        var protocol = newBoltProtocol(version);
        var protocolRegistry = newProtocolFactory(version, protocol);

        when(protocolRegistry.get(eq(new ProtocolVersion(2, 0)))).thenReturn(Optional.of(protocol));

        var channel = ctx.channel();
        var connection = ConnectionMockFactory.newFactory()
                .withConnector(factory -> factory.withProtocolRegistry(protocolRegistry))
                .attachTo(channel, new LegacyProtocolHandshakeHandler(logProvider));

        // When
        channel.writeInbound(new ProtocolNegotiationRequest(
                0x6060B017,
                List.of(
                        new ProtocolVersion(1, 0),
                        new ProtocolVersion(2, 0),
                        version,
                        new ProtocolVersion(3, 0),
                        ProtocolVersion.INVALID)));

        // Then
        var msg = channel.<ProtocolNegotiationResponse>readOutbound();

        verify(connection).selectProtocol(protocol, EnumSet.noneOf(ProtocolCapability.class));
        verify(protocol).requestMessageRegistry();
        verify(protocol).responseMessageRegistry();

        assertThat(msg).isEqualTo(new ProtocolNegotiationResponse(version));
    }

    @Test
    void shouldChooseFirstAvailableProtocol(StrictBufferContext ctx) {
        // Given
        var version = new ProtocolVersion(3, 0);
        var protocol = newBoltProtocol(version);
        var protocolRegistry = newProtocolFactory(version, protocol);

        when(protocolRegistry.get(eq(new ProtocolVersion(3, 0)))).thenReturn(Optional.of(protocol));

        var channel = ctx.channel();
        var connection = ConnectionMockFactory.newFactory()
                .withConnector(factory -> factory.withProtocolRegistry(protocolRegistry))
                // Negotiation en- and decoders included at the end of the pipeline as removal will fail hard if
                // they
                // are not present within the pipeline
                .attachTo(
                        channel,
                        new LegacyProtocolHandshakeHandler(logProvider),
                        new ProtocolNegotiationRequestDecoder(),
                        new ProtocolNegotiationResponseEncoder());

        // When
        channel.writeInbound(new ProtocolNegotiationRequest(
                0x6060B017,
                List.of(new ProtocolVersion(2, 0), version, new ProtocolVersion(4, 0), ProtocolVersion.INVALID)));

        // Then
        var msg = channel.<ProtocolNegotiationResponse>readOutbound();

        assertThat(msg).isEqualTo(new ProtocolNegotiationResponse(version));

        verify(connection).selectProtocol(protocol, EnumSet.noneOf(ProtocolCapability.class));
        verify(protocol).requestMessageRegistry();
        verify(protocol).responseMessageRegistry();

        var requestHandler = channel.pipeline().get(RequestHandler.class);
        assertThat(requestHandler).isNotNull();
    }

    @Test
    void shouldFailOutOfRangeProtocol(StrictBufferContext ctx) {
        // Given
        var version = new ProtocolVersion(5, 0);
        var protocolRegistry = newProtocolFactory(version);

        var memoryTracker = mock(MemoryTracker.class);
        var scopedTracker = mock(MemoryTracker.class);
        when(memoryTracker.getScopedMemoryTracker()).thenReturn(scopedTracker);

        var channel = ctx.withConnection(
                conn -> conn.withConnector(factory -> factory.withProtocolRegistry(protocolRegistry))
                        .withMemoryTracker(memoryTracker),
                new LegacyProtocolHandshakeHandler(logProvider));

        // When
        channel.writeInbound(new ProtocolNegotiationRequest(
                0x6060B017,
                List.of(
                        new ProtocolVersion(4, 4, 2),
                        new ProtocolVersion(4, 1),
                        new ProtocolVersion(1, 0),
                        ProtocolVersion.INVALID)));

        // Then
        var msg = channel.readOutbound();

        assertThat(msg).isNotNull().isEqualTo(new ProtocolNegotiationResponse(ProtocolVersion.INVALID));

        assertThat(channel.pipeline().get(LegacyProtocolHandshakeHandler.class)).isNull();
        assertThat(channel.isActive()).isFalse();
    }

    @Test
    void shouldRejectIfWrongPreamble(StrictBufferContext ctx) {
        // Given
        var channel = ctx.withConnection(new LegacyProtocolHandshakeHandler(logProvider));

        // When
        channel.writeInbound(new ProtocolNegotiationRequest(
                0xDEADB017,
                List.of(
                        new ProtocolVersion(5, 0),
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID)));

        // Then
        var msg = ctx.output(channel.readOutbound());

        assertThat(msg).isNull();
        assertThat(channel.isActive()).isFalse();
    }

    @Test
    void shouldFreeMemoryUponRemoval(StrictBufferContext ctx) {
        var memoryTracker = mock(MemoryTracker.class);

        var channel = ctx.withConnection(
                conn -> conn.withMemoryTracker(memoryTracker), new LegacyProtocolHandshakeHandler(logProvider));

        channel.pipeline().removeFirst();

        verify(memoryTracker).releaseHeap(LegacyProtocolHandshakeHandler.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);
    }

    @Test
    void shouldInstallProtocolLoggingHandlers(StrictBufferContext ctx) {
        var memoryTracker = mock(MemoryTracker.class);

        var version = new ProtocolVersion(5, 0);
        var protocol = newBoltProtocol(version);
        var protocolRegistry = newProtocolFactory(version, protocol);

        when(protocolRegistry.get(eq(version))).thenReturn(Optional.of(protocol));

        var channel = ctx.withConnection(
                conn -> conn.withConnector(factory -> factory.withProtocolRegistry(protocolRegistry)
                                .withConfiguration(config -> config.enableProtocolLogging(true)
                                        .protocolLoggingMode(ProtocolLoggingMode.BOTH)
                                        .enableInboundBufferThrottle(512, 1024)))
                        .withMemoryTracker(memoryTracker),
                new LegacyProtocolHandshakeHandler(logProvider));

        // pre-install handlers as would be the case if the prior protocol stage had initialized the
        // pipeline
        channel.pipeline()
                .addLast(ProtocolLoggingHandler.RAW_NAME, new ProtocolLoggingHandler(NullLogProvider.getInstance()))
                .addLast(ProtocolLoggingHandler.DECODED_NAME, new ProtocolLoggingHandler(NullLogProvider.getInstance()))
                .addLast(new ProtocolNegotiationRequestDecoder())
                .addLast(new ProtocolNegotiationResponseEncoder());

        channel.writeInbound(new ProtocolNegotiationRequest(
                0x6060B017,
                List.of(
                        new ProtocolVersion(5, 0),
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID)));

        var handlers = channel.pipeline().names();

        assertThat(handlers)
                .containsSubsequence("chunkFrameDecoder", ProtocolLoggingHandler.RAW_NAME)
                .containsSubsequence("readThrottleHandler", ProtocolLoggingHandler.DECODED_NAME);

        Mockito.verify(memoryTracker, Mockito.never()).allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);

        // do not care about outbound data
        channel.releaseOutbound();
    }

    @Test
    void shouldInstallRawProtocolLoggingHandlers(StrictBufferContext ctx) {
        var memoryTracker = mock(MemoryTracker.class);

        var version = new ProtocolVersion(5, 0);
        var protocol = newBoltProtocol(version);
        var protocolRegistry = newProtocolFactory(version, protocol);

        when(protocolRegistry.get(eq(version))).thenReturn(Optional.of(protocol));

        var channel = ctx.withConnection(
                conn -> conn.withConnector(factory -> factory.withProtocolRegistry(protocolRegistry)
                                .withConfiguration(config -> config.enableProtocolLogging(ProtocolLoggingMode.RAW)))
                        .withMemoryTracker(memoryTracker),
                new LegacyProtocolHandshakeHandler(logProvider));

        // pre-install handlers as would be the case if the prior protocol stage had initialized the
        // pipeline
        channel.pipeline()
                .addLast(ProtocolLoggingHandler.RAW_NAME, new ProtocolLoggingHandler(NullLogProvider.getInstance()))
                .addLast(new ProtocolNegotiationRequestDecoder())
                .addLast(new ProtocolNegotiationResponseEncoder());

        channel.writeInbound(new ProtocolNegotiationRequest(
                0x6060B017,
                List.of(
                        new ProtocolVersion(5, 0),
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID)));

        var handlers = channel.pipeline().names();

        assertThat(handlers)
                .containsSubsequence("chunkFrameDecoder", ProtocolLoggingHandler.RAW_NAME)
                .doesNotContain(ProtocolLoggingHandler.DECODED_NAME);

        Mockito.verify(memoryTracker, Mockito.never()).allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);

        // do not care about outbound data
        channel.releaseOutbound();
    }

    @Test
    void shouldInstallDecodedProtocolLoggingHandlers(StrictBufferContext ctx) {
        var memoryTracker = mock(MemoryTracker.class);

        var version = new ProtocolVersion(5, 0);
        var protocol = newBoltProtocol(version);
        var protocolRegistry = newProtocolFactory(version, protocol);

        when(protocolRegistry.get(eq(version))).thenReturn(Optional.of(protocol));

        var channel = ctx.withConnection(
                conn -> conn.withConnector(factory -> factory.withProtocolRegistry(protocolRegistry)
                                .withConfiguration(config -> config.enableProtocolLogging(ProtocolLoggingMode.DECODED)
                                        .enableInboundBufferThrottle(512, 1024)))
                        .withMemoryTracker(memoryTracker),
                new LegacyProtocolHandshakeHandler(logProvider));

        // pre-install handlers as would be the case if the prior protocol stage had initialized the
        // pipeline
        channel.pipeline()
                .addLast(ProtocolLoggingHandler.DECODED_NAME, new ProtocolLoggingHandler(NullLogProvider.getInstance()))
                .addLast(new ProtocolNegotiationRequestDecoder())
                .addLast(new ProtocolNegotiationResponseEncoder());

        channel.writeInbound(new ProtocolNegotiationRequest(
                0x6060B017,
                List.of(
                        new ProtocolVersion(5, 0),
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID)));

        var handlers = channel.pipeline().names();

        assertThat(handlers)
                .containsSubsequence("readThrottleHandler", ProtocolLoggingHandler.DECODED_NAME)
                .doesNotContain(ProtocolLoggingHandler.RAW_NAME);

        Mockito.verify(memoryTracker, Mockito.never()).allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);

        // do not care about outbound data
        channel.releaseOutbound();
    }

    @Test
    void shouldSwitchToModernNegotiation(StrictBufferContext ctx) {
        var memoryTracker = mock(MemoryTracker.class);

        var version = new ProtocolVersion(5, 0);
        var protocol = newBoltProtocol(version);
        var protocolRegistry = newProtocolFactory(version, protocol);

        when(protocolRegistry.get(eq(version))).thenReturn(Optional.of(protocol));

        var channel = ctx.withConnection(
                conn -> conn.withMemoryTracker(memoryTracker), new LegacyProtocolHandshakeHandler(logProvider));

        var handlers = channel.pipeline().names();

        assertThat(handlers).contains("LegacyProtocolHandshakeHandler#0");

        // pre-install handlers as would be the case if the prior protocol stage had initialized the
        // pipeline
        channel.pipeline()
                .addLast(new ProtocolNegotiationRequestDecoder())
                .addLast(new ProtocolNegotiationResponseEncoder());

        channel.writeInbound(new ProtocolNegotiationRequest(
                0x6060B017,
                List.of(
                        ProtocolVersion.NEGOTIATION_V2,
                        new ProtocolVersion(5, 0),
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID)));

        handlers = channel.pipeline().names();

        assertThat(handlers)
                .contains("ModernProtocolHandshakeHandler#0")
                .doesNotContain("LegacyProtocolHandshakeHandler#0");

        // don't care about the outbound data
        channel.releaseOutbound();
    }
}
