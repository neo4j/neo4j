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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.LogAssertions.assertThat;

import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationRequestDecoder;
import org.neo4j.bolt.negotiation.codec.ProtocolNegotiationResponseEncoder;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationRequest;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationResponse;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.MemoryTracker;

class ProtocolHandshakeHandlerTest {
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    private static BoltProtocol newBoltProtocol(ProtocolVersion version) {
        BoltProtocol handler = mock(BoltProtocol.class);

        when(handler.version()).thenReturn(version);

        return handler;
    }

    private static BoltProtocolRegistry newProtocolFactory(ProtocolVersion version) {
        var protocol = newBoltProtocol(version);
        return newProtocolFactory(version, protocol);
    }

    private static BoltProtocolRegistry newProtocolFactory(ProtocolVersion version, BoltProtocol protocol) {
        var registry = mock(BoltProtocolRegistry.class);

        when(registry.get(eq(version))).thenReturn(Optional.of(protocol));

        return registry;
    }

    @Test
    void shouldNegotiateProtocol() throws Exception {
        // Given
        var version = new ProtocolVersion(2, 0);
        var protocol = newBoltProtocol(version);
        var protocolRegistry = newProtocolFactory(version, protocol);

        when(protocolRegistry.get(eq(new ProtocolVersion(2, 0)))).thenReturn(Optional.of(protocol));

        var channel = new EmbeddedChannel();
        var connection = ConnectionMockFactory.newFactory()
                .withConnector(factory -> factory.withProtocolRegistry(protocolRegistry))
                .attachTo(channel, new ProtocolHandshakeHandler(Config.defaults(), logProvider));

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

        verify(connection).selectProtocol(protocol);
        verify(protocol).requestMessageRegistry();
        verify(protocol).responseMessageRegistry();

        assertThat(msg).isEqualTo(new ProtocolNegotiationResponse(version));
    }

    @Test
    void shouldChooseFirstAvailableProtocol() throws Exception {
        // Given
        var version = new ProtocolVersion(3, 0);
        var protocol = newBoltProtocol(version);
        var protocolRegistry = newProtocolFactory(version, protocol);

        when(protocolRegistry.get(eq(new ProtocolVersion(3, 0)))).thenReturn(Optional.of(protocol));

        var channel = new EmbeddedChannel();
        var connection = ConnectionMockFactory.newFactory()
                .withConnector(factory -> factory.withProtocolRegistry(protocolRegistry))
                // Negotiation en- and decoders included at the end of the pipeline as removal will fail hard if they
                // are not present within the pipeline
                .attachTo(
                        channel,
                        new ProtocolHandshakeHandler(Config.defaults(), logProvider),
                        new ProtocolNegotiationRequestDecoder(),
                        new ProtocolNegotiationResponseEncoder());

        // When
        channel.writeInbound(new ProtocolNegotiationRequest(
                0x6060B017,
                List.of(new ProtocolVersion(2, 0), version, new ProtocolVersion(4, 0), ProtocolVersion.INVALID)));

        // Then
        var msg = channel.<ProtocolNegotiationResponse>readOutbound();

        assertThat(msg).isEqualTo(new ProtocolNegotiationResponse(version));

        verify(connection).selectProtocol(protocol);
        verify(protocol).requestMessageRegistry();
        verify(protocol).responseMessageRegistry();

        var requestHandler = channel.pipeline().get(RequestHandler.class);
        assertThat(requestHandler).isNotNull();
    }

    @Test
    void shouldFailOutOfRangeProtocol() {
        // Given
        var version = new ProtocolVersion(5, 0);
        var protocolRegistry = newProtocolFactory(version);

        var memoryTracker = mock(MemoryTracker.class);
        var scopedTracker = mock(MemoryTracker.class);
        when(memoryTracker.getScopedMemoryTracker()).thenReturn(scopedTracker);

        var channel = ConnectionMockFactory.newFactory()
                .withConnector(factory -> factory.withProtocolRegistry(protocolRegistry))
                .withMemoryTracker(memoryTracker)
                .createChannel(new ProtocolHandshakeHandler(Config.defaults(), logProvider));

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

        assertThat(channel.pipeline().get(ProtocolHandshakeHandler.class)).isNull();
        assertThat(channel.isActive()).isFalse();
    }

    @Test
    void shouldRejectIfWrongPreamble() {
        // Given
        var channel = ConnectionMockFactory.newFactory()
                .createChannel(new ProtocolHandshakeHandler(Config.defaults(), logProvider));

        // When
        channel.writeInbound(new ProtocolNegotiationRequest(
                0xDEADB017,
                List.of(
                        new ProtocolVersion(5, 0),
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID,
                        ProtocolVersion.INVALID)));

        // Then
        var msg = channel.readOutbound();

        assertThat(msg).isNull();
        assertThat(channel.isActive()).isFalse();
    }

    @Test
    void shouldFreeMemoryUponRemoval() {
        var memoryTracker = mock(MemoryTracker.class);

        var channel = ConnectionMockFactory.newFactory()
                .withMemoryTracker(memoryTracker)
                .createChannel(new ProtocolHandshakeHandler(Config.defaults(), logProvider));

        channel.pipeline().removeFirst();

        verify(memoryTracker).releaseHeap(ProtocolHandshakeHandler.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);
    }
}
