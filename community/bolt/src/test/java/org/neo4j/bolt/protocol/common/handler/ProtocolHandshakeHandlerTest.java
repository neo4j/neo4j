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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.BoltChannelFactory.newTestBoltChannel;
import static org.neo4j.logging.LogAssertions.assertThat;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationRequest;
import org.neo4j.bolt.negotiation.message.ProtocolNegotiationResponse;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionFactory;
import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.MemoryTracker;

class ProtocolHandshakeHandlerTest {
    private final BoltChannel boltChannel = newTestBoltChannel();
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

        when(registry.get(eq(version), any(BoltChannel.class))).thenReturn(Optional.of(protocol));

        return registry;
    }

    @AfterEach
    void tearDown() {
        boltChannel.close();
    }

    @Test
    void shouldChooseFirstAvailableProtocol() throws Exception {
        // Given
        var ctx = mock(ChannelHandlerContext.class, RETURNS_MOCKS);
        var pipeline = mock(ChannelPipeline.class, RETURNS_MOCKS);

        var version = new ProtocolVersion(3, 0);
        var protocol = newBoltProtocol(version);
        var protocolRegistry = newProtocolFactory(version, protocol);
        var connectionFactory = mock(BoltConnectionFactory.class);

        var responseCaptor = ArgumentCaptor.forClass(ProtocolNegotiationResponse.class);

        when(protocolRegistry.get(eq(new ProtocolVersion(3, 0)), any())).thenReturn(Optional.of(protocol));

        var channel = new EmbeddedChannel(new ProtocolHandshakeHandler(
                protocolRegistry, connectionFactory, boltChannel, logProvider, Config.defaults()));

        // When
        channel.writeInbound(new ProtocolNegotiationRequest(
                0x6060B017,
                List.of(new ProtocolVersion(2, 0), version, new ProtocolVersion(3, 0), ProtocolVersion.INVALID)));

        // Then
        var msg = channel.<ProtocolNegotiationResponse>readOutbound();

        verify(protocol).createStateMachine(boltChannel);

        assertThat(msg).isEqualTo(new ProtocolNegotiationResponse(version));
        doReturn(pipeline).when(ctx).pipeline();
        doReturn(mock(ChannelFuture.class)).when(ctx).writeAndFlush(responseCaptor.capture());
        doReturn(pipeline).when(pipeline).addLast(anyString(), any());
    }

    @Test
    void shouldFailOutOfRangeProtocol() {
        // Given
        var version = new ProtocolVersion(5, 0);
        var protocolRegistry = newProtocolFactory(version);
        var connectionFactory = mock(BoltConnectionFactory.class);

        var memoryTracker = mock(MemoryTracker.class);
        var scopedTracker = mock(MemoryTracker.class);
        when(memoryTracker.getScopedMemoryTracker()).thenReturn(scopedTracker);

        var channel = new EmbeddedChannel(new ProtocolHandshakeHandler(
                protocolRegistry, connectionFactory, boltChannel, logProvider, Config.defaults()));

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
        var handlerFactory = newProtocolFactory(new ProtocolVersion(5, 0));
        var connectionFactory = mock(BoltConnectionFactory.class);
        var memoryTracker = mock(MemoryTracker.class);

        var channel = new EmbeddedChannel(new ProtocolHandshakeHandler(
                handlerFactory, connectionFactory, boltChannel, logProvider, Config.defaults()));

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
        var boltChannel = spy(this.boltChannel);
        var handlerFactory = newProtocolFactory(new ProtocolVersion(1, 0));
        var memoryTracker = mock(MemoryTracker.class);
        var connectionFactory = mock(BoltConnectionFactory.class);

        doReturn(memoryTracker).when(boltChannel).memoryTracker();

        var channel = new EmbeddedChannel(new ProtocolHandshakeHandler(
                handlerFactory, connectionFactory, boltChannel, logProvider, Config.defaults()));

        channel.pipeline().removeFirst();

        verify(memoryTracker).releaseHeap(ProtocolHandshakeHandler.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);
    }
}
