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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.codec.ModernProtocolNegotiationFinalizeMessageDecoder;
import org.neo4j.bolt.negotiation.codec.ModernProtocolNegotiationInitMessageEncoder;
import org.neo4j.bolt.negotiation.message.ModernProtocolNegotiationFinalizeMessage;
import org.neo4j.bolt.negotiation.message.ModernProtocolNegotiationInitMessage;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.protocol.common.handler.ProtocolLoggingHandler;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.assertions.ChannelAssertions;
import org.neo4j.bolt.testing.channel.StrictBufferContext;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings.ProtocolLoggingMode;
import org.neo4j.logging.AssertableLogProvider.Level;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;

@StrictBufferExtension
public class ModernProtocolHandshakeHandlerTest extends AbstractProtocolHandshakeHandlerTest {

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
                .attachTo(channel, new ModernProtocolHandshakeHandler(logProvider));

        var msg = channel.<ModernProtocolNegotiationInitMessage>readOutbound();

        Assertions.assertThat(msg).isNotNull();

        Assertions.assertThat(msg.negotiationVersion()).isEqualTo(ProtocolVersion.NEGOTIATION_V2);

        Assertions.assertThat(msg.supportedVersions()).containsAll(protocolRegistry.versionsAvailable());

        channel.writeInbound(
                new ModernProtocolNegotiationFinalizeMessage(version, EnumSet.noneOf(ProtocolCapability.class)));

        verify(connection).selectProtocol(protocol, EnumSet.of(ProtocolCapability.HANDSHAKE_V2));
        verify(protocol).requestMessageRegistry();
        verify(protocol).responseMessageRegistry();
    }

    @Test
    void shouldFreeMemoryUponRemoval(StrictBufferContext ctx) {
        var memoryTracker = mock(MemoryTracker.class);

        var channel = ctx.withConnection(
                conn -> conn.withMemoryTracker(memoryTracker), new ModernProtocolHandshakeHandler(logProvider));

        channel.pipeline().removeFirst();

        verify(memoryTracker).releaseHeap(ModernProtocolHandshakeHandler.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);

        // do not validate outbound communication
        channel.releaseOutbound();
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
                                .withConfiguration(config -> config.enableProtocolLogging(ProtocolLoggingMode.BOTH)
                                        .enableInboundBufferThrottle(512, 1024)))
                        .withMemoryTracker(memoryTracker),
                new ModernProtocolHandshakeHandler(logProvider));

        // pre-install handlers as would be the case if the prior protocol stage had initialized the
        // pipeline
        channel.pipeline()
                .addLast(ProtocolLoggingHandler.RAW_NAME, new ProtocolLoggingHandler(NullLogProvider.getInstance()))
                .addLast(ProtocolLoggingHandler.DECODED_NAME, new ProtocolLoggingHandler(NullLogProvider.getInstance()))
                .addLast(new ModernProtocolNegotiationInitMessageEncoder())
                .addLast(new ModernProtocolNegotiationFinalizeMessageDecoder());

        channel.writeInbound(
                new ModernProtocolNegotiationFinalizeMessage(version, EnumSet.noneOf(ProtocolCapability.class)));

        var handlers = channel.pipeline().names();

        Assertions.assertThat(handlers)
                .containsSubsequence("chunkFrameDecoder", ProtocolLoggingHandler.RAW_NAME)
                .containsSubsequence("readThrottleHandler", ProtocolLoggingHandler.DECODED_NAME);

        Mockito.verify(memoryTracker, Mockito.never()).allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);

        // do not validate outbound communication
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
                new ModernProtocolHandshakeHandler(logProvider));

        // pre-install handlers as would be the case if the prior protocol stage had initialized the
        // pipeline
        channel.pipeline()
                .addLast(ProtocolLoggingHandler.RAW_NAME, new ProtocolLoggingHandler(NullLogProvider.getInstance()))
                .addLast(new ModernProtocolNegotiationInitMessageEncoder())
                .addLast(new ModernProtocolNegotiationFinalizeMessageDecoder());

        channel.writeInbound(
                new ModernProtocolNegotiationFinalizeMessage(version, EnumSet.noneOf(ProtocolCapability.class)));

        var handlers = channel.pipeline().names();

        Assertions.assertThat(handlers)
                .containsSubsequence("chunkFrameDecoder", ProtocolLoggingHandler.RAW_NAME)
                .doesNotContain(ProtocolLoggingHandler.DECODED_NAME);

        Mockito.verify(memoryTracker, Mockito.never()).allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);

        // do not validate outbound communication
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
                new ModernProtocolHandshakeHandler(logProvider));

        // pre-install handlers as would be the case if the prior protocol stage had initialized the
        // pipeline
        channel.pipeline()
                .addLast(ProtocolLoggingHandler.DECODED_NAME, new ProtocolLoggingHandler(NullLogProvider.getInstance()))
                .addLast(new ModernProtocolNegotiationInitMessageEncoder())
                .addLast(new ModernProtocolNegotiationFinalizeMessageDecoder());

        channel.writeInbound(
                new ModernProtocolNegotiationFinalizeMessage(version, EnumSet.noneOf(ProtocolCapability.class)));

        var handlers = channel.pipeline().names();

        Assertions.assertThat(handlers)
                .containsSubsequence("readThrottleHandler", ProtocolLoggingHandler.DECODED_NAME)
                .doesNotContain(ProtocolLoggingHandler.RAW_NAME);

        Mockito.verify(memoryTracker, Mockito.never()).allocateHeap(ProtocolLoggingHandler.SHALLOW_SIZE);

        // do not validate outbound communication
        channel.releaseOutbound();
    }

    @Test
    void shouldLogRejectedNegotiations(StrictBufferContext ctx) {
        var memoryTracker = mock(MemoryTracker.class);

        var version = new ProtocolVersion(5, 0);
        var protocol = newBoltProtocol(version);
        var protocolRegistry = newProtocolFactory(version, protocol);

        var channel = ctx.withConnection(
                conn -> conn.withConnector(factory -> factory.withProtocolRegistry(protocolRegistry)
                                .withConfiguration(config -> config.enableProtocolLogging(ProtocolLoggingMode.DECODED)
                                        .enableInboundBufferThrottle(512, 1024)))
                        .withMemoryTracker(memoryTracker),
                new ModernProtocolHandshakeHandler(logProvider));

        // pre-install handlers as would be the case if the prior protocol stage had initialized the
        // pipeline
        channel.pipeline()
                .addLast(ProtocolLoggingHandler.DECODED_NAME, new ProtocolLoggingHandler(NullLogProvider.getInstance()))
                .addLast(new ModernProtocolNegotiationInitMessageEncoder())
                .addLast(new ModernProtocolNegotiationFinalizeMessageDecoder());

        channel.writeInbound(new ModernProtocolNegotiationFinalizeMessage(
                ProtocolVersion.INVALID, EnumSet.noneOf(ProtocolCapability.class)));

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.DEBUG)
                .containsMessages(
                        "Failed Bolt handshake: Client does not support any of the proposed Bolt versions supported by this server.");

        ChannelAssertions.assertThat(channel).isInactive().isClosed();

        // do not validate outbound communication
        channel.releaseOutbound();
    }
}
