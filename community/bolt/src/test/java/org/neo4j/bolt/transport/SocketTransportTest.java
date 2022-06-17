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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionFactory;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.handler.AuthenticationTimeoutHandler;
import org.neo4j.bolt.protocol.common.handler.ProtocolLoggingHandler;
import org.neo4j.bolt.protocol.common.handler.TransportSelectionHandler;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

class SocketTransportTest {
    @Test
    void shouldManageChannelsInChannelInitializer() {
        NetworkConnectionTracker connectionTracker = mock(NetworkConnectionTracker.class);
        SocketTransport socketTransport = newSocketTransport(connectionTracker, false);

        EmbeddedChannel channel = new EmbeddedChannel(socketTransport.channelInitializer());

        ArgumentCaptor<TrackedNetworkConnection> captor = ArgumentCaptor.forClass(TrackedNetworkConnection.class);
        verify(connectionTracker).add(captor.capture());
        verify(connectionTracker, never()).remove(any());

        channel.close();

        verify(connectionTracker).remove(captor.getValue());
    }

    @Test
    void shouldInstallAuthTimeoutHandler() {
        SocketTransport socketTransport = newSocketTransport(NetworkConnectionTracker.NO_OP, false);

        EmbeddedChannel channel = new EmbeddedChannel(socketTransport.channelInitializer());

        assertNotNull(channel.pipeline().get(AuthenticationTimeoutHandler.class));
    }

    @Test
    void shouldInstallProtocolLoggingHandlerWhenEnabled() {
        var transport = newSocketTransport(NetworkConnectionTracker.NO_OP, true);
        var channel = new EmbeddedChannel(transport.channelInitializer());

        assertNotNull(channel.pipeline().get(ProtocolLoggingHandler.class));
    }

    @Test
    void shouldSkipProtocolLoggingHandlerWhenDisabled() {
        var transport = newSocketTransport(NetworkConnectionTracker.NO_OP, false);
        var channel = new EmbeddedChannel(transport.channelInitializer());

        assertNull(channel.pipeline().get(ProtocolLoggingHandler.class));
    }

    @Test
    void shouldInstallTransportSelectionHandler() {
        SocketTransport socketTransport = newSocketTransport(NetworkConnectionTracker.NO_OP, false);

        EmbeddedChannel channel = new EmbeddedChannel(socketTransport.channelInitializer());

        TransportSelectionHandler handler = channel.pipeline().get(TransportSelectionHandler.class);
        assertNotNull(handler);
    }

    private static SocketTransport newSocketTransport(
            NetworkConnectionTracker connectionTracker, boolean enableProtocolLogging) {
        var config = mock(Config.class);

        when(config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout))
                .thenReturn(Duration.ZERO);
        when(config.get(BoltConnectorInternalSettings.protocol_logging)).thenReturn(enableProtocolLogging);

        return new SocketTransport(
                "bolt",
                new InetSocketAddress("localhost", 7687),
                null,
                false,
                NullLogProvider.getInstance(),
                mock(BoltProtocolRegistry.class),
                mock(BoltConnectionFactory.class),
                connectionTracker,
                PooledByteBufAllocator.DEFAULT,
                mock(MemoryPool.class),
                mock(Authentication.class),
                mock(AuthConfigProvider.class),
                mock(ConnectionHintProvider.class),
                config);
    }
}
