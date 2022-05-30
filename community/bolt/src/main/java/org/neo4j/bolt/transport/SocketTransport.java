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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.ssl.SslContext;
import java.net.SocketAddress;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionFactory;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.handler.DiscoveryResponseHandler;
import org.neo4j.bolt.protocol.common.handler.TransportSelectionHandler;
import org.neo4j.bolt.protocol.common.protector.ChannelProtector;
import org.neo4j.bolt.protocol.common.protector.UnauthenticatedChannelProtector;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.config.AuthConfigProvider;

/**
 * Implements a transport for the Neo4j Messaging Protocol that uses good old regular sockets.
 */
public class SocketTransport implements NettyServer.ProtocolInitializer {
    private final String connector;
    private final SocketAddress address;
    private final SslContext sslCtx;
    private final boolean encryptionRequired;
    private final InternalLogProvider logging;
    private final BoltConnectionFactory connectionFactory;
    private final BoltProtocolRegistry protocolRegistry;
    private final Config config;
    private final NetworkConnectionTracker connectionTracker;
    private final ByteBufAllocator allocator;
    private final MemoryPool memoryPool;
    private final Authentication authentication;
    private final AuthConfigProvider authConfigProvider;
    private final ConnectionHintProvider connectionHintProvider;

    public SocketTransport(
            String connector,
            SocketAddress address,
            SslContext sslCtx,
            boolean encryptionRequired,
            InternalLogProvider logging,
            BoltProtocolRegistry protocolRegistry,
            BoltConnectionFactory connectionFactory,
            NetworkConnectionTracker connectionTracker,
            ByteBufAllocator allocator,
            MemoryPool memoryPool,
            Authentication authentication,
            AuthConfigProvider authConfigProvider,
            ConnectionHintProvider connectionHintProvider,
            Config config) {
        this.connector = connector;
        this.address = address;
        this.sslCtx = sslCtx;
        this.encryptionRequired = encryptionRequired;
        this.logging = logging;
        this.protocolRegistry = protocolRegistry;
        this.connectionFactory = connectionFactory;
        this.connectionTracker = connectionTracker;
        this.allocator = allocator;
        this.memoryPool = memoryPool;
        this.authentication = authentication;
        this.connectionHintProvider = connectionHintProvider;
        this.authConfigProvider = authConfigProvider;
        this.config = config;
    }

    @Override
    public ChannelInitializer<Channel> channelInitializer() {
        return new ChannelInitializer<>() {
            @Override
            public void initChannel(Channel ch) {
                ch.config().setAllocator(allocator);

                var memoryTracker = new LocalMemoryTracker(memoryPool, 0, 64, null);
                ch.closeFuture().addListener(future -> memoryTracker.close());

                memoryTracker.allocateHeap(HeapEstimator.sizeOf(ch));

                memoryTracker.allocateHeap(UnauthenticatedChannelProtector.SHALLOW_SIZE + BoltChannel.SHALLOW_SIZE);
                var channelProtector = new UnauthenticatedChannelProtector(
                        ch,
                        config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout),
                        memoryTracker);
                var boltChannel = newBoltChannel(ch, channelProtector, memoryTracker);
                connectionTracker.add(boltChannel);
                ch.closeFuture().addListener(future -> connectionTracker.remove(boltChannel));

                memoryTracker.allocateHeap(TransportSelectionHandler.SHALLOW_SIZE);
                var discoveryServiceHandler = new DiscoveryResponseHandler(authConfigProvider);

                var transportSelectionHandler = new TransportSelectionHandler(
                        boltChannel,
                        sslCtx,
                        encryptionRequired,
                        false,
                        logging,
                        connectionFactory,
                        protocolRegistry,
                        discoveryServiceHandler,
                        config);

                ch.pipeline().addLast(transportSelectionHandler);
            }
        };
    }

    @Override
    public SocketAddress address() {
        return address;
    }

    private BoltChannel newBoltChannel(Channel ch, ChannelProtector channelProtector, MemoryTracker memoryTracker) {
        var channel = new BoltChannel(
                connectionTracker.newConnectionId(connector),
                connector,
                ch,
                authentication,
                channelProtector,
                connectionHintProvider,
                memoryTracker);
        channelProtector.afterChannelCreated();
        return channel;
    }
}
