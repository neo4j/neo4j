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
package org.neo4j.bolt.protocol.common.connector.netty;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import java.net.SocketAddress;
import java.time.Clock;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.hint.ConnectionHintRegistry;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.NoopTrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.protocol.common.handler.BoltChannelInitializer;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

/**
 * Connector that uses netty's {@link io.netty.channel.local.LocalServerChannel} for intra-JVM communication.
 */
public class LocalNettyConnector extends AbstractNettyConnector {

    private final ConnectorTransport transport;
    private final EventLoopGroup workerGroup;

    private final ByteBufAllocator byteBufAllocator;
    private final Config config;

    private final InternalLogProvider internalLogProvider;

    public LocalNettyConnector(
            String id,
            SocketAddress bindAddress,
            MemoryPool memoryPool,
            Clock clock,
            Connection.Factory connectionFactory,
            NetworkConnectionTracker connectionTracker,
            BoltProtocolRegistry protocolRegistry,
            Authentication authentication,
            AuthConfigProvider authConfigProvider,
            DefaultDatabaseResolver defaultDatabaseResolver,
            ConnectionHintRegistry connectionHintRegistry,
            TransactionManager transactionManager,
            RoutingService routingService,
            ErrorAccountant errorAccountant,
            BoltDriverMetricsMonitor driverMetricsMonitor,
            int streamingBufferSize,
            int streamingFlushThreshold,
            InternalLogProvider userLogProvider,
            InternalLogProvider internalLogProvider,
            ConnectorTransport connectorTransport,
            EventLoopGroup workerGroup,
            Config config,
            ByteBufAllocator byteBufAllocator) {
        super(
                id,
                bindAddress,
                memoryPool,
                clock,
                connectionFactory,
                connectionTracker,
                false,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintRegistry,
                transactionManager,
                routingService,
                errorAccountant,
                NoopTrafficAccountant.getInstance(),
                driverMetricsMonitor,
                streamingBufferSize,
                streamingFlushThreshold,
                userLogProvider,
                internalLogProvider);
        this.transport = connectorTransport;
        this.workerGroup = workerGroup;
        this.byteBufAllocator = byteBufAllocator;
        this.config = config;
        this.internalLogProvider = internalLogProvider;
    }

    @Override
    protected EventLoopGroup workerGroup() {
        return workerGroup;
    }

    @Override
    protected Class<? extends ServerChannel> channelType() {
        return transport.getLocalChannelType();
    }

    @Override
    protected ChannelInitializer<Channel> channelInitializer() {
        return new BoltChannelInitializer(config, this, byteBufAllocator, internalLogProvider);
    }
}
