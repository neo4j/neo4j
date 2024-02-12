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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Clock;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.hint.ConnectionHintRegistry;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.TrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.protocol.common.handler.BoltChannelInitializer;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

public class SocketNettyConnector extends AbstractNettyConnector {
    private final Config config;
    private final ByteBufAllocator allocator;
    private final ConnectorTransport transport;
    private final SslContext sslContext;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ConnectorType connectorType;
    private final ConnectorPortRegister portRegister;
    private final boolean enableTcpKeepAlive;
    private final InternalLogProvider logging;

    public SocketNettyConnector(
            String id,
            SocketAddress bindAddress,
            Config config,
            ConnectorType connectorType,
            ConnectorPortRegister portRegister,
            MemoryPool memoryPool,
            Clock clock,
            ByteBufAllocator allocator,
            EventLoopGroup bossGroup,
            EventLoopGroup workerGroup,
            ConnectorTransport transport,
            Connection.Factory connectionFactory,
            NetworkConnectionTracker connectionTracker,
            SslContext sslContext,
            boolean encryptionRequired,
            boolean enableTcpKeepAlive,
            BoltProtocolRegistry protocolRegistry,
            Authentication authentication,
            AuthConfigProvider authConfigProvider,
            DefaultDatabaseResolver defaultDatabaseResolver,
            ConnectionHintRegistry connectionHintRegistry,
            TransactionManager transactionManager,
            RoutingService routingService,
            ErrorAccountant errorAccountant,
            TrafficAccountant trafficAccountant,
            BoltDriverMetricsMonitor driverMetricsMonitor,
            int streamingBufferSize,
            int streamingFlushThreshold,
            InternalLogProvider userLogProvider,
            InternalLogProvider logging) {
        super(
                id,
                bindAddress,
                memoryPool,
                clock,
                connectionFactory,
                connectionTracker,
                encryptionRequired,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintRegistry,
                transactionManager,
                routingService,
                errorAccountant,
                trafficAccountant,
                driverMetricsMonitor,
                streamingBufferSize,
                streamingFlushThreshold,
                userLogProvider,
                logging);
        if (encryptionRequired && sslContext == null) {
            throw new IllegalArgumentException("SslContext must be specified when encryption is required");
        }

        this.config = config;
        this.connectorType = connectorType;
        this.portRegister = portRegister;
        this.allocator = allocator;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.transport = transport;
        this.sslContext = sslContext;
        this.enableTcpKeepAlive = enableTcpKeepAlive;
        this.logging = logging;
    }

    public SocketNettyConnector(
            String id,
            SocketAddress bindAddress,
            Config config,
            ConnectorType connectorType,
            ConnectorPortRegister portRegister,
            MemoryPool memoryPool,
            Clock clock,
            ByteBufAllocator allocator,
            EventLoopGroup eventLoopGroup,
            ConnectorTransport transport,
            Connection.Factory connectionFactory,
            NetworkConnectionTracker connectionTracker,
            SslContext sslContext,
            boolean encryptionRequired,
            boolean enableTcpKeepAlive,
            BoltProtocolRegistry protocolRegistry,
            Authentication authentication,
            AuthConfigProvider authConfigProvider,
            DefaultDatabaseResolver defaultDatabaseResolver,
            ConnectionHintRegistry connectionHintRegistry,
            TransactionManager transactionManager,
            RoutingService routingService,
            ErrorAccountant errorAccountant,
            TrafficAccountant trafficAccountant,
            BoltDriverMetricsMonitor driverMetricsMonitor,
            int streamingBufferSize,
            int streamingFlushThreshold,
            InternalLogProvider userLogProvider,
            InternalLogProvider logging) {
        this(
                id,
                bindAddress,
                config,
                connectorType,
                portRegister,
                memoryPool,
                clock,
                allocator,
                eventLoopGroup,
                eventLoopGroup,
                transport,
                connectionFactory,
                connectionTracker,
                sslContext,
                encryptionRequired,
                enableTcpKeepAlive,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintRegistry,
                transactionManager,
                routingService,
                errorAccountant,
                trafficAccountant,
                driverMetricsMonitor,
                streamingBufferSize,
                streamingFlushThreshold,
                userLogProvider,
                logging);
    }

    @Override
    protected EventLoopGroup bossGroup() {
        return bossGroup;
    }

    @Override
    protected EventLoopGroup workerGroup() {
        return workerGroup;
    }

    @Override
    protected Class<? extends ServerChannel> channelType() {
        return transport.getSocketChannelType();
    }

    @Override
    protected ChannelInitializer<Channel> channelInitializer() {
        return new BoltChannelInitializer(config, this, allocator, sslContext, logging);
    }

    @Override
    protected void configureServer(ServerBootstrap bootstrap) {
        super.configureServer(bootstrap);

        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, enableTcpKeepAlive);
    }

    @Override
    protected void onChannelBound(Channel channel) {
        portRegister.register(connectorType, (InetSocketAddress) address());
    }

    @Override
    protected void onChannelClose(Channel channel) {
        portRegister.deregister(connectorType);
    }

    @Override
    protected void logStartupMessage() {
        var inetSocketAddress = (InetSocketAddress) bindAddress;
        String connectorName;
        if (connectorType == ConnectorType.BOLT) {
            connectorName = "Bolt";
        } else if (connectorType == ConnectorType.INTRA_BOLT) {
            connectorName = "Bolt (Routing)";
        } else {
            connectorName = connectorType.name();
        }

        userLog.info(
                connectorName + " enabled on %s.",
                org.neo4j.configuration.helpers.SocketAddress.format(
                        inetSocketAddress.getHostName(), inetSocketAddress.getPort()));
    }
}
