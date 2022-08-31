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
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.protocol.common.handler.BoltChannelInitializer;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
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
            ConnectionHintProvider connectionHintProvider,
            InternalLogProvider userLogProvider,
            InternalLogProvider logging) {
        super(
                id,
                bindAddress,
                memoryPool,
                connectionFactory,
                connectionTracker,
                encryptionRequired,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintProvider,
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
            ConnectionHintProvider connectionHintProvider,
            InternalLogProvider userLogProvider,
            InternalLogProvider logging) {
        this(
                id,
                bindAddress,
                config,
                connectorType,
                portRegister,
                memoryPool,
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
                connectionHintProvider,
                userLogProvider,
                logging);
    }

    @Override
    protected EventLoopGroup bossGroup() {
        return this.bossGroup;
    }

    @Override
    protected EventLoopGroup workerGroup() {
        return this.workerGroup;
    }

    @Override
    protected Class<? extends ServerChannel> channelType() {
        return this.transport.getSocketChannelType();
    }

    @Override
    protected ChannelInitializer<Channel> channelInitializer() {
        return new BoltChannelInitializer(this.config, this, this.allocator, this.sslContext, this.logging);
    }

    @Override
    protected void configureServer(ServerBootstrap bootstrap) {
        super.configureServer(bootstrap);

        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, this.enableTcpKeepAlive);
    }

    @Override
    protected void onChannelBound(Channel channel) {
        this.portRegister.register(this.connectorType, (InetSocketAddress) this.address());
    }

    @Override
    protected void onChannelClose(Channel channel) {
        this.portRegister.deregister(this.connectorType);
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
