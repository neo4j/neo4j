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

import static org.neo4j.util.Preconditions.checkArgument;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.helpers.PortBindException;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

/**
 * Provides a connector which provides its services through a domain socket.
 * <p />
 * This implementation is currently limited for internal use only and is enabled via an unsupported switch.
 * <p />
 * Note that protocol level authentication will be unavailable for domain socket based communication. All authorization
 * is provided through the socket file itself and will thus occur on OS level.
 */
public class DomainSocketNettyConnector extends AbstractNettyConnector {
    private final Path path;
    private final Config config;
    private final ByteBufAllocator allocator;
    private final ConnectorTransport transport;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final InternalLogProvider logging;

    DomainSocketNettyConnector(
            String id,
            Path path,
            Config config,
            MemoryPool memoryPool,
            Clock clock,
            ByteBufAllocator allocator,
            EventLoopGroup bossGroup,
            EventLoopGroup workerGroup,
            ConnectorTransport transport,
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
            InternalLogProvider logging) {
        super(
                id,
                new DomainSocketAddress(path.toFile()),
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
                logging);
        checkArgument(
                transport.getDomainSocketChannelType() != null,
                "Unsupported transport: " + transport.getName() + " does not support domain sockets");

        this.path = path;
        this.config = config;
        this.allocator = allocator;
        this.transport = transport;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.logging = logging;
    }

    public DomainSocketNettyConnector(
            String id,
            Path path,
            Config config,
            MemoryPool memoryPool,
            Clock clock,
            ByteBufAllocator allocator,
            EventLoopGroup eventLoopGroup,
            ConnectorTransport transport,
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
            InternalLogProvider logging) {
        this(
                id,
                path,
                config,
                memoryPool,
                clock,
                allocator,
                eventLoopGroup,
                eventLoopGroup,
                transport,
                connectionFactory,
                connectionTracker,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintRegistry,
                transactionManager,
                routingService,
                errorAccountant,
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
        return transport.getDomainSocketChannelType();
    }

    @Override
    protected ChannelInitializer<Channel> channelInitializer() {
        return new BoltChannelInitializer(config, this, allocator, logging);
    }

    @Override
    protected void onStart() throws Exception {
        super.onStart();

        if (Files.exists(path)) {
            if (!config.get(BoltConnectorInternalSettings.unsupported_loopback_delete)) {
                throw new PortBindException(
                        bindAddress, new BindException("Loopback listen file: " + path + " already exists."));
            }

            try {
                Files.deleteIfExists(path);
            } catch (IOException ex) {
                throw new PortBindException(bindAddress, ex);
            }
        }
    }

    @Override
    protected void logStartupMessage() {
        userLog.info("Bolt (loopback) enabled on file %s", path);
    }
}
