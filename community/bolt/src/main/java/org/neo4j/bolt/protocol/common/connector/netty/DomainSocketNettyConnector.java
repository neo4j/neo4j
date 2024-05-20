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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.hint.ConnectionHintRegistry;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.NoopTrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.netty.DomainSocketNettyConnector.DomainSocketConfiguration;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings.ProtocolLoggingMode;
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
public class DomainSocketNettyConnector extends AbstractNettyConnector<DomainSocketConfiguration> {
    private final Path path;
    private final ConnectorTransport transport;

    public DomainSocketNettyConnector(
            String id,
            Path path,
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
            DomainSocketConfiguration configuration,
            InternalLogProvider userLogProvider,
            InternalLogProvider logging) {
        super(
                id,
                new DomainSocketAddress(path.toFile()),
                memoryPool,
                clock,
                allocator,
                bossGroup,
                workerGroup,
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
                NoopTrafficAccountant.getInstance(),
                driverMetricsMonitor,
                configuration,
                userLogProvider,
                logging);
        checkArgument(
                transport.getDomainSocketChannelType() != null,
                "Unsupported transport: " + transport.getName() + " does not support domain sockets");

        this.path = path;
        this.transport = transport;
    }

    @Override
    protected Class<? extends ServerChannel> channelType() {
        return transport.getDomainSocketChannelType();
    }

    @Override
    protected void onStart() throws Exception {
        super.onStart();

        if (Files.exists(path)) {
            if (!this.configuration().deleteSocketFile()) {
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

    public static class DomainSocketConfiguration extends NettyConfiguration {
        private final boolean deleteSocketFile;

        public DomainSocketConfiguration(
                boolean enableProtocolCapture,
                Path protocolCapturePath,
                boolean enableProtocolLogging,
                ProtocolLoggingMode protocolLoggingMode,
                long maxAuthenticationInboundBytes,
                int maxAuthenticationStructureElements,
                int maxAuthenticationStructureDepth,
                boolean enableOutboundBufferThrottle,
                int outboundBufferThrottleLowWatermark,
                int outboundBufferThrottleHighWatermark,
                Duration outboundBufferThrottleDuration,
                int inboundBufferThrottleLowWatermark,
                int inboundBufferThrottleHighWatermark,
                int streamingBufferSize,
                int streamingFlushThreshold,
                Duration connectionShutdownDuration,
                boolean enableMergeCumulator,
                boolean deleteSocketFile) {
            super(
                    enableProtocolCapture,
                    protocolCapturePath,
                    enableProtocolLogging,
                    protocolLoggingMode,
                    maxAuthenticationInboundBytes,
                    maxAuthenticationStructureElements,
                    maxAuthenticationStructureDepth,
                    enableOutboundBufferThrottle,
                    outboundBufferThrottleLowWatermark,
                    outboundBufferThrottleHighWatermark,
                    outboundBufferThrottleDuration,
                    inboundBufferThrottleLowWatermark,
                    inboundBufferThrottleHighWatermark,
                    streamingBufferSize,
                    streamingFlushThreshold,
                    connectionShutdownDuration,
                    enableMergeCumulator,
                    false, // Currently always disabled on UNIX socket
                    null);

            this.deleteSocketFile = deleteSocketFile;
        }

        /**
         * Identifies whether the socket file shall be deleted when it already exists at the
         * configured location.
         * <p />
         * When the specified socket file already exists and this option is enabled, the connector
         * will attempt to delete it. If disabled, an exception will be raised instead.
         *
         * @return true if deletion shall be attempted, false otherwise.
         */
        public boolean deleteSocketFile() {
            return this.deleteSocketFile;
        }
    }
}
