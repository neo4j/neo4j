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

import static java.lang.Boolean.TRUE;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.SslContext;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.hint.ConnectionHintRegistry;
import org.neo4j.bolt.protocol.common.connector.AbstractConnector;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.TrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.netty.AbstractNettyConnector.NettyConfiguration;
import org.neo4j.bolt.protocol.common.handler.BoltChannelInitializer;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings.ProtocolLoggingMode;
import org.neo4j.configuration.helpers.PortBindException;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

/**
 * Provides a basis for connectors which rely on netty.
 */
public abstract class AbstractNettyConnector<CFG extends NettyConfiguration> extends AbstractConnector<CFG> {
    protected final SocketAddress bindAddress;
    private final ByteBufAllocator allocator;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    protected final InternalLogProvider logging;
    protected final InternalLog userLog;
    protected final InternalLog log;

    private Channel channel;

    AbstractNettyConnector(
            String id,
            SocketAddress bindAddress,
            MemoryPool memoryPool,
            Clock clock,
            ByteBufAllocator allocator,
            EventLoopGroup bossGroup,
            EventLoopGroup workerGroup,
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
            TrafficAccountant trafficAccountant,
            BoltDriverMetricsMonitor driverMetricsMonitor,
            CFG configuration,
            InternalLogProvider userLogProvider,
            InternalLogProvider internalLogProvider) {
        super(
                id,
                memoryPool,
                clock,
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
                trafficAccountant,
                driverMetricsMonitor,
                configuration,
                internalLogProvider);

        this.bindAddress = bindAddress;
        this.allocator = allocator;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.logging = internalLogProvider;

        this.userLog = userLogProvider.getLog(getClass());
        this.log = internalLogProvider.getLog(getClass());
    }

    /**
     * Retrieves the "boss" group which handles the accepting of new connections to this connector.
     * <p />
     * When the implementation of this method is omitted, {@link #workerGroup()} is utilized as a boss group.
     *
     * @return a thread group.
     */
    protected EventLoopGroup bossGroup() {
        return this.bossGroup;
    }

    /**
     * Retrieves the "worker" group which handles the processing of incoming and outgoing data from/to this connector.
     *
     * @return a thread group.
     */
    protected EventLoopGroup workerGroup() {
        return this.workerGroup;
    }

    /**
     * Retrieves the channel type which shall be used when binding a new address for use with this connector.
     * <p />
     * The returned implementation must be compatible with the specific implementation returned by {@link #bossGroup()}
     * and {@link #workerGroup()}. Refer to the netty documentation for more information.
     *
     * @return a channel type.
     */
    protected abstract Class<? extends ServerChannel> channelType();

    /**
     * Customizes the server bootstrap prior of binding to the desired address.
     *
     * @param bootstrap a pre-configured server bootstrap.
     */
    protected void configureServer(ServerBootstrap bootstrap) {
        bootstrap.option(ChannelOption.SO_REUSEADDR, TRUE);
    }

    /**
     * Performs additional implementation specific tasks when the server channel has been bound.
     *
     * @param channel a server channel.
     */
    protected void onChannelBound(Channel channel) {}

    /**
     * Performs additional implementation specific tasks when the server channel is about to be closed.
     *
     * @param channel a server channel.
     */
    protected void onChannelClose(Channel channel) {}

    /**
     * Retrieves a channel initializer which performs initialization tasks and populates the pipelines of newly accepted
     * client connections.
     *
     * @return a channel initializer.
     */
    protected ChannelInitializer<Channel> channelInitializer() {
        return new BoltChannelInitializer(this, this.allocator, this.logging);
    }

    @Override
    public SocketAddress address() {
        var channel = this.channel;
        if (channel == null) {
            return null;
        }

        return channel.localAddress();
    }

    @Override
    public void start() throws Exception {
        if (channel != null) {
            throw new IllegalStateException("Connector " + id() + " is already running");
        }

        onStart();

        var bootstrap = new ServerBootstrap()
                .channel(channelType())
                .group(bossGroup(), workerGroup())
                .childHandler(channelInitializer());

        configureServer(bootstrap);

        var f = bootstrap.bind(bindAddress);
        try {
            f.await();
        } catch (InterruptedException ex) {
            throw new PortBindException(bindAddress, ex);
        }

        if (!f.isSuccess()) {
            throw new PortBindException(bindAddress, f.cause());
        }

        channel = f.channel();
        onChannelBound(channel);
        logStartupMessage();
    }

    /**
     * Handles the validation of startup requests.
     */
    protected void onStart() throws Exception {}

    @Override
    public void stop() throws Exception {
        var channel = this.channel;
        if (channel == null) {
            return;
        }

        super.stop();
        onChannelClose(channel);

        var f = channel.close().awaitUninterruptibly();
        if (!f.isSuccess()) {
            log.warn("Failed to close channel " + channel + " for connector " + id(), f.cause());
        }
    }

    protected void logStartupMessage() {}

    public static class NettyConfiguration extends AbstractConfiguration {
        private final boolean requireEncryption;
        private final boolean enableMergeCumulator;
        private final SslContext sslContext;

        public NettyConfiguration(
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
                Duration outboundBufferMaxThrottleDuration,
                int inboundBufferThrottleLowWatermark,
                int inboundBufferThrottleHighWatermark,
                int streamingBufferSize,
                int streamingFlushThreshold,
                Duration connectionShutdownDuration,
                boolean enableMergeCumulator,
                boolean requireEncryption,
                SslContext sslContext) {
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
                    outboundBufferMaxThrottleDuration,
                    inboundBufferThrottleLowWatermark,
                    inboundBufferThrottleHighWatermark,
                    streamingBufferSize,
                    streamingFlushThreshold,
                    connectionShutdownDuration);
            if (requireEncryption && sslContext == null) {
                throw new IllegalArgumentException("SslContext must be specified when encryption is required");
            }

            this.requireEncryption = requireEncryption;
            this.enableMergeCumulator = enableMergeCumulator;
            this.sslContext = sslContext;
        }

        /**
         * Identifies whether encryption is required in order to establish a connection via this
         * connector.
         *
         * @return true if encryption is required for new connections.
         */
        public boolean requiresEncryption() {
            return this.requireEncryption;
        }

        /**
         * Identifies whether this connector shall use the merge cumulator instead of making use
         * of a composite based cumulator implementation.
         * <p />
         * This configuration may lead to additional memory consumption as well as performance
         * degradation.
         *
         * @return true if enabled, false otherwise.
         */
        public boolean enableMergeCumulator() {
            return this.enableMergeCumulator;
        }

        public SslContext sslContext() {
            return this.sslContext;
        }
    }
}
