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
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import java.net.SocketAddress;
import java.time.Clock;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.hint.ConnectionHintRegistry;
import org.neo4j.bolt.protocol.common.connector.AbstractConnector;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.TrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.tx.TransactionManager;
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
public abstract class AbstractNettyConnector extends AbstractConnector {
    protected final SocketAddress bindAddress;
    protected final InternalLog userLog;
    protected final InternalLog log;

    private Channel channel;

    AbstractNettyConnector(
            String id,
            SocketAddress bindAddress,
            MemoryPool memoryPool,
            Clock clock,
            Connection.Factory connectionFactory,
            NetworkConnectionTracker connectionTracker,
            boolean encryptionRequired,
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
            InternalLogProvider internalLogProvider) {
        super(
                id,
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
                internalLogProvider);
        this.bindAddress = bindAddress;
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
        return workerGroup();
    }

    /**
     * Retrieves the "worker" group which handles the processing of incoming and outgoing data from/to this connector.
     *
     * @return a thread group.
     */
    protected abstract EventLoopGroup workerGroup();

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
    protected abstract ChannelInitializer<Channel> channelInitializer();

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
}
