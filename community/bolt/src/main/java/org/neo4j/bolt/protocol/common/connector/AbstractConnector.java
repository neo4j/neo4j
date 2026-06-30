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
package org.neo4j.bolt.protocol.common.connector;

import io.netty.channel.Channel;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.hint.ConnectionHintRegistry;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.thread.ThreadAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.TrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.config.ConnectorConfiguration;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.listener.ConnectorListener;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

/**
 * Provides a generic base implementation for connectors.
 */
public abstract class AbstractConnector<CFG extends ConnectorConfiguration> implements Connector<CFG> {

    private final String id;
    private final MemoryPool memoryPool;
    private final Clock clock;
    private final Connection.Factory connectionFactory;
    private final BoltProtocolRegistry protocolRegistry;
    private final Authentication authentication;
    private final AuthConfigProvider authConfigProvider;
    private final DefaultDatabaseResolver defaultDatabaseResolver;
    private final ConnectionHintRegistry connectionHintRegistry;
    private final TransactionManager transactionManager;
    private final RoutingService routingService;
    private final ErrorAccountant errorAccountant;
    private final TrafficAccountant trafficAccountant;
    private final ThreadAccountant threadAccountant;
    private final BoltDriverMetricsMonitor driverMetricsMonitor;
    private final CFG configuration;

    private final ConnectionRegistry connectionRegistry;

    private final Set<ProtocolCapability> supportedProtocolCapabilities;

    private final List<ConnectorListener> listeners = new ArrayList<>();

    public AbstractConnector(
            String id,
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
            TrafficAccountant trafficAccountant,
            ThreadAccountant threadAccountant,
            BoltDriverMetricsMonitor driverMetricsMonitor,
            CFG configuration,
            InternalLogProvider logging) {
        this.id = id;
        this.clock = clock;
        this.memoryPool = memoryPool;
        this.connectionFactory = connectionFactory;
        this.protocolRegistry = protocolRegistry;
        this.authentication = authentication;
        this.authConfigProvider = authConfigProvider;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
        this.connectionHintRegistry = connectionHintRegistry;
        this.transactionManager = transactionManager;
        this.routingService = routingService;
        this.driverMetricsMonitor = driverMetricsMonitor;
        this.configuration = configuration;

        this.errorAccountant = errorAccountant;
        this.trafficAccountant = trafficAccountant;
        this.threadAccountant = threadAccountant;

        var protocolCapabilities = EnumSet.noneOf(ProtocolCapability.class);
        this.registerSupportedProtocolCapabilities(protocolCapabilities);
        this.supportedProtocolCapabilities = Collections.unmodifiableSet(protocolCapabilities);

        this.connectionRegistry = new ConnectionRegistry(id, connectionTracker, logging);
    }

    protected void registerSupportedProtocolCapabilities(EnumSet<ProtocolCapability> capabilities) {}

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public MemoryPool memoryPool() {
        return this.memoryPool;
    }

    @Override
    public Clock clock() {
        return this.clock;
    }

    @Override
    public ConnectionRegistry connectionRegistry() {
        return this.connectionRegistry;
    }

    @Override
    public BoltProtocolRegistry protocolRegistry() {
        return this.protocolRegistry;
    }

    @Override
    public Set<ProtocolCapability> supportedProtocolCapabilities() {
        return this.supportedProtocolCapabilities;
    }

    @Override
    public Authentication authentication() {
        return this.authentication;
    }

    @Override
    public AuthConfigProvider authConfigProvider() {
        return this.authConfigProvider;
    }

    @Override
    public DefaultDatabaseResolver defaultDatabaseResolver() {
        return this.defaultDatabaseResolver;
    }

    @Override
    public ConnectionHintRegistry connectionHintRegistry() {
        return this.connectionHintRegistry;
    }

    @Override
    public TransactionManager transactionManager() {
        return this.transactionManager;
    }

    @Override
    public RoutingService routingService() {
        return this.routingService;
    }

    @Override
    public ErrorAccountant errorAccountant() {
        return errorAccountant;
    }

    @Override
    public TrafficAccountant trafficAccountant() {
        return trafficAccountant;
    }

    @Override
    public ThreadAccountant threadAccountant() {
        return threadAccountant;
    }

    @Override
    public BoltDriverMetricsMonitor driverMetricsMonitor() {
        return driverMetricsMonitor;
    }

    @Override
    public CFG configuration() {
        return this.configuration;
    }

    @Override
    public void registerListener(ConnectorListener listener) {
        // TODO: Does this behavior need to be thread safe (similar to ConnectionListener)?
        if (this.listeners.contains(listener)) {
            return;
        }

        this.listeners.add(listener);

        listener.onListenerAdded();
    }

    @Override
    public void removeListener(ConnectorListener listener) {
        this.listeners.remove(listener);

        listener.onListenerRemoved();
    }

    @Override
    public void notifyListeners(Consumer<ConnectorListener> notifierFunction) {
        this.listeners.forEach(notifierFunction);
    }

    @Override
    public Connection createConnection(Channel channel) {
        var connectionId = this.connectionRegistry.allocateId();
        var connection = this.connectionFactory.create(this, connectionId, channel);

        // since Connection is a central object within the architecture, we'll register it with the underlying
        // channel as a channel attribute - this allows handlers to pull dependencies as-needed without having
        // to pass them all the way through the chain
        Connection.setAttribute(channel, connection);

        // register the new Connection with our local connection registry and register a lifecycle callback with it in
        // order to ensure its clean termination and de-registration from the registry in case the network channel is
        // terminated
        this.connectionRegistry.register(connection);
        channel.closeFuture().addListener(future -> {
            try {
                connection.close();
            } finally {
                this.connectionRegistry.unregister(connection);
            }
        });

        this.notifyListeners(listener -> listener.onConnectionCreated(connection));
        return connection;
    }

    @Override
    public void start() throws Exception {}

    @Override
    public void stop() throws Exception {
        this.connectionRegistry.stopIdling(this.configuration().connectionShutdownDuration());
    }

    @Override
    public void shutdown() throws Exception {
        this.connectionRegistry.stopAll(this.configuration().connectionShutdownDuration());
    }
}
