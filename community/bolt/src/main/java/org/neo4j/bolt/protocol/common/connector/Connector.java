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
import java.net.SocketAddress;
import java.time.Clock;
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
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

/**
 * Encapsulates the configuration and dependencies of a dedicated connector.
 */
public interface Connector<CFG extends ConnectorConfiguration> extends Lifecycle {

    /**
     * Retrieves the unique identifier via which connections provided by this connector will be
     * grouped together.
     *
     * @return an identifier.
     */
    String id();

    /**
     * Retrieves the local address on which this connector accepts connections.
     *
     * @return a local socket address or null if the connector has yet to be started.
     */
    SocketAddress address();

    /**
     * Retrieves the memory pool from which all memory for connections owned by this connector shall
     * be allocated.
     *
     * @return a memory pool.
     */
    MemoryPool memoryPool();

    /**
     * Retrieves the clock which shall provide the current date and time for operations within the
     * scope of this connector.
     *
     * @return a clock.
     */
    Clock clock();

    /**
     * Retrieves the registry with which this connector registers all newly created connections.
     *
     * @return a connection registry.
     */
    ConnectionRegistry connectionRegistry();

    /**
     * Retrieves the protocol registry which keeps registrations for all supported protocol revisions
     * supported by this connector.
     *
     * @return a protocol registry.
     */
    BoltProtocolRegistry protocolRegistry();

    /**
     * Retrieves a set of optionally supported capabilities within this connector.
     *
     * @return a set of supported capabilities.
     */
    Set<ProtocolCapability> supportedProtocolCapabilities();

    /**
     * Retrieves the authentication implementation which is responsible for authorizing connections
     * established via this connector.
     *
     * @return an authentication provider.
     */
    Authentication authentication();

    /**
     * Retrieves the OIDC authentication configuration provider which provides discovery information
     * for connecting clients.
     *
     * @return an authentication configuration provider.
     */
    AuthConfigProvider authConfigProvider();

    /**
     * Retrieves the database resolver which resolves the home database for a given authenticated user
     * for this connector.
     *
     * @return a default database resolver.
     */
    DefaultDatabaseResolver defaultDatabaseResolver();

    /**
     * Retrieves the factory which generates connection hints which are to be given to the driver upon
     * successful authentication.
     *
     * @return a hint provider
     * @see ConnectionHintRegistry
     */
    ConnectionHintRegistry connectionHintRegistry();

    /**
     * Retrieves the transaction manager which shall manage the transactions within this connector.
     *
     * @return a transaction manager.
     */
    TransactionManager transactionManager();

    /**
     * Retrieves the bolt driver metrics monitor that allows us to increment the metrics for the
     * different driver types
     *
     * @return a BoltDriverMetricsMonitor
     */
    BoltDriverMetricsMonitor driverMetricsMonitor();

    /**
     * Retrieves the configured routingService
     *
     * @return a routing service
     */
    RoutingService routingService();

    /**
     * Retrieves the error accountant responsible for accumulating connection and scheduling related
     * issues within this connector.
     *
     * @return a connector scoped error accountant.
     */
    ErrorAccountant errorAccountant();

    /**
     * Retrieves the traffic accountant responsible for accumulating inbound and outgoing traffic
     * statistics.
     *
     * @return a connector scoped traffic accountant.
     */
    TrafficAccountant trafficAccountant();

    /**
     * Retrieves the thread accountant responsible for detecting deadlock and overload conditions.
     *
     * @return a connector scoped thread accountant.
     */
    ThreadAccountant threadAccountant();

    /**
     * Retrieves the configuration parameters assigned to this connector.
     *
     * @return a set of configuration parameters.
     */
    CFG configuration();

    /**
     * Registers a new listener with this connector.
     *
     * @param listener a listener.
     */
    void registerListener(ConnectorListener listener);

    /**
     * Removes a listener from this connector.
     *
     * @param listener a listener.
     */
    void removeListener(ConnectorListener listener);

    /**
     * Notifies all registered listeners on this connector.
     *
     * @param notifierFunction a notifier function to be invoked on all listeners.
     */
    void notifyListeners(Consumer<ConnectorListener> notifierFunction);

    /**
     * Allocates a new connection for the specified network channel.
     *
     * @param channel an established network channel.
     * @return a connection.
     */
    Connection createConnection(Channel channel);

    /**
     * Return whether this connector only allows local query execution.
     * @return true if connector only allows local execution.
     */
    default boolean localQueryExecutionOnly() {
        return false;
    }

    /**
     * Return whether this connection supports keep alive messages.
     * @return true if connector allows keep alive messages
     */
    default boolean supportsKeepAlive() {
        return true;
    }

    @Override
    default void init() throws Exception {}

    @Override
    default void shutdown() throws Exception {}
}
