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
import java.util.function.Consumer;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.hint.ConnectionHintRegistry;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.TrafficAccountant;
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
public interface Connector extends Lifecycle {

    /**
     * Retrieves the unique identifier via which connections provided by this connector will be grouped together.
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
     * Retrieves the memory pool from which all memory for connections owned by this connector shall be allocated.
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
     * Evaluates whether TLS encryption is required for this connector.
     *
     * @return true if encryption is required.
     */
    boolean isEncryptionRequired();

    /**
     * Retrieves the protocol registry which keeps registrations for all supported protocol revisions supported by
     * this connector.
     *
     * @return a protocol registry.
     */
    BoltProtocolRegistry protocolRegistry();

    /**
     * Retrieves the authentication implementation which is responsible for authorizing connections established via this
     * connector.
     *
     * @return an authentication provider.
     */
    Authentication authentication();

    /**
     * Retrieves the OIDC authentication configuration provider which provides discovery information for connecting
     * clients.
     *
     * @return an authentication configuration provider.
     */
    AuthConfigProvider authConfigProvider();

    /**
     * Retrieves the database resolver which resolves the home database for a given authenticated user for this
     * connector.
     *
     * @return a default database resolver.
     */
    DefaultDatabaseResolver defaultDatabaseResolver();

    /**
     * Retrieves the factory which generates connection hints which are to be given to the driver upon successful
     * authentication.
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
     * Retrieves the bolt driver metrics monitor that allows us to increment the metrics for the different driver types
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
     * Identifies the total number of bytes to be requested from the pool when streaming records.
     * <p />
     * Lower values may improve overall operation performance but may result in more frequent
     * allocations when larger values are streamed at regular intervals.
     *
     * @return a buffer size (in bytes).
     */
    int streamingBufferSize();

    /**
     * Identifies the total number of bytes expected to be present within the outgoing record buffer
     * while streaming before the buffers are flushed.
     * <p />
     * Lower values will improve overall latency but might impact performance as records are more
     * frequently flushed to the client.
     * <p />
     * Flushing always occurs upon completion of a streaming operation regardless of the value
     * configured within this property.
     * <p />
     * A value of zero indicates no threshold (e.g. flushing occurs on a per-record basis).
     *
     * @return a flush threshold (in bytes).
     */
    int streamingFlushThreshold();

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

    @Override
    default void init() throws Exception {}

    @Override
    default void shutdown() throws Exception {}
}
