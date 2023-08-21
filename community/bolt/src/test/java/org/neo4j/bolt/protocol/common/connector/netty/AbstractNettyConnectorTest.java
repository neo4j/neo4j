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

import java.net.SocketAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

public abstract class AbstractNettyConnectorTest<C extends AbstractNettyConnector> {

    protected String id;
    protected MemoryPool memoryPool;
    protected Connection.Factory connectionFactory;
    protected NetworkConnectionTracker connectionTracker;
    protected BoltProtocolRegistry protocolRegistry;
    protected Authentication authentication;
    protected AuthConfigProvider authConfigProvider;
    protected DefaultDatabaseResolver defaultDatabaseResolver;
    protected ConnectionHintProvider connectionHintProvider;
    protected AssertableLogProvider logging;

    protected C connector;

    @BeforeEach
    protected void prepareDependencies() {
        memoryPool = Mockito.mock(MemoryPool.class, Mockito.RETURNS_MOCKS);
        connectionFactory = Mockito.mock(Connection.Factory.class);
        connectionTracker = Mockito.mock(NetworkConnectionTracker.class);
        protocolRegistry = Mockito.mock(BoltProtocolRegistry.class);
        authentication = Mockito.mock(Authentication.class);
        authConfigProvider = Mockito.mock(AuthConfigProvider.class);
        defaultDatabaseResolver = Mockito.mock(DefaultDatabaseResolver.class);
        connectionHintProvider = Mockito.mock(ConnectionHintProvider.class);
        logging = new AssertableLogProvider();
    }

    @AfterEach
    protected void cleanupConnector() {
        var connector = this.connector;
        if (connector != null) {
            this.connector = null;

            try {
                connector.stop();
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * Creates a new connector instance for the given bind address.
     *
     * @param address an address to bind to.
     * @return a connector.
     */
    protected abstract C createConnector(SocketAddress address);

    /**
     * Retrieves a default socket address to which the connector shall bind when no specific address is given.
     *
     * @return a default address.
     */
    protected abstract SocketAddress getDefaultAddress();

    /**
     * Retrieves a new connector instance with a predetermined default address.
     *
     * @return a default address.
     */
    protected C createConnector() {
        return createConnector(getDefaultAddress());
    }
}
