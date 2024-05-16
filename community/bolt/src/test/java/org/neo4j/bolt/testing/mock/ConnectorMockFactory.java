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
package org.neo4j.bolt.testing.mock;

import java.util.function.Consumer;
import java.util.function.Supplier;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.hint.ConnectionHintRegistry;
import org.neo4j.bolt.protocol.common.connector.ConnectionRegistry;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.netty.AbstractNettyConnector;
import org.neo4j.bolt.protocol.common.connector.netty.AbstractNettyConnector.NettyConfiguration;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

public final class ConnectorMockFactory extends AbstractMockFactory<AbstractNettyConnector, ConnectorMockFactory> {
    private final String id;

    private ConnectorMockFactory(String id) {
        super(AbstractNettyConnector.class);

        this.id = id;
        this.withStaticValue(Connector::id, id);
        this.withStaticValue(Connector::configuration, ConnectorConfigurationMockFactory.newInstance());
    }

    public static ConnectorMockFactory newFactory(String id) {
        return new ConnectorMockFactory(id);
    }

    public static ConnectorMockFactory newFactory() {
        return newFactory(BoltConnector.NAME);
    }

    public static Connector newInstance() {
        return newFactory().build();
    }

    public static Connector newInstance(String id) {
        return newFactory(id).build();
    }

    public static Connector newInstance(String id, Consumer<ConnectorMockFactory> configurer) {
        var factory = newFactory(id);
        configurer.accept(factory);
        return factory.build();
    }

    public static Connector newInstance(Consumer<ConnectorMockFactory> configurer) {
        return newInstance(BoltConnector.NAME, configurer);
    }

    public ConnectorMockFactory withId(String id) {
        return this.withStaticValue(Connector::id, id);
    }

    public ConnectorMockFactory withAddress(SocketAddress address) {
        return this.withStaticValue(Connector::address, address);
    }

    public ConnectorMockFactory withMemoryPool(MemoryPool pool) {
        return this.withStaticValue(Connector::memoryPool, pool);
    }

    public ConnectorMockFactory withConnectionRegistry(ConnectionRegistry registry) {
        return this.withStaticValue(Connector::connectionRegistry, registry);
    }

    public ConnectorMockFactory withConnectionTracker(NetworkConnectionTracker tracker) {
        return this.withConnectionRegistry(new ConnectionRegistry(this.id, tracker, NullLogProvider.getInstance()));
    }

    public ConnectorMockFactory withProtocolRegistry(BoltProtocolRegistry protocolRegistry) {
        return this.withStaticValue(Connector::protocolRegistry, protocolRegistry);
    }

    public ConnectorMockFactory withAuthentication(Authentication authentication) {
        return this.withStaticValue(Connector::authentication, authentication);
    }

    public ConnectorMockFactory withAuthConfigProvider(AuthConfigProvider authConfigProvider) {
        return this.withStaticValue(Connector::authConfigProvider, authConfigProvider);
    }

    public ConnectorMockFactory withDefaultDatabaseResolver(DefaultDatabaseResolver defaultDatabaseResolver) {
        return this.withStaticValue(Connector::defaultDatabaseResolver, defaultDatabaseResolver);
    }

    public ConnectorMockFactory withDefaultDatabase(String defaultDatabase) {
        var resolver = Mockito.mock(DefaultDatabaseResolver.class);
        Mockito.when(resolver.defaultDatabase(ArgumentMatchers.anyString())).thenReturn(defaultDatabase);

        return this.withDefaultDatabaseResolver(resolver);
    }

    public ConnectorMockFactory withConnectionHintRegistry(ConnectionHintRegistry connectionHintRegistry) {
        return this.withStaticValue(Connector::connectionHintRegistry, connectionHintRegistry);
    }

    public ConnectorMockFactory withTransactionManager(TransactionManager transactionManager) {
        return this.withStaticValue(Connector::transactionManager, transactionManager);
    }

    public ConnectorMockFactory withConnectionCreationFunction(Supplier<Connection> factory) {
        return this.when(connector -> connector.createConnection(ArgumentMatchers.any()), invocation -> factory.get());
    }

    public ConnectorMockFactory withConnection(Connection connection) {
        return this.withStaticValue(connector -> connector.createConnection(ArgumentMatchers.any()), connection);
    }

    public ConnectorMockFactory withConfiguration(NettyConfiguration configuration) {
        return this.withStaticValue(Connector::configuration, configuration);
    }

    public ConnectorMockFactory withConfiguration(Consumer<ConnectorConfigurationMockFactory> configurer) {
        return this.withStaticValue(
                Connector::configuration, ConnectorConfigurationMockFactory.newInstance(configurer));
    }
}
