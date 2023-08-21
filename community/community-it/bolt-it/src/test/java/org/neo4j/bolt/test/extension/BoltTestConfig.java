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
package org.neo4j.bolt.test.extension;

import java.util.List;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.neo4j.bolt.test.extension.db.ServerInstanceContext;
import org.neo4j.bolt.test.extension.lifecycle.ServerInstanceManager;
import org.neo4j.bolt.test.extension.lifecycle.TransportConnectionManager;
import org.neo4j.bolt.test.extension.resolver.connection.ConnectionProviderParameterResolver;
import org.neo4j.bolt.test.extension.resolver.connection.SocketAddressParameterResolver;
import org.neo4j.bolt.test.extension.resolver.connection.TransportConnectionParameterResolver;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.extension.parameter.StaticParameterResolver;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * Encapsulates the configuration with which a given test method is to be invoked.
 *
 * @param databaseFactoryType database factory implementation class reference.
 * @param wire selected wire.
 * @param transport selected transport.
 */
record BoltTestConfig(
        Class<? extends TestDatabaseManagementServiceBuilder> databaseFactoryType,
        ServerInstanceContext instanceContext,
        TransportType transport,
        BoltWire wire)
        implements TestTemplateInvocationContext {

    @Override
    public String getDisplayName(int invocationIndex) {
        return this.wire.getProtocolVersion() + " via " + this.transport.name();
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        var connectionManager = new TransportConnectionManager(this.transport);

        return List.of(
                new ServerInstanceManager(this.instanceContext),
                connectionManager,
                new StaticParameterResolver<>(BoltWire.class, this.wire),
                new StaticParameterResolver<>(TransportType.class, this.transport),
                new SocketAddressParameterResolver(),
                new ConnectionProviderParameterResolver(connectionManager, this.wire, this.transport),
                new TransportConnectionParameterResolver(connectionManager, this.wire, this.transport));
    }
}
