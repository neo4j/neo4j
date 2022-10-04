/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.test.extension.context;

import java.util.List;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.neo4j.bolt.test.extension.lifecycle.ServerInstanceManager;
import org.neo4j.bolt.test.extension.lifecycle.TransportConnectionManager;
import org.neo4j.bolt.test.extension.resolver.StaticParameterResolver;
import org.neo4j.bolt.test.extension.resolver.SupplierParameterResolver;
import org.neo4j.bolt.test.extension.resolver.connection.ConnectionProviderParameterResolver;
import org.neo4j.bolt.test.extension.resolver.connection.TransportConnectionParameterResolver;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketSupportExtension;
import org.neo4j.internal.helpers.HostnamePort;

public class BoltTestTemplateInvocationContext implements TestTemplateInvocationContext {
    private final BiConsumer<ExtensionContext, Neo4jWithSocket> serverInitializer;
    private final TransportType transportType;
    private final BoltWire wire;

    public BoltTestTemplateInvocationContext(
            BiConsumer<ExtensionContext, Neo4jWithSocket> serverInitializer,
            TransportType transportType,
            BoltWire wire) {
        this.serverInitializer = serverInitializer;
        this.transportType = transportType;
        this.wire = wire;
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        return "v" + this.wire.getProtocolVersion() + " via " + this.transportType.name();
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        var connectionManager = new TransportConnectionManager(this.transportType);

        return List.of(
                new StaticParameterResolver<>(this.transportType),
                new StaticParameterResolver<>(BoltWire.class, this.wire),
                new ServerInstanceManager(this.serverInitializer),
                connectionManager,
                new SupplierParameterResolver<>(
                        HostnamePort.class, Neo4jWithSocketSupportExtension::getDefaultConnectorAddress),
                new TransportConnectionParameterResolver(connectionManager, wire),
                new ConnectionProviderParameterResolver(connectionManager, wire));
    }
}
