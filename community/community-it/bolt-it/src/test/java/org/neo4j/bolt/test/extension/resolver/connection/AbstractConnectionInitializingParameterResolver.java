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
package org.neo4j.bolt.test.extension.resolver.connection;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.test.connection.initializer.ConnectionInitializer;
import org.neo4j.bolt.test.connection.resolver.address.AddressResolver;
import org.neo4j.bolt.test.connection.resolver.address.DefaultAddressResolver;
import org.neo4j.bolt.test.connection.transport.TransportSelector;
import org.neo4j.bolt.test.extension.lifecycle.TransportConnectionManager;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketSupportExtension;

public abstract class AbstractConnectionInitializingParameterResolver implements ParameterResolver {
    private final TransportConnectionManager connectionManager;
    private final BoltWire wire;

    private final ConnectorTransport transport;
    private final TransportType transportType;

    public AbstractConnectionInitializingParameterResolver(
            TransportConnectionManager connectionManager,
            BoltWire wire,
            ConnectorTransport transport,
            TransportType transportType) {
        this.connectionManager = connectionManager;
        this.wire = wire;
        this.transport = transport;
        this.transportType = transportType;
    }

    protected TransportType getTransportType(ExtensionContext extensionContext, ParameterContext context) {
        return TransportSelector.findTransportOverride(context).orElse(this.transportType);
    }

    protected BoltTestConnection acquireConnection(ExtensionContext extensionContext, ParameterContext context) {
        var server = Neo4jWithSocketSupportExtension.getInstance(extensionContext);

        var resolver = AddressResolver.findResolver(context).orElseGet(DefaultAddressResolver::new);
        var initializers = ConnectionInitializer.findInitializers(context);

        var transportType = this.getTransportType(extensionContext, context);

        var address = resolver.resolve(extensionContext, context, server, transportType);

        var connection = this.connectionManager.acquire(this.transport, this.wire, address, transportType);

        try {
            for (var initializer : initializers) {
                initializer.initialize(extensionContext, context, this.wire, connection);
            }
        } catch (ParameterResolutionException ex) {
            // close the connection early as we do not need it to linger any longer than necessary
            try {
                connection.close();
            } catch (Exception ignore) {
            }

            throw ex;
        }

        return connection;
    }
}
