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
package org.neo4j.bolt.test.connection.resolver;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.neo4j.bolt.test.annotation.connection.resolver.Connector;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.internal.helpers.HostnamePort;

/**
 * Provides an address resolver which resolves the address of a connector based on its specified {@link ConnectorType}
 * identification.
 * <p />
 * This resolver <em>must be</em> referenced through the {@link Connector} annotation.
 */
public class ConnectorAddressResolver extends AbstractAddressResolver {

    @Override
    protected HostnamePort doResolve(
            ExtensionContext extensionContext, ParameterContext context, Neo4jWithSocket server)
            throws ParameterResolutionException {
        return server.lookupConnector(this.getConnectorType(context));
    }

    private ConnectorType getConnectorType(ParameterContext context) throws ParameterResolutionException {
        return context.findAnnotation(Connector.class)
                .map(Connector::value)
                .orElseThrow(() -> new ParameterResolutionException(
                        "Illegal parameter configuration: " + ConnectorAddressResolver.class.getSimpleName()
                                + " must be referenced via @" + Connector.class.getSimpleName()));
    }
}
