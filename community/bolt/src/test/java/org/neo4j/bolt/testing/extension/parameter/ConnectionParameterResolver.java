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
package org.neo4j.bolt.testing.extension.parameter;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.testing.extension.provider.StateMachineConnectionRegistry;

public class ConnectionParameterResolver implements ParameterResolver {
    private final StateMachineConnectionRegistry connectionRegistry;

    public ConnectionParameterResolver(StateMachineConnectionRegistry connectionRegistry) {
        this.connectionRegistry = connectionRegistry;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return Connection.class.isAssignableFrom(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        var connections = this.connectionRegistry.getConnections();

        if (connections.isEmpty()) {
            throw new ParameterResolutionException(
                    "Illegal test configuration: State machine must be initialized first");
        }
        if (connections.size() != 1) {
            throw new ParameterResolutionException(
                    "Illegal test configuration: Cannot directly inject Connection in tests with multiple StateMachine instances - Use ConnectionProvider instead");
        }

        return connections.get(0);
    }
}
