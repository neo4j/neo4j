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
import org.neo4j.bolt.test.annotation.connection.Resolver;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.internal.helpers.HostnamePort;

/**
 * Provides a fallback address resolver which will return the address of the default connector (e.g. the standard Bolt
 * port if enabled).
 * <p />
 * This implementation is chosen when none is explicitly given through the {@link Resolver} annotation or one of its
 * children.
 */
public final class DefaultAddressResolver extends AbstractAddressResolver {

    @Override
    public HostnamePort doResolve(ExtensionContext extensionContext, ParameterContext context, Neo4jWithSocket server) {
        return server.lookupDefaultConnector();
    }
}
