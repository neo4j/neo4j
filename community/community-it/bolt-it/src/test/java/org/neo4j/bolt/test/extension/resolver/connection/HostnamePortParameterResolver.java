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
package org.neo4j.bolt.test.extension.resolver.connection;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.neo4j.bolt.test.connection.resolver.AddressResolver;
import org.neo4j.bolt.test.connection.resolver.DefaultAddressResolver;
import org.neo4j.bolt.transport.Neo4jWithSocketSupportExtension;
import org.neo4j.internal.helpers.HostnamePort;

/**
 * Resolves {@link HostnamePort} addresses of a test server instance.
 */
public class HostnamePortParameterResolver implements ParameterResolver {

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return HostnamePort.class.equals(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext context, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        var server = Neo4jWithSocketSupportExtension.getInstance(extensionContext);

        var resolver = AddressResolver.findResolver(context).orElseGet(DefaultAddressResolver::new);
        return resolver.resolve(extensionContext, context, server);
    }
}
