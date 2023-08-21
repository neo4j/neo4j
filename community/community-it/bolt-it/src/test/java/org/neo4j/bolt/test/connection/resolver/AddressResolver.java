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
package org.neo4j.bolt.test.connection.resolver;

import java.net.SocketAddress;
import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.neo4j.bolt.test.annotation.connection.Resolver;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.util.AnnotationUtil;
import org.neo4j.bolt.transport.Neo4jWithSocket;

/**
 * Resolves the address to which a connection shall be established within a given test invocation.
 * <p />
 * This interface is typically implemented in conjunction with a {@link Resolver} annotation or a custom annotation
 * bearing the {@link Resolver} annotation.
 * <p />
 * Implementations of this interface are expected to provide a publicly accessible no-args construction via which they
 * are Resolved when referenced using the {@link Resolver} annotation or one of its children.
 * <p />
 * Refer to the {@link org.neo4j.bolt.test.annotation.connection} package for examples on how to utilize this interface
 * in the most optimal fashion.
 */
public interface AddressResolver {

    /**
     * Resolves the target address for a given annotated connection.
     *
     * @param extensionContext the context of the extension which is invoking this resolver..
     * @param context the context of the parameter which is currently being injected.
     * @param server a server instance allocated for the current test.
     * @param transportType the transport type that is being resolved
     * @throws ParameterResolutionException when the address cannot be resolved.
     */
    SocketAddress resolve(
            ExtensionContext extensionContext,
            ParameterContext context,
            Neo4jWithSocket server,
            TransportType transportType)
            throws ParameterResolutionException;

    static Optional<AddressResolver> findResolver(ParameterContext context) {
        return AnnotationUtil.selectProvider(context.getParameter(), Resolver.class, Resolver::value);
    }
}
