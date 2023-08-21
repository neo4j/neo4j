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
package org.neo4j.bolt.test.connection.initializer;

import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.neo4j.bolt.test.annotation.connection.InitializeConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.testing.util.AnnotationUtil;

/**
 * Handles the initialization of a connection.
 * <p />
 * This interface is typically implemented in conjunction with an {@link InitializeConnection} annotation or a custom annotation
 * bearing the {@link InitializeConnection} annotation.
 * <p />
 * Implementations of this interface are expected to provide a publicly accessible no-args construction via which they
 * are initialized when referenced using the {@link InitializeConnection} annotation or one of its children.
 * <p />
 * Refer to the {@link org.neo4j.bolt.test.annotation.connection} package for examples on how to utilize this interface
 * in the most optimal fashion.
 */
public interface ConnectionInitializer {

    /**
     * Initializes a given connection.
     * <p />
     * Passed connections will already be established (e.g. {@link TransportConnection#connect()} has already been
     * invoked).
     *
     * @param extensionContext the context of the extension which is invoking this initializer.
     * @param context the context of the parameter which is currently being injected.
     * @param wire the Bolt protocol revision selected for this test invocation.
     * @param connection the allocated connection.
     * @throws ParameterResolutionException when the connection fails to initialize.
     */
    void initialize(
            ExtensionContext extensionContext, ParameterContext context, BoltWire wire, TransportConnection connection)
            throws ParameterResolutionException;

    static List<ConnectionInitializer> findInitializers(ParameterContext context) {
        return AnnotationUtil.selectProviders(
                context.getParameter(), InitializeConnection.class, InitializeConnection::value, true);
    }
}
