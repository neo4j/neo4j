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
package org.neo4j.bolt.test.provider;

import java.io.IOException;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.connection.initializer.Negotiated;
import org.neo4j.bolt.test.annotation.connection.resolver.Connector;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.client.TransportConnection;

/**
 * Provides a factory function capable of dynamically creating new managed connections within a test class.
 * <p />
 * Instances of this interface may be injected within tests marked via {@link ProtocolTest} and/or {@link TransportTest}
 * and will follow the same semantics as the injection of {@link TransportConnection} instances.
 * <p />
 * @see Authenticated for creating pre-authenticated connections.
 * @see Connector for selecting a specific connector.
 * @see Negotiated for creating pre-negotiated connections.
 */
@FunctionalInterface
public interface ConnectionProvider {

    /**
     * Creates a new connection to the target server.
     *
     * @throws IOException when a connection cannot be established.
     */
    TransportConnection create() throws IOException;
}
