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
package org.neo4j.bolt.test.connection.transport;

import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.testing.client.TransportType;

/**
 * Provides a fallback transport selector which solely selects the {@link TransportType#TCP} transport type for test
 * executions.
 * <p />
 * This selector implementation is primarily used as a default value for protocol focused tests which do not need to be
 * executed through various transports.
 */
public class DefaultTransportSelector implements TransportSelector {

    @Override
    public Stream<TransportType> select(ExtensionContext context) {
        return Stream.of(TransportType.TCP);
    }
}
