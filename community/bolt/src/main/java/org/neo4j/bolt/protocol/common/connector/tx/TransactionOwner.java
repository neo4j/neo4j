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
package org.neo4j.bolt.protocol.common.connector.tx;

import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.memory.MemoryTracker;

/**
 * Provides necessary metadata and dependencies for transaction owners.
 */
public interface TransactionOwner {

    /**
     * Retrieves the memory tracker which tracks all memory consumption for this transaction owner.
     *
     * @return a memory tracker.
     */
    MemoryTracker memoryTracker();

    /**
     * Retrieves a condensed version of the connection parameters.
     *
     * @return a connection information object.
     */
    ClientConnectionInfo info();

    LoginContext loginContext();

    RoutingContext routingContext();

    // TODO: Databases should be identified by their UUID in the future to avoid confusion
    // TODO: This should probably live in the FSM context
    /**
     * Retrieves the currently selected default database for this transaction owner.
     *
     * @return a selected default database or null, if none has been selected.
     */
    String selectedDefaultDatabase();
}
