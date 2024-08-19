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
package org.neo4j.internal.kernel.api.connectioninfo;

import java.util.Map;

/**
 * Information used for routing the transaction.
 *
 * @param parameters an opaque key-value map that is provided by a client's driver and passed
 *                   to the logic dealing with routing policies where it is interpreted.
 */
public record RoutingInfo(AccessMode accessMode, Map<String, String> parameters) {

    /**
     * Used to decide if a transaction should be routed to a write server or a read server in a cluster.
     * When running a transaction, a write transaction requires a server that supports writes.
     * A read transaction, on the other hand, requires a server that supports read operations.
     * This classification is key for routing driver to route transactions to a cluster correctly.
     */
    public enum AccessMode {
        /**
         * Use this for transactions that require a write server in a cluster
         */
        WRITE,
        /**
         * Use this for transactions that require a read server in a cluster
         */
        READ
    }
}
