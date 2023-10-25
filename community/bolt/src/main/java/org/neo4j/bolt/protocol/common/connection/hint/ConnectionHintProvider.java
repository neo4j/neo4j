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
package org.neo4j.bolt.protocol.common.connection.hint;

import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.values.virtual.MapValueBuilder;

@FunctionalInterface
public interface ConnectionHintProvider {

    default boolean isApplicable() {
        return true;
    }

    /**
     * Retrieves the protocol version in which the connection hints defined by this provider became
     * first available.
     *
     * @return a minimal protocol version.
     */
    default ProtocolVersion supportedSince() {
        return ProtocolVersion.INVALID;
    }

    /**
     * Retrieves the protocol version in which the connection hints defined by this provider were
     * last available.
     *
     * @return a maximum protocol version.
     */
    default ProtocolVersion supportedUntil() {
        return ProtocolVersion.MAX;
    }

    /**
     * Appends a set of connection hints to the given map.
     *
     * @param hints a map consisting of zero or more hints.
     */
    void append(MapValueBuilder hints);
}
