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
package org.neo4j.bolt.protocol.common.connector.listener;

import org.neo4j.bolt.protocol.common.connector.connection.Connection;

/**
 * Provides hooks for various events within the connector lifecycle.
 */
public interface ConnectorListener {

    /**
     * Handles the addition of this listener to a given connector.
     */
    default void onListenerAdded() {}

    /**
     * Handles the removal of this listener from a given connector.
     */
    default void onListenerRemoved() {}

    /**
     * Handles the creation of a new connection.
     *
     * @param connection a newly established connection.
     */
    default void onConnectionCreated(Connection connection) {}
}
