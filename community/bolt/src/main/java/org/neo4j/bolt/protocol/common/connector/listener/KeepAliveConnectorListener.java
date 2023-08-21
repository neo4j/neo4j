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
import org.neo4j.bolt.protocol.common.connector.connection.listener.KeepAliveConnectionListener;
import org.neo4j.logging.InternalLogProvider;

/**
 * Attaches a {@link KeepAliveConnectionListener} to all newly established connections.
 */
public class KeepAliveConnectorListener implements ConnectorListener {
    private final boolean legacyMode;
    private final long writeIdleTime;
    private final InternalLogProvider logging;

    public KeepAliveConnectorListener(boolean legacyMode, long writeIdleTime, InternalLogProvider logging) {
        this.legacyMode = legacyMode;
        this.writeIdleTime = writeIdleTime;
        this.logging = logging;
    }

    @Override
    public void onConnectionCreated(Connection connection) {
        connection.memoryTracker().allocateHeap(KeepAliveConnectionListener.SHALLOW_SIZE);

        connection.registerListener(
                new KeepAliveConnectionListener(connection, this.legacyMode, this.writeIdleTime, this.logging));
    }
}
