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
package org.neo4j.bolt.protocol.common.connector.connection.listener;

import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.handler.KeepAliveHandler;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;

/**
 * Introduces the keep-alive handler on newly established connections once the protocol permits such messages.
 */
public class KeepAliveConnectionListener implements ConnectionListener {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(KeepAliveConnectionListener.class);

    private final Connection connection;
    private final boolean legacyMode;
    private final long writerIdleTimeSeconds;
    private final InternalLogProvider logging;
    private final InternalLog log;

    public KeepAliveConnectionListener(
            Connection connection, boolean legacyMode, long writeIdleTime, InternalLogProvider logging) {
        this.connection = connection;
        this.legacyMode = legacyMode;
        this.writerIdleTimeSeconds = writeIdleTime;
        this.logging = logging;

        this.log = logging.getLog(KeepAliveConnectionListener.class);
    }

    @Override
    public void onListenerRemoved() {
        this.connection.memoryTracker().releaseHeap(SHALLOW_SIZE);
    }

    @Override
    public void onProtocolSelected(BoltProtocol protocol) {
        this.log.debug("[%s] Installing keep alive handler", this.connection.id());

        this.connection.memoryTracker().allocateHeap(KeepAliveHandler.SHALLOW_SIZE);

        this.connection
                .channel()
                .pipeline()
                .addLast(new KeepAliveHandler(this.legacyMode, this.writerIdleTimeSeconds, this.logging));

        this.connection.removeListener(this);
    }
}
