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

import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.packstream.codec.transport.ChunkFrameDecoder;

/**
 * Imposes a maximum permitted number of bytes to be received on a connection during the negotiation and authentication
 * phase.
 */
public class ReadLimitConnectionListener implements ConnectionListener {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ReadLimitConnectionListener.class);

    private final Connection connection;
    private final InternalLog log;
    private final long limit;

    public ReadLimitConnectionListener(Connection connection, InternalLogProvider logging, long limit) {
        this.connection = connection;
        this.log = logging.getLog(ReadLimitConnectionListener.class);
        this.limit = limit;
    }

    @Override
    public void onListenerRemoved() {
        this.connection.memoryTracker().releaseHeap(SHALLOW_SIZE);
    }

    @Override
    public void onLogon(LoginContext ctx) {
        log.debug("[%s] Removing read limit", this.connection.id());

        try (var memoryTracker = this.connection.memoryTracker().getScopedMemoryTracker()) {
            // temporarily allocate additional memory for a replacement ChunkFrameDecoder as we'll need to create a new
            // instance in order to remove the imposed read limit
            memoryTracker.allocateHeap(ChunkFrameDecoder.SHALLOW_SIZE);

            var pipeline = this.connection.channel().pipeline();

            var oldDecoder = pipeline.get(ChunkFrameDecoder.class);
            var newDecoder = oldDecoder.unlimited();

            pipeline.replace(oldDecoder, ChunkFrameDecoder.NAME, newDecoder);
        }
    }

    @Override
    public void onLogoff() {
        log.debug("[%s] Re-adding read limit of [%o]", this.connection.id(), limit);

        try (var memoryTracker = this.connection.memoryTracker().getScopedMemoryTracker()) {
            // temporarily allocate additional memory for a replacement ChunkFrameDecoder as we'll need to create a new
            // instance in order to remove the imposed read limit
            memoryTracker.allocateHeap(ChunkFrameDecoder.SHALLOW_SIZE);

            var pipeline = this.connection.channel().pipeline();

            var oldDecoder = pipeline.get(ChunkFrameDecoder.class);
            var newDecoder = oldDecoder.limit(limit);

            pipeline.replace(oldDecoder, ChunkFrameDecoder.NAME, newDecoder);
        }
    }
}
