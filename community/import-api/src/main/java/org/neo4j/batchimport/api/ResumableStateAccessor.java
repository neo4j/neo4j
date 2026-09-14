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
package org.neo4j.batchimport.api;

import java.io.DataInputStream;
import java.io.IOException;
import java.time.Duration;
import org.neo4j.batchimport.api.input.ResumableStateData;

/**
 * Exposes methods to read and write the state of a resumable import from/to disk.
 */
public interface ResumableStateAccessor {
    long NO_PREVIOUS_NODES_PER_RANGE = -1L;

    /**
     * Called once the number of nodes per range has been settled, before ranges are populated. Persists the
     * calculated nodesPerRange to file for resumable import.
     */
    void persistNodesPerRange(long nodesPerRange);

    /**
     * Returns the last persisted nodesPerRange value, or -1 if none has been persisted yet.
     */
    long lastNodesPerRange();

    /**
     * Replaces the checkpoint {@link #lastCheckpoint()} returns with the given one, atomically.
     */
    void writeCheckpoint(ResumableStateData resumableStateData) throws IOException;

    /**
     * Returns a {@link DataInputStream} to read the most recently written checkpoint from. The caller must
     * {@link DataInputStream#close() close} the stream when done.
     * Returns {@code null} if no checkpoint has been written.
     */
    DataInputStream lastCheckpoint();

    /**
     * Returns the interval at which to write checkpoints during a resumable import.
     */
    Duration resumableImportCheckpointInterval();

    /**
     * A no-op implementation.
     */
    ResumableStateAccessor NOOP = new ResumableStateAccessor() {
        @Override
        public void persistNodesPerRange(long nodesPerRange) {
            // no-op
        }

        @Override
        public long lastNodesPerRange() {
            return NO_PREVIOUS_NODES_PER_RANGE;
        }

        @Override
        public void writeCheckpoint(ResumableStateData resumableStateData) {
            // no-op
        }

        @Override
        public DataInputStream lastCheckpoint() {
            return null;
        }

        @Override
        public Duration resumableImportCheckpointInterval() {
            return Duration.ofSeconds(0);
        }
    };
}
