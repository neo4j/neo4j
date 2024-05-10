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
package org.neo4j.kernel.impl.transaction.log.rotation;

import java.io.IOException;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvents;

/**
 * Used to check if a log rotation is needed, and also to execute a log rotation.
 *
 * The implementation also makes sure that stores are forced to disk.
 *
 */
public interface LogRotation {
    LogRotation NO_ROTATION = new LogRotation() {
        @Override
        public boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) {
            return false;
        }

        @Override
        public boolean batchedRotateLogIfNeeded(LogRotateEvents logRotateEvents, long transactionId, long appendIndex) {
            return false;
        }

        @Override
        public boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents) {
            return false;
        }

        @Override
        public void rotateLogFile(LogRotateEvents logRotateEvents) {}

        @Override
        public long rotationSize() {
            return 0;
        }
    };

    /**
     * Rotates the underlying log if it is required. Returns true if rotation happened, false otherwise
     * @param logRotateEvents A trace event for the current log append operation.
     */
    boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) throws IOException;

    /**
     * Rotates the underlying log if it is required for batch updates. Returns true if rotation happened, false otherwise.
     * Batch rotation does not perform any metadata or lover version store updates and only perform log file rotations.
     */
    boolean batchedRotateLogIfNeeded(LogRotateEvents logRotateEvents, long lastTransactionId, long lastAppendIndex)
            throws IOException;

    /**
     * Rotates the underlying log if it is required. Returns true if rotation happened, false otherwise
     * @param logRotateEvents A trace event for the current log append operation.
     */
    boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents) throws IOException;

    /**
     * Force a log rotation.
     * @throws IOException
     */
    void rotateLogFile(LogRotateEvents logRotateEvents) throws IOException;

    long rotationSize();
}
