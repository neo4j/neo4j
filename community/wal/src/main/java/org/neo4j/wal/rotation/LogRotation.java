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
package org.neo4j.wal.rotation;

import java.io.IOException;

/**
 * Used to check if a log rotation is needed, and also to execute a log rotation.
 * <p>
 * The header of the new file is described by a {@link NextLogHeader.Builder}: values set on it are used as given,
 * and unset ones are taken from the file's live state inside the rotation critical section. Methods prefixed {@code lockless} must only be called by a thread that
 * already has exclusive access to the log.
 * <p>
 * The implementation also makes sure that stores are forced to disk.
 */
public interface LogRotation {
    LogRotation NO_ROTATION = new LogRotation() {
        @Override
        public boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) {
            return false;
        }

        @Override
        public boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) {
            return false;
        }

        @Override
        public boolean locklessBatchedRotateLogIfNeeded(
                LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) {
            return false;
        }

        @Override
        public void rotateLogFile(LogRotateEvents logRotateEvents) {}

        @Override
        public void locklessRotateLogFile(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) {}

        @Override
        public long rotationSize() {
            return 0;
        }
    };

    /**
     * Rotates the underlying log if it is required, taking the log file lock. The new header is derived from the
     * live state. Returns true if rotation happened, false otherwise.
     * @param logRotateEvents A trace event for the current log append operation.
     */
    boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) throws IOException;

    /**
     * Rotates the underlying log if it is required. Returns true if rotation happened, false otherwise.
     * @param logRotateEvents A trace event for the current log append operation.
     * @param headerBuilder Explicit values for the new header; unset ones are taken from the live state.
     */
    boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder)
            throws IOException;

    /**
     * Rotates the underlying log if it is required for raw batch appends. Returns true if rotation happened, false
     * otherwise. Batch rotation does not perform any metadata or lower version store updates and only performs log
     * file rotations.
     */
    boolean locklessBatchedRotateLogIfNeeded(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder)
            throws IOException;

    /**
     * Force a log rotation, taking the log file lock, with a header derived from the live state. Generally should
     * only be used in tests and tools.
     * @param logRotateEvents - A trace event for the current log append operation.
     * @throws IOException - Thrown on file system errors during rotation
     */
    void rotateLogFile(LogRotateEvents logRotateEvents) throws IOException;

    /**
     * Force a log rotation without taking any additional locks.
     * Only use this if the logFile lock is already taken, or there can be no other concurrent operations.
     * @param logRotateEvents - A trace event for the current log append operation.
     * @param headerBuilder - Explicit values for the new header; unset ones are taken from the live state.
     * @throws IOException - Thrown on file system errors during rotation
     */
    void locklessRotateLogFile(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) throws IOException;

    long rotationSize();
}
