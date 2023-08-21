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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;

/**
 * Reads and writes {@link LogEntry}s, given a provided version.
 */
public abstract class LogEntrySerializer<T extends LogEntry> {
    protected static final int NO_RETURN_VALUE = 0;

    private final byte type;

    protected LogEntrySerializer(byte type) {
        this.type = type;
    }

    /**
     * @return code representing the type of log entry. See {@link LogEntryTypeCodes}.
     */
    byte type() {
        return type;
    }

    /**
     * Parses the next {@link LogEntry} read from the {@code channel}.
     *
     * @param version version this log entry is determined to be of.
     * @param channel {@link ReadableChannel} to read the data from.
     * @param marker {@link LogPositionMarker} marking the position in the {@code channel} that is the
     * start of this entry.
     * @param commandReaderFactory {@link CommandReaderFactory} for retrieving a {@link CommandReader}
     * for reading commands from, for log entry types that need that.
     * @return the next {@link LogEntry} read and parsed from the {@code channel}.
     * @throws IOException I/O error from channel or if data was read past the end of the channel.
     */
    public abstract T parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory)
            throws IOException;

    /**
     * Write a {@link LogEntry} to a {@code channel}.
     *
     * @param channel  to write entry to.
     * @param logEntry to serialize.
     * @return checksum of the entry, or {@link #NO_RETURN_VALUE} if no checksum is needed.
     * @throws IOException I/O error when writing to the channel.
     */
    public abstract int write(WritableChannel channel, T logEntry) throws IOException;

    public static void writeLogEntryHeader(KernelVersion kernelVersion, byte type, WritableChannel channel)
            throws IOException {
        channel.putVersion(kernelVersion.version()).put(type);
    }
}
