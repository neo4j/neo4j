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
package org.neo4j.kernel.impl.transaction.log;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;

import java.io.IOException;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.memory.MemoryTracker;

public final class ReadAheadUtils {

    private ReadAheadUtils() {}

    /**
     * @param channel log channel to wrap with read-ahead channel
     * @param logHeader provides information about log header and envelope sizes
     * @param memoryTracker memory tracker for the read-ahead buffer
     * @return the appropriate read-ahead checksum channel for the log-version channel provided
     */
    public static ReadableLogChannel newChannel(
            LogVersionedStoreChannel channel, LogHeader logHeader, MemoryTracker memoryTracker) throws IOException {
        return newChannel(channel, NO_MORE_CHANNELS, logHeader, memoryTracker, false);
    }

    /**
     * @param channel log channel to wrap with read-ahead channel
     * @param logVersionBridge version bridge
     * @param logHeader provides information about log header and envelope sizes
     * @param memoryTracker memory tracker for the read-ahead buffer
     * @return the appropriate read-ahead checksum channel for the log-version channel provided
     */
    public static ReadableLogChannel newChannel(
            LogVersionedStoreChannel channel,
            LogVersionBridge logVersionBridge,
            LogHeader logHeader,
            MemoryTracker memoryTracker)
            throws IOException {
        return newChannel(channel, logVersionBridge, logHeader, memoryTracker, false);
    }

    /**
     * @param channel log channel to wrap with read-ahead channel
     * @param logVersionBridge version bridge
     * @param logHeader provides information about log header and envelope sizes
     * @param memoryTracker memory tracker for the read-ahead buffer
     * @param raw if channel is raw
     * @return the appropriate read-ahead checksum channel for the log-version channel provided
     */
    public static ReadableLogChannel newChannel(
            LogVersionedStoreChannel channel,
            LogVersionBridge logVersionBridge,
            LogHeader logHeader,
            MemoryTracker memoryTracker,
            boolean raw)
            throws IOException {
        LogFormat formatVersion = channel.getLogFormatVersion();
        if (formatVersion.usesSegments()) {
            return new EnvelopeReadChannel(
                    channel, logHeader.getSegmentBlockSize(), logVersionBridge, memoryTracker, raw);
        } else {
            return new ReadAheadLogChannel(channel, logVersionBridge, memoryTracker, raw);
        }
    }

    /**
     *
     * @param logFile the log file to read
     * @param version the version of the log file to read
     * @param memoryTracker memory tracker for the read-ahead buffer
     * @return the appropriate read-ahead checksum channel for the log-version channel provided
     */
    public static ReadableLogChannel newChannel(LogFile logFile, long version, MemoryTracker memoryTracker)
            throws IOException {
        return newChannel(logFile, version, NO_MORE_CHANNELS, memoryTracker);
    }

    /**
     *
     * @param logFile the log file to read
     * @param version the version of the log file to read
     * @param logVersionBridge version bridge
     * @param memoryTracker memory tracker for the read-ahead buffer
     * @return the appropriate read-ahead checksum channel for the log-version channel provided
     */
    public static ReadableLogChannel newChannel(
            LogFile logFile, long version, LogVersionBridge logVersionBridge, MemoryTracker memoryTracker)
            throws IOException {
        final var channel = logFile.openForVersion(version);
        LogFormat formatVersion = channel.getLogFormatVersion();
        if (formatVersion.usesSegments()) {
            LogHeader logHeader = logFile.extractHeader(version);
            return newChannel(channel, logVersionBridge, logHeader, memoryTracker);
        } else {
            return new ReadAheadLogChannel(channel, logVersionBridge, memoryTracker, false);
        }
    }
}
