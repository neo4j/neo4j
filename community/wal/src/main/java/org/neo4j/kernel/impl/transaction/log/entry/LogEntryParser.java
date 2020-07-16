/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;

/**
 * Reads and parses the next {@link LogEntry} from {@link ReadableChecksumChannel}, given the version.
 *
 * {@link #parse(byte, ReadableChecksumChannel, LogPositionMarker, CommandReaderFactory)}.
 */
public abstract class LogEntryParser
{
    private final byte type;

    public LogEntryParser( byte type )
    {
        this.type = type;
    }

    /**
     * @return code representing the type of log entry. See {@link LogEntryTypeCodes}.
     */
    byte type()
    {
        return type;
    }

    /**
     * Parses the next {@link LogEntry} read from the {@code channel}.
     *
     * @param version version this log entry is determined to be of.
     * @param channel {@link ReadableChecksumChannel} to read the data from.
     * @param marker {@link LogPositionMarker} marking the position in the {@code channel} that is the
     * start of this entry.
     * @param commandReaderFactory {@link CommandReaderFactory} for retrieving a {@link CommandReader}
     * for reading commands from, for log entry types that need that.
     * @return the next {@link LogEntry} read and parsed from the {@code channel}.
     * @throws IOException I/O error from channel or if data was read past the end of the channel.
     */
    abstract LogEntry parse( byte version, ReadableChecksumChannel channel, LogPositionMarker marker, CommandReaderFactory commandReaderFactory )
            throws IOException;
}
