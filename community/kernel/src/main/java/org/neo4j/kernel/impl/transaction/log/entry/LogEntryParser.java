/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;

/**
 * Reads and parses the next {@link LogEntry} from {@link ReadableClosableChannel}, given the {@link LogEntryVersion}.
 *
 * @param <T> Specific type of {@link LogEntry} returned from
 * {@link #parse(LogEntryVersion, ReadableClosableChannel, LogPositionMarker, CommandReaderFactory)}.
 */
public interface LogEntryParser<T extends LogEntry>
{
    /**
     * Parses the next {@link LogEntry} read from the {@code channel}.
     *
     * @param version {@link LogEntryVersion} this log entry is determined to be of.
     * @param channel {@link ReadableClosableChannel} to read the data from.
     * @param marker {@link LogPositionMarker} marking the position in the {@code channel} that is the
     * start of this entry.
     * @param commandReaderFactory {@link CommandReaderFactory} for retrieving a {@link CommandReader}
     * for reading commands from, for log entry types that need that.
     * @return the next {@link LogEntry} read and parsed from the {@code channel}.
     * @throws IOException I/O error from channel or if data was read past the end of the channel.
     */
    T parse( LogEntryVersion version, ReadableClosableChannel channel,
             LogPositionMarker marker, CommandReaderFactory commandReaderFactory ) throws IOException;

    /**
     * @return code representing the type of log entry.
     */
    byte byteCode();

    /**
     * @return whether or not entries parsed by this parser should be skipped, like an empty entry.
     */
    boolean skip();
}
