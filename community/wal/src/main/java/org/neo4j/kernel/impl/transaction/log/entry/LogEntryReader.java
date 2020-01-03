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

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChecksumChannel;

/**
 * Reads {@link LogEntry} instances from a {@link ReadableChannel source}. Instances are expected to be
 * immutable and handle concurrent calls from multiple threads.
 */
public interface LogEntryReader
{
    /**
     * Reads the next {@link LogEntry} from the given source.
     *
     * @param source {@link ReadableChannel} to read from.
     * @return the read {@link LogEntry} or {@code null} if there were no more complete entries in the given source.
     * @throws IOException if source throws exception.
     */
    LogEntry readLogEntry( ReadableClosablePositionAwareChecksumChannel source ) throws IOException;

    LogPosition lastPosition();
}
