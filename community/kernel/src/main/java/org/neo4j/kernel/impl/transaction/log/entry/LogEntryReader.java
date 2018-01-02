/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

/**
* Reads a stream of {@link LogEntry log entries} from a {@link ReadableLogChannel}.
*
* @param <S> type of source to read from
*/
public interface LogEntryReader<S extends ReadableLogChannel>
{
    /**
     * Reads next {@link LogEntry} from the source.
     *
     * @param source to read from.
     * @return the read {@link LogEntry}.
     * @throws IOException on error reading from source.
     * @throws ReadPastEndException if there were no more {@link LogEntry log entries} to read.
     */
    LogEntry readLogEntry( S source ) throws IOException;
}
