/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;

public class LogEntryCursor<T extends ReadableLogChannel> implements IOCursor<LogEntry>
{
    private final T channel;
    private final LogEntryReader<T> entryReader;
    private LogEntry current;

    public LogEntryCursor( T channel, LogEntryReader<T> entryReader )
    {
        this.channel = channel;
        this.entryReader = entryReader;
    }

    @Override
    public LogEntry get()
    {
        return current;
    }

    @Override
    public boolean next() throws IOException
    {
        LogEntry entry = entryReader.readLogEntry( channel );
        if ( entry == null )
        {
            current = null;
            return false;
        }

        current = entry;
        return true;
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
