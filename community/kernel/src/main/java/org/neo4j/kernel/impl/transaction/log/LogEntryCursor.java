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
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;

public class LogEntryCursor implements IOCursor<LogEntry>
{
    private final LogEntryReader<ReadableVersionableLogChannel> logEntryReader;
    private final ReadableVersionableLogChannel channel;
    private LogEntry entry;

    public LogEntryCursor( ReadableVersionableLogChannel channel )
    {
        this( new VersionAwareLogEntryReader<ReadableVersionableLogChannel>(), channel );
    }

    public LogEntryCursor( LogEntryReader<ReadableVersionableLogChannel> logEntryReader, ReadableVersionableLogChannel channel )
    {
        this.logEntryReader = logEntryReader;
        this.channel = channel;
    }

    @Override
    public LogEntry get()
    {
        return entry;
    }

    @Override
    public boolean next( ) throws IOException
    {
        entry = logEntryReader.readLogEntry( channel );

        return entry != null;
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
