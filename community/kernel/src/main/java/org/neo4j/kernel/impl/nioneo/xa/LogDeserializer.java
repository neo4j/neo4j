/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.xa.command.LogReader;
import org.neo4j.kernel.impl.transaction.xaframework.IOCursor;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.VersionAwareLogEntryReader;

public class LogDeserializer implements LogReader<ReadableLogChannel>
{
    private final LogEntryReader<ReadableLogChannel> logEntryReader;

    public LogDeserializer( CommandReaderFactory commandReaderFactory )
    {
        logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );
    }

    @Override
    public IOCursor<LogEntry> logEntries( ReadableLogChannel channel )
    {
        return new LogCursor( channel );
    }

    private class LogCursor implements IOCursor<LogEntry>
    {
        private final ReadableLogChannel channel;
        private LogEntry entry;

        public LogCursor( ReadableLogChannel channel )
        {
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
}
