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

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.xa.command.LogReader;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntryReader;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.util.Cursor;

public class LogDeserializer implements LogReader<ReadableLogChannel>
{
    private final LogEntryReader<ReadableLogChannel> logEntryReader;

    public LogDeserializer( CommandReaderFactory commandReaderFactory )
    {
        logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );
    }

    @Override
    public Cursor<IOException> cursor( ReadableLogChannel channel, Visitor<LogEntry, IOException> visitor )
    {
        return new LogCursor( channel, visitor );
    }

    private class LogCursor implements Cursor<IOException>
    {
        private final ReadableLogChannel channel;
        private Visitor<LogEntry, IOException> visitor;

        public LogCursor( ReadableLogChannel channel, Visitor<LogEntry, IOException> visitor )
        {
            this.channel = channel;
            this.visitor = visitor;
        }

        @Override
        public boolean next( ) throws IOException
        {
            LogEntry entry = logEntryReader.readLogEntry( channel );

            if ( entry == null )
            {
                return false;
            }

            return visitor.visit( entry );
        }

        @Override
        public void close() throws IOException
        {
            channel.close();
        }
    }
}
