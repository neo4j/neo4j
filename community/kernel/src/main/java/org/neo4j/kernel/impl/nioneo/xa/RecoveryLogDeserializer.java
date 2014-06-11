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
import org.neo4j.kernel.impl.transaction.xaframework.LogPosition;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.util.Cursor;

// TODO 2.2-future check out how deserialization happens on recovery and transfer over anything useful
public class RecoveryLogDeserializer implements LogReader<ReadableLogChannel>
{
    private final LogEntryReader logEntryReader;

    public RecoveryLogDeserializer( CommandReaderFactory commandReaderFactory )
    {
        logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );
    }

    @Override
    public Cursor<IOException> cursor( ReadableLogChannel channel, Visitor<LogEntry, IOException> visitor )
    {
        return new RecoveryCursor( channel, visitor );
    }

    private class RecoveryCursor implements Cursor<IOException>
    {
        private final ReadableLogChannel channel;
        private Visitor<LogEntry, IOException> visitor;

        private RecoveryCursor( ReadableLogChannel channel, Visitor<LogEntry, IOException> visitor )
        {
            this.channel = channel;
            this.visitor = visitor;
        }

        @Override
        public boolean next(  ) throws IOException
        {
            LogPosition position = channel.getCurrentPosition();

            LogEntry entry = logEntryReader.readLogEntry( channel );
            if ( entry instanceof LogEntry.Start )
            {
//                ((LogEntry.Start) entry).setStartPosition( position );
            }
            else if ( entry == null )
            {
                return false;
            }

            visitor.visit( entry );

            return true;
        }

        @Override
        public void close() throws IOException
        {
            channel.close();
        }
    }
}
