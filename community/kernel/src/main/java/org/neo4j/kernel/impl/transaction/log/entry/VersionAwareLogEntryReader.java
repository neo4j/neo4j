/**
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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.command.CommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

/**
 * Version aware implementation of LogEntryReader
 * Starting with Neo4j version 2.1, log entries are prefixed with a version. This allows for Neo4j instances of
 * different versions to exchange transaction data, either directly or via logical logs. This implementation of
 * LogEntryReader makes use of the version information to deserialize command entries that hold commands created
 * with previous versions of Neo4j. Support for this comes from the required {@link org.neo4j.kernel.impl.transaction.command.CommandReaderFactory} which can
 * provide deserializers for Commands given the version.
 */
class VersionAwareLogEntryReader
{
    private final LogEntryParserFactory logEntryParserFactory;
    private final CommandReaderFactory commandReaderFactory;
    private final LogPositionMarker positionMarker = new LogPositionMarker();

    VersionAwareLogEntryReader( LogEntryParserFactory logEntryParserFactory, CommandReaderFactory commandReaderFactory )
    {
        this.logEntryParserFactory = logEntryParserFactory;
        this.commandReaderFactory = commandReaderFactory;
    }

    LogEntry readLogEntry( ReadableLogChannel channel, byte logFormatVersion ) throws IOException
    {
        try
        {
            channel.getCurrentPosition( positionMarker );
            LogEntryParserDispatcher dispatcher = logEntryParserFactory.newInstance( logFormatVersion );
            while ( true )
            {
                /*
                 * if the read type is negative than it is actually the log entry version
                 * so we need to read an extra byte which will contain the type
                 */
                byte type = channel.get();
                byte version = 0;
                if ( type < 0 )
                {
                    version = type;
                    type = channel.get();
                }

                LogEntryParser reader = dispatcher.dispatch( type );
                if ( reader == null )
                {
                    LogPosition position = positionMarker.newPosition();
                    throw new IOException( "Log format version: " + logFormatVersion +
                            " - Unknown entry[" + type + "] at " + "position " + position +
                            " and entry version " + version );

                }

                LogEntry entry = reader.parse( version, channel, positionMarker, commandReaderFactory );
                if ( !reader.skip() )
                {
                    return entry;
                }
            }
        }
        catch ( ReadPastEndException e )
        {
            return null;
        }
    }
}
