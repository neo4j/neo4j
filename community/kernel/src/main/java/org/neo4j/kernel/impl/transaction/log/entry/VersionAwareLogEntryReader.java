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

import org.neo4j.kernel.impl.transaction.command.CommandReader;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.Exceptions.withMessage;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion.NO_PARTICULAR_LOG_HEADER_FORMAT_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion.byVersion;

/**
 * Version aware implementation of LogEntryReader
 * Starting with Neo4j version 2.1, log entries are prefixed with a version. This allows for Neo4j instances of
 * different versions to exchange transaction data, either directly or via logical logs. This implementation of
 * LogEntryReader makes use of the version information to deserialize command entries that hold commands created
 * with previous versions of Neo4j.
 *
 * Read all about it at {@link LogEntryVersion}.
 */
public class VersionAwareLogEntryReader<SOURCE extends ReadableLogChannel> implements LogEntryReader<SOURCE>
{
    private final LogPositionMarker positionMarker = new LogPositionMarker();

    // Exists for backwards compatibility until we drop support for one of the two versions (1.9 and 2.0)
    // that doesn't have log entry version in its format.
    private final byte logHeaderFormatVersion;

    // Caching of CommandReader instance, we use the LogEntryVersion instance for comparison
    private LogEntryVersion lastVersion;
    private CommandReader currentCommandReader;

    public VersionAwareLogEntryReader( byte logHeaderFormatVersion )
    {
        this.logHeaderFormatVersion = logHeaderFormatVersion;
    }

    public VersionAwareLogEntryReader()
    {
        this( NO_PARTICULAR_LOG_HEADER_FORMAT_VERSION );
    }

    @Override
    public LogEntry readLogEntry( SOURCE channel ) throws IOException
    {
        try
        {
            channel.getCurrentPosition( positionMarker );
            while ( true )
            {
                LogEntryVersion version = null;
                LogEntryParser<LogEntry> entryReader;
                CommandReader commandReader;
                try
                {
                    /*
                     * if the read type is negative than it is actually the log entry version
                     * so we need to read an extra byte which will contain the type
                     */
                    byte typeCode = channel.get();
                    byte versionCode = 0;
                    if ( typeCode < 0 )
                    {
                        versionCode = typeCode;
                        typeCode = channel.get();
                    }

                    version = byVersion( versionCode, logHeaderFormatVersion );
                    entryReader = version.entryParser( typeCode );
                    commandReader = commandReader( version );
                }
                catch ( ReadPastEndException e )
                {   // Make these exceptions slip by straight out to the outer handler
                    throw e;
                }
                catch ( Exception e )
                {   // Tag all other exceptions with log position and other useful information
                    LogPosition position = positionMarker.newPosition();
                    e = withMessage( e, e.getMessage() + ". At position " + position +
                            " and entry version " + version );
                    throw launderedException( IOException.class, e );
                }

                LogEntry entry = entryReader.parse( version, channel, positionMarker, commandReader );
                if ( !entryReader.skip() )
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

    private CommandReader commandReader( LogEntryVersion version )
    {
        if ( version != lastVersion )
        {
            lastVersion = version;
            currentCommandReader = version.newCommandReader();
        }
        return currentCommandReader;
    }
}
