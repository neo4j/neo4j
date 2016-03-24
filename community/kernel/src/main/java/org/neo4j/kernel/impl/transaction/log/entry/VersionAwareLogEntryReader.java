/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ReadPastEndException;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.Exceptions.withMessage;
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
public class VersionAwareLogEntryReader<SOURCE extends ReadableClosablePositionAwareChannel> implements LogEntryReader<SOURCE>
{
    private final CommandReaderFactory commandReaderFactory;

    public VersionAwareLogEntryReader()
    {
        this( new RecordStorageCommandReaderFactory() );
    }

    public VersionAwareLogEntryReader( CommandReaderFactory commandReaderFactory )
    {
        this.commandReaderFactory = commandReaderFactory;
    }

    @Override
    public LogEntry readLogEntry( SOURCE channel ) throws IOException
    {
        try
        {
            LogPositionMarker positionMarker = new LogPositionMarker();
            channel.getCurrentPosition( positionMarker );
            while ( true )
            {
                LogEntryVersion version = null;
                LogEntryParser<LogEntry> entryReader;
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

                    version = byVersion( versionCode );
                    entryReader = version.entryParser( typeCode );
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

                LogEntry entry = entryReader.parse( version, channel, positionMarker, commandReaderFactory );
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
}
