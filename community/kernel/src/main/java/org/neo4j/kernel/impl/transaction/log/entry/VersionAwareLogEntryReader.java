/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PositionableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ReadPastEndException;

import static org.neo4j.helpers.Exceptions.throwIfInstanceOf;
import static org.neo4j.helpers.Exceptions.withMessage;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntrySanity.logEntryMakesSense;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion.byVersion;

/**
 * Version aware implementation of LogEntryReader
 * Starting with Neo4j version 2.1, log entries are prefixed with a version. This allows for Neo4j instances of
 * different versions to exchange transaction data, either directly or via logical logs.
 *
 * Read all about it at {@link LogEntryVersion}.
 */
public class VersionAwareLogEntryReader<SOURCE extends ReadableClosablePositionAwareChannel> implements LogEntryReader<SOURCE>
{
    private final CommandReaderFactory commandReaderFactory;
    private final InvalidLogEntryHandler invalidLogEntryHandler;

    public VersionAwareLogEntryReader()
    {
        this( new RecordStorageCommandReaderFactory(), InvalidLogEntryHandler.STRICT );
    }

    public VersionAwareLogEntryReader( CommandReaderFactory commandReaderFactory,
            InvalidLogEntryHandler invalidLogEntryHandler )
    {
        this.commandReaderFactory = commandReaderFactory;
        this.invalidLogEntryHandler = invalidLogEntryHandler;
    }

    @Override
    public LogEntry readLogEntry( SOURCE channel ) throws IOException
    {
        try
        {
            LogPositionMarker positionMarker = new LogPositionMarker();
            long skipped = 0;
            while ( true )
            {
                channel.getCurrentPosition( positionMarker );

                byte versionCode = channel.get();
                byte typeCode = channel.get();

                LogEntryVersion version = null;
                LogEntryParser<LogEntry> entryReader;
                LogEntry entry;
                try
                {
                    version = byVersion( versionCode );
                    entryReader = version.entryParser( typeCode );
                    entry = entryReader.parse( version, channel, positionMarker, commandReaderFactory );
                    if ( entry != null && skipped > 0 )
                    {
                        // Take extra care when reading an entry in a bad section. Just because entry reading
                        // didn't throw exception doesn't mean that it's a sane entry.
                        if ( !logEntryMakesSense( entry ) )
                        {
                            throw new IllegalArgumentException( "Log entry " + entry + " which was read after " +
                                    "a bad section of " + skipped + " bytes was read successfully, but " +
                                    "its contents is unrealistic, so treating as part of bad section" );
                        }
                        invalidLogEntryHandler.bytesSkipped( skipped );
                        skipped = 0;
                    }
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

                    if ( channelSupportsPositioning( channel ) &&
                            invalidLogEntryHandler.handleInvalidEntry( e, position ) )
                    {
                        ((PositionableChannel)channel).setCurrentPosition( positionMarker.getByteOffset() + 1 );
                        skipped++;
                        continue;
                    }
                    throwIfInstanceOf( e, UnsupportedLogVersionException.class );
                    throw new IOException( e );
                }

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

    private boolean channelSupportsPositioning( SOURCE channel )
    {
        return channel instanceof PositionableChannel;
    }
}
