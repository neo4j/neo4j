/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.io.fs.ReadableChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;

public class LogEntryParserSetV2_3 extends LogEntryParserSet
{
    public static final LogEntryParserSet V2_3 = new LogEntryParserSetV2_3();

    private LogEntryParserSetV2_3()
    {
        super( LogEntryParserSetVersion.LogEntryV2_3 );
        register( new LogEntryParser( LogEntryTypeCodes.TX_START )
        {
            @Override
            public LogEntry parse( byte version, ReadableChecksumChannel channel, LogPositionMarker marker, CommandReaderFactory commandReaderFactory )
                    throws IOException
            {
                LogPosition position = marker.newPosition();
                channel.getInt(); // masterId
                channel.getInt(); // authorId
                long timeWritten = channel.getLong();
                long latestCommittedTxWhenStarted = channel.getLong();
                int additionalHeaderLength = channel.getInt();
                byte[] additionalHeader = new byte[additionalHeaderLength];
                channel.get( additionalHeader, additionalHeaderLength );
                return new LogEntryStart( version, timeWritten, latestCommittedTxWhenStarted, 0, additionalHeader, position );
            }
        } );
        register( new LogEntryParser( LogEntryTypeCodes.COMMAND )
        {
            @Override
            public LogEntry parse( byte version, ReadableChecksumChannel channel, LogPositionMarker marker, CommandReaderFactory commandReaderFactory )
                    throws IOException
            {
                StorageCommand command = commandReaderFactory.get( version ).read( channel );
                return command == null ? null : new LogEntryCommand( version, command );
            }
        } );
        register( new LogEntryParser( LogEntryTypeCodes.TX_COMMIT )
        {
            @Override
            public LogEntry parse( byte version, ReadableChecksumChannel channel, LogPositionMarker marker, CommandReaderFactory commandReaderFactory )
                    throws IOException
            {
                long txId = channel.getLong();
                long timeWritten = channel.getLong();
                return new LogEntryCommit( version, txId, timeWritten, 0 );
            }
        } );
        register( new LogEntryParser( LogEntryTypeCodes.LEGACY_CHECK_POINT )
        {
            @Override
            public LogEntry parse( byte version, ReadableChecksumChannel channel, LogPositionMarker marker, CommandReaderFactory commandReaderFactory )
                    throws IOException
            {
                long logVersion = channel.getLong();
                long byteOffset = channel.getLong();
                return new LogEntryInlinedCheckPoint( version, new LogPosition( logVersion, byteOffset ) );
            }
        } );
    }
}
