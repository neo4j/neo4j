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

import org.neo4j.io.fs.ReadableChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;

public enum LogEntryParsersV4_0 implements LogEntryParser
{
    TX_START
            {
                @Override
                public LogEntry parse( LogEntryVersion version, ReadableChecksumChannel channel, LogPositionMarker marker,
                                       CommandReaderFactory commandReader ) throws IOException
                {
                    LogPosition position = marker.newPosition();
                    long timeWritten = channel.getLong();
                    long latestCommittedTxWhenStarted = channel.getLong();
                    int previousChecksum = channel.getInt();
                    int additionalHeaderLength = channel.getInt();
                    byte[] additionalHeader = new byte[additionalHeaderLength];
                    channel.get( additionalHeader, additionalHeaderLength );
                    return new LogEntryStart( version, timeWritten, latestCommittedTxWhenStarted, previousChecksum, additionalHeader, position );
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.TX_START;
                }
            },

    COMMAND
            {
                @Override
                public LogEntry parse( LogEntryVersion version, ReadableChecksumChannel channel, LogPositionMarker marker,
                                       CommandReaderFactory commandReader ) throws IOException
                {
                    StorageCommand command = commandReader.get( version.version() ).read( channel );
                    return command == null ? null : new LogEntryCommand( version, command );
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.COMMAND;
                }
            },

    TX_COMMIT
            {
                @Override
                public LogEntry parse( LogEntryVersion version, ReadableChecksumChannel channel, LogPositionMarker marker,
                                       CommandReaderFactory commandReader ) throws IOException
                {
                    long txId = channel.getLong();
                    long timeWritten = channel.getLong();
                    int checksum = channel.endChecksumAndValidate();
                    return new LogEntryCommit( version, txId, timeWritten, checksum );
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.TX_COMMIT;
                }
            },

    CHECK_POINT
            {
                @Override
                public LogEntry parse( LogEntryVersion version, ReadableChecksumChannel channel, LogPositionMarker marker,
                                       CommandReaderFactory commandReader ) throws IOException
                {
                    long logVersion = channel.getLong();
                    long byteOffset = channel.getLong();
                    channel.endChecksumAndValidate();
                    return new CheckPoint( version, new LogPosition( logVersion, byteOffset ) );
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.CHECK_POINT;
                }
            }
}
