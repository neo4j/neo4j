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

import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.CommandReader;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

// 2.3
public enum LogEntryParsersV2_3 implements LogEntryParser<LogEntry>
{
    EMPTY
            {
                @Override
                public LogEntry parse( LogEntryVersion version, ReadableLogChannel channel, LogPositionMarker marker,
                        CommandReader commandReader ) throws IOException
                {
                    return null;

                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.EMPTY;
                }

                @Override
                public boolean skip()
                {
                    return false;
                }
            },

    TX_START
            {
                @Override
                public LogEntry parse( LogEntryVersion version, ReadableLogChannel channel, LogPositionMarker marker,
                        CommandReader commandReader ) throws IOException
                {
                    LogPosition position = marker.newPosition();
                    int masterId = channel.getInt();
                    int authorId = channel.getInt();
                    long timeWritten = channel.getLong();
                    long latestCommittedTxWhenStarted = channel.getLong();
                    int additionalHeaderLength = channel.getInt();
                    byte[] additionalHeader = new byte[additionalHeaderLength];
                    channel.get( additionalHeader, additionalHeaderLength );
                    return new LogEntryStart( version, masterId, authorId, timeWritten,
                            latestCommittedTxWhenStarted,
                            additionalHeader, position );
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.TX_START;
                }

                @Override
                public boolean skip()
                {
                    return false;
                }
            },

    COMMAND
            {
                @Override
                public LogEntry parse( LogEntryVersion version, ReadableLogChannel channel, LogPositionMarker marker,
                        CommandReader commandReader ) throws IOException
                {
                    Command command = commandReader.read( channel );
                    return command == null ? null : new LogEntryCommand( version, command );
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.COMMAND;
                }

                @Override
                public boolean skip()
                {
                    return false;
                }
            },

    TX_1P_COMMIT
            {
                @Override
                public LogEntry parse( LogEntryVersion version, ReadableLogChannel channel, LogPositionMarker marker,
                        CommandReader commandReader ) throws IOException
                {
                    long txId = channel.getLong();
                    long timeWritten = channel.getLong();
                    return new OnePhaseCommit( version, txId, timeWritten );
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.TX_1P_COMMIT;
                }

                @Override
                public boolean skip()
                {
                    return false;
                }
            },
    CHECK_POINT
            {
                @Override
                public LogEntry parse( LogEntryVersion version, ReadableLogChannel channel, LogPositionMarker marker,
                        CommandReader commandReader ) throws IOException
                {
                    long logVersion = channel.getLong();
                    long byteOffset = channel.getLong();
                    return new CheckPoint( version, new LogPosition( logVersion, byteOffset ) );
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.CHECK_POINT;
                }

                @Override
                public boolean skip()
                {
                    return false;
                }
            }
}
