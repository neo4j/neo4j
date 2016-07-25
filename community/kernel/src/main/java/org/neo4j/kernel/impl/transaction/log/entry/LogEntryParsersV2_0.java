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

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart.EMPTY_ADDITIONAL_ARRAY;

// 2.0
public enum LogEntryParsersV2_0 implements LogEntryParser<IdentifiableLogEntry>
{
    EMPTY
            {
                @Override
                public IdentifiableLogEntry parse( LogEntryVersion version, ReadableClosableChannel channel,
                        LogPositionMarker marker, CommandReaderFactory commandReader ) throws IOException

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

    TX_PREPARE
            {
                @Override
                public IdentifiableLogEntry parse( LogEntryVersion version, ReadableClosableChannel channel,
                        LogPositionMarker marker, CommandReaderFactory commandReader ) throws IOException

                {
                    // we ignore this we do not this in the new log format, just parse data to be skipped in the
                    // channel
                    // ignored identifier
                    channel.getInt();
                    // ignored timeWritten
                    channel.getLong();
                    return null;
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.TX_PREPARE;
                }

                @Override
                public boolean skip()
                {
                    return true;
                }
            },
    TX_START
            {
                @Override
                public IdentifiableLogEntry parse( LogEntryVersion version, ReadableClosableChannel channel,
                        LogPositionMarker marker, CommandReaderFactory commandReader ) throws IOException

                {
                    LogPosition position = marker.newPosition();
                    byte globalIdLength = channel.get();
                    byte branchIdLength = channel.get();
                    // ignored globalId
                    channel.get( new byte[globalIdLength], globalIdLength );
                    // ignored branchId
                    channel.get( new byte[branchIdLength], branchIdLength );
                    int identifier = channel.getInt();
                    // ignored formatId
                    channel.getInt();

                    int masterId = channel.getInt();
                    int authorId = channel.getInt();
                    long timeWritten = channel.getLong();
                    long latestCommittedTxWhenStarted = channel.getLong();

                    return new IdentifiableLogEntry(
                            new LogEntryStart( masterId, authorId, timeWritten, latestCommittedTxWhenStarted,
                                    EMPTY_ADDITIONAL_ARRAY, position ),
                            identifier );
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
                public IdentifiableLogEntry parse( LogEntryVersion version, ReadableClosableChannel channel,
                        LogPositionMarker marker, CommandReaderFactory commandReader ) throws IOException

                {
                    int identifier = channel.getInt();
                    StorageCommand command = commandReader.byVersion( version.byteCode() ).read( channel );
                    return command == null
                            ? null
                            : new IdentifiableLogEntry( new LogEntryCommand( version, command ), identifier );
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
    DONE
            {
                @Override
                public IdentifiableLogEntry parse( LogEntryVersion version, ReadableClosableChannel channel,
                        LogPositionMarker marker, CommandReaderFactory commandReader ) throws IOException

                {
                    // we ignore this we do not this in the new log format, just parse data to be skipped in the
                    // channel
                    // ignored identifier
                    channel.getInt();
                    return null;
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.DONE;
                }

                @Override
                public boolean skip()
                {
                    return true;
                }
            },
    TX_1P_COMMIT
            {
                @Override
                public IdentifiableLogEntry parse( LogEntryVersion version, ReadableClosableChannel channel,
                        LogPositionMarker marker, CommandReaderFactory commandReader ) throws IOException

                {
                    int identifier = channel.getInt();
                    long txId = channel.getLong();
                    long timeWritten = channel.getLong();
                    return new IdentifiableLogEntry( new OnePhaseCommit( txId, timeWritten ), identifier );
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
    TX_2P_COMMIT
            {
                @Override
                public IdentifiableLogEntry parse( LogEntryVersion version, ReadableClosableChannel channel,
                        LogPositionMarker marker, CommandReaderFactory commandReader ) throws IOException

                {
                    int identifier = channel.getInt();
                    long txId = channel.getLong();
                    long timeWritten = channel.getLong();

                    // let's map the 2 phase commit into 1 phase commit since the 2 phase commits are gone
                    return new IdentifiableLogEntry( new OnePhaseCommit( txId, timeWritten ), identifier );
                }

                @Override
                public byte byteCode()
                {
                    return LogEntryByteCodes.TX_2P_COMMIT;
                }

                @Override
                public boolean skip()
                {
                    return false;
                }
            }

}
