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
package org.neo4j.kernel.impl.transaction.xaframework.log.entry;

import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.xa.CommandReader;
import org.neo4j.kernel.impl.nioneo.xa.CommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.transaction.xaframework.LogPosition;
import org.neo4j.kernel.impl.transaction.xaframework.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;

// 2.1
public class LogEntryParserDispatcherV4 implements LogEntryParserDispatcher
{
    @Override
    public LogEntryParser dispatch( byte type )
    {
        switch ( type )
        {
            case LogEntryByteCodeV11111110.EMPTY:
                return LogEntryParsersV4.EMPTY;
            case LogEntryByteCodeV11111110.TX_PREPARE:
                return LogEntryParsersV4.TX_PREPARE;
            case LogEntryByteCodeV11111110.TX_START:
                return LogEntryParsersV4.TX_START;
            case LogEntryByteCodeV11111110.COMMAND:
                return LogEntryParsersV4.COMMAND;
            case LogEntryByteCodeV11111110.DONE:
                return LogEntryParsersV4.DONE;
            case LogEntryByteCodeV11111110.TX_1P_COMMIT:
                return LogEntryParsersV4.TX_1P_COMMIT;
            case LogEntryByteCodeV11111110.TX_2P_COMMIT:
                return LogEntryParsersV4.TX_2P_COMMIT;
            default:
                return null;

        }
    }

    private static enum LogEntryParsersV4 implements LogEntryParser
    {
        EMPTY
                {
                    @Override
                    public LogEntry parse( byte version, ReadableLogChannel channel, LogPositionMarker marker,
                                           CommandReaderFactory commandReaderFactory ) throws IOException
                    {
                        return null;

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
                    public LogEntry parse( byte version, ReadableLogChannel channel, LogPositionMarker marker,
                                           CommandReaderFactory commandReaderFactory ) throws IOException
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
                    public boolean skip()
                    {
                        return true;
                    }
                },
        TX_START
                {
                    @Override
                    public LogEntry parse( byte version, ReadableLogChannel channel, LogPositionMarker marker,
                                           CommandReaderFactory commandReaderFactory ) throws IOException
                    {
                        LogPosition position = marker.newPosition();
                        byte globalIdLength = channel.get();
                        byte branchIdLength = channel.get();
                        // ignored globalId
                        channel.get( new byte[globalIdLength], globalIdLength );
                        // ignored branchId
                        channel.get( new byte[branchIdLength], branchIdLength );
                        // ignored identifier
                        channel.getInt();
                        // ignored formatId
                        channel.getInt();

                        int masterId = channel.getInt();
                        int authorId = channel.getInt();
                        long timeWritten = channel.getLong();
                        long latestCommittedTxWhenStarted = channel.getLong();

                        return new LogEntryStart( masterId, authorId, timeWritten,
                                latestCommittedTxWhenStarted, new byte[]{}, position );
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
                    public LogEntry parse( byte version, ReadableLogChannel channel, LogPositionMarker marker,
                                           CommandReaderFactory commandReaderFactory ) throws IOException
                    {
                        // ignore identifier
                        channel.getInt();
                        CommandReader commandReader = commandReaderFactory.newInstance( version );
                        Command command = commandReader.read( channel );
                        return command == null ? null : new LogEntryCommand( version, command );
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
                    public LogEntry parse( byte version, ReadableLogChannel channel, LogPositionMarker marker,
                                           CommandReaderFactory commandReaderFactory ) throws IOException
                    {
                        // we ignore this we do not this in the new log format, just parse data to be skipped in the
                        // channel
                        // ignored identifier
                        channel.getInt();
                        return null;
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
                    public LogEntry parse( byte version, ReadableLogChannel channel, LogPositionMarker marker,
                                           CommandReaderFactory commandReaderFactory ) throws IOException
                    {
                        // ignore identifier
                        channel.getInt();
                        long txId = channel.getLong();
                        long timeWritten = channel.getLong();
                        return new OnePhaseCommit( txId, timeWritten );
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
                    public LogEntry parse( byte version, ReadableLogChannel channel, LogPositionMarker marker,
                                           CommandReaderFactory commandReaderFactory ) throws IOException
                    {
                        // ignore identifier
                        channel.getInt();
                        long txId = channel.getLong();
                        long timeWritten = channel.getLong();

                        // let's map the 2 phase commit into 1 phase commit since the 2 phase commits are gone
                        return new OnePhaseCommit( txId, timeWritten );
                    }

                    @Override
                    public boolean skip()
                    {
                        return false;
                    }
                }
    }
}
