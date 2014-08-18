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

// 2.2
public class LogEntryParserDispatcherV5 implements LogEntryParserDispatcher
{
    @Override
    public LogEntryParser dispatch( byte type )
    {
        switch ( type )
        {
            case LogEntryByteCodeV11111110.EMPTY:
                return LogEntryParsersV5.EMPTY;
            case LogEntryByteCodeV11111110.TX_START:
                return LogEntryParsersV5.TX_START;
            case LogEntryByteCodeV11111110.COMMAND:
                return LogEntryParsersV5.COMMAND;
            case LogEntryByteCodeV11111110.TX_1P_COMMIT:
                return LogEntryParsersV5.TX_1P_COMMIT;
            default:
                return null;

        }
    }

    private static enum LogEntryParsersV5 implements LogEntryParser
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

        TX_START
                {
                    @Override
                    public LogEntry parse( byte version, ReadableLogChannel channel, LogPositionMarker marker,
                                           CommandReaderFactory commandReaderFactory ) throws IOException
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

        TX_1P_COMMIT
                {
                    @Override
                    public LogEntry parse( byte version, ReadableLogChannel channel, LogPositionMarker marker,
                                           CommandReaderFactory commandReaderFactory ) throws IOException
                    {
                        long txId = channel.getLong();
                        long timeWritten = channel.getLong();
                        return new OnePhaseCommit( version, txId, timeWritten );
                    }

                    @Override
                    public boolean skip()
                    {
                        return false;
                    }
                };
    }

}
