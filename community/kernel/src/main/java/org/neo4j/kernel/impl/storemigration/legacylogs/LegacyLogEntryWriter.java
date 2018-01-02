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
package org.neo4j.kernel.impl.storemigration.legacylogs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.function.Function;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalWritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter;

import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.getLegacyLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

class LegacyLogEntryWriter
{
    private static final Function<WritableLogChannel, LogEntryWriter> defaultLogEntryWriterFactory =
            new Function<WritableLogChannel, LogEntryWriter>()
            {
                @Override
                public LogEntryWriter apply( WritableLogChannel channel )
                {
                    return new LogEntryWriter( channel, new CommandWriter( channel ) );
                }
            };

    private final FileSystemAbstraction fs;
    private final Function<WritableLogChannel, LogEntryWriter> factory;

    LegacyLogEntryWriter( FileSystemAbstraction fs )
    {
        this( fs, defaultLogEntryWriterFactory );
    }

    LegacyLogEntryWriter( FileSystemAbstraction fs, Function<WritableLogChannel, LogEntryWriter> factory )
    {
        this.fs = fs;
        this.factory = factory;
    }

    public LogVersionedStoreChannel openWritableChannel( File file ) throws IOException
    {
        final StoreChannel storeChannel = fs.open( file, "rw" );
        final long version = getLegacyLogVersion( file.getName() );
        return new PhysicalLogVersionedStoreChannel( storeChannel, version, CURRENT_LOG_VERSION );
    }

    public void writeLogHeader( LogVersionedStoreChannel channel, LogHeader header ) throws IOException
    {
        LogHeaderWriter.writeLogHeader( channel, header.logVersion, header.lastCommittedTxId );
    }

    public void writeAllLogEntries( LogVersionedStoreChannel channel, IOCursor<LogEntry> cursor ) throws IOException
    {
        try ( PhysicalWritableLogChannel writable = new PhysicalWritableLogChannel( channel ) )
        {
            final LogEntryWriter writer = factory.apply( writable );
            List<Command> commands = new ArrayList<>();
            while ( cursor.next() )
            {
                LogEntry entry = cursor.get();
                if ( entry instanceof LogEntryStart )
                {
                    final LogEntryStart startEntry = entry.as();
                    writer.writeStartEntry( startEntry.getMasterId(), startEntry.getLocalId(),
                            startEntry.getTimeWritten(), startEntry.getLastCommittedTxWhenTransactionStarted(),
                            startEntry.getAdditionalHeader() );
                }
                else if ( entry instanceof LogEntryCommit )
                {
                    if ( !commands.isEmpty() )
                    {
                        writer.serialize( new PhysicalTransactionRepresentation( commands ) );
                        commands = new ArrayList<>();
                    }

                    final LogEntryCommit commitEntry = (LogEntryCommit) entry;
                    writer.writeCommitEntry( commitEntry.getTxId(), commitEntry.getTimeWritten() );
                }
                else if ( entry instanceof LogEntryCommand )
                {
                    commands.add( ((LogEntryCommand) entry).getXaCommand() );
                }
                else
                {
                    throw new IllegalStateException( "Unknown entry: " + entry );
                }
            }
        }
    }
}
