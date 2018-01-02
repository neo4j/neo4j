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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.storemigration.FileOperation;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.allLegacyLogFilesFilter;
import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.getLegacyLogVersion;
import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.versionedLegacyLogFilesFilter;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.DEFAULT_NAME;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.DEFAULT_VERSION_SUFFIX;

public class LegacyLogs
{
    private final FileSystemAbstraction fs;
    private final LegacyLogEntryReader reader;
    private final LegacyLogEntryWriter writer;
    private final Comparator<File> NEWEST_FIRST = new Comparator<File>()
    {
        @Override
        public int compare( File o1, File o2 )
        {
            return versionOf( o1 ).compareTo( versionOf( o2 ) );
        }

        private Long versionOf( File file )
        {
            Pair<LogHeader, IOCursor<LogEntry>> pair = null;
            try
            {
                pair = reader.openReadableChannel( file );
                LogHeader header = pair.first();
                return Long.valueOf( header.logVersion );
            }
            catch ( IOException e )
            {   // Shouldn't happen
                throw new RuntimeException( e );
            }
            finally
            {
                if ( pair != null )
                {
                    try
                    {
                        pair.other().close();
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
        }
    };

    public LegacyLogs( FileSystemAbstraction fs )
    {
        this( fs, new LegacyLogEntryReader( fs ), new LegacyLogEntryWriter( fs ) );
    }

    LegacyLogs( FileSystemAbstraction fs, LegacyLogEntryReader reader, LegacyLogEntryWriter writer )
    {
        this.fs = fs;
        this.reader = reader;
        this.writer = writer;
    }

    public void migrateLogs( File storeDir, File migrationDir ) throws IOException
    {
        File[] logFiles = fs.listFiles( storeDir, versionedLegacyLogFilesFilter );
        for ( File file : logFiles )
        {
            final Pair<LogHeader, IOCursor<LogEntry>> pair = reader.openReadableChannel( file );
            final LogHeader header = pair.first();

            try ( IOCursor<LogEntry> cursor = pair.other();
                  LogVersionedStoreChannel channel =
                          writer.openWritableChannel( new File( migrationDir, file.getName() ) ) )
            {
                writer.writeLogHeader( channel, header );
                writer.writeAllLogEntries( channel, cursor );
            }
        }
    }

    public TransactionId getTransactionInformation( File storeDir, long transactionId ) throws IOException
    {
        List<File> logFiles = Arrays.asList( fs.listFiles( storeDir, versionedLegacyLogFilesFilter ) );
        Collections.sort( logFiles, NEWEST_FIRST );
        for ( File file : logFiles )
        {
            Pair<LogHeader, IOCursor<LogEntry>> pair = reader.openReadableChannel( file );
            boolean hadAnyTransactions = false;
            try ( IOCursor<LogEntry> cursor = pair.other() )
            {
                // The log entries will come sorted from this cursor, so no need to keep track of identifiers and such.
                LogEntryStart startEntry = null;
                while ( cursor.next() )
                {
                    LogEntry logEntry = cursor.get();
                    if ( logEntry instanceof LogEntryStart )
                    {
                        startEntry = (LogEntryStart) logEntry;
                    }
                    else if ( logEntry instanceof LogEntryCommit )
                    {
                        hadAnyTransactions = true;
                        LogEntryCommit commitEntry = logEntry.as();
                        if ( commitEntry.getTxId() == transactionId )
                        {
                            return new TransactionId( transactionId, startEntry.checksum(),
                                    commitEntry.getTimeWritten() );
                        }
                    }
                }
            }
            if ( hadAnyTransactions )
            {
                // No need to go further back than this. We're looking for the last transaction
                break;
            }
        }
        throw new NoSuchTransactionException( transactionId );
    }

    public void operate( FileOperation op, File from, File to ) throws IOException
    {
        File[] logFiles = fs.listFiles( from, versionedLegacyLogFilesFilter );
        for ( File file : logFiles )
        {
            op.perform( fs, file.getName(), from, false, to, true );
        }
    }

    public void renameLogFiles( File storeDir ) throws IOException
    {
        // rename files
        for ( File file : fs.listFiles( storeDir, versionedLegacyLogFilesFilter ) )
        {
            final String oldName = file.getName();
            final long version = getLegacyLogVersion( oldName );
            final String newName = DEFAULT_NAME + DEFAULT_VERSION_SUFFIX + version;
            fs.renameFile( file, new File( file.getParent(), newName ) );
        }

        deleteUnusedLogFiles( storeDir );
    }

    public void deleteUnusedLogFiles( File storeDir )
    {
        // delete old an unused log files
        for ( File file : fs.listFiles( storeDir, allLegacyLogFilesFilter ) )
        {
            fs.deleteFile( file );
        }
    }
}
