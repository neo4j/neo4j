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
package org.neo4j.kernel.impl.transaction.log.files;

import java.io.File;
import java.io.IOException;

import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionMetaDataStore;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

/**
 * Provides methods for ensuring that transaction log files are properly initialised for a store.
 * This includes making sure that the log files are ready to be replicated in a cluster.
 */
public class TransactionLogInitializer
{
    private final FileSystemAbstraction fs;
    private final TransactionMetaDataStore store;

    /**
     * Get a {@link LogFilesInitializer} implementation, suitable for e.g. passing to a batch importer.
     * @return A {@link LogFilesInitializer} instance.
     */
    public static LogFilesInitializer getLogFilesInitializer()
    {
        return ( databaseLayout, store, fileSystem ) ->
        {
            try
            {
                TransactionLogInitializer initializer = new TransactionLogInitializer( fileSystem, store );
                initializer.initializeEmptyLogFile( databaseLayout, databaseLayout.getTransactionLogsDirectory() );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Fail to create empty transaction log file.", e );
            }
        };
    }

    public TransactionLogInitializer( FileSystemAbstraction fs, TransactionMetaDataStore store )
    {
        this.fs = fs;
        this.store = store;
    }

    /**
     * Create new empty log files in the given transaction logs directory, for a database that doesn't have any already.
     */
    public void initializeEmptyLogFile( DatabaseLayout layout, File transactionLogsDirectory ) throws IOException
    {
        try ( LogFilesSpan span = buildLogFiles( layout, transactionLogsDirectory ) )
        {
            LogFiles logFiles = span.getLogFiles();
            appendEmptyTransactionAndCheckPoint( logFiles );
        }
    }

    /**
     * Make sure that any existing log files in the given transaction logs directory are initialised.
     * This is done when we migrate 3.x stores into a 4.x world.
     */
    public void initializeExistingLogFiles( DatabaseLayout layout, File transactionLogsDirectory ) throws Exception
    {
        // If there are no transactions in any of the log files,
        // append an empty transaction, and a checkpoint, to the last log file.
        try ( LogFilesSpan span = buildLogFiles( layout, transactionLogsDirectory ) )
        {
            LogFiles logFiles = span.getLogFiles();
            LogHeader logHeader = logFiles.extractHeader( logFiles.getLowestLogVersion() );
            ReadableLogChannel readableChannel = logFiles.getLogFile().getReader( logHeader.getStartPosition() );
            try ( LogEntryCursor cursor = new LogEntryCursor( new VersionAwareLogEntryReader( false ), readableChannel ) )
            {
                while ( cursor.next() )
                {
                    LogEntry entry = cursor.get();
                    if ( entry.getType() == LogEntryByteCodes.TX_COMMIT )
                    {
                        // The log files already contain a transaction, so there is nothing for us to do.
                        return;
                    }
                }
            }

            appendEmptyTransactionAndCheckPoint( logFiles );
        }
    }

    private LogFilesSpan buildLogFiles( DatabaseLayout layout, File transactionLogsDirectory ) throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( layout, fs )
                                           .withLogVersionRepository( store )
                                           .withTransactionIdStore( store )
                                           .withStoreId( store.getStoreId() )
                                           .withLogsDirectory( transactionLogsDirectory )
                                           .build();
        return new LogFilesSpan( new Lifespan( logFiles ), logFiles );
    }

    private void appendEmptyTransactionAndCheckPoint( LogFiles logFiles ) throws IOException
    {
        TransactionId committedTx = store.getLastCommittedTransaction();
        long timestamp = committedTx.commitTimestamp();
        long transactionId = committedTx.transactionId();
        FlushablePositionAwareChecksumChannel writableChannel = logFiles.getLogFile().getWriter();
        LogEntryWriter writer = new LogEntryWriter( writableChannel );
        writer.writeStartEntry( timestamp, BASE_TX_ID, BASE_TX_CHECKSUM, EMPTY_BYTE_ARRAY );
        int checksum = writer.writeCommitEntry( transactionId, timestamp );
        LogPositionMarker marker = new LogPositionMarker();
        writableChannel.getCurrentPosition( marker );
        LogPosition position = marker.newPosition();
        writer.writeCheckPointEntry( position );
        store.setLastCommittedAndClosedTransactionId( transactionId, checksum, timestamp, position.getByteOffset(), position.getLogVersion() );
    }
}
