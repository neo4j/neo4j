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
package org.neo4j.kernel.impl.storemigration;

import org.apache.commons.lang3.mutable.MutableLong;

import java.io.File;
import java.io.IOException;

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
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

class TransactionLogsMigrator
{
    private final FileSystemAbstraction fs;
    private final TransactionIdStore idStore;
    private final StoreId storeId;
    private final LogVersionRepository logVersionRepository;

    TransactionLogsMigrator( FileSystemAbstraction fs, TransactionIdStore idStore, StoreId storeId,
                             LogVersionRepository logVersionRepository )
    {
        this.fs = fs;
        this.idStore = idStore;
        this.storeId = storeId;
        this.logVersionRepository = logVersionRepository;
    }

    void createEmptyLogFile( DatabaseLayout layout, File transactionLogsDirectory ) throws Exception
    {
        try ( Lifespan lifespan = buildLogFiles( layout, transactionLogsDirectory ) )
        {
            LogFiles logFiles = lifespan.unwrap( LogFiles.class );
            appendEmptyTransactionAndCheckPoint( logFiles );
        }
    }

    void migrateLogFile( DatabaseLayout layout, File transactionLogsDirectory ) throws Exception
    {
        // If there are no transactions in any of the log files,
        // append an empty transaction, and a checkpoint, to the last log file.
        try ( Lifespan lifespan = buildLogFiles( layout, transactionLogsDirectory ) )
        {
            LogFiles logFiles = lifespan.unwrap( LogFiles.class );
            MutableLong minVersion = new MutableLong( Long.MAX_VALUE );
            logFiles.accept( ( file, logVersion ) -> minVersion.setValue( Math.min( minVersion.longValue(), logVersion ) ) );
            LogHeader logHeader = logFiles.extractHeader( minVersion.longValue() );
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

    private Lifespan buildLogFiles( DatabaseLayout layout, File transactionLogsDirectory ) throws Exception
    {
        LogFiles logFiles = LogFilesBuilder.builder( layout, fs )
                                           .withLogVersionRepository( logVersionRepository )
                                           .withTransactionIdStore( idStore )
                                           .withStoreId( storeId )
                                           .withLogsDirectory( transactionLogsDirectory )
                                           .build();
        return new Lifespan( logFiles );
    }

    private void appendEmptyTransactionAndCheckPoint( LogFiles logFiles ) throws IOException
    {
        TransactionId committedTx = idStore.getLastCommittedTransaction();
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
        idStore.setLastCommittedAndClosedTransactionId( transactionId, checksum, timestamp, position.getByteOffset(), position.getLogVersion() );
    }
}
