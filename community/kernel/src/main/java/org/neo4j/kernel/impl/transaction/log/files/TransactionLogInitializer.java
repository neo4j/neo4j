/*
 * Copyright (c) "Neo4j"
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

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;

import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.PanicEventGenerator;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionId;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.neo4j.internal.kernel.api.security.AuthSubject.ANONYMOUS;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

/**
 * Provides methods for ensuring that transaction log files are properly initialised for a store.
 * This includes making sure that the log files are ready to be replicated in a cluster.
 */
public class TransactionLogInitializer
{
    private final FileSystemAbstraction fs;
    private final MetadataProvider store;
    private final StorageEngineFactory storageEngineFactory;

    /**
     * Get a {@link LogFilesInitializer} implementation, suitable for e.g. passing to a batch importer.
     * @return A {@link LogFilesInitializer} instance.
     */
    public static LogFilesInitializer getLogFilesInitializer()
    {
        return ( databaseLayout, store, fileSystem, checkpointReason ) ->
        {
            try
            {
                TransactionLogInitializer initializer = new TransactionLogInitializer(
                        fileSystem, store, StorageEngineFactory.defaultStorageEngine() );
                initializer.initializeEmptyLogFile( databaseLayout, databaseLayout.getTransactionLogsDirectory(), checkpointReason );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Fail to create empty transaction log file.", e );
            }
        };
    }

    public TransactionLogInitializer( FileSystemAbstraction fs, MetadataProvider store, StorageEngineFactory storageEngineFactory )
    {
        this.fs = fs;
        this.store = store;
        this.storageEngineFactory = storageEngineFactory;
    }

    /**
     * Create new empty log files in the given transaction logs directory, for a database that doesn't have any already.
     */
    public void initializeEmptyLogFile( DatabaseLayout layout, Path transactionLogsDirectory, String checkpointReason )
            throws IOException
    {
        try ( LogFilesSpan span = buildLogFiles( layout, transactionLogsDirectory) )
        {
            LogFiles logFiles = span.getLogFiles();
            appendEmptyTransactionAndCheckPoint( logFiles, checkpointReason );
        }
    }

    public void upgradeExistingLogFiles( DatabaseLayout layout, Path transactionLogsDirectory, String checkpointReason )
            throws Exception
    {
        try ( LogFilesSpan span = buildLogFiles( layout, transactionLogsDirectory ) )
        {
            LogFiles logFiles = span.getLogFiles();
            LogFile logFile = logFiles.getLogFile();
            for ( long version = logFile.getLowestLogVersion(); version <= logFile.getHighestLogVersion() ; version++ )
            {
                fs.deleteFile( logFile.getLogFileForVersion( version ) );
            }
            CheckpointFile checkpointFile = logFiles.getCheckpointFile();
            for ( long version = checkpointFile.getLowestLogVersion(); version <= checkpointFile.getHighestLogVersion() ; version++ )
            {
                fs.deleteFile( checkpointFile.getDetachedCheckpointFileForVersion( version ) );
            }
            logFile.rotate();
            checkpointFile.rotate();
            appendEmptyTransactionAndCheckPoint( logFiles, checkpointReason );
        }
    }

    private LogFilesSpan buildLogFiles( DatabaseLayout layout, Path transactionLogsDirectory ) throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( layout, fs )
                                           .withLogVersionRepository( store )
                                           .withTransactionIdStore( store )
                                           .withStoreId( store.getStoreId() )
                                           .withLogsDirectory( transactionLogsDirectory )
                                           .withStorageEngineFactory( storageEngineFactory )
                                           .withDatabaseHealth( new DatabaseHealth( PanicEventGenerator.NO_OP, NullLog.getInstance() ) )
                                           .build();
        return new LogFilesSpan( new Lifespan( logFiles ), logFiles );
    }

    private void appendEmptyTransactionAndCheckPoint( LogFiles logFiles, String reason ) throws IOException
    {
        TransactionId committedTx = store.getLastCommittedTransaction();
        long timestamp = committedTx.commitTimestamp();
        long upgradeTransactionId = committedTx.transactionId() + 1;
        LogFile logFile = logFiles.getLogFile();
        TransactionLogWriter transactionLogWriter = logFile.getTransactionLogWriter();
        PhysicalTransactionRepresentation emptyTx = emptyTransaction( timestamp, upgradeTransactionId );
        int checksum = transactionLogWriter.append( emptyTx, upgradeTransactionId, BASE_TX_CHECKSUM );
        logFile.forceAfterAppend( LogAppendEvent.NULL );
        LogPosition position = transactionLogWriter.getCurrentPosition();
        appendCheckpoint( logFiles, reason, position, new TransactionId( upgradeTransactionId, checksum, timestamp ) );
    }

    private static PhysicalTransactionRepresentation emptyTransaction( long timestamp, long txId )
    {
        return new PhysicalTransactionRepresentation( Collections.emptyList(), EMPTY_BYTE_ARRAY, timestamp, txId, timestamp, NO_LEASE, ANONYMOUS );
    }

    private static void appendCheckpoint( LogFiles logFiles, String reason, LogPosition position, TransactionId transactionId ) throws IOException
    {
        var checkpointAppender = logFiles.getCheckpointFile().getCheckpointAppender();
        checkpointAppender.checkPoint( LogCheckPointEvent.NULL, transactionId, position,
                Instant.now(), reason );
    }
}
