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
package org.neo4j.kernel.recovery;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.neo4j.common.ProgressReporter;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PositionAwarePhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;
import static org.neo4j.kernel.recovery.RecoveryStartInformation.NO_RECOVERY_REQUIRED;
import static org.neo4j.kernel.recovery.RecoveryStartInformationProvider.NO_MONITOR;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

@TestDirectoryExtension
class TransactionLogsRecoveryTest
{
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory directory;
    private final LogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore( 5L, 0,
            BASE_TX_COMMIT_TIMESTAMP, 0, 0 );
    private final int logVersion = 0;

    private LogEntry lastCommittedTxStartEntry;
    private LogEntry lastCommittedTxCommitEntry;
    private LogEntry expectedStartEntry;
    private LogEntry expectedCommitEntry;
    private Monitors monitors = new Monitors();
    private final SimpleLogVersionRepository versionRepository = new SimpleLogVersionRepository();
    private LogFiles logFiles;
    private File storeDir;
    private Lifecycle schemaLife;

    @BeforeEach
    void setUp() throws Exception
    {
        storeDir = directory.storeDir();
        DatabaseLayout databaseLayout = directory.databaseLayout();
        logFiles = LogFilesBuilder.builder( databaseLayout, fileSystem )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( transactionIdStore )
                .withLogEntryReader( logEntryReader() )
                .build();
        schemaLife = new LifecycleAdapter();
    }

    @Test
    void shouldRecoverExistingData() throws Exception
    {
        File file = logFiles.getLogFileForVersion( logVersion );

        writeSomeData( file, pair ->
        {
            LogEntryWriter writer = pair.first();
            Consumer<LogPositionMarker> consumer = pair.other();
            LogPositionMarker marker = new LogPositionMarker();

            // last committed tx
            consumer.accept( marker );
            LogPosition lastCommittedTxPosition = marker.newPosition();
            writer.writeStartEntry( 0, 1, 2L, 3L, new byte[0] );
            lastCommittedTxStartEntry = new LogEntryStart( 0, 1, 2L, 3L, new byte[0], lastCommittedTxPosition );
            writer.writeCommitEntry( 4L, 5L );
            lastCommittedTxCommitEntry = new LogEntryCommit( 4L, 5L );

            // check point pointing to the previously committed transaction
            writer.writeCheckPointEntry( lastCommittedTxPosition );

            // tx committed after checkpoint
            consumer.accept( marker );
            writer.writeStartEntry( 0, 1, 6L, 4L, new byte[0] );
            expectedStartEntry = new LogEntryStart( 0, 1, 6L, 4L, new byte[0], marker.newPosition() );

            writer.writeCommitEntry( 5L, 7L );
            expectedCommitEntry = new LogEntryCommit( 5L, 7L );

            return true;
        } );

        LifeSupport life = new LifeSupport();
        RecoveryMonitor monitor = mock( RecoveryMonitor.class );
        final AtomicBoolean recoveryRequired = new AtomicBoolean();
        try
        {
            StorageEngine storageEngine = mock( StorageEngine.class );
            final LogEntryReader<ReadableClosablePositionAwareChannel> reader = logEntryReader();
            LogTailScanner tailScanner = getTailScanner( logFiles, reader );

            TransactionMetadataCache metadataCache = new TransactionMetadataCache();
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFiles, metadataCache, reader,
                    monitors, false );
            CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator( storeDir, logFiles, fileSystem );
            life.add( new TransactionLogsRecovery( new DefaultRecoveryService( storageEngine, tailScanner, transactionIdStore,
                    txStore, versionRepository, NO_MONITOR, mock( Log.class ) )
            {
                private int nr;

                @Override
                public void startRecovery()
                {
                    recoveryRequired.set( true );
                }

                @Override
                public RecoveryApplier getRecoveryApplier( TransactionApplicationMode mode )
                        throws Exception
                {
                    RecoveryApplier actual = super.getRecoveryApplier( mode );
                    if ( mode == TransactionApplicationMode.REVERSE_RECOVERY )
                    {
                        return actual;
                    }

                    return new RecoveryApplier()
                    {
                        @Override
                        public void close() throws Exception
                        {
                            actual.close();
                        }

                        @Override
                        public boolean visit( CommittedTransactionRepresentation tx ) throws Exception
                        {
                            actual.visit( tx );
                            switch ( nr++ )
                            {
                            case 0:
                                assertEquals( lastCommittedTxStartEntry, tx.getStartEntry() );
                                assertEquals( lastCommittedTxCommitEntry, tx.getCommitEntry() );
                                break;
                            case 1:
                                assertEquals( expectedStartEntry, tx.getStartEntry() );
                                assertEquals( expectedCommitEntry, tx.getCommitEntry() );
                                break;
                            default:
                                fail( "Too many recovered transactions" );
                            }
                            return false;
                        }
                    };
                }
            }, logPruner, schemaLife, monitor, ProgressReporter.SILENT, false ) );

            life.start();

            InOrder order = inOrder( monitor );
            order.verify( monitor ).recoveryRequired( any( LogPosition.class ) );
            order.verify( monitor ).recoveryCompleted( eq( 2 ), anyLong() );
            assertTrue( recoveryRequired.get() );
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    void shouldSeeThatACleanDatabaseShouldNotRequireRecovery() throws Exception
    {
        File file = logFiles.getLogFileForVersion( logVersion );

        writeSomeData( file, pair ->
        {
            LogEntryWriter writer = pair.first();
            Consumer<LogPositionMarker> consumer = pair.other();
            LogPositionMarker marker = new LogPositionMarker();

            // last committed tx
            consumer.accept( marker );
            writer.writeStartEntry( 0, 1, 2L, 3L, new byte[0] );
            writer.writeCommitEntry( 4L, 5L );

            // check point
            consumer.accept( marker );
            writer.writeCheckPointEntry( marker.newPosition() );

            return true;
        } );

        LifeSupport life = new LifeSupport();
        RecoveryMonitor monitor = mock( RecoveryMonitor.class );
        try
        {
            StorageEngine storageEngine = mock( StorageEngine.class );
            final LogEntryReader<ReadableClosablePositionAwareChannel> reader = logEntryReader();
            LogTailScanner tailScanner = getTailScanner( logFiles, reader );

            TransactionMetadataCache metadataCache = new TransactionMetadataCache();
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFiles, metadataCache, reader,
                    monitors, false );
            CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator( storeDir, logFiles, fileSystem );
            life.add( new TransactionLogsRecovery( new DefaultRecoveryService( storageEngine, tailScanner, transactionIdStore,
                    txStore, versionRepository, NO_MONITOR, mock( Log.class ) )
            {
                @Override
                public void startRecovery()
                {
                    fail( "Recovery should not be required" );
                }
            }, logPruner, schemaLife, monitor, ProgressReporter.SILENT, false ) );

            life.start();

            verifyZeroInteractions( monitor );
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    void shouldTruncateLogAfterSinglePartialTransaction() throws Exception
    {
        // GIVEN
        File file = logFiles.getLogFileForVersion( logVersion );
        final LogPositionMarker marker = new LogPositionMarker();

        writeSomeData( file, pair ->
        {
            LogEntryWriter writer = pair.first();
            Consumer<LogPositionMarker> consumer = pair.other();

            // incomplete tx
            consumer.accept( marker ); // <-- marker has the last good position
            writer.writeStartEntry( 0, 1, 5L, 4L, new byte[0] );

            return true;
        } );

        // WHEN
        boolean recoveryRequired = recover( storeDir, logFiles );

        // THEN
        assertTrue( recoveryRequired );
        assertEquals( marker.getByteOffset(), file.length() );
    }

    @Test
    void doNotTruncateCheckpointsAfterLastTransaction() throws IOException
    {
        File file = logFiles.getLogFileForVersion( logVersion );
        LogPositionMarker marker = new LogPositionMarker();
        writeSomeData( file, pair ->
        {
            LogEntryWriter writer = pair.first();
            writer.writeStartEntry( 1, 1, 1L, 1L, ArrayUtils.EMPTY_BYTE_ARRAY );
            writer.writeCommitEntry( 1L, 2L );
            writer.writeCheckPointEntry( new LogPosition( logVersion, LogHeader.LOG_HEADER_SIZE ) );
            writer.writeCheckPointEntry( new LogPosition( logVersion, LogHeader.LOG_HEADER_SIZE ) );
            writer.writeCheckPointEntry( new LogPosition( logVersion, LogHeader.LOG_HEADER_SIZE ) );
            writer.writeCheckPointEntry( new LogPosition( logVersion, LogHeader.LOG_HEADER_SIZE ) );
            Consumer<LogPositionMarker> other = pair.other();
            other.accept( marker );
            return true;
        } );
        assertTrue( recover( storeDir, logFiles ) );

        assertEquals( marker.getByteOffset(), file.length() );
    }

    @Test
    void shouldTruncateLogAfterLastCompleteTransactionAfterSuccessfulRecovery() throws Exception
    {
        // GIVEN
        File file = logFiles.getLogFileForVersion( logVersion );
        final LogPositionMarker marker = new LogPositionMarker();

        writeSomeData( file, pair ->
        {
            LogEntryWriter writer = pair.first();
            Consumer<LogPositionMarker> consumer = pair.other();

            // last committed tx
            writer.writeStartEntry( 0, 1, 2L, 3L, new byte[0] );
            writer.writeCommitEntry( 4L, 5L );

            // incomplete tx
            consumer.accept( marker ); // <-- marker has the last good position
            writer.writeStartEntry( 0, 1, 5L, 4L, new byte[0] );

            return true;
        } );

        // WHEN
        boolean recoveryRequired = recover( storeDir, logFiles );

        // THEN
        assertTrue( recoveryRequired );
        assertEquals( marker.getByteOffset(), file.length() );
    }

    @Test
    void shouldTellTransactionIdStoreAfterSuccessfulRecovery() throws Exception
    {
        // GIVEN
        File file = logFiles.getLogFileForVersion( logVersion );
        final LogPositionMarker marker = new LogPositionMarker();

        final byte[] additionalHeaderData = new byte[0];
        final int masterId = 0;
        final int authorId = 1;
        final long transactionId = 4;
        final long commitTimestamp = 5;
        writeSomeData( file, pair ->
        {
            LogEntryWriter writer = pair.first();
            Consumer<LogPositionMarker> consumer = pair.other();

            // last committed tx
            writer.writeStartEntry( masterId, authorId, 2L, 3L, additionalHeaderData );
            writer.writeCommitEntry( transactionId, commitTimestamp );
            consumer.accept( marker );

            return true;
        } );

        // WHEN
        boolean recoveryRequired = recover( storeDir, logFiles );

        // THEN
        assertTrue( recoveryRequired );
        long[] lastClosedTransaction = transactionIdStore.getLastClosedTransaction();
        assertEquals( transactionId, lastClosedTransaction[0] );
        assertEquals( LogEntryStart.checksum( additionalHeaderData, masterId, authorId ),
                transactionIdStore.getLastCommittedTransaction().checksum() );
        assertEquals( commitTimestamp, transactionIdStore.getLastCommittedTransaction().commitTimestamp() );
        assertEquals( logVersion, lastClosedTransaction[1] );
        assertEquals( marker.getByteOffset(), lastClosedTransaction[2] );
    }

    @Test
    void shouldInitSchemaLifeWhenRecoveryNotRequired() throws Exception
    {
        Lifecycle schemaLife = mock( Lifecycle.class );

        RecoveryService recoveryService = mock( RecoveryService.class );
        when( recoveryService.getRecoveryStartInformation() ).thenReturn( NO_RECOVERY_REQUIRED );

        CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator( storeDir, logFiles, fileSystem );
        RecoveryMonitor monitor = mock( RecoveryMonitor.class );

        TransactionLogsRecovery logsRecovery = new TransactionLogsRecovery( recoveryService, logPruner, schemaLife, monitor, ProgressReporter.SILENT, true );

        logsRecovery.init();

        verify( monitor, never() ).recoveryRequired( any() );
        verify( schemaLife ).init();
    }

    private boolean recover( File storeDir, LogFiles logFiles )
    {
        LifeSupport life = new LifeSupport();
        RecoveryMonitor monitor = mock( RecoveryMonitor.class );
        final AtomicBoolean recoveryRequired = new AtomicBoolean();
        try
        {
            StorageEngine storageEngine = mock( StorageEngine.class );
            final LogEntryReader<ReadableClosablePositionAwareChannel> reader = logEntryReader();
            LogTailScanner tailScanner = getTailScanner( logFiles, reader );

            TransactionMetadataCache metadataCache = new TransactionMetadataCache();
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFiles, metadataCache, reader, monitors, false );
            CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator( storeDir, logFiles, fileSystem );
            life.add( new TransactionLogsRecovery( new DefaultRecoveryService( storageEngine, tailScanner, transactionIdStore,
                    txStore, versionRepository, NO_MONITOR, mock( Log.class ) )
            {
                @Override
                public void startRecovery()
                {
                    recoveryRequired.set( true );
                }
            }, logPruner, schemaLife, monitor, ProgressReporter.SILENT, false ) );

            life.start();
        }
        finally
        {
            life.shutdown();
        }
        return recoveryRequired.get();
    }

    private LogTailScanner getTailScanner( LogFiles logFiles,
            LogEntryReader<ReadableClosablePositionAwareChannel> reader )
    {
        return new LogTailScanner( logFiles, reader, monitors, false );
    }

    private void writeSomeData( File file, Visitor<Pair<LogEntryWriter,Consumer<LogPositionMarker>>,IOException> visitor ) throws IOException
    {

        try ( LogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel( fileSystem.write( file ), logVersion,
                CURRENT_LOG_VERSION, file );
              PositionAwarePhysicalFlushableChannel writableLogChannel = new PositionAwarePhysicalFlushableChannel( versionedStoreChannel,
                        ByteBuffer.allocate( 1024 ) ) )
        {
            writeLogHeader( writableLogChannel, logVersion, 2L );
            Consumer<LogPositionMarker> consumer = marker ->
            {
                try
                {
                    writableLogChannel.getCurrentPosition( marker );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            };
            LogEntryWriter first = new LogEntryWriter( writableLogChannel );
            visitor.visit( Pair.of( first, consumer ) );
        }
    }
}
