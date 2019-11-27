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
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.neo4j.common.ProgressReporter;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PositionAwarePhysicalFlushableChecksumChannel;
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
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.UUID.randomUUID;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.io.ByteUnit.KibiByte;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_FORMAT_VERSION;
import static org.neo4j.kernel.recovery.RecoveryStartInformation.NO_RECOVERY_REQUIRED;
import static org.neo4j.kernel.recovery.RecoveryStartInformationProvider.NO_MONITOR;
import static org.neo4j.kernel.recovery.RecoveryStartupChecker.EMPTY_CHECKER;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

@Neo4jLayoutExtension
class TransactionLogsRecoveryTest
{
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private TestDirectory testDirectory;
    private final LogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore( 5L, 0,
            BASE_TX_COMMIT_TIMESTAMP, 0, 0 );
    private final int logVersion = 0;

    private LogEntry lastCommittedTxStartEntry;
    private LogEntry lastCommittedTxCommitEntry;
    private LogEntry expectedStartEntry;
    private LogEntry expectedCommitEntry;
    private final Monitors monitors = new Monitors();
    private final SimpleLogVersionRepository versionRepository = new SimpleLogVersionRepository();
    private LogFiles logFiles;
    private File storeDir;
    private Lifecycle schemaLife;

    @BeforeEach
    void setUp() throws Exception
    {
        storeDir = testDirectory.homeDir();
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
            int previousChecksum = BASE_TX_CHECKSUM;
            consumer.accept( marker );
            LogPosition lastCommittedTxPosition = marker.newPosition();
            writer.writeStartEntry( 2L, 3L, previousChecksum, new byte[0] );
            lastCommittedTxStartEntry = new LogEntryStart( 2L, 3L, previousChecksum, new byte[0], lastCommittedTxPosition );
            previousChecksum = writer.writeCommitEntry( 4L, 5L );
            lastCommittedTxCommitEntry = new LogEntryCommit( 4L, 5L, previousChecksum );

            // check point pointing to the previously committed transaction
            writer.writeCheckPointEntry( lastCommittedTxPosition );

            // tx committed after checkpoint
            consumer.accept( marker );
            writer.writeStartEntry( 6L, 4L, previousChecksum, new byte[0] );
            expectedStartEntry = new LogEntryStart( 6L, 4L, previousChecksum, new byte[0], marker.newPosition() );

            previousChecksum = writer.writeCommitEntry( 5L, 7L );
            expectedCommitEntry = new LogEntryCommit( 5L, 7L, previousChecksum );

            return true;
        } );

        LifeSupport life = new LifeSupport();
        var recoveryRequired = new AtomicBoolean();
        var recoveredTransactions = new MutableInt();
        RecoveryMonitor monitor = new RecoveryMonitor()
        {
            @Override
            public void recoveryRequired( LogPosition recoveryPosition )
            {
                recoveryRequired.set( true );
            }

            @Override
            public void recoveryCompleted( int numberOfRecoveredTransactions, long recoveryTimeInMilliseconds )
            {
                recoveredTransactions.setValue( numberOfRecoveredTransactions );
            }
        };
        try
        {
            StorageEngine storageEngine = mock( StorageEngine.class );
            final LogEntryReader reader = logEntryReader();
            LogTailScanner tailScanner = getTailScanner( logFiles, reader );

            TransactionMetadataCache metadataCache = new TransactionMetadataCache();
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFiles, metadataCache, reader,
                    monitors, false );
            CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator( storeDir, logFiles, fileSystem );
            monitors.addMonitorListener( monitor );
            life.add( new TransactionLogsRecovery( new DefaultRecoveryService( storageEngine, tailScanner, transactionIdStore,
                    txStore, versionRepository, logFiles, NO_MONITOR, mock( Log.class ) )
            {
                private int nr;

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
            }, logPruner, schemaLife, monitor, ProgressReporter.SILENT, false, EMPTY_CHECKER ) );

            life.start();

            assertTrue( recoveryRequired.get() );
            assertEquals( 2, recoveredTransactions.getValue() );
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
            writer.writeStartEntry( 2L, 3L, BASE_TX_CHECKSUM, new byte[0] );
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
            final LogEntryReader reader = logEntryReader();
            LogTailScanner tailScanner = getTailScanner( logFiles, reader );

            TransactionMetadataCache metadataCache = new TransactionMetadataCache();
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFiles, metadataCache, reader,
                    monitors, false );
            CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator( storeDir, logFiles, fileSystem );
            monitors.addMonitorListener( new RecoveryMonitor()
            {
                @Override
                public void recoveryRequired( LogPosition recoveryPosition )
                {
                    fail( "Recovery should not be required" );
                }
            } );
            life.add( new TransactionLogsRecovery( new DefaultRecoveryService( storageEngine, tailScanner, transactionIdStore,
                    txStore, versionRepository, logFiles, NO_MONITOR, mock( Log.class ) ),
                    logPruner, schemaLife, monitor, ProgressReporter.SILENT, false, EMPTY_CHECKER ) );

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
            writer.writeStartEntry( 5L, 4L, 0, new byte[0] );

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
            writer.writeStartEntry( 1L, 1L, BASE_TX_CHECKSUM, ArrayUtils.EMPTY_BYTE_ARRAY );
            writer.writeCommitEntry( 1L, 2L );
            writer.writeCheckPointEntry( new LogPosition( logVersion, CURRENT_FORMAT_LOG_HEADER_SIZE ) );
            writer.writeCheckPointEntry( new LogPosition( logVersion, CURRENT_FORMAT_LOG_HEADER_SIZE ) );
            writer.writeCheckPointEntry( new LogPosition( logVersion, CURRENT_FORMAT_LOG_HEADER_SIZE ) );
            writer.writeCheckPointEntry( new LogPosition( logVersion, CURRENT_FORMAT_LOG_HEADER_SIZE ) );
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
            int previousChecksum = BASE_TX_CHECKSUM;
            writer.writeStartEntry( 2L, 3L, previousChecksum, new byte[0] );
            previousChecksum = writer.writeCommitEntry( 4L, 5L );

            // incomplete tx
            consumer.accept( marker ); // <-- marker has the last good position
            writer.writeStartEntry( 5L, 4L, previousChecksum, new byte[0] );

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
        final long transactionId = 4;
        final long commitTimestamp = 5;
        writeSomeData( file, pair ->
        {
            LogEntryWriter writer = pair.first();
            Consumer<LogPositionMarker> consumer = pair.other();

            // last committed tx
            writer.writeStartEntry( 2L, 3L, BASE_TX_CHECKSUM, additionalHeaderData );
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

        TransactionLogsRecovery logsRecovery = new TransactionLogsRecovery( recoveryService, logPruner, schemaLife, monitor, ProgressReporter.SILENT,
                true, EMPTY_CHECKER );

        logsRecovery.init();

        verify( monitor, never() ).recoveryRequired( any() );
        verify( schemaLife ).init();
    }

    @Test
    void shouldFailRecoveryWhenCanceled() throws Exception
    {
        File file = logFiles.getLogFileForVersion( logVersion );
        final LogPositionMarker marker = new LogPositionMarker();

        final byte[] additionalHeaderData = new byte[0];
        final long transactionId = 4;
        final long commitTimestamp = 5;
        writeSomeData( file, pair ->
        {
            LogEntryWriter writer = pair.first();
            Consumer<LogPositionMarker> consumer = pair.other();

            // last committed tx
            writer.writeStartEntry( 2L, 3L, BASE_TX_CHECKSUM, additionalHeaderData );
            writer.writeCommitEntry( transactionId, commitTimestamp );
            consumer.accept( marker );

            return true;
        } );

        RecoveryMonitor monitor = mock( RecoveryMonitor.class );
        var startupController = mock( DatabaseStartupController.class );
        var databaseId = from( "db", randomUUID() );
        when( startupController.shouldAbort( databaseId ) ).thenReturn( false, true );
        var recoveryStartupChecker = new RecoveryStartupChecker( startupController, databaseId );
        var logsTruncator = mock( CorruptedLogsTruncator.class );

        var exception = assertThrows( Exception.class, () -> recover( storeDir, logFiles, monitor, recoveryStartupChecker, logsTruncator ) );
        var rootCause = getRootCause( exception );
        assertThat( rootCause ).isInstanceOf( DatabaseStartAbortedException.class );

        verify( logsTruncator, never() ).truncate( any() );
        verify( monitor, never() ).recoveryCompleted( anyInt(), anyLong() );
    }

    private boolean recover( File storeDir, LogFiles logFiles )
    {
        RecoveryMonitor monitor = mock( RecoveryMonitor.class );
        CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator( storeDir, logFiles, fileSystem );
        return recover( storeDir, logFiles, monitor, EMPTY_CHECKER, logPruner );
    }

    private boolean recover( File storeDir, LogFiles logFiles, RecoveryMonitor recoveryMonitor, RecoveryStartupChecker startupChecker,
            CorruptedLogsTruncator logsTruncator )
    {
        LifeSupport life = new LifeSupport();

        final AtomicBoolean recoveryRequired = new AtomicBoolean();
        RecoveryMonitor monitor = new RecoveryMonitor()
        {
            @Override
            public void recoveryRequired( LogPosition recoveryPosition )
            {
                recoveryRequired.set( true );
            }
        };
        try
        {
            StorageEngine storageEngine = mock( StorageEngine.class );
            final LogEntryReader reader = logEntryReader();
            LogTailScanner tailScanner = getTailScanner( logFiles, reader );

            TransactionMetadataCache metadataCache = new TransactionMetadataCache();
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFiles, metadataCache, reader, monitors, false );
            CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator( storeDir, logFiles, fileSystem );
            monitors.addMonitorListener( monitor );
            life.add( new TransactionLogsRecovery( new DefaultRecoveryService( storageEngine, tailScanner, transactionIdStore,
                    txStore, versionRepository, logFiles, NO_MONITOR, mock( Log.class ) ),
                    logPruner, schemaLife, monitor, ProgressReporter.SILENT, false, startupChecker ) );

            life.start();
        }
        finally
        {
            life.shutdown();
        }
        return recoveryRequired.get();
    }

    private LogTailScanner getTailScanner( LogFiles logFiles, LogEntryReader reader )
    {
        return new LogTailScanner( logFiles, reader, monitors, false );
    }

    private void writeSomeData( File file, Visitor<Pair<LogEntryWriter,Consumer<LogPositionMarker>>,IOException> visitor ) throws IOException
    {

        try ( LogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel( fileSystem.write( file ), logVersion,
                CURRENT_LOG_FORMAT_VERSION, file, logFiles.getChannelNativeAccessor() );
              PositionAwarePhysicalFlushableChecksumChannel writableLogChannel = new PositionAwarePhysicalFlushableChecksumChannel( versionedStoreChannel,
                        ByteBuffers.allocate( 1, KibiByte ) ) )
        {
            writeLogHeader( writableLogChannel, new LogHeader( logVersion, 2L, StoreId.UNKNOWN ) );
            writableLogChannel.beginChecksum();
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
