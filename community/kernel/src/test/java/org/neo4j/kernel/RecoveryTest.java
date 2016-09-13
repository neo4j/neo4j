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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.function.Consumer;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.api.RecoveryLegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.RecoveryIndexingUpdatesValidator;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalWritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.DefaultRecoverySPI;
import org.neo4j.kernel.recovery.LatestCheckPointFinder;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyZeroInteractions;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

public class RecoveryTest
{
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final LogVersionRepository logVersionRepository = new DeadSimpleLogVersionRepository( 1L );
    private final TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 5L, 0,
            BASE_TX_COMMIT_TIMESTAMP, 0, 0 );
    private final int logVersion = 0;

    private LogEntry lastCommittedTxStartEntry;
    private LogEntry lastCommittedTxCommitEntry;
    private LogEntry expectedStartEntry;
    private LogEntry expectedCommitEntry;
    private LogEntry expectedCheckPointEntry;

    @Test
    public void shouldRecoverExistingData() throws Exception
    {
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), "log", fs );
        File file = logFiles.getLogFileForVersion( logVersion );

        writeSomeData( file, new Visitor<Pair<LogEntryWriter, Consumer<LogPositionMarker>>,IOException>()
        {
            @Override
            public boolean visit( Pair<LogEntryWriter,Consumer<LogPositionMarker>> pair ) throws IOException
            {
                LogEntryWriter writer = pair.first();
                Consumer<LogPositionMarker> consumer = pair.other();
                LogPositionMarker marker = new LogPositionMarker();

                // last committed tx
                consumer.accept( marker );
                LogPosition lastCommittedTxPosition = marker.newPosition();
                writer.writeStartEntry( 0, 1, 2l, 3l, new byte[0] );
                lastCommittedTxStartEntry = new LogEntryStart( 0, 1, 2l, 3l, new byte[0], lastCommittedTxPosition );
                writer.writeCommitEntry( 4l, 5l );
                lastCommittedTxCommitEntry = new OnePhaseCommit( 4l, 5l );

                // check point pointing to the previously committed transaction
                writer.writeCheckPointEntry( lastCommittedTxPosition );
                expectedCheckPointEntry = new CheckPoint( lastCommittedTxPosition );

                // tx committed after checkpoint
                consumer.accept( marker );
                writer.writeStartEntry( 0, 1, 6l, 4l, new byte[0] );
                expectedStartEntry = new LogEntryStart( 0, 1, 6l, 4l, new byte[0], marker.newPosition() );

                writer.writeCommitEntry( 5l, 7l );
                expectedCommitEntry = new OnePhaseCommit( 5l, 7l );

                return true;
            }
        } );

        LifeSupport life = new LifeSupport();
        Recovery.Monitor monitor = mock( Recovery.Monitor.class );
        final AtomicBoolean recoveryRequired = new AtomicBoolean();
        try
        {
            RecoveryLabelScanWriterProvider provider = mock( RecoveryLabelScanWriterProvider.class );
            RecoveryLegacyIndexApplierLookup lookup = mock( RecoveryLegacyIndexApplierLookup.class );
            RecoveryIndexingUpdatesValidator validator = mock( RecoveryIndexingUpdatesValidator.class );

            StoreFlusher flusher = mock( StoreFlusher.class );
            final LogEntryReader<ReadableLogChannel> reader = new VersionAwareLogEntryReader<>(
                    LogEntryVersion.CURRENT.byteCode() );
            LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );

            TransactionMetadataCache metadataCache = new TransactionMetadataCache( 10, 100 );
            LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 50,
                    transactionIdStore, logVersionRepository, mock( PhysicalLogFile.Monitor.class ),
                    metadataCache ) );
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFile,
                    metadataCache );
            TransactionRepresentationStoreApplier storeApplier = mock( TransactionRepresentationStoreApplier.class );

            life.add( new Recovery( new DefaultRecoverySPI( provider, lookup, flusher, mock( NeoStores.class ),
                    logFiles, fs, logVersionRepository, finder, validator, transactionIdStore, txStore, storeApplier )
            {
                private int nr = 0;

                @Override
                public Visitor<CommittedTransactionRepresentation,Exception> startRecovery()
                {
                    recoveryRequired.set( true );
                    final Visitor<CommittedTransactionRepresentation,Exception> actual = super.startRecovery();
                    return new Visitor<CommittedTransactionRepresentation,Exception>()
                    {
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
                            default: fail( "Too many recovered transactions" );
                            }
                            return false;
                        }
                    };
                }
            }, monitor ) );

            life.start();

            InOrder order = inOrder( monitor );
            order.verify( monitor, times( 1 ) ).recoveryRequired( any( LogPosition.class ) );
            order.verify( monitor, times( 1 ) ).recoveryCompleted( 2 );
            assertTrue( recoveryRequired.get() );
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    public void shouldSeeThatACleanDatabaseShouldNotRequireRecovery() throws Exception
    {
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), "log", fs );
        File file = logFiles.getLogFileForVersion( logVersion );

        writeSomeData( file, new Visitor<Pair<LogEntryWriter, Consumer<LogPositionMarker>>,IOException>()
        {
            @Override
            public boolean visit( Pair<LogEntryWriter,Consumer<LogPositionMarker>> pair ) throws IOException
            {
                LogEntryWriter writer = pair.first();
                Consumer<LogPositionMarker> consumer = pair.other();
                LogPositionMarker marker = new LogPositionMarker();

                // last committed tx
                consumer.accept( marker );
                writer.writeStartEntry( 0, 1, 2l, 3l, new byte[0] );
                writer.writeCommitEntry( 4l, 5l );

                // check point
                consumer.accept( marker );
                writer.writeCheckPointEntry( marker.newPosition() );

                return true;
            }
        } );

        LifeSupport life = new LifeSupport();
        Recovery.Monitor monitor = mock( Recovery.Monitor.class );
        try
        {
            RecoveryLabelScanWriterProvider provider = mock( RecoveryLabelScanWriterProvider.class );
            RecoveryLegacyIndexApplierLookup lookup = mock( RecoveryLegacyIndexApplierLookup.class );
            RecoveryIndexingUpdatesValidator validator = mock( RecoveryIndexingUpdatesValidator.class );

            StoreFlusher flusher = mock( StoreFlusher.class );
            final LogEntryReader<ReadableLogChannel> reader = new VersionAwareLogEntryReader<>(
                    LogEntryVersion.CURRENT.byteCode() );
            LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );

            TransactionMetadataCache metadataCache = new TransactionMetadataCache( 10, 100 );
            LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 50,
                    transactionIdStore, logVersionRepository, mock( PhysicalLogFile.Monitor.class ), metadataCache ) );
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFile, metadataCache );
            TransactionRepresentationStoreApplier storeApplier = mock( TransactionRepresentationStoreApplier.class );

            life.add( new Recovery( new DefaultRecoverySPI( provider, lookup, flusher, mock( NeoStores.class ),
                    logFiles, fs, logVersionRepository, finder, validator, transactionIdStore, txStore, storeApplier )
            {
                @Override
                public Visitor<CommittedTransactionRepresentation,Exception> startRecovery()
                {
                    fail( "Recovery should not be required" );
                    return null; // <-- to satisfy the compiler
                }
            }, monitor ));


            life.start();

            verifyZeroInteractions( monitor );
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    public void shouldTruncateLogAfterLastCompleteTransactionAfterSuccessfullRecovery() throws Exception
    {
        // GIVEN
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), "log", fs );
        File file = logFiles.getLogFileForVersion( logVersion );
        final LogPositionMarker marker = new LogPositionMarker();

        writeSomeData( file, new Visitor<Pair<LogEntryWriter, Consumer<LogPositionMarker>>,IOException>()
        {
            @Override
            public boolean visit( Pair<LogEntryWriter,Consumer<LogPositionMarker>> pair ) throws IOException
            {
                LogEntryWriter writer = pair.first();
                Consumer<LogPositionMarker> consumer = pair.other();

                // last committed tx
                writer.writeStartEntry( 0, 1, 2l, 3l, new byte[0] );
                writer.writeCommitEntry( 4l, 5l );

                // incomplete tx
                consumer.accept( marker ); // <-- marker has the last good position
                writer.writeStartEntry( 0, 1, 5l, 4l, new byte[0] );

                return true;
            }
        } );

        // WHEN
        boolean recoveryRequired = recover( logFiles );

        // THEN
        assertTrue( recoveryRequired );
        assertEquals( marker.getByteOffset(), file.length() );
    }

    @Test
    public void shouldTellTransactionIdStoreAfterSuccessfullRecovery() throws Exception
    {
        // GIVEN
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), "log", fs );
        File file = logFiles.getLogFileForVersion( logVersion );
        final LogPositionMarker marker = new LogPositionMarker();

        final byte[] additionalHeaderData = new byte[0];
        final int masterId = 0;
        final int authorId = 1;
        final long transactionId = 4;
        final long commitTimestamp = 5;
        writeSomeData( file, new Visitor<Pair<LogEntryWriter, Consumer<LogPositionMarker>>,IOException>()
        {
            @Override
            public boolean visit( Pair<LogEntryWriter,Consumer<LogPositionMarker>> pair ) throws IOException
            {
                LogEntryWriter writer = pair.first();
                Consumer<LogPositionMarker> consumer = pair.other();

                // last committed tx
                writer.writeStartEntry( masterId, authorId, 2l, 3l, additionalHeaderData );
                writer.writeCommitEntry( transactionId, commitTimestamp );
                consumer.accept( marker );

                return true;
            }
        } );

        // WHEN
        boolean recoveryRequired = recover( logFiles );

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

    private boolean recover( PhysicalLogFiles logFiles )
    {
        LifeSupport life = new LifeSupport();
        Recovery.Monitor monitor = mock( Recovery.Monitor.class );
        final AtomicBoolean recoveryRequired = new AtomicBoolean();
        try
        {
            RecoveryLabelScanWriterProvider provider = mock( RecoveryLabelScanWriterProvider.class );
            RecoveryLegacyIndexApplierLookup lookup = mock( RecoveryLegacyIndexApplierLookup.class );
            RecoveryIndexingUpdatesValidator validator = mock( RecoveryIndexingUpdatesValidator.class );

            StoreFlusher flusher = mock( StoreFlusher.class );
            final LogEntryReader<ReadableLogChannel> reader = new VersionAwareLogEntryReader<>(
                    LogEntryVersion.CURRENT.byteCode() );
            LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );

            TransactionMetadataCache metadataCache = new TransactionMetadataCache( 10, 100 );
            LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 50,
                    transactionIdStore, logVersionRepository, mock( PhysicalLogFile.Monitor.class ), metadataCache ) );
            LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFile, metadataCache );
            TransactionRepresentationStoreApplier storeApplier = mock( TransactionRepresentationStoreApplier.class );

            life.add( new Recovery( new DefaultRecoverySPI( provider, lookup, flusher, mock( NeoStores.class ),
                    logFiles, fs, logVersionRepository, finder, validator, transactionIdStore, txStore, storeApplier )
            {
                @Override
                public Visitor<CommittedTransactionRepresentation,Exception> startRecovery()
                {
                    recoveryRequired.set( true );
                    return super.startRecovery();
                }
            }, monitor ) );

            life.start();
        }
        finally
        {
            life.shutdown();
        }
        return recoveryRequired.get();
    }

    private void writeSomeData( File file, Visitor<Pair<LogEntryWriter,Consumer<LogPositionMarker>>,IOException> visitor ) throws IOException
    {

        try (  LogVersionedStoreChannel versionedStoreChannel =
                       new PhysicalLogVersionedStoreChannel( fs.open( file, "rw" ), logVersion, CURRENT_LOG_VERSION );
              final PhysicalWritableLogChannel writableLogChannel = new PhysicalWritableLogChannel( versionedStoreChannel ) )
        {
            writeLogHeader( writableLogChannel, logVersion, 2l );

            Consumer<LogPositionMarker> consumer = new Consumer<LogPositionMarker>()
            {
                @Override
                public void accept( LogPositionMarker marker )
                {
                    try
                    {
                        writableLogChannel.getCurrentPosition( marker );
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            };
            LogEntryWriter first = new LogEntryWriter( writableLogChannel, CommandHandler.EMPTY );
            visitor.visit( Pair.of( first, consumer ) );
        }
    }
}
