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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache.TransactionMetadata;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.util.monitoring.SilentProgressReporter;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.CorruptedLogsTruncator;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.kernel.recovery.RecoveryApplier;
import org.neo4j.kernel.recovery.RecoveryMonitor;
import org.neo4j.kernel.recovery.RecoveryService;
import org.neo4j.kernel.recovery.RecoveryStartInformation;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.NO_ROTATION;
import static org.neo4j.kernel.impl.util.IdOrderingQueue.BYPASS;

public class PhysicalLogicalTransactionStoreTest
{
    private static final DatabaseHealth DATABASE_HEALTH = mock( DatabaseHealth.class );

    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory();
    private File testDir;
    private Monitors monitors = new Monitors();

    @Before
    public void setup()
    {
        testDir = dir.graphDbDir();
    }

    @Test
    public void extractTransactionFromLogFilesSkippingLastLogFileWithoutHeader() throws IOException
    {
        TransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 100 );
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2;
        int authorId = 1;
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = 4545;
        long timeCommitted = timeStarted + 10;
        LifeSupport life = new LifeSupport();
        final LogFiles logFiles = LogFilesBuilder.builder( testDir, fileSystemRule.get() )
                                                 .withTransactionIdStore( transactionIdStore )
                                                 .withLogVersionRepository( mock( LogVersionRepository.class ) ).build();
        life.add( logFiles );
        life.start();
        try
        {
            addATransactionAndRewind( life, logFiles, positionCache, transactionIdStore,
                    additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted, timeCommitted );
        }
        finally
        {
            life.shutdown();
        }

        // create empty transaction log file and clear transaction cache to force re-read
        fileSystemRule.get().create( logFiles.getLogFileForVersion( logFiles.getHighestLogVersion() + 1 ) ).close();
        positionCache.clear();

        final LogicalTransactionStore store = new PhysicalLogicalTransactionStore( logFiles, positionCache,
                new VersionAwareLogEntryReader<>(), monitors, true );
        verifyTransaction( transactionIdStore, positionCache, additionalHeader, masterId, authorId, timeStarted,
                latestCommittedTxWhenStarted, timeCommitted, store );
    }

    @Test
    public void shouldOpenCleanStore() throws Exception
    {
        // GIVEN
        TransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 1000 );

        LifeSupport life = new LifeSupport();
        final LogFiles logFiles = LogFilesBuilder.builder( testDir, fileSystemRule.get() )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( mock( LogVersionRepository.class ) ).build();
        life.add( logFiles );

        life.add( new BatchingTransactionAppender( logFiles, NO_ROTATION, positionCache, transactionIdStore, BYPASS,
                DATABASE_HEALTH ) );

        try
        {
            // WHEN
            life.start();
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    public void shouldOpenAndRecoverExistingData() throws Exception
    {
        // GIVEN
        TransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 100 );
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2;
        int authorId = 1;
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = 4545;
        long timeCommitted = timeStarted + 10;
        LifeSupport life = new LifeSupport();
        final LogFiles logFiles = LogFilesBuilder.builder( testDir, fileSystemRule.get() )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( mock( LogVersionRepository.class ) ).build();

        life.start();
        life.add( logFiles );
        try
        {
            addATransactionAndRewind( life, logFiles, positionCache, transactionIdStore,
                    additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted, timeCommitted );
        }
        finally
        {
            life.shutdown();
        }

        life = new LifeSupport();
        life.add( logFiles );
        final AtomicBoolean recoveryRequired = new AtomicBoolean();
        FakeRecoveryVisitor visitor = new FakeRecoveryVisitor( additionalHeader, masterId,
                authorId, timeStarted, timeCommitted, latestCommittedTxWhenStarted );

        LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFiles, positionCache,
                new VersionAwareLogEntryReader<>(), monitors, true );

        life.add( new BatchingTransactionAppender( logFiles, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, DATABASE_HEALTH ) );
        CorruptedLogsTruncator logPruner = new CorruptedLogsTruncator( testDir, logFiles, fileSystemRule.get() );
        life.add( new Recovery( new RecoveryService()
        {
            @Override
            public void startRecovery()
            {
                recoveryRequired.set( true );
            }

            @Override
            public RecoveryApplier getRecoveryApplier( TransactionApplicationMode mode )
            {
                return mode == TransactionApplicationMode.REVERSE_RECOVERY ? mock( RecoveryApplier.class ) : visitor;
            }

            @Override
            public RecoveryStartInformation getRecoveryStartInformation()
            {
                return new RecoveryStartInformation( LogPosition.start( 0 ), 1 );
            }

            @Override
            public TransactionCursor getTransactions( LogPosition position ) throws IOException
            {
                return txStore.getTransactions( position );
            }

            @Override
            public TransactionCursor getTransactionsInReverseOrder( LogPosition position ) throws IOException
            {
                return txStore.getTransactionsInReverseOrder( position );
            }

            public void transactionsRecovered( CommittedTransactionRepresentation lastRecoveredTransaction,
                    LogPosition positionAfterLastRecoveredTransaction )
            {
            }
        }, new StartupStatisticsProvider(), logPruner, mock( RecoveryMonitor.class ), SilentProgressReporter.INSTANCE,
                false ) );

        // WHEN
        try
        {
            life.start();
        }
        finally
        {
            life.shutdown();
        }

        // THEN
        assertEquals( 1, visitor.getVisitedTransactions() );
        assertTrue( recoveryRequired.get() );
    }

    @Test
    public void shouldExtractMetadataFromExistingTransaction() throws Exception
    {
        // GIVEN
        TransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 100 );
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2;
        int  authorId = 1;
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = 4545;
        long timeCommitted = timeStarted + 10;
        LifeSupport life = new LifeSupport();
        final LogFiles logFiles = LogFilesBuilder.builder( testDir, fileSystemRule.get() )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( mock( LogVersionRepository.class ) ).build();
        life.start();
        life.add( logFiles );
        try
        {
            addATransactionAndRewind( life, logFiles, positionCache, transactionIdStore,
                    additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted, timeCommitted );
        }
        finally
        {
            life.shutdown();
        }

        life = new LifeSupport();
        life.add( logFiles );
        final LogicalTransactionStore store = new PhysicalLogicalTransactionStore( logFiles, positionCache,
                new VersionAwareLogEntryReader<>(), monitors, true );

        // WHEN
        life.start();
        try
        {
            verifyTransaction( transactionIdStore, positionCache, additionalHeader, masterId, authorId, timeStarted,
                    latestCommittedTxWhenStarted, timeCommitted, store );
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    public void shouldThrowNoSuchTransactionExceptionIfMetadataNotFound() throws Exception
    {
        // GIVEN
        LogFiles logFiles = mock( LogFiles.class );
        TransactionMetadataCache cache = new TransactionMetadataCache( 10 );

        LifeSupport life = new LifeSupport();

        final LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFiles, cache,
                new VersionAwareLogEntryReader<>(), monitors, true );

        try
        {
            life.start();
            // WHEN
            try
            {
                txStore.getMetadataFor( 10 );
                fail( "Should have thrown" );
            }
            catch ( NoSuchTransactionException e )
            {   // THEN Good
            }
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    public void shouldThrowNoSuchTransactionExceptionIfLogFileIsMissing() throws Exception
    {
        // GIVEN
        LogFile logFile = mock( LogFile.class );
        LogFiles logFiles = mock( LogFiles.class );
        // a missing file
        when( logFiles.getLogFile() ).thenReturn( logFile );
        when( logFile.getReader( any( LogPosition.class) ) ).thenThrow( new FileNotFoundException() );
        // Which is nevertheless in the metadata cache
        TransactionMetadataCache cache = new TransactionMetadataCache( 10 );
        cache.cacheTransactionMetadata( 10, new LogPosition( 2, 130 ), 1, 1, 100, System.currentTimeMillis() );

        LifeSupport life = new LifeSupport();

        final LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFiles, cache,
                new VersionAwareLogEntryReader<>(), monitors, true );

        try
        {
            life.start();

            // WHEN
            // we ask for that transaction and forward
            try
            {
                txStore.getTransactions( 10 );
                fail();
            }
            catch ( NoSuchTransactionException e )
            {
                // THEN
                // We don't get a FileNotFoundException but a NoSuchTransactionException instead
            }
        }
        finally
        {
            life.shutdown();
        }

    }

    private void addATransactionAndRewind( LifeSupport life, LogFiles logFiles,
                                           TransactionMetadataCache positionCache,
                                           TransactionIdStore transactionIdStore,
                                           byte[] additionalHeader, int masterId, int authorId, long timeStarted,
                                           long latestCommittedTxWhenStarted, long timeCommitted ) throws IOException
    {
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFiles, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, DATABASE_HEALTH ) );
        PhysicalTransactionRepresentation transaction =
                new PhysicalTransactionRepresentation( singleCreateNodeCommand() );
        transaction.setHeader( additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted,
                timeCommitted, -1 );
        appender.append( new TransactionToApply( transaction ), LogAppendEvent.NULL );
    }

    private Collection<StorageCommand> singleCreateNodeCommand()
    {
        Collection<StorageCommand> commands = new ArrayList<>();

        long id = 0;
        NodeRecord before = new NodeRecord( id );
        NodeRecord after = new NodeRecord( id );
        after.setInUse( true );
        commands.add( new Command.NodeCommand( before, after ) );
        return commands;
    }

    private void verifyTransaction( TransactionIdStore transactionIdStore, TransactionMetadataCache positionCache,
            byte[] additionalHeader, int masterId, int authorId, long timeStarted, long latestCommittedTxWhenStarted,
            long timeCommitted, LogicalTransactionStore store ) throws IOException
    {
        TransactionMetadata expectedMetadata;
        try ( TransactionCursor cursor = store.getTransactions( TransactionIdStore.BASE_TX_ID + 1 ) )
        {
            boolean hasNext = cursor.next();
            assertTrue( hasNext );
            CommittedTransactionRepresentation tx = cursor.get();
            TransactionRepresentation transaction = tx.getTransactionRepresentation();
            assertArrayEquals( additionalHeader, transaction.additionalHeader() );
            assertEquals( masterId, transaction.getMasterId() );
            assertEquals( authorId, transaction.getAuthorId() );
            assertEquals( timeStarted, transaction.getTimeStarted() );
            assertEquals( timeCommitted, transaction.getTimeCommitted() );
            assertEquals( latestCommittedTxWhenStarted, transaction.getLatestCommittedTxWhenStarted() );
            expectedMetadata = new TransactionMetadata( masterId, authorId,
                    tx.getStartEntry().getStartPosition(), tx.getStartEntry().checksum(), timeCommitted );
        }

        positionCache.clear();

        TransactionMetadata actualMetadata = store.getMetadataFor( transactionIdStore.getLastCommittedTransactionId() );
        assertEquals( expectedMetadata, actualMetadata );
    }

    private static class FakeRecoveryVisitor implements RecoveryApplier
    {
        private final byte[] additionalHeader;
        private final int masterId;
        private final int authorId;
        private final long timeStarted;
        private final long timeCommitted;
        private final long latestCommittedTxWhenStarted;
        private int visitedTransactions;

        FakeRecoveryVisitor( byte[] additionalHeader, int masterId, int authorId, long timeStarted, long timeCommitted,
                long latestCommittedTxWhenStarted )
        {
            this.additionalHeader = additionalHeader;
            this.masterId = masterId;
            this.authorId = authorId;
            this.timeStarted = timeStarted;
            this.timeCommitted = timeCommitted;
            this.latestCommittedTxWhenStarted = latestCommittedTxWhenStarted;
        }

        @Override
        public boolean visit( CommittedTransactionRepresentation tx )
        {
            TransactionRepresentation transaction = tx.getTransactionRepresentation();
            assertArrayEquals( additionalHeader, transaction.additionalHeader() );
            assertEquals( masterId, transaction.getMasterId() );
            assertEquals( authorId, transaction.getAuthorId() );
            assertEquals( timeStarted, transaction.getTimeStarted() );
            assertEquals( timeCommitted, transaction.getTimeCommitted() );
            assertEquals( latestCommittedTxWhenStarted, transaction.getLatestCommittedTxWhenStarted() );
            visitedTransactions++;
            return false;
        }

        int getVisitedTransactions()
        {
            return visitedTransactions;
        }

        @Override
        public void close()
        {
        }
    }
}
