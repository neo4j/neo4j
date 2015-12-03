/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.collection.CloseableVisitor;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.Monitor;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.state.RecoverableTransaction;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.test.TargetDirectory;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.DEFAULT_NAME;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.NO_ROTATION;
import static org.neo4j.kernel.impl.util.IdOrderingQueue.BYPASS;
import static org.neo4j.test.TargetDirectory.testDirForTest;

public class PhysicalLogicalTransactionStoreTest
{
    private static final KernelHealth kernelHealth = mock( KernelHealth.class );

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    @Rule
    public TargetDirectory.TestDirectory dir = testDirForTest( getClass() );
    private File testDir;

    @Before
    public void setup()
    {
        testDir = dir.graphDbDir();
    }

    @Test
    public void shouldOpenCleanStore() throws Exception
    {
        // GIVEN
        TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore();
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 1000 );

        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( testDir, DEFAULT_NAME, fs );
        Monitor monitor = new Monitors().newMonitor( PhysicalLogFile.Monitor.class );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000,
                transactionIdStore, mock( LogVersionRepository.class ), monitor, positionCache ) );

        life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache, transactionIdStore, BYPASS,
                kernelHealth ) );

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
        TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore();
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 100 );
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeStarted = 12345, latestCommittedTxWhenStarted = 4545, timeCommitted = timeStarted + 10;
        LifeSupport life = new LifeSupport();
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( testDir, DEFAULT_NAME, fs );
        Monitor monitor = new Monitors().newMonitor( PhysicalLogFile.Monitor.class );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000, transactionIdStore,
                mock( LogVersionRepository.class ), monitor, positionCache ) );

        life.start();
        try
        {
            addATransactionAndRewind(life,  logFile, positionCache, transactionIdStore,
                    additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted, timeCommitted );
        }
        finally
        {
            life.shutdown();
        }

        life = new LifeSupport();
        final AtomicBoolean recoveryRequiredCalled = new AtomicBoolean();
        FakeRecoveryVisitor visitor = new FakeRecoveryVisitor( additionalHeader, masterId,
                authorId, timeStarted, timeCommitted, latestCommittedTxWhenStarted );
        final LogFileRecoverer recoverer = new LogFileRecoverer( new VersionAwareLogEntryReader<>(), visitor );
        logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000, transactionIdStore, mock( LogVersionRepository.class ), monitor, positionCache ) );

        life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, kernelHealth ) );

        life.add( new Recovery( new Recovery.SPI()
        {
            @Override
            public void forceEverything()
            {
            }

            @Override
            public Visitor<LogVersionedStoreChannel, IOException> getRecoverer()
            {
                return recoverer;
            }

            @Override
            public Iterator<LogVersionedStoreChannel> getLogFiles( final long recoveryVersion ) throws IOException
            {
                return new Iterator<LogVersionedStoreChannel>()
                {
                    private final StoreChannel channel = PhysicalLogFile.openForVersion( logFiles, fs,
                            recoveryVersion );
                    private boolean consumed = false;

                    @Override
                    public boolean hasNext()
                    {
                        return !consumed;
                    }

                    @Override
                    public LogVersionedStoreChannel next()
                    {
                        PhysicalLogVersionedStoreChannel physicalLogVersionedStoreChannel;
                        try
                        {
                            physicalLogVersionedStoreChannel = new
                                    PhysicalLogVersionedStoreChannel( channel, recoveryVersion, CURRENT_LOG_VERSION );
                        }
                        catch ( IOException e )
                        {
                            throw new RuntimeException( e );
                        }
                        consumed = true;
                        return physicalLogVersionedStoreChannel;
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public LogPosition getPositionToRecoverFrom() throws IOException
            {
                return LogPosition.start( 0 );
            }

            @Override
            public void recoveryRequired()
            {
                recoveryRequiredCalled.set( true );
            }
        }, mock(Recovery.Monitor.class)));

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
        assertTrue( recoveryRequiredCalled.get() );
    }

    @Test
    public void shouldExtractMetadataFromExistingTransaction() throws Exception
    {
        // GIVEN
        TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore();
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 100 );
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeStarted = 12345, latestCommittedTxWhenStarted = 4545, timeCommitted = timeStarted + 10;
        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( testDir, DEFAULT_NAME, fs );
        Monitor monitor = new Monitors().newMonitor( PhysicalLogFile.Monitor.class );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000,
                transactionIdStore, mock( LogVersionRepository.class ), monitor,
                positionCache ) );

        life.start();
        try
        {
            addATransactionAndRewind( life, logFile, positionCache, transactionIdStore,
                    additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted, timeCommitted );
        }
        finally
        {
            life.shutdown();
        }

        life = new LifeSupport();
        FakeRecoveryVisitor visitor = new FakeRecoveryVisitor( additionalHeader, masterId,
                authorId, timeStarted, timeCommitted, latestCommittedTxWhenStarted );
        final LogFileRecoverer recoverer = new LogFileRecoverer( new VersionAwareLogEntryReader<>(), visitor );
        logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000,
                transactionIdStore, mock( LogVersionRepository.class ), monitor,
                positionCache ) );
        final LogicalTransactionStore store = new PhysicalLogicalTransactionStore( logFile, positionCache );

        // WHEN
        life.start();
        try
        {
            recoverer.visit(PhysicalLogFile.openForVersion( logFiles, fs, 0 ));

            positionCache.clear();

            assertThat( store.getMetadataFor( transactionIdStore.getLastCommittedTransactionId() ).toString(),
                    equalTo(
                            format( "TransactionMetadata[masterId=%d, authorId=%d, startPosition=%s, checksum=%d]",
                                    masterId, authorId, visitor.getStartPosition(), visitor.getChecksum() ) ) );
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
        LogFile logFile = mock( LogFile.class );
        TransactionMetadataCache cache = new TransactionMetadataCache( 10, 10 );

        LifeSupport life = new LifeSupport();

        final LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFile, cache );

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
        } finally {
            life.shutdown();
        }
    }

    @Test
    public void shouldThrowNoSuchTransactionExceptionIfLogFileIsMissing() throws Exception
    {
        // GIVEN
        LogFile logFile = mock( LogFile.class );
        // a missing file
        when( logFile.getReader( any( LogPosition.class) ) ).thenThrow( new FileNotFoundException() );
        // Which is nevertheless in the metadata cache
        TransactionMetadataCache cache = new TransactionMetadataCache( 10, 10 );
        cache.cacheTransactionMetadata( 10, new LogPosition( 2, 130 ), 1, 1, 100 );

        LifeSupport life = new LifeSupport();

        final LogicalTransactionStore txStore = new PhysicalLogicalTransactionStore( logFile, cache );

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
        } finally
        {
            life.shutdown();
        }

    }

    private void addATransactionAndRewind( LifeSupport life, LogFile logFile,
                                           TransactionMetadataCache positionCache,
                                           TransactionIdStore transactionIdStore,
                                           byte[] additionalHeader, int masterId, int authorId, long timeStarted,
                                           long latestCommittedTxWhenStarted, long timeCommitted ) throws IOException
    {
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, kernelHealth ) );
        PhysicalTransactionRepresentation transaction =
                new PhysicalTransactionRepresentation( singleCreateNodeCommand() );
        transaction.setHeader( additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted,
                timeCommitted, -1 );
        appender.append( transaction, LogAppendEvent.NULL );
    }

    private Collection<Command> singleCreateNodeCommand()
    {
        Collection<Command> commands = new ArrayList<>();
        Command.NodeCommand command = new Command.NodeCommand();

        long id = 0;
        NodeRecord before = new NodeRecord( id );
        NodeRecord after = new NodeRecord( id );
        after.setInUse( true );
        command.init( before, after );

        commands.add( command );
        return commands;
    }

    private static class FakeRecoveryVisitor implements CloseableVisitor<RecoverableTransaction, IOException>
    {
        private final byte[] additionalHeader;
        private final int masterId;
        private final int authorId;
        private final long timeStarted;
        private final long timeCommitted;
        private final long latestCommittedTxWhenStarted;
        private int visitedTransactions;
        private long checksum;
        private LogPosition startPosition;

        public FakeRecoveryVisitor( byte[] additionalHeader, int masterId,
                int authorId, long timeStarted, long timeCommitted, long latestCommittedTxWhenStarted )
        {
            this.additionalHeader = additionalHeader;
            this.masterId = masterId;
            this.authorId = authorId;
            this.timeStarted = timeStarted;
            this.timeCommitted = timeCommitted;
            this.latestCommittedTxWhenStarted = latestCommittedTxWhenStarted;
        }

        @Override
        public boolean visit( RecoverableTransaction committedTx ) throws IOException
        {
            TransactionRepresentation transaction = committedTx.representation().getTransactionRepresentation();
            assertArrayEquals( additionalHeader, transaction.additionalHeader() );
            assertEquals( masterId, transaction.getMasterId() );
            assertEquals( authorId, transaction.getAuthorId() );
            assertEquals( timeStarted, transaction.getTimeStarted() );
            assertEquals( timeCommitted, transaction.getTimeCommitted() );
            assertEquals( latestCommittedTxWhenStarted, transaction.getLatestCommittedTxWhenStarted() );
            visitedTransactions++;
            checksum = committedTx.representation().getStartEntry().checksum();
            startPosition = committedTx.representation().getStartEntry().getStartPosition();
            return false;
        }

        @Override
        public void close() throws IOException
        {
            // nothing to do
        }

        public int getVisitedTransactions()
        {
            return visitedTransactions;
        }

        public long getChecksum()
        {
            return checksum;
        }

        public LogPosition getStartPosition()
        {
            return startPosition;
        }
    }
}
