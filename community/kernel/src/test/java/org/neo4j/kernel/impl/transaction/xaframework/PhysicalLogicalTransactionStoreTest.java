/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies.NO_PRUNING;
import static org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile.DEFAULT_NAME;
import static org.neo4j.kernel.impl.util.Providers.singletonProvider;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.CommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.LogFileRecoverer;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TargetDirectory;

public class PhysicalLogicalTransactionStoreTest
{
    // TODO 2.2-future this breaks the test in interesting ways
//    private final FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    private TargetDirectory dir = TargetDirectory.forTest( getClass() );

    private File testDir;

    @Before
    public void setup()
    {
        testDir = dir.cleanDirectory( "dir" );
        fs.mkdir( testDir );
    }

    @Test
    public void shouldOpenCleanStore() throws Exception
    {
        // GIVEN
        TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 0l );
        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 1000 );

        LifeSupport life = new LifeSupport(  );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( testDir, DEFAULT_NAME, fs );
        LogFile logFile = life.add(new PhysicalLogFile( fs, logFiles, 1000, NO_PRUNING,
                transactionIdStore, mock( LogVersionRepository.class), new Monitors().newMonitor( PhysicalLogFile.Monitor.class ), logRotationControl,
                positionCache, new Visitor<ReadableLogChannel, IOException>()
                            {
                                @Override
                                public boolean visit( ReadableLogChannel channel ) throws IOException
                                {
                                    // THEN
                                    fail( "Should be nothing to recover" );
                                    return false;
                                }
                            } ) );
        TxIdGenerator txIdGenerator = new DefaultTxIdGenerator( singletonProvider( transactionIdStore ) );
        life.add( new PhysicalLogicalTransactionStore( logFile, txIdGenerator, positionCache,
                new VersionAwareLogEntryReader( CommandReaderFactory.DEFAULT ) ) );

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
        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 0l );
        TxIdGenerator txIdGenerator = new DefaultTxIdGenerator( singletonProvider( transactionIdStore ) );
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 100 );
        final byte[] additionalHeader = new byte[] {1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeWritten = 12345, latestCommittedTxWhenStarted = 4545;
        LifeSupport life = new LifeSupport(  );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( testDir, DEFAULT_NAME, fs );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000, NO_PRUNING,
                transactionIdStore, mock( LogVersionRepository.class), new Monitors().newMonitor( PhysicalLogFile.Monitor.class ), logRotationControl,
                positionCache, new Visitor<ReadableLogChannel, IOException>()
                        {
                            @Override
                            public boolean visit( ReadableLogChannel element ) throws IOException
                            {
                                return false;
                            }
                        } ));

        life.start();

        addATransactionAndRewind( logFile, txIdGenerator, positionCache,
                additionalHeader, masterId, authorId, timeWritten, latestCommittedTxWhenStarted );

        life.shutdown();

        life = new LifeSupport(  );
        final AtomicInteger recoveredTransactions = new AtomicInteger();
        logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000, NO_PRUNING,
                        transactionIdStore, mock( LogVersionRepository.class), new Monitors().newMonitor( PhysicalLogFile.Monitor.class ), logRotationControl,
                        positionCache, new LogFileRecoverer( new VersionAwareLogEntryReader( CommandReaderFactory.DEFAULT ), new Visitor<CommittedTransactionRepresentation, IOException>()
        {
            @Override
            public boolean visit( CommittedTransactionRepresentation committedTx ) throws IOException
            {
                TransactionRepresentation transaction = committedTx.getTransactionRepresentation();
                assertArrayEquals( additionalHeader, transaction.additionalHeader() );
                assertEquals( masterId, transaction.getMasterId() );
                assertEquals( authorId, transaction.getAuthorId() );
                assertEquals( timeWritten, transaction.getTimeWritten() );
                assertEquals( latestCommittedTxWhenStarted, transaction.getLatestCommittedTxWhenStarted() );
                recoveredTransactions.incrementAndGet();
                return true;
            }
        } )));

        life.add( new PhysicalLogicalTransactionStore( logFile, txIdGenerator, positionCache, new VersionAwareLogEntryReader(
                CommandReaderFactory.DEFAULT ) ) );

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
        assertEquals( 1, recoveredTransactions.get() );
    }

    @Test
    public void shouldExtractMetadataFromExistingTransaction() throws Exception
    {
        // GIVEN
        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 0l );
        TxIdGenerator txIdGenerator = new DefaultTxIdGenerator( singletonProvider( transactionIdStore ) );
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 100 );
        final byte[] additionalHeader = new byte[] {1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeWritten = 12345, latestCommittedTxWhenStarted = 4545;
        LifeSupport life = new LifeSupport(  );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( testDir, DEFAULT_NAME, fs );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000, NO_PRUNING,
                transactionIdStore, mock( LogVersionRepository.class), new Monitors().newMonitor( PhysicalLogFile.Monitor.class ), logRotationControl,
                positionCache, new Visitor<ReadableLogChannel, IOException>()
        {
            @Override
            public boolean visit( ReadableLogChannel element ) throws IOException
            {
                return false;
            }
        } ));

        life.start();

        addATransactionAndRewind( logFile, txIdGenerator, positionCache,
                additionalHeader, masterId, authorId, timeWritten, latestCommittedTxWhenStarted );

        life.shutdown();

        life = new LifeSupport();
        final AtomicInteger recoveredTransactions = new AtomicInteger();
        logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000, NO_PRUNING,
                transactionIdStore, mock( LogVersionRepository.class), new Monitors().newMonitor( PhysicalLogFile.Monitor.class ), logRotationControl,
                positionCache, new LogFileRecoverer( new VersionAwareLogEntryReader( CommandReaderFactory.DEFAULT ), new Visitor<CommittedTransactionRepresentation, IOException>()
        {
            @Override
            public boolean visit( CommittedTransactionRepresentation committedTx ) throws IOException
            {
                TransactionRepresentation transaction = committedTx.getTransactionRepresentation();
                assertArrayEquals( additionalHeader, transaction.additionalHeader() );
                assertEquals( masterId, transaction.getMasterId() );
                assertEquals( authorId, transaction.getAuthorId() );
                assertEquals( timeWritten, transaction.getTimeWritten() );
                assertEquals( latestCommittedTxWhenStarted, transaction.getLatestCommittedTxWhenStarted() );
                recoveredTransactions.incrementAndGet();
                return true;
            }
        } )));

        LogicalTransactionStore store = life.add( new PhysicalLogicalTransactionStore( logFile, txIdGenerator,
                positionCache, new VersionAwareLogEntryReader( CommandReaderFactory.DEFAULT ) ) );

        // WHEN
        life.start();

        positionCache.clear();

        // TODO 2.2-future make this into a proper test
        System.out.println( store.getMetadataFor( transactionIdStore.getLastCommittingTransactionId() ) );
    }

    private void addATransactionAndRewind( LogFile logFile, TxIdGenerator txIdGenerator,
                                           TransactionMetadataCache positionCache, byte[] additionalHeader,
                                           int masterId, int authorId, long timeWritten,
                                           long latestCommittedTxWhenStarted ) throws IOException
    {
        try ( TransactionAppender appender = new PhysicalTransactionAppender( logFile, txIdGenerator, positionCache ) )
        {
            PhysicalTransactionRepresentation transaction =
                    new PhysicalTransactionRepresentation( singleCreateNodeCommand() );
            transaction.setHeader( additionalHeader, masterId, authorId, timeWritten, latestCommittedTxWhenStarted );
            appender.append( transaction );
        }
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

}
