/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.tools.dump;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.collection.primitive.PrimitiveLongArrayQueue;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;
import org.neo4j.tools.dump.TransactionLogAnalyzer.Monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler.STRICT;

public class TransactionLogAnalyzerTest
{
    private final FileSystemRule<DefaultFileSystemAbstraction> fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );
    private final LifeRule life = new LifeRule( true );
    private final RandomRule random = new RandomRule();
    private final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( fs ).around( directory ).around( life )
            .around( expectedException );

    private LogFile logFile;
    private FlushablePositionAwareChannel writer;
    private TransactionLogWriter transactionLogWriter;
    private AtomicLong lastCommittedTxId;
    private VerifyingMonitor monitor;
    private LogVersionRepository logVersionRepository;
    private LogFiles logFiles;

    @Before
    public void before() throws IOException
    {
        lastCommittedTxId = new AtomicLong( BASE_TX_ID );
        logVersionRepository = new SimpleLogVersionRepository();
        logFiles = LogFilesBuilder.builder( directory.absolutePath(), fs )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .build();
        life.add( logFiles );
        logFile = logFiles.getLogFile();
        writer = logFile.getWriter();
        transactionLogWriter = new TransactionLogWriter( new LogEntryWriter( writer ) );
        monitor = new VerifyingMonitor();
    }

    @After
    public void after()
    {
        life.shutdown();
    }

    @Test
    public void shouldSeeTransactionsInOneLogFile() throws Exception
    {
        // given
        writeTransactions( 5 );

        // when
        TransactionLogAnalyzer.analyze( fs, directory.absolutePath(), STRICT, monitor );

        // then
        assertEquals( 1, monitor.logFiles );
        assertEquals( 5, monitor.transactions );
    }

    @Test
    public void throwExceptionWithErrorMessageIfLogFilesNotFound() throws Exception
    {
        File emptyDirectory = directory.directory( "empty" );
        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage( "not found." );
        TransactionLogAnalyzer.analyze( fs, emptyDirectory, STRICT, monitor );
    }

    @Test
    public void shouldSeeCheckpointsInBetweenTransactionsInOneLogFile() throws Exception
    {
        // given
        writeTransactions( 3 ); // txs 2, 3, 4
        writeCheckpoint();
        writeTransactions( 2 ); // txs 5, 6
        writeCheckpoint();
        writeTransactions( 4 ); // txs 7, 8, 9, 10

        // when
        TransactionLogAnalyzer.analyze( fs, directory.absolutePath(), STRICT, monitor );

        // then
        assertEquals( 1, monitor.logFiles );
        assertEquals( 2, monitor.checkpoints );
        assertEquals( 9, monitor.transactions );
    }

    @Test
    public void shouldSeeLogFileTransitions() throws Exception
    {
        // given
        writeTransactions( 1 );
        rotate();
        writeTransactions( 1 );
        rotate();
        writeTransactions( 1 );

        // when
        TransactionLogAnalyzer.analyze( fs, directory.absolutePath(), STRICT, monitor );

        // then
        assertEquals( 3, monitor.logFiles );
        assertEquals( 0, monitor.checkpoints );
        assertEquals( 3, monitor.transactions );
    }

    @Test
    public void shouldSeeLogFileTransitionsTransactionsAndCheckpointsInMultipleLogFiles() throws Exception
    {
        // given
        int expectedTransactions = 0;
        int expectedCheckpoints = 0;
        int expectedLogFiles = 1;
        for ( int i = 0; i < 30; i++ )
        {
            float chance = random.nextFloat();
            if ( chance < 0.5 )
            {   // tx
                int count = random.nextInt( 1, 5 );
                writeTransactions( count );
                expectedTransactions += count;
            }
            else if ( chance < 0.75 )
            {   // checkpoint
                writeCheckpoint();
                expectedCheckpoints++;
            }
            else
            {   // rotate
                rotate();
                expectedLogFiles++;
            }
        }
        writer.prepareForFlush().flush();

        // when
        TransactionLogAnalyzer.analyze( fs, directory.absolutePath(), STRICT, monitor );

        // then
        assertEquals( expectedLogFiles, monitor.logFiles );
        assertEquals( expectedCheckpoints, monitor.checkpoints );
        assertEquals( expectedTransactions, monitor.transactions );
    }

    @Test
    public void shouldAnalyzeSingleLogWhenExplicitlySelected() throws Exception
    {
        // given
        writeTransactions( 2 ); // txs 2, 3
        long version = rotate();
        writeTransactions( 3 ); // txs 4, 5, 6
        writeCheckpoint();
        writeTransactions( 4 ); // txs 7, 8, 9, 10
        rotate();
        writeTransactions( 2 ); // txs 11, 12

        // when
        monitor.nextExpectedTxId = 4;
        monitor.nextExpectedLogVersion = version;
        TransactionLogAnalyzer.analyze( fs, logFiles.getLogFileForVersion( version ), STRICT, monitor );

        // then
        assertEquals( 1, monitor.logFiles );
        assertEquals( 1, monitor.checkpoints );
        assertEquals( 7, monitor.transactions );
    }

    private long rotate() throws IOException
    {
        logFile.rotate();
        return logVersionRepository.getCurrentLogVersion();
    }

    private static void assertTransaction( LogEntry[] transactionEntries, long expectedId )
    {
        assertTrue( Arrays.toString( transactionEntries ), transactionEntries[0] instanceof LogEntryStart );
        assertTrue( transactionEntries[1] instanceof LogEntryCommand );
        LogEntryCommand command = transactionEntries[1].as();
        assertEquals( expectedId, ((Command.NodeCommand)command.getCommand()).getKey() );
        assertTrue( transactionEntries[2] instanceof LogEntryCommit );
        LogEntryCommit commit = transactionEntries[2].as();
        assertEquals( expectedId, commit.getTxId() );
    }

    private void writeCheckpoint() throws IOException
    {
        transactionLogWriter.checkPoint( writer.getCurrentPosition( new LogPositionMarker() ).newPosition() );
        monitor.expectCheckpointAfter( lastCommittedTxId.get() );
    }

    private void writeTransactions( int count ) throws IOException
    {
        for ( int i = 0; i < count; i++ )
        {
            long id = lastCommittedTxId.incrementAndGet();
            transactionLogWriter.append( tx( id ), id );
        }
        writer.prepareForFlush().flush();
    }

    private TransactionRepresentation tx( long nodeId )
    {
        List<StorageCommand> commands = new ArrayList<>();
        commands.add( new Command.NodeCommand( new NodeRecord( nodeId ), new NodeRecord( nodeId )
                .initialize( true, nodeId, false, nodeId, 0 ) ) );
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return tx;
    }

    private static class VerifyingMonitor implements Monitor
    {
        private int transactions;
        private int checkpoints;
        private int logFiles;
        private long nextExpectedTxId = BASE_TX_ID + 1;
        private final PrimitiveLongArrayQueue expectedCheckpointsAt = new PrimitiveLongArrayQueue();
        private long nextExpectedLogVersion = BASE_TX_LOG_VERSION;

        void expectCheckpointAfter( long txId )
        {
            expectedCheckpointsAt.enqueue( txId );
        }

        @Override
        public void logFile( File file, long logVersion )
        {
            logFiles++;
            assertEquals( nextExpectedLogVersion++, logVersion );
        }

        @Override
        public void transaction( LogEntry[] transactionEntries )
        {
            transactions++;
            assertTransaction( transactionEntries, nextExpectedTxId++ );
        }

        @Override
        public void checkpoint( CheckPoint checkpoint, LogPosition checkpointEntryPosition )
        {
            checkpoints++;
            Long expected = expectedCheckpointsAt.dequeue();
            assertNotNull( "Unexpected checkpoint", expected );
            assertEquals( expected.longValue(), nextExpectedTxId - 1 );
        }
    }
}
