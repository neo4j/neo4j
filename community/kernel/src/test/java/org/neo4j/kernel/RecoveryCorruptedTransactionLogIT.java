/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MultiSet;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.CorruptedLogsTruncator;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_START;

public class RecoveryCorruptedTransactionLogIT
{
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fileSystemRule );
    private final ExpectedException expectedException = ExpectedException.none();
    private final RandomRule random = new RandomRule();
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( directory )
            .around( expectedException ).around( random );

    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private final RecoveryMonitor recoveryMonitor = new RecoveryMonitor();
    private File storeDir;
    private Monitors monitors = new Monitors();
    private PhysicalLogFiles logFiles;
    private TestGraphDatabaseFactory databaseFactory;

    @Before
    public void setUp() throws Exception
    {
        storeDir = directory.graphDbDir();
        monitors.addMonitorListener( recoveryMonitor );
        logFiles = new PhysicalLogFiles( storeDir, fileSystemRule );
        databaseFactory = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider ).setMonitors( monitors );
    }

    @After
    public void tearDown()
    {
        FeatureToggles.set( NeoStoreDataSource.class, "failOnCorruptedLogFiles", false );
    }

    @Test
    public void evenTruncateNewerTransactionLogFile() throws IOException
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long lastClosedTrandactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        long numberOfClosedTransactions = getTransactionIdStore( database ).getLastClosedTransactionId() -
                lastClosedTrandactionBeforeStart;
        database.shutdown();
        removeLastCheckpointRecordFromLastLogFile();
        addRandomLongsToLastLogFile();

        database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        database.shutdown();

        assertEquals( numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
    }

    @Test
    public void doNotTruncateNewerTransactionLogFileWhenFailOnError() throws IOException
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        database.shutdown();
        removeLastCheckpointRecordFromLastLogFile();
        addRandomLongsToLastLogFile();

        FeatureToggles.set( NeoStoreDataSource.class, "failOnCorruptedLogFiles", true );
        expectedException.expectCause( new RootCauseMatcher<>( UnsupportedLogVersionException.class ) );

        database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        database.shutdown();
    }

    @Test
    public void truncateNewerTransactionLogFileWhenForced() throws IOException
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long numberOfClosedTransactions = transactionIdStore.getLastClosedTransactionId() - 1;
        database.shutdown();

        removeLastCheckpointRecordFromLastLogFile();
        addRandomLongsToLastLogFile();

        database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        database.shutdown();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 0." );
        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 0. Last valid transaction start offset is: 5668." );
        assertEquals( numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
    }

    @Test
    public void recoverFirstCorruptedTransactionSingleFileNoCheckpoint() throws IOException
    {
        PositiveLogFilesBasedLogVersionRepository versionRepository = addCorruptedCommandsToLastLogFile();

        GraphDatabaseService recoveredDatabase = databaseFactory.newEmbeddedDatabase( storeDir );
        recoveredDatabase.shutdown();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 0." );
        logProvider.assertContainsMessageContaining( "Fail to read first transaction of log version 0." );
        logProvider.assertContainsMessageContaining(
                "Recovery required from position LogPosition{logVersion=0, byteOffset=16}" );
        logProvider.assertContainsMessageContaining( "Fail to recover all transactions. Any later transactions after" +
                " position LogPosition{logVersion=0, byteOffset=16} are unreadable and will be truncated." );

        assertEquals( 0, logFiles.getHighestLogVersion() );
        MultiSet<Class> logEntriesDistribution = getLogEntriesDistribution( logFiles, versionRepository );
        assertEquals( 1, logEntriesDistribution.size() );
        assertEquals( 1, logEntriesDistribution.count( CheckPoint.class ) );
    }

    @Test
    public void failToRecoverFirstCorruptedTransactionSingleFileNoCheckpointIfFailOnCorruption() throws IOException
    {
        addCorruptedCommandsToLastLogFile();

        FeatureToggles.set( NeoStoreDataSource.class, "failOnCorruptedLogFiles", true );
        expectedException.expectCause( new RootCauseMatcher<>( NegativeArraySizeException.class ) );

        GraphDatabaseService recoveredDatabase = databaseFactory.newEmbeddedDatabase( storeDir );
        recoveredDatabase.shutdown();
    }

    @Test
    public void recoverNotAFirstCorruptedTransactionSingleFileNoCheckpoint() throws IOException
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long lastClosedTrandactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        long numberOfTransactions = transactionIdStore.getLastClosedTransactionId() - lastClosedTrandactionBeforeStart;
        database.shutdown();

        File highestLogFile = logFiles.getHighestLogFile();
        long originalFileLength = highestLogFile.length();
        removeLastCheckpointRecordFromLastLogFile();

        PositiveLogFilesBasedLogVersionRepository versionRepository = addCorruptedCommandsToLastLogFile();
        long modifiedFileLength = highestLogFile.length();

        assertThat( modifiedFileLength, greaterThan( originalFileLength ) );

        database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        database.shutdown();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 0." );
        logProvider.assertContainsMessageContaining(
                "Recovery required from position LogPosition{logVersion=0, byteOffset=16}" );
        logProvider.assertContainsMessageContaining( "Fail to recover all transactions." );
        logProvider.assertContainsMessageContaining(
                "Any later transaction after LogPosition{logVersion=0, byteOffset=6245} are unreadable and will be truncated." );

        assertEquals( 0, logFiles.getHighestLogVersion() );
        MultiSet<Class> logEntriesDistribution = getLogEntriesDistribution( logFiles, versionRepository );
        assertEquals( 1, logEntriesDistribution.count( CheckPoint.class ) );
        assertEquals( numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
        assertEquals( originalFileLength, highestLogFile.length() );
    }

    @Test
    public void recoverNotAFirstCorruptedTransactionMultipleFilesNoCheckpoints() throws IOException
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long lastClosedTrandactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        generateTransactionsAndRotate( database, 3 );
        for ( int i = 0; i < 7; i++ )
        {
            generateTransaction( database );
        }
        long numberOfTransactions = transactionIdStore.getLastClosedTransactionId() - lastClosedTrandactionBeforeStart;
        database.shutdown();

        PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fileSystemRule );
        File highestLogFile = logFiles.getHighestLogFile();
        long originalFileLength = highestLogFile.length();
        removeLastCheckpointRecordFromLastLogFile();

        PositiveLogFilesBasedLogVersionRepository versionRepository = addCorruptedCommandsToLastLogFile();
        long modifiedFileLength = highestLogFile.length();

        assertThat( modifiedFileLength, greaterThan( originalFileLength ) );

        database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        database.shutdown();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 3." );
        logProvider.assertContainsMessageContaining(
                "Recovery required from position LogPosition{logVersion=0, byteOffset=16}" );
        logProvider.assertContainsMessageContaining( "Fail to recover all transactions." );
        logProvider.assertContainsMessageContaining(
                "Any later transaction after LogPosition{logVersion=3, byteOffset=4632} are unreadable and will be truncated." );

        assertEquals( 3, logFiles.getHighestLogVersion() );
        MultiSet<Class> logEntriesDistribution = getLogEntriesDistribution( logFiles, versionRepository );
        assertEquals( 1, logEntriesDistribution.count( CheckPoint.class ) );
        assertEquals( numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
        assertEquals( originalFileLength, highestLogFile.length() );
    }

    @Test
    public void recoverNotAFirstCorruptedTransactionMultipleFilesMultipleCheckpoints() throws IOException
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        long transactionsToRecover = 7;
        generateTransactionsAndRotateWithCheckpoint( database, 3 );
        for ( int i = 0; i < transactionsToRecover; i++ )
        {
            generateTransaction( database );
        }
        database.shutdown();

        File highestLogFile = logFiles.getHighestLogFile();
        long originalFileLength = highestLogFile.length();
        removeLastCheckpointRecordFromLastLogFile();

        PositiveLogFilesBasedLogVersionRepository versionRepository = addCorruptedCommandsToLastLogFile();
        long modifiedFileLength = highestLogFile.length();

        assertThat( modifiedFileLength, greaterThan( originalFileLength ) );

        database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        database.shutdown();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 3." );
        logProvider.assertContainsMessageContaining(
                "Recovery required from position LogPosition{logVersion=3, byteOffset=593}" );
        logProvider.assertContainsMessageContaining( "Fail to recover all transactions." );
        logProvider.assertContainsMessageContaining(
                "Any later transaction after LogPosition{logVersion=3, byteOffset=4650} are unreadable and will be truncated." );

        assertEquals( 3, logFiles.getHighestLogVersion() );
        MultiSet<Class> logEntriesDistribution = getLogEntriesDistribution( logFiles, versionRepository );
        assertEquals( 4, logEntriesDistribution.count( CheckPoint.class ) );
        assertEquals( transactionsToRecover, recoveryMonitor.getNumberOfRecoveredTransactions() );
        assertEquals( originalFileLength, highestLogFile.length() );
    }

    @Test
    public void recoverFirstCorruptedTransactionAfterCheckpointInLastLogFile() throws IOException
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        generateTransactionsAndRotate( database, 5 );
        database.shutdown();

        File highestLogFile = logFiles.getHighestLogFile();
        long originalFileLength = highestLogFile.length();
        PositiveLogFilesBasedLogVersionRepository versionRepository = addCorruptedCommandsToLastLogFile();
        long modifiedFileLength = highestLogFile.length();

        assertThat( modifiedFileLength, greaterThan( originalFileLength ) );

        database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        database.shutdown();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 5." );
        logProvider.assertContainsMessageContaining( "Fail to read first transaction of log version 5." );
        logProvider.assertContainsMessageContaining(
                "Recovery required from position LogPosition{logVersion=5, byteOffset=593}" );
        logProvider.assertContainsMessageContaining( "Fail to recover all transactions. " +
                "Any later transactions after position LogPosition{logVersion=5, byteOffset=593} " +
                "are unreadable and will be truncated." );

        assertEquals( 5, logFiles.getHighestLogVersion() );
        MultiSet<Class> logEntriesDistribution = getLogEntriesDistribution( logFiles, versionRepository );
        assertEquals( 1, logEntriesDistribution.count( CheckPoint.class ) );
        assertEquals( originalFileLength, highestLogFile.length() );
    }

    @Test
    public void repetitiveRecoveryOfCorruptedLogs() throws IOException
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        generateTransactionsAndRotate( database, 4, false );
        database.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        int expectedRecoveredTransactions = 7;
        while ( expectedRecoveredTransactions > 0 )
        {
            truncateBytesFromLastLogFile( 1 + random.nextInt( 10 ) );
            databaseFactory.newEmbeddedDatabase( storeDir ).shutdown();
            int numberOfRecoveredTransactions = recoveryMonitor.getNumberOfRecoveredTransactions();
            assertEquals( expectedRecoveredTransactions, numberOfRecoveredTransactions );
            expectedRecoveredTransactions--;
            removeLastCheckpointRecordFromLastLogFile();
        }
    }

    @Test
    public void repetitiveRecoveryIfCorruptedLogsWithCheckpoints() throws IOException
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseFactory.newEmbeddedDatabase( storeDir );
        generateTransactionsAndRotate( database, 4, true );
        database.shutdown();

        while ( logFiles.getHighestLogVersion() > 0 )
        {
            truncateBytesFromLastLogFile( 1 + random.nextInt( 100 ) );
            databaseFactory.newEmbeddedDatabase( storeDir ).shutdown();
            int numberOfRecoveredTransactions = recoveryMonitor.getNumberOfRecoveredTransactions();
            assertThat( numberOfRecoveredTransactions, Matchers.greaterThanOrEqualTo( 0 ) );
        }

        File corruptedLogArchives = new File( storeDir, CorruptedLogsTruncator.CORRUPTED_TX_LOGS_FOLDER_NAME );
        assertThat( corruptedLogArchives.listFiles(), not( emptyArray() ) );
    }

    private TransactionIdStore getTransactionIdStore( GraphDatabaseAPI database )
    {
        return database.getDependencyResolver().resolveDependency( TransactionIdStore.class );
    }

    private void removeLastCheckpointRecordFromLastLogFile() throws IOException
    {
        PositiveLogFilesBasedLogVersionRepository versionRepository =
                new PositiveLogFilesBasedLogVersionRepository( logFiles );
        LogPosition checkpointPosition = null;
        try ( Lifespan lifespan = new Lifespan() )
        {
            PhysicalLogFile physicalLogFile =
                    new PhysicalLogFile( fileSystemRule, logFiles, Long.MAX_VALUE, () -> Long.MAX_VALUE,
                            versionRepository, PhysicalLogFile.NO_MONITOR, new LogHeaderCache( 100 ) );
            lifespan.add( physicalLogFile );
            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader();
            ReadableLogChannel reader =
                    physicalLogFile.getReader( LogPosition.start( logFiles.getHighestLogVersion() ) );
            LogEntry logEntry;
            do
            {
                logEntry = entryReader.readLogEntry( reader );
                if ( logEntry instanceof CheckPoint )
                {
                    checkpointPosition = ((CheckPoint) logEntry).getLogPosition();
                }
            }
            while ( logEntry != null );
        }
        if ( checkpointPosition != null )
        {
            try ( StoreChannel storeChannel = fileSystemRule.open( logFiles.getHighestLogFile(), "rw" ) )
            {
                storeChannel.truncate( checkpointPosition.getByteOffset() );
            }
        }
    }

    private void truncateBytesFromLastLogFile( long bytesToTrim ) throws IOException
    {
        File highestLogFile = logFiles.getHighestLogFile();
        long fileSize = fileSystemRule.getFileSize( highestLogFile );
        if ( bytesToTrim > fileSize )
        {
            fileSystemRule.deleteFile( highestLogFile );
        }
        else
        {
            fileSystemRule.truncate( highestLogFile, fileSize - bytesToTrim );
        }
    }

    private void addRandomLongsToLastLogFile() throws IOException
    {
        PositiveLogFilesBasedLogVersionRepository versionRepository =
                new PositiveLogFilesBasedLogVersionRepository( logFiles );
        try ( Lifespan lifespan = new Lifespan() )
        {
            PhysicalLogFile physicalLogFile =
                    new PhysicalLogFile( fileSystemRule, logFiles, Long.MAX_VALUE, () -> Long.MAX_VALUE,
                            versionRepository, PhysicalLogFile.NO_MONITOR, new LogHeaderCache( 100 ) );
            lifespan.add( physicalLogFile );

            FlushablePositionAwareChannel logFileWriter = physicalLogFile.getWriter();
            ThreadLocalRandom current = ThreadLocalRandom.current();
            for ( int i = 0; i < 10; i++ )
            {
                logFileWriter.putLong( current.nextLong() );
            }
        }
    }

    private PositiveLogFilesBasedLogVersionRepository addCorruptedCommandsToLastLogFile()
            throws IOException
    {
        PositiveLogFilesBasedLogVersionRepository versionRepository =
                new PositiveLogFilesBasedLogVersionRepository( logFiles );
        try ( Lifespan lifespan = new Lifespan() )
        {
            PhysicalLogFile physicalLogFile =
                    new PhysicalLogFile( fileSystemRule, logFiles, Long.MAX_VALUE, () -> Long.MAX_VALUE,
                            versionRepository, PhysicalLogFile.NO_MONITOR, new LogHeaderCache( 100 ) );
            lifespan.add( physicalLogFile );

            FlushablePositionAwareChannel channel = physicalLogFile.getWriter();
            TransactionLogWriter writer = new TransactionLogWriter( new CorruptedLogEntryWriter( channel ) );

            Collection<StorageCommand> commands = new ArrayList<>();
            commands.add( new Command.PropertyCommand( new PropertyRecord( 1 ), new PropertyRecord( 2 ) ) );
            commands.add( new Command.NodeCommand( new NodeRecord( 2 ), new NodeRecord( 3 ) ) );
            PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( commands );
            writer.append( transaction, 1000 );
        }
        return versionRepository;
    }

    private MultiSet<Class> getLogEntriesDistribution( PhysicalLogFiles logFiles,
            PositiveLogFilesBasedLogVersionRepository versionRepository ) throws IOException
    {
        try ( Lifespan lifespan = new Lifespan() )
        {
            PhysicalLogFile physicalLogFile =
                    new PhysicalLogFile( fileSystemRule, logFiles, Long.MAX_VALUE, () -> Long.MAX_VALUE,
                            versionRepository, PhysicalLogFile.NO_MONITOR, new LogHeaderCache( 100 ) );
            lifespan.add( physicalLogFile );

            LogPosition fileStartPosition = new LogPosition( 0, LogHeader.LOG_HEADER_SIZE );
            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader();

            MultiSet<Class> multiset = new MultiSet<>();
            ReadableLogChannel fileReader = physicalLogFile.getReader( fileStartPosition );
            LogEntry logEntry = entryReader.readLogEntry( fileReader );
            while ( logEntry != null )
            {
                multiset.add( logEntry.getClass() );
                logEntry = entryReader.readLogEntry( fileReader );
            }
            return multiset;
        }
    }

    private void generateTransactionsAndRotateWithCheckpoint( GraphDatabaseAPI database, int logFilesToGenerate )
            throws IOException
    {
        generateTransactionsAndRotate( database, logFilesToGenerate, true );
    }

    private void generateTransactionsAndRotate( GraphDatabaseAPI database, int logFilesToGenerate ) throws IOException
    {
        generateTransactionsAndRotate( database, logFilesToGenerate, false );
    }

    private void generateTransactionsAndRotate( GraphDatabaseAPI database, int logFilesToGenerate, boolean checkpoint )
            throws IOException
    {
        DependencyResolver resolver = database.getDependencyResolver();
        PhysicalLogFile logFile = resolver.resolveDependency( PhysicalLogFile.class );
        CheckPointer checkpointer = resolver.resolveDependency( CheckPointer.class );
        while ( logFile.currentLogVersion() < logFilesToGenerate )
        {
            logFile.rotate();
            generateTransaction( database );
            if ( checkpoint )
            {
                checkpointer.forceCheckPoint( new SimpleTriggerInfo( "testForcedCheckpoint" ) );
            }
        }
    }

    private void generateTransaction( GraphDatabaseAPI database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Node startNode = database.createNode( Label.label( "startNode" ) );
            startNode.setProperty( "key", "value" );
            Node endNode = database.createNode( Label.label( "endNode" ) );
            endNode.setProperty( "key", "value" );
            startNode.createRelationshipTo( endNode, RelationshipType.withName( "connects" ) );
            transaction.success();
        }
    }

    private static class CorruptedLogEntryWriter extends LogEntryWriter
    {

        CorruptedLogEntryWriter( FlushableChannel channel )
        {
            super( channel );
        }

        @Override
        public void writeStartEntry( int masterId, int authorId, long timeWritten, long latestCommittedTxWhenStarted,
                byte[] additionalHeaderData ) throws IOException
        {
            writeLogEntryHeader( TX_START );
        }
    }

    private static class RecoveryMonitor implements org.neo4j.kernel.recovery.RecoveryMonitor
    {
        private List<Long> recoveredTransactions = new ArrayList<>();
        private int numberOfRecoveredTransactions;

        @Override
        public void recoveryRequired( LogPosition recoveryPosition )
        {
        }

        @Override
        public void transactionRecovered( long txId )
        {
            recoveredTransactions.add( txId );
        }

        @Override
        public void recoveryCompleted( int numberOfRecoveredTransactions )
        {
            this.numberOfRecoveredTransactions = numberOfRecoveredTransactions;
        }

        int getNumberOfRecoveredTransactions()
        {
            return numberOfRecoveredTransactions;
        }
    }

    private class PositiveLogFilesBasedLogVersionRepository implements LogVersionRepository
    {

        private long version;

        PositiveLogFilesBasedLogVersionRepository( PhysicalLogFiles logFiles )
        {
            this.version = (logFiles.getHighestLogVersion() == -1) ? 0 : logFiles.getHighestLogVersion();
        }

        @Override
        public long getCurrentLogVersion()
        {
            return version;
        }

        @Override
        public void setCurrentLogVersion( long version )
        {
            this.version = version;
        }

        @Override
        public long incrementAndGetVersion()
        {
            version++;
            return version;
        }
    }
}
