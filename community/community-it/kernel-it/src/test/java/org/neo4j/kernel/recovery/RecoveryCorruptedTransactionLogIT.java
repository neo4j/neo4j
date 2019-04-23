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

import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.api.map.primitive.ObjectLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_START;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class, RandomExtension.class} )
class RecoveryCorruptedTransactionLogIT
{
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory directory;
    @Inject
    private RandomRule random;

    private static final byte CHECKPOINT_COMMAND_SIZE = 2 /*header*/ + 2 * Long.BYTES /*command content*/;
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private final RecoveryMonitor recoveryMonitor = new RecoveryMonitor();
    private File databaseDirectory;
    private final Monitors monitors = new Monitors();
    private LogFiles logFiles;
    private TestDatabaseManagementServiceBuilder databaseFactory;

    @BeforeEach
    void setUp() throws Exception
    {
        databaseDirectory = directory.storeDir();
        monitors.addMonitorListener( recoveryMonitor );
        databaseFactory = new TestDatabaseManagementServiceBuilder( databaseDirectory )
                .setInternalLogProvider( logProvider )
                .setMonitors( monitors )
                .setFileSystem( fileSystem );
        createEmptyDatabase();
        logFiles = buildDefaultLogFiles();
    }

    @Test
    void evenTruncateNewerTransactionLogFile() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        long numberOfClosedTransactions = getTransactionIdStore( database ).getLastClosedTransactionId() -
                lastClosedTransactionBeforeStart;
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();
        addRandomBytesToLastLogFile( this::randomBytes );

        startStopDbNoRecoveryOfCorruptedLogs();

        assertEquals( numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
    }

    @Test
    void doNotTruncateNewerTransactionLogFileWhenFailOnError() throws IOException
    {
        DatabaseManagementService managementService1 = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService1.database( DEFAULT_DATABASE_NAME );
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        managementService1.shutdown();
        removeLastCheckpointRecordFromLastLogFile();
        addRandomBytesToLastLogFile( this::randomPositiveBytes );

        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            DatabaseManager<?> databaseManager = db.getDependencyResolver().resolveDependency( DatabaseManager.class );
            DatabaseContext databaseContext = databaseManager.getDatabaseContext( new DatabaseId( DEFAULT_DATABASE_NAME ) ).get();
            assertTrue( databaseContext.isFailed() );
            assertThat( databaseContext.failureCause(), new RootCauseMatcher<>( UnsupportedLogVersionException.class ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void truncateNewerTransactionLogFileWhenForced() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long numberOfClosedTransactions = transactionIdStore.getLastClosedTransactionId() - 1;
        managementService.shutdown();

        removeLastCheckpointRecordFromLastLogFile();
        addRandomBytesToLastLogFile( this::randomBytes );

        startStopDbNoRecoveryOfCorruptedLogs();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 0." );
        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 0. Last valid transaction start offset is: 5686." );
        assertEquals( numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
    }

    @Test
    void recoverFirstCorruptedTransactionSingleFileNoCheckpoint() throws IOException
    {
        addCorruptedCommandsToLastLogFile();

        startStopDbNoRecoveryOfCorruptedLogs();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 0." );
        logProvider.assertContainsMessageContaining( "Fail to read first transaction of log version 0." );
        logProvider.assertContainsMessageContaining(
                "Recovery required from position LogPosition{logVersion=0, byteOffset=16}" );
        logProvider.assertContainsMessageContaining( "Fail to recover all transactions. Any later transactions after" +
                " position LogPosition{logVersion=0, byteOffset=16} are unreadable and will be truncated." );

        assertEquals( 0, logFiles.getHighestLogVersion() );
        ObjectLongMap<Class> logEntriesDistribution = getLogEntriesDistribution( logFiles );
        assertEquals( 1, logEntriesDistribution.size() );
        assertEquals( 2, logEntriesDistribution.get( CheckPoint.class ) );
    }

    @Test
    void failToRecoverFirstCorruptedTransactionSingleFileNoCheckpointIfFailOnCorruption() throws IOException
    {
        addCorruptedCommandsToLastLogFile();

        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            DatabaseManager<?> databaseManager = db.getDependencyResolver().resolveDependency( DatabaseManager.class );
            DatabaseContext databaseContext = databaseManager.getDatabaseContext( new DatabaseId( DEFAULT_DATABASE_NAME ) ).get();
            assertTrue( databaseContext.isFailed() );
            assertThat( databaseContext.failureCause(), new RootCauseMatcher<>( NegativeArraySizeException.class ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverNotAFirstCorruptedTransactionSingleFileNoCheckpoint() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        long numberOfTransactions = transactionIdStore.getLastClosedTransactionId() - lastClosedTransactionBeforeStart;
        managementService.shutdown();

        File highestLogFile = logFiles.getHighestLogFile();
        long originalFileLength = highestLogFile.length();
        removeLastCheckpointRecordFromLastLogFile();

        addCorruptedCommandsToLastLogFile();
        long modifiedFileLength = highestLogFile.length();

        assertThat( modifiedFileLength, greaterThan( originalFileLength ) );

        startStopDbNoRecoveryOfCorruptedLogs();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 0." );
        logProvider.assertContainsMessageContaining(
                "Recovery required from position LogPosition{logVersion=0, byteOffset=16}" );
        logProvider.assertContainsMessageContaining( "Fail to recover all transactions." );
        logProvider.assertContainsMessageContaining(
                "Any later transaction after LogPosition{logVersion=0, byteOffset=6263} are unreadable and will be truncated." );

        assertEquals( 0, logFiles.getHighestLogVersion() );
        ObjectLongMap<Class> logEntriesDistribution = getLogEntriesDistribution( logFiles );
        // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
        // during the setup and starting db as part of the test
        assertEquals( 3, logEntriesDistribution.get( CheckPoint.class ) );
        assertEquals( numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
        assertEquals( originalFileLength + CHECKPOINT_COMMAND_SIZE, highestLogFile.length() );
    }

    @Test
    void recoverNotAFirstCorruptedTransactionMultipleFilesNoCheckpoints() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        generateTransactionsAndRotate( database, 3 );
        for ( int i = 0; i < 7; i++ )
        {
            generateTransaction( database );
        }
        long numberOfTransactions = transactionIdStore.getLastClosedTransactionId() - lastClosedTransactionBeforeStart;
        managementService.shutdown();

        LogFiles logFiles = buildDefaultLogFiles();
        File highestLogFile = logFiles.getHighestLogFile();
        long originalFileLength = highestLogFile.length();
        removeLastCheckpointRecordFromLastLogFile();

        addCorruptedCommandsToLastLogFile();
        long modifiedFileLength = highestLogFile.length();

        assertThat( modifiedFileLength, greaterThan( originalFileLength ) );

        startStopDbNoRecoveryOfCorruptedLogs();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 3." );
        logProvider.assertContainsMessageContaining(
                "Recovery required from position LogPosition{logVersion=0, byteOffset=16}" );
        logProvider.assertContainsMessageContaining( "Fail to recover all transactions." );
        logProvider.assertContainsMessageContaining(
                "Any later transaction after LogPosition{logVersion=3, byteOffset=4632} are unreadable and will be truncated." );

        assertEquals( 3, logFiles.getHighestLogVersion() );
        ObjectLongMap<Class> logEntriesDistribution = getLogEntriesDistribution( logFiles );
        // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
        // during the setup and starting db as part of the test
        assertEquals( 3, logEntriesDistribution.get( CheckPoint.class ) );
        assertEquals( numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
        assertEquals( originalFileLength + CHECKPOINT_COMMAND_SIZE, highestLogFile.length() );
    }

    @Test
    void recoverNotAFirstCorruptedTransactionMultipleFilesMultipleCheckpoints() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        long transactionsToRecover = 7;
        generateTransactionsAndRotateWithCheckpoint( database, 3 );
        for ( int i = 0; i < transactionsToRecover; i++ )
        {
            generateTransaction( database );
        }
        managementService.shutdown();

        File highestLogFile = logFiles.getHighestLogFile();
        long originalFileLength = highestLogFile.length();
        removeLastCheckpointRecordFromLastLogFile();

        addCorruptedCommandsToLastLogFile();
        long modifiedFileLength = highestLogFile.length();

        assertThat( modifiedFileLength, greaterThan( originalFileLength ) );

        startStopDbNoRecoveryOfCorruptedLogs();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 3." );
        logProvider.assertContainsMessageContaining(
                "Recovery required from position LogPosition{logVersion=3, byteOffset=593}" );
        logProvider.assertContainsMessageContaining( "Fail to recover all transactions." );
        logProvider.assertContainsMessageContaining(
                "Any later transaction after LogPosition{logVersion=3, byteOffset=4650} are unreadable and will be truncated." );

        assertEquals( 3, logFiles.getHighestLogVersion() );
        ObjectLongMap<Class> logEntriesDistribution = getLogEntriesDistribution( logFiles );
        assertEquals( 6, logEntriesDistribution.get( CheckPoint.class ) );
        assertEquals( transactionsToRecover, recoveryMonitor.getNumberOfRecoveredTransactions() );
        assertEquals( originalFileLength + CHECKPOINT_COMMAND_SIZE, highestLogFile.length() );
    }

    @Test
    void recoverFirstCorruptedTransactionAfterCheckpointInLastLogFile() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        generateTransactionsAndRotate( database, 5 );
        managementService.shutdown();

        File highestLogFile = logFiles.getHighestLogFile();
        long originalFileLength = highestLogFile.length();
        addCorruptedCommandsToLastLogFile();
        long modifiedFileLength = highestLogFile.length();

        assertThat( modifiedFileLength, greaterThan( originalFileLength ) );

        startStopDbNoRecoveryOfCorruptedLogs();

        logProvider.assertContainsMessageContaining( "Fail to read transaction log version 5." );
        logProvider.assertContainsMessageContaining( "Fail to read first transaction of log version 5." );
        logProvider.assertContainsMessageContaining(
                "Recovery required from position LogPosition{logVersion=5, byteOffset=593}" );
        logProvider.assertContainsMessageContaining( "Fail to recover all transactions. " +
                "Any later transactions after position LogPosition{logVersion=5, byteOffset=593} " +
                "are unreadable and will be truncated." );

        assertEquals( 5, logFiles.getHighestLogVersion() );
        ObjectLongMap<Class> logEntriesDistribution = getLogEntriesDistribution( logFiles );
        // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
        // during the setup and starting db as part of the test
        assertEquals( 3, logEntriesDistribution.get( CheckPoint.class ) );
        assertEquals( originalFileLength + CHECKPOINT_COMMAND_SIZE, highestLogFile.length() );
    }

    @Test
    void repetitiveRecoveryOfCorruptedLogs() throws IOException
    {
        DatabaseManagementService managementService1 = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService1.database( DEFAULT_DATABASE_NAME );
        generateTransactionsAndRotate( database, 4, false );
        managementService1.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        int expectedRecoveredTransactions = 7;
        while ( expectedRecoveredTransactions > 0 )
        {
            truncateBytesFromLastLogFile( 1 + random.nextInt( 10 ) );
            DatabaseManagementService managementService = databaseFactory.build();
            managementService.shutdown();
            int numberOfRecoveredTransactions = recoveryMonitor.getNumberOfRecoveredTransactions();
            assertEquals( expectedRecoveredTransactions, numberOfRecoveredTransactions );
            expectedRecoveredTransactions--;
            removeLastCheckpointRecordFromLastLogFile();
        }
    }

    @Test
    void repetitiveRecoveryIfCorruptedLogsWithCheckpoints() throws IOException
    {
        DatabaseManagementService managementService1 = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService1.database( DEFAULT_DATABASE_NAME );
        generateTransactionsAndRotate( database, 4, true );
        managementService1.shutdown();

        while ( logFiles.getHighestLogVersion() > 0 )
        {
            int bytesToTrim = 1 + CHECKPOINT_COMMAND_SIZE + random.nextInt( 100 );
            truncateBytesFromLastLogFile( bytesToTrim );
            DatabaseManagementService managementService = databaseFactory.build();
            managementService.shutdown();
            int numberOfRecoveredTransactions = recoveryMonitor.getNumberOfRecoveredTransactions();
            assertThat( numberOfRecoveredTransactions, Matchers.greaterThanOrEqualTo( 0 ) );
        }

        File corruptedLogArchives = new File( databaseDirectory, CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME );
        assertThat( corruptedLogArchives.listFiles(), not( emptyArray() ) );
    }

    @Test
    void repetitiveRecoveryIfCorruptedLogsSmallTailsWithCheckpoints() throws IOException
    {
        DatabaseManagementService managementService1 = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService1.database( DEFAULT_DATABASE_NAME );
        generateTransactionsAndRotate( database, 4, true );
        managementService1.shutdown();

        byte[] trimSizes = new byte[]{4, 22};
        int trimSize = 0;
        while ( logFiles.getHighestLogVersion() > 0 )
        {
            byte bytesToTrim = (byte) (trimSizes[trimSize++ % trimSizes.length] + CHECKPOINT_COMMAND_SIZE);
            truncateBytesFromLastLogFile( bytesToTrim );
            DatabaseManagementService managementService = databaseFactory.build();
            managementService.shutdown();
            int numberOfRecoveredTransactions = recoveryMonitor.getNumberOfRecoveredTransactions();
            assertThat( numberOfRecoveredTransactions, Matchers.greaterThanOrEqualTo( 0 ) );
        }

        File corruptedLogArchives = new File( databaseDirectory, CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME );
        assertThat( corruptedLogArchives.listFiles(), not( emptyArray() ) );
    }

    private static TransactionIdStore getTransactionIdStore( GraphDatabaseAPI database )
    {
        return database.getDependencyResolver().resolveDependency( TransactionIdStore.class );
    }

    private void removeLastCheckpointRecordFromLastLogFile() throws IOException
    {
        LogPosition checkpointPosition = null;

        LogFile transactionLogFile = logFiles.getLogFile();
        VersionAwareLogEntryReader<ReadableLogChannel> entryReader = new VersionAwareLogEntryReader<>();
        LogPosition startPosition = LogPosition.start( logFiles.getHighestLogVersion() );
        try ( ReadableLogChannel reader = transactionLogFile.getReader( startPosition ) )
        {
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
            try ( StoreChannel storeChannel = fileSystem.write( logFiles.getHighestLogFile() ) )
            {
                storeChannel.truncate( checkpointPosition.getByteOffset() );
            }
        }
    }

    private void truncateBytesFromLastLogFile( long bytesToTrim ) throws IOException
    {
        File highestLogFile = logFiles.getHighestLogFile();
        long fileSize = fileSystem.getFileSize( highestLogFile );
        if ( bytesToTrim > fileSize )
        {
            fileSystem.deleteFile( highestLogFile );
        }
        else
        {
            fileSystem.truncate( highestLogFile, fileSize - bytesToTrim );
        }
    }

    private void addRandomBytesToLastLogFile( Supplier<Byte> byteSource ) throws IOException
    {
        try ( Lifespan lifespan = new Lifespan() )
        {
            LogFile transactionLogFile = logFiles.getLogFile();
            lifespan.add( logFiles );

            FlushablePositionAwareChannel logFileWriter = transactionLogFile.getWriter();
            for ( int i = 0; i < 10; i++ )
            {
                logFileWriter.put( byteSource.get() );
            }
        }
    }

    private byte randomPositiveBytes()
    {
        return (byte) random.nextInt( 0, Byte.MAX_VALUE );
    }

    private byte randomBytes()
    {
        return (byte) random.nextInt( Byte.MIN_VALUE, Byte.MAX_VALUE );
    }

    private void addCorruptedCommandsToLastLogFile() throws IOException
    {
        PositiveLogFilesBasedLogVersionRepository versionRepository = new PositiveLogFilesBasedLogVersionRepository( logFiles );
        LogFiles internalLogFiles = LogFilesBuilder.builder( directory.databaseLayout(), fileSystem )
                .withLogVersionRepository( versionRepository )
                .withTransactionIdStore( new SimpleTransactionIdStore() ).build();
        try ( Lifespan lifespan = new Lifespan( internalLogFiles ) )
        {
            LogFile transactionLogFile = internalLogFiles.getLogFile();

            FlushablePositionAwareChannel channel = transactionLogFile.getWriter();
            TransactionLogWriter writer = new TransactionLogWriter( new CorruptedLogEntryWriter( channel ) );

            Collection<StorageCommand> commands = new ArrayList<>();
            commands.add( new Command.PropertyCommand( new PropertyRecord( 1 ), new PropertyRecord( 2 ) ) );
            commands.add( new Command.NodeCommand( new NodeRecord( 2 ), new NodeRecord( 3 ) ) );
            PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( commands );
            writer.append( transaction, 1000 );
        }
    }

    private static ObjectLongMap<Class> getLogEntriesDistribution( LogFiles logFiles ) throws IOException
    {
        LogFile transactionLogFile = logFiles.getLogFile();

        LogPosition fileStartPosition = new LogPosition( 0, LogHeader.LOG_HEADER_SIZE );
        VersionAwareLogEntryReader<ReadableLogChannel> entryReader = new VersionAwareLogEntryReader<>();

        MutableObjectLongMap<Class> multiset = new ObjectLongHashMap<>();
        try ( ReadableLogChannel fileReader = transactionLogFile.getReader( fileStartPosition ) )
        {
            LogEntry logEntry = entryReader.readLogEntry( fileReader );
            while ( logEntry != null )
            {
                multiset.addToValue( logEntry.getClass(), 1 );
                logEntry = entryReader.readLogEntry( fileReader );
            }
        }
        return multiset;
    }

    private LogFiles buildDefaultLogFiles() throws IOException
    {
        return LogFilesBuilder.builder( directory.databaseLayout(), fileSystem )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() ).build();
    }

    private static void generateTransactionsAndRotateWithCheckpoint( GraphDatabaseAPI database, int logFilesToGenerate )
            throws IOException
    {
        generateTransactionsAndRotate( database, logFilesToGenerate, true );
    }

    private static void generateTransactionsAndRotate( GraphDatabaseAPI database, int logFilesToGenerate ) throws IOException
    {
        generateTransactionsAndRotate( database, logFilesToGenerate, false );
    }

    private static void generateTransactionsAndRotate( GraphDatabaseAPI database, int logFilesToGenerate, boolean checkpoint )
            throws IOException
    {
        DependencyResolver resolver = database.getDependencyResolver();
        LogFiles logFiles = resolver.resolveDependency( TransactionLogFiles.class );
        CheckPointer checkpointer = resolver.resolveDependency( CheckPointer.class );
        while ( logFiles.getHighestLogVersion() < logFilesToGenerate )
        {
            logFiles.getLogFile().rotate();
            generateTransaction( database );
            if ( checkpoint )
            {
                checkpointer.forceCheckPoint( new SimpleTriggerInfo( "testForcedCheckpoint" ) );
            }
        }
    }

    private static void generateTransaction( GraphDatabaseAPI database )
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

    private void startStopDbNoRecoveryOfCorruptedLogs()
    {
        DatabaseManagementService managementService = databaseFactory
                .setConfig( GraphDatabaseSettings.fail_on_corrupted_log_files, Settings.FALSE ).build();
        managementService.shutdown();
    }

    private void createEmptyDatabase()
    {
        DatabaseManagementService managementService = databaseFactory.build();
        managementService.shutdown();
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
            writeLogEntryHeader( TX_START, channel );
        }
    }

    private static class RecoveryMonitor implements org.neo4j.kernel.recovery.RecoveryMonitor
    {
        private final List<Long> recoveredTransactions = new ArrayList<>();
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

    private static class PositiveLogFilesBasedLogVersionRepository implements LogVersionRepository
    {

        private long version;

        PositiveLogFilesBasedLogVersionRepository( LogFiles logFiles )
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
