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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

import static java.lang.String.valueOf;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP;
import static org.neo4j.kernel.impl.store.MetaDataStore.getRecord;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;

@PageCacheExtension
@Neo4jLayoutExtension
class RecoveryIT
{
    private static final int TEN_KB = (int) ByteUnit.kibiBytes( 10 );
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    @Inject
    private Neo4jLayout neo4jLayout;
    @Inject
    private DatabaseLayout databaseLayout;
    private DatabaseManagementService managementService;

    @Test
    void recoveryRequiredOnDatabaseWithoutCorrectCheckpoints() throws Exception
    {
        GraphDatabaseService database = createDatabase();
        generateSomeData( database );
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        assertTrue( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );
    }

    @Test
    void recoveryNotRequiredWhenDatabaseNotFound() throws Exception
    {
        DatabaseLayout absentDatabase = neo4jLayout.databaseLayout( "absent" );
        assertFalse( isRecoveryRequired( fileSystem, absentDatabase, defaults() ) );
    }

    @Test
    void recoverEmptyDatabase() throws Exception
    {
        createDatabase();
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        assertFalse( isRecoveryRequired( databaseLayout, defaults() ) );
    }

    @Test
    void recoverDatabaseWithNodes() throws Exception
    {
        GraphDatabaseService database = createDatabase();

        int numberOfNodes = 10;
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            createSingleNode( database );
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try ( Transaction tx = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfNodes, count( tx.getAllNodes() ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithNodesAndRelationshipsAndRelationshipTypes() throws Exception
    {
        GraphDatabaseService database = createDatabase();

        int numberOfRelationships = 10;
        int numberOfNodes = numberOfRelationships * 2;
        for ( int i = 0; i < numberOfRelationships; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node start = transaction.createNode();
                Node stop = transaction.createNode();
                start.createRelationshipTo( stop, withName( valueOf( i ) ) );
                transaction.commit();
            }
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try ( Transaction transaction = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfNodes, count( transaction.getAllNodes() ) );
            assertEquals( numberOfRelationships, count( transaction.getAllRelationships() ) );
            assertEquals( numberOfRelationships, count( transaction.getAllRelationshipTypesInUse() ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithProperties() throws Exception
    {
        GraphDatabaseService database = createDatabase();

        int numberOfRelationships = 10;
        int numberOfNodes = numberOfRelationships * 2;
        for ( int i = 0; i < numberOfRelationships; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node start = transaction.createNode();
                Node stop = transaction.createNode();
                start.setProperty( "start" + i, i );
                stop.setProperty( "stop" + i, i );
                start.createRelationshipTo( stop, withName( valueOf( i ) ) );
                transaction.commit();
            }
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try ( Transaction transaction = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfNodes, count( transaction.getAllNodes() ) );
            assertEquals( numberOfRelationships, count( transaction.getAllRelationships() ) );
            assertEquals( numberOfRelationships, count( transaction.getAllRelationshipTypesInUse() ) );
            assertEquals( numberOfNodes, count( transaction.getAllPropertyKeys() ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithIndex() throws Exception
    {
        GraphDatabaseService database = createDatabase();

        int numberOfRelationships = 10;
        int numberOfNodes = numberOfRelationships * 2;
        String startProperty = "start";
        String stopProperty = "stop";
        Label startMarker = Label.label( "start" );
        Label stopMarker = Label.label( "stop" );

        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().indexFor( startMarker ).on( startProperty ).create();
            transaction.schema().constraintFor( stopMarker ).assertPropertyIsUnique( stopProperty ).create();
            transaction.commit();
        }
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            transaction.commit();
        }

        for ( int i = 0; i < numberOfRelationships; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node start = transaction.createNode( startMarker );
                Node stop = transaction.createNode( stopMarker );

                start.setProperty( startProperty, i );
                stop.setProperty( stopProperty, i );
                start.createRelationshipTo( stop, withName( valueOf( i ) ) );
                transaction.commit();
            }
        }
        long numberOfPropertyKeys;
        try ( Transaction transaction = database.beginTx() )
        {
            numberOfPropertyKeys = count( transaction.getAllPropertyKeys() );
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        recoverDatabase();

        GraphDatabaseService recoveredDatabase = createDatabase();
        try ( Transaction transaction = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfNodes, count( transaction.getAllNodes() ) );
            assertEquals( numberOfRelationships, count( transaction.getAllRelationships() ) );
            assertEquals( numberOfRelationships, count( transaction.getAllRelationshipTypesInUse() ) );
            assertEquals( numberOfPropertyKeys, count( transaction.getAllPropertyKeys() ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithFirstTransactionLogFileWithoutShutdownCheckpoint() throws Exception
    {
        GraphDatabaseService database = createDatabase();
        generateSomeData( database );
        managementService.shutdown();
        assertEquals( 1, countCheckPointsInTransactionLogs() );
        removeLastCheckpointRecordFromLastLogFile();

        assertEquals( 0, countCheckPointsInTransactionLogs() );
        assertTrue( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );

        startStopDatabase();

        assertFalse( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );
        // we will have 2 checkpoints: first will be created after successful recovery and another on shutdown
        assertEquals( 2, countCheckPointsInTransactionLogs() );
    }

    @Test
    void failToStartDatabaseWithRemovedTransactionLogs() throws Exception
    {
        GraphDatabaseAPI database = createDatabase();
        generateSomeData( database );
        managementService.shutdown();

        removeTransactionLogs();

        GraphDatabaseAPI restartedDb = createDatabase();
        try
        {
            DatabaseStateService dbStateService = restartedDb.getDependencyResolver().resolveDependency( DatabaseStateService.class );

            var failure = dbStateService.causeOfFailure( restartedDb.databaseId() );
            assertTrue( failure.isPresent() );
            assertThat( getRootCause( failure.get() ).getMessage() ).contains( "Transaction logs are missing and recovery is not possible." );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void startDatabaseWithRemovedSingleTransactionLogFile() throws Exception
    {
        GraphDatabaseAPI database = createDatabase();
        PageCache pageCache = getDatabasePageCache( database );
        generateSomeData( database );

        assertEquals( -1, getRecord( pageCache, database.databaseLayout().metadataStore(), LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP ) );

        managementService.shutdown();

        removeTransactionLogs();

        startStopDatabaseWithForcedRecovery();
        assertFalse( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals( 2, countCheckPointsInTransactionLogs() );

        verifyRecoveryTimestampPresent( database );
    }

    @Test
    void startDatabaseWithRemovedMultipleTransactionLogFiles() throws Exception
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( neo4jLayout )
                        .setConfig( logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) )
                        .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        while ( countTransactionLogFiles() < 5 )
        {
            generateSomeData( database );
        }
        managementService.shutdown();

        removeTransactionLogs();

        startStopDatabaseWithForcedRecovery();

        assertFalse( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals( 2, countCheckPointsInTransactionLogs() );
    }

    @Test
    void killAndStartDatabaseAfterTransactionLogsRemoval() throws Exception
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( neo4jLayout )
                        .setConfig( logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) )
                        .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        while ( countTransactionLogFiles() < 5 )
        {
            generateSomeData( database );
        }
        managementService.shutdown();

        removeTransactionLogs();
        assertTrue( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );
        assertEquals( 0, countTransactionLogFiles() );

        DatabaseManagementService forcedRecoveryManagementService = forcedRecoveryManagement();
        GraphDatabaseService service = forcedRecoveryManagementService.database( DEFAULT_DATABASE_NAME );
        createSingleNode( service );
        forcedRecoveryManagementService.shutdown();

        assertEquals( 1, countTransactionLogFiles() );
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        removeLastCheckpointRecordFromLastLogFile();

        startStopDatabase();

        assertFalse( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );
        // we will have 3 checkpoints: one from logs before recovery, second will be created as part of recovery and another on shutdown
        assertEquals( 3, countCheckPointsInTransactionLogs() );
    }

    @Test
    void killAndStartDatabaseAfterTransactionLogsRemovalWithSeveralFilesWithoutCheckpoint() throws Exception
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( neo4jLayout )
                        .setConfig( logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) )
                        .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        while ( countTransactionLogFiles() < 5 )
        {
            generateSomeData( database );
        }
        managementService.shutdown();

        removeHighestLogFile();

        assertEquals( 4, countTransactionLogFiles() );
        assertEquals( 0, countCheckPointsInTransactionLogs() );
        assertTrue( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );

        startStopDatabase();
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        removeLastCheckpointRecordFromLastLogFile();
        removeLastCheckpointRecordFromLastLogFile();

        startStopDatabase();

        assertFalse( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals( 2, countCheckPointsInTransactionLogs() );
    }

    @Test
    void startDatabaseAfterTransactionLogsRemovalAndKillAfterRecovery() throws Exception
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( neo4jLayout )
                        .setConfig( logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) )
                        .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        while ( countTransactionLogFiles() < 5 )
        {
            generateSomeData( database );
        }
        managementService.shutdown();

        removeHighestLogFile();

        assertEquals( 4, countTransactionLogFiles() );
        assertEquals( 0, countCheckPointsInTransactionLogs() );
        assertTrue( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );

        startStopDatabase();
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        removeLastCheckpointRecordFromLastLogFile();

        startStopDatabase();

        assertFalse( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );
        // we will have 2 checkpoints here because offset in both of them will be the same and 2 will be truncated instead since truncation is based on position
        // next start-stop cycle will have transaction between so we will have 3 checkpoints as expected.
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        removeLastCheckpointRecordFromLastLogFile();

        GraphDatabaseService service = createDatabase();
        createSingleNode( service );
        this.managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();
        startStopDatabase();

        assertFalse( isRecoveryRequired( fileSystem, databaseLayout, defaults() ) );
        assertEquals( 3, countCheckPointsInTransactionLogs() );
    }

    @Test
    void recoverDatabaseWithoutOneIdFile() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        fileSystem.deleteFileOrThrow( layout.idRelationshipStore() );
        assertTrue( isRecoveryRequired( fileSystem, layout, defaults() ) );

        performRecovery( fileSystem, pageCache, defaults(), layout );
        assertFalse( isRecoveryRequired( fileSystem, layout, defaults() ) );

        assertTrue( fileSystem.fileExists( layout.idRelationshipStore() ) );
    }

    @Test
    void recoverDatabaseWithoutIdFiles() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        for ( File idFile : layout.idFiles() )
        {
            fileSystem.deleteFileOrThrow( idFile );
        }
        assertTrue( isRecoveryRequired( fileSystem, layout, defaults() ) );

        performRecovery( fileSystem, pageCache, defaults(), layout );
        assertFalse( isRecoveryRequired( fileSystem, layout, defaults() ) );

        for ( File idFile : layout.idFiles() )
        {
            assertTrue( fileSystem.fileExists( idFile ) );
        }
    }

    @Test
    void cancelRecoveryInTheMiddle() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        removeLastCheckpointRecordFromLastLogFile();
        assertTrue( isRecoveryRequired( fileSystem, layout, defaults() ) );

        Monitors monitors = new Monitors();
        var recoveryMonitor = new RecoveryMonitor()
        {
            @Override
            public void reverseStoreRecoveryCompleted( long lowestRecoveredTxId )
            {
                GlobalGuardConsumer.globalGuard.stop();
            }
        };
        monitors.addMonitorListener( recoveryMonitor );
        var service = new TestDatabaseManagementServiceBuilder( layout.getNeo4jLayout() )
                .addExtension( new GlobalGuardConsumerTestExtensionFactory() )
                .setMonitors( monitors ).build();
        try
        {
            var e = assertThrows( Exception.class, () -> service.database( DEFAULT_DATABASE_NAME ).beginTx() );
            assertThat( getRootCause( e ) ).isInstanceOf( DatabaseStartAbortedException.class );
        }
        finally
        {
            service.shutdown();
        }
    }

    private void createSingleNode( GraphDatabaseService service )
    {
        try ( Transaction transaction = service.beginTx() )
        {
            transaction.createNode();
            transaction.commit();
        }
    }

    private void startStopDatabase()
    {
        createDatabase();
        managementService.shutdown();
    }

    private void recoverDatabase() throws Exception
    {
        assertTrue( isRecoveryRequired( databaseLayout, defaults() ) );
        performRecovery( databaseLayout );
        assertFalse( isRecoveryRequired( databaseLayout, defaults() ) );
    }

    private int countCheckPointsInTransactionLogs() throws IOException
    {
        int checkpointCounter = 0;

        LogFiles logFiles = buildLogFiles();
        LogFile transactionLogFile = logFiles.getLogFile();
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader();
        LogPosition startPosition = logFiles.extractHeader( logFiles.getHighestLogVersion() ).getStartPosition();
        try ( ReadableLogChannel reader = transactionLogFile.getReader( startPosition ) )
        {
            LogEntry logEntry;
            do
            {
                logEntry = entryReader.readLogEntry( reader );
                if ( logEntry instanceof CheckPoint )
                {
                    checkpointCounter++;
                }
            }
            while ( logEntry != null );
        }
        return checkpointCounter;
    }

    private LogFiles buildLogFiles() throws IOException
    {
        return LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fileSystem ).build();
    }

    private void removeTransactionLogs() throws IOException
    {
        LogFiles logFiles = buildLogFiles();
        File[] txLogFiles = logFiles.logFiles();
        for ( File logFile : txLogFiles )
        {
            fileSystem.deleteFile( logFile );
        }
    }

    private void removeHighestLogFile() throws IOException
    {
        LogFiles logFiles = buildLogFiles();
        long highestLogVersion = logFiles.getHighestLogVersion();
        removeFileByVersion( logFiles, highestLogVersion );
    }

    private void removeFileByVersion( LogFiles logFiles, long version )
    {
        File versionFile = logFiles.getLogFileForVersion( version );
        assertNotNull( versionFile );
        fileSystem.deleteFile( versionFile );
    }

    private int countTransactionLogFiles() throws IOException
    {
        LogFiles logFiles = buildLogFiles();
        return logFiles.logFiles().length;
    }

    private void removeLastCheckpointRecordFromLastLogFile() throws IOException
    {
        LogPosition checkpointPosition = null;

        LogFiles logFiles = buildLogFiles();
        LogFile transactionLogFile = logFiles.getLogFile();
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader();
        LogPosition startPosition = logFiles.extractHeader( logFiles.getHighestLogVersion() ).getStartPosition();
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

    private static void generateSomeData( GraphDatabaseService database )
    {
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node1 = transaction.createNode();
                Node node2 = transaction.createNode();
                node1.createRelationshipTo( node2, withName( "Type" + i ) );
                node2.setProperty( "a", randomAlphanumeric( TEN_KB ) );
                transaction.commit();
            }
        }
    }

    private GraphDatabaseAPI createDatabase()
    {
        managementService = new TestDatabaseManagementServiceBuilder( neo4jLayout )
                .setConfig( logical_log_rotation_threshold, logical_log_rotation_threshold.defaultValue() )
                .build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void startStopDatabaseWithForcedRecovery()
    {
        DatabaseManagementService forcedRecoveryManagementService = forcedRecoveryManagement();
        forcedRecoveryManagementService.shutdown();
    }

    private DatabaseManagementService forcedRecoveryManagement()
    {
        return new TestDatabaseManagementServiceBuilder( neo4jLayout ).setConfig( fail_on_missing_files, false ).build();
    }

    private PageCache getDatabasePageCache( GraphDatabaseAPI databaseAPI )
    {
        return databaseAPI.getDependencyResolver().resolveDependency( PageCache.class );
    }

    private void verifyRecoveryTimestampPresent( GraphDatabaseAPI databaseAPI ) throws IOException
    {
        GraphDatabaseService restartedDatabase = createDatabase();
        try
        {
            PageCache restartedCache = getDatabasePageCache( (GraphDatabaseAPI) restartedDatabase );
            assertThat( getRecord( restartedCache, databaseAPI.databaseLayout().metadataStore(), LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP ) ).isGreaterThan(
                    0L );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static class GlobalGuardConsumerTestExtensionFactory extends ExtensionFactory<GlobalGuardConsumerTestExtensionFactory.Dependencies>
    {
        interface Dependencies
        {
            CompositeDatabaseAvailabilityGuard globalGuard();
        }

        GlobalGuardConsumerTestExtensionFactory()
        {
            super( "globalGuardConsumer" );
        }

        @Override
        public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
        {
            return new GlobalGuardConsumer( dependencies );
        }
    }

    private static class GlobalGuardConsumer extends LifecycleAdapter
    {
        private static CompositeDatabaseAvailabilityGuard globalGuard;

        GlobalGuardConsumer( GlobalGuardConsumerTestExtensionFactory.Dependencies dependencies )
        {
            globalGuard = dependencies.globalGuard();
        }

        public CompositeDatabaseAvailabilityGuard getGlobalGuard()
        {
            return globalGuard;
        }
    }

}
