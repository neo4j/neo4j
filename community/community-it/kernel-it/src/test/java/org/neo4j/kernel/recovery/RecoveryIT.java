/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.storemigration.LegacyTransactionLogsLocator;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.MINUTES;
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
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.index.label.RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store;
import static org.neo4j.internal.kernel.api.IndexQuery.fulltextSearch;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.database.DatabaseTracers.EMPTY;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP;
import static org.neo4j.kernel.impl.store.MetaDataStore.getRecord;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

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
    private TestDatabaseManagementServiceBuilder builder;
    private DatabaseManagementService managementService;
    private StorageEngineFactory storageEngineFactory;

    boolean enableRelationshipTypeScanStore()
    {
        return false;
    }

    @Test
    void recoveryRequiredOnDatabaseWithoutCorrectCheckpoints() throws Throwable
    {
        GraphDatabaseService database = createDatabase();
        generateSomeData( database );
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        assertTrue( isRecoveryRequired( databaseLayout ) );
    }

    @Test
    void recoveryNotRequiredWhenDatabaseNotFound() throws Exception
    {
        DatabaseLayout absentDatabase = neo4jLayout.databaseLayout( "absent" );
        assertFalse( isRecoveryRequired( absentDatabase ) );
    }

    @Test
    void recoverEmptyDatabase() throws Throwable
    {
        createDatabase();
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        assertFalse( isRecoveryRequired( databaseLayout, defaults() ) );
    }

    @Test
    void recoverDatabaseWithNodes() throws Throwable
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
    void tracePageCacheAccessOnDatabaseRecovery() throws Throwable
    {
        GraphDatabaseService database = createDatabase();

        int numberOfNodes = 10;
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            createSingleNode( database );
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        var pageCacheTracer = new DefaultPageCacheTracer();
        var tracers = new DatabaseTracers( DatabaseTracer.NULL, LockTracer.NONE, pageCacheTracer );
        recoverDatabase( tracers );

        assertThat( pageCacheTracer.pins() ).isEqualTo( pageCacheTracer.unpins() );
        assertThat( pageCacheTracer.hits() + pageCacheTracer.faults() ).isEqualTo( pageCacheTracer.pins() );

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
    void recoverDatabaseWithNodesAndRelationshipsAndRelationshipTypes() throws Throwable
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
    void recoverDatabaseWithProperties() throws Throwable
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
    void recoverDatabaseWithIndex() throws Throwable
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
        awaitIndexesOnline( database );

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
    void recoverDatabaseWithRelationshipIndex() throws Throwable
    {
        GraphDatabaseService database = createDatabase();

        int numberOfRelationships = 10;
        RelationshipType type = RelationshipType.withName( "TYPE" );
        String property = "prop";
        String indexName = "my index";

        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().indexFor( type ).on( property ).withIndexType( IndexType.FULLTEXT ).withName( indexName ).create();
            transaction.commit();
        }
        awaitIndexesOnline( database );

        try ( Transaction transaction = database.beginTx() )
        {
            Node start = transaction.createNode();
            Node stop = transaction.createNode();
            for ( int i = 0; i < numberOfRelationships; i++ )
            {
                Relationship relationship = start.createRelationshipTo( stop, type );
                relationship.setProperty( property, "value" );
            }
            transaction.commit();
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        awaitIndexesOnline( recoveredDatabase );
        try ( Transaction transaction = recoveredDatabase.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) transaction).kernelTransaction();
            IndexDescriptor index = ktx.schemaRead().indexGetForName( indexName );
            int relationshipsInIndex = 0;
            try ( RelationshipIndexCursor cursor = ktx.cursors().allocateRelationshipIndexCursor( ktx.pageCursorTracer() ) )
            {
                ktx.dataRead().relationshipIndexSeek( index, cursor, unconstrained(), fulltextSearch( "*" ) );
                while ( cursor.next() )
                {
                    relationshipsInIndex++;
                }
            }
            assertEquals( numberOfRelationships, relationshipsInIndex );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithFirstTransactionLogFileWithoutShutdownCheckpoint() throws Throwable
    {
        GraphDatabaseService database = createDatabase();
        generateSomeData( database );
        managementService.shutdown();
        assertEquals( 1, countCheckPointsInTransactionLogs() );
        removeLastCheckpointRecordFromLastLogFile();

        assertEquals( 0, countCheckPointsInTransactionLogs() );
        assertTrue( isRecoveryRequired( databaseLayout ) );

        startStopDatabase();

        assertFalse( isRecoveryRequired( databaseLayout ) );
        // we will have 2 checkpoints: first will be created after successful recovery and another on shutdown
        assertEquals( 2, countCheckPointsInTransactionLogs() );
    }

    @Test
    void failToStartDatabaseWithRemovedTransactionLogs() throws Throwable
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
    void failToStartDatabaseWithTransactionLogsInLegacyLocation() throws Exception
    {
        GraphDatabaseAPI database = createDatabase();
        generateSomeData( database );
        managementService.shutdown();

        File[] txLogFiles = buildLogFiles().logFiles();
        File databasesDirectory = databaseLayout.getNeo4jLayout().databasesDirectory();
        DatabaseLayout legacyLayout = Neo4jLayout.ofFlat( databasesDirectory ).databaseLayout( databaseLayout.getDatabaseName() );
        LegacyTransactionLogsLocator logsLocator = new LegacyTransactionLogsLocator( Config.defaults(), legacyLayout );
        File transactionLogsDirectory = logsLocator.getTransactionLogsDirectory();
        assertNotNull( txLogFiles );
        assertTrue( txLogFiles.length > 0 );
        for ( File logFile : txLogFiles )
        {
            fileSystem.moveToDirectory( logFile, transactionLogsDirectory );
        }

        AssertableLogProvider logProvider = new AssertableLogProvider();
        builder.setInternalLogProvider( logProvider );
        GraphDatabaseAPI restartedDb = createDatabase();
        try
        {
            DatabaseStateService dbStateService = restartedDb.getDependencyResolver().resolveDependency( DatabaseStateService.class );

            var failure = dbStateService.causeOfFailure( restartedDb.databaseId() );
            assertTrue( failure.isPresent() );
            assertThat( failure.get() ).hasRootCauseMessage( "Transaction logs are missing and recovery is not possible." );
            assertThat( logProvider.serialize() ).contains( txLogFiles[0].getName() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void startDatabaseWithRemovedSingleTransactionLogFile() throws Throwable
    {
        GraphDatabaseAPI database = createDatabase();
        PageCache pageCache = getDatabasePageCache( database );
        generateSomeData( database );

        assertEquals( -1, getRecord( pageCache, database.databaseLayout().metadataStore(), LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP, NULL ) );

        managementService.shutdown();

        removeTransactionLogs();

        startStopDatabaseWithForcedRecovery();
        assertFalse( isRecoveryRequired( databaseLayout ) );
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals( 2, countCheckPointsInTransactionLogs() );

        verifyRecoveryTimestampPresent( database );
    }

    @Test
    void startDatabaseWithRemovedMultipleTransactionLogFiles() throws Throwable
    {
        GraphDatabaseService database = createDatabase( ByteUnit.mebiBytes( 1 ) );
        while ( countTransactionLogFiles() < 5 )
        {
            generateSomeData( database );
        }
        managementService.shutdown();

        removeTransactionLogs();

        startStopDatabaseWithForcedRecovery();

        assertFalse( isRecoveryRequired( databaseLayout ) );
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals( 2, countCheckPointsInTransactionLogs() );
    }

    @Test
    void killAndStartDatabaseAfterTransactionLogsRemoval() throws Throwable
    {
        GraphDatabaseService database = createDatabase( ByteUnit.mebiBytes( 1 ) );
        while ( countTransactionLogFiles() < 5 )
        {
            generateSomeData( database );
        }
        managementService.shutdown();

        removeTransactionLogs();
        assertTrue( isRecoveryRequired( databaseLayout ) );
        assertEquals( 0, countTransactionLogFiles() );

        DatabaseManagementService forcedRecoveryManagementService = forcedRecoveryManagement();
        GraphDatabaseService service = forcedRecoveryManagementService.database( DEFAULT_DATABASE_NAME );
        createSingleNode( service );
        forcedRecoveryManagementService.shutdown();

        assertEquals( 1, countTransactionLogFiles() );
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        removeLastCheckpointRecordFromLastLogFile();

        startStopDatabase();

        assertFalse( isRecoveryRequired( databaseLayout ) );
        // we will have 3 checkpoints: one from logs before recovery, second will be created as part of recovery and another on shutdown
        assertEquals( 3, countCheckPointsInTransactionLogs() );
    }

    @Test
    void killAndStartDatabaseAfterTransactionLogsRemovalWithSeveralFilesWithoutCheckpoint() throws Throwable
    {
        GraphDatabaseService database = createDatabase( ByteUnit.mebiBytes( 1 ) );
        while ( countTransactionLogFiles() < 5 )
        {
            generateSomeData( database );
        }
        managementService.shutdown();

        removeHighestLogFile();

        assertEquals( 4, countTransactionLogFiles() );
        assertEquals( 0, countCheckPointsInTransactionLogs() );
        assertTrue( isRecoveryRequired( databaseLayout ) );

        startStopDatabase();
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        removeLastCheckpointRecordFromLastLogFile();
        removeLastCheckpointRecordFromLastLogFile();

        startStopDatabase();

        assertFalse( isRecoveryRequired( databaseLayout ) );
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals( 2, countCheckPointsInTransactionLogs() );
    }

    @Test
    void startDatabaseAfterTransactionLogsRemovalAndKillAfterRecovery() throws Throwable
    {
        long logThreshold = ByteUnit.mebiBytes( 1 );
        GraphDatabaseService database = createDatabase( logThreshold );
        while ( countTransactionLogFiles() < 5 )
        {
            generateSomeData( database );
        }
        managementService.shutdown();

        removeHighestLogFile();

        assertEquals( 4, countTransactionLogFiles() );
        assertEquals( 0, countCheckPointsInTransactionLogs() );
        assertTrue( isRecoveryRequired( databaseLayout ) );

        startStopDatabase();
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        removeLastCheckpointRecordFromLastLogFile();

        startStopDatabase();

        assertFalse( isRecoveryRequired( databaseLayout ) );
        // we will have 2 checkpoints here because offset in both of them will be the same
        // and 2 will be truncated instead since truncation is based on position
        // next start-stop cycle will have transaction between so we will have 3 checkpoints as expected.
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        removeLastCheckpointRecordFromLastLogFile();
        builder = null; // Reset log rotation threshold setting to avoid immediate rotation on `createSingleNode()`.

        GraphDatabaseService service = createDatabase( logThreshold * 2 ); // Bigger log, to avoid rotation.
        createSingleNode( service );
        this.managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();
        startStopDatabase();

        assertFalse( isRecoveryRequired( databaseLayout ) );
        assertEquals( 3, countCheckPointsInTransactionLogs() );
    }

    @Test
    void recoverDatabaseWithoutOneIdFile() throws Throwable
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        fileSystem.deleteFileOrThrow( layout.idRelationshipStore() );
        assertTrue( isRecoveryRequired( layout ) );

        performRecovery( fileSystem, pageCache, EMPTY, defaults(), layout, INSTANCE );
        assertFalse( isRecoveryRequired( layout ) );

        assertTrue( fileSystem.fileExists( layout.idRelationshipStore() ) );
    }

    @Test
    void recoverDatabaseWithoutIdFiles() throws Throwable
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        for ( File idFile : layout.idFiles() )
        {
            fileSystem.deleteFileOrThrow( idFile );
        }
        assertTrue( isRecoveryRequired( layout ) );

        recoverDatabase();
        assertFalse( isRecoveryRequired( layout ) );

        for ( File idFile : layout.idFiles() )
        {
            assertTrue( fileSystem.fileExists( idFile ) );
        }
    }

    @Test
    void cancelRecoveryInTheMiddle() throws Throwable
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        removeLastCheckpointRecordFromLastLogFile();
        assertTrue( isRecoveryRequired( layout ) );

        Monitors monitors = new Monitors();
        var guardExtensionFactory = new GlobalGuardConsumerTestExtensionFactory();
        var recoveryMonitor = new RecoveryMonitor()
        {
            private final AtomicBoolean reverseCompleted = new AtomicBoolean();
            private final AtomicBoolean recoveryCompleted = new AtomicBoolean();

            @Override
            public void reverseStoreRecoveryCompleted( long lowestRecoveredTxId )
            {
                guardExtensionFactory.getProvidedGuardConsumer().globalGuard.stop();
                reverseCompleted.set( true );
            }

            @Override
            public void recoveryCompleted( int numberOfRecoveredTransactions, long recoveryTimeInMilliseconds )
            {
                recoveryCompleted.set( true );
            }

            public boolean isReverseCompleted()
            {
                return reverseCompleted.get();
            }

            public boolean isRecoveryCompleted()
            {
                return recoveryCompleted.get();
            }
        };
        monitors.addMonitorListener( recoveryMonitor );
        var service = builderWithRelationshipTypeScanStoreSet( layout.getNeo4jLayout() )
                .addExtension( guardExtensionFactory )
                .setMonitors( monitors ).build();
        try
        {
            var database = service.database( layout.getDatabaseName() );
            assertTrue( recoveryMonitor.isReverseCompleted() );
            assertFalse( recoveryMonitor.isRecoveryCompleted() );
            assertFalse( guardExtensionFactory.getProvidedGuardConsumer().globalGuard.isAvailable() );
            assertFalse( database.isAvailable( 0 ) );
            var e = assertThrows( Exception.class, database::beginTx );
            assertThat( getRootCause( e ) ).isInstanceOf( DatabaseStartAbortedException.class );
        }
        finally
        {
            service.shutdown();
        }
    }

    private static void awaitIndexesOnline( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 10, MINUTES );
            transaction.commit();
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
        GraphDatabaseService db = createDatabase();
        db.beginTx().close();
        managementService.shutdown();
    }

    private void recoverDatabase() throws Exception
    {
        recoverDatabase( EMPTY );
    }

    private void recoverDatabase( DatabaseTracers databaseTracers ) throws Exception
    {
        Config config = Config.newBuilder().set( enable_relationship_type_scan_store, enableRelationshipTypeScanStore() ).build();
        assertTrue( isRecoveryRequired( databaseLayout, config ) );
        performRecovery( fileSystem, pageCache, databaseTracers, config, databaseLayout, INSTANCE );
        assertFalse( isRecoveryRequired( databaseLayout, config ) );
    }

    private boolean isRecoveryRequired( DatabaseLayout layout ) throws Exception
    {
        Config config = Config.newBuilder().set( enable_relationship_type_scan_store, enableRelationshipTypeScanStore() ).build();
        return isRecoveryRequired( layout, config );
    }

    private boolean isRecoveryRequired( DatabaseLayout layout, Config config ) throws Exception
    {
        return Recovery.isRecoveryRequired( fileSystem, layout, config, INSTANCE );
    }

    private int countCheckPointsInTransactionLogs() throws IOException
    {
        int checkpointCounter = 0;

        LogFiles logFiles = buildLogFiles();
        LogFile transactionLogFile = logFiles.getLogFile();
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );
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
        return LogFilesBuilder
                .logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fileSystem )
                .withCommandReaderFactory( StorageEngineFactory.selectStorageEngine().commandReaderFactory() )
                .build();
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
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );
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
        return createDatabase( logical_log_rotation_threshold.defaultValue() );
    }

    private GraphDatabaseAPI createDatabase( long logThreshold )
    {
        createBuilder( logThreshold );
        managementService = builder.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( databaseLayout.getDatabaseName() );
        storageEngineFactory = database.getDependencyResolver().resolveDependency( StorageEngineFactory.class );
        return database;
    }

    private void createBuilder( long logThreshold )
    {
        if ( builder == null )
        {
            builder = builderWithRelationshipTypeScanStoreSet()
                    .setConfig( preallocate_logical_logs, false )
                    .setConfig( logical_log_rotation_threshold, logThreshold );
        }
    }

    private void startStopDatabaseWithForcedRecovery()
    {
        DatabaseManagementService forcedRecoveryManagementService = forcedRecoveryManagement();
        forcedRecoveryManagementService.shutdown();
    }

    private DatabaseManagementService forcedRecoveryManagement()
    {
        return builderWithRelationshipTypeScanStoreSet()
                .setConfig( fail_on_missing_files, false )
                .build();
    }

    private TestDatabaseManagementServiceBuilder builderWithRelationshipTypeScanStoreSet()
    {
        return builderWithRelationshipTypeScanStoreSet( neo4jLayout );
    }

    private TestDatabaseManagementServiceBuilder builderWithRelationshipTypeScanStoreSet( Neo4jLayout neo4jLayout )
    {
        return new TestDatabaseManagementServiceBuilder( neo4jLayout )
                .setConfig( enable_relationship_type_scan_store, enableRelationshipTypeScanStore() );
    }

    private PageCache getDatabasePageCache( GraphDatabaseAPI databaseAPI )
    {
        return databaseAPI.getDependencyResolver().resolveDependency( PageCache.class );
    }

    private void verifyRecoveryTimestampPresent( GraphDatabaseAPI databaseAPI ) throws IOException
    {
        GraphDatabaseAPI restartedDatabase = createDatabase();
        try
        {
            PageCache restartedCache = getDatabasePageCache( restartedDatabase );
            final long record = getRecord( restartedCache, databaseAPI.databaseLayout().metadataStore(), LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP, NULL );
            assertThat( record ).isGreaterThan( 0L );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    interface Dependencies
    {
        CompositeDatabaseAvailabilityGuard globalGuard();
    }

    private static class GlobalGuardConsumerTestExtensionFactory extends ExtensionFactory<Dependencies>
    {
        private GlobalGuardConsumer providedConsumer;

        GlobalGuardConsumerTestExtensionFactory()
        {
            super( "globalGuardConsumer" );
        }

        @Override
        public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
        {
            providedConsumer = new GlobalGuardConsumer( dependencies );
            return providedConsumer;
        }

        public GlobalGuardConsumer getProvidedGuardConsumer()
        {
            return providedConsumer;
        }
    }

    private static class GlobalGuardConsumer extends LifecycleAdapter
    {
        private final CompositeDatabaseAvailabilityGuard globalGuard;

        GlobalGuardConsumer( Dependencies dependencies )
        {
            globalGuard = dependencies.globalGuard();
        }
    }
}
