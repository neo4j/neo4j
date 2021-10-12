/*
 * Copyright (c) "Neo4j"
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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdSlotDistribution;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.storemigration.LegacyTransactionLogsLocator;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DetachedCheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.recovery.facade.RecoveryCriteria;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.fulltextSearch;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.database.DatabaseTracers.EMPTY;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.CHECKPOINT_LOG_VERSION;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP;
import static org.neo4j.kernel.impl.store.MetaDataStore.getRecord;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.DEFAULT_NAME;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;
import static org.neo4j.kernel.recovery.facade.RecoveryCriteria.ALL;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.storageengine.api.StorageEngineFactory.defaultStorageEngine;

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
    private FakeClock fakeClock;
    private AssertableLogProvider logProvider;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoveryRequiredOnDatabaseWithoutCorrectCheckpoints() throws Throwable
    {
        GraphDatabaseService database = createDatabase();
        generateSomeData( database );
        managementService.shutdown();
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

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
        // The database is only completely empty if we skip the creation of the default indexes initially.
        // Without skipping there will be entries in the transaction logs for the default token indexes, so recovery is required if we remove the checkpoint.
        Config config = Config.newBuilder()
                              .set( GraphDatabaseInternalSettings.skip_default_indexes_on_creation, true )
                              .set( preallocate_logical_logs, false )
                              .build();

        managementService = new TestDatabaseManagementServiceBuilder( neo4jLayout ).setConfig( config ).build();
        managementService.database( databaseLayout.getDatabaseName() );
        managementService.shutdown();
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

        assertFalse( isRecoveryRequired( databaseLayout, config ) );
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
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

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
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

        var pageCacheTracer = new DefaultPageCacheTracer();
        var tracers = new DatabaseTracers( DatabaseTracer.NULL, LockTracer.NONE, pageCacheTracer );
        recoverDatabase( tracers );

        long pins = pageCacheTracer.pins();
        assertThat( pins ).isGreaterThan( 0 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( pins );
        assertThat( pageCacheTracer.hits() ).isGreaterThan( 0 ).isLessThanOrEqualTo( pins );
        assertThat( pageCacheTracer.faults() ).isGreaterThan( 0 ).isLessThanOrEqualTo( pins );

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
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

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
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

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

    @ParameterizedTest
    @EnumSource( value = IndexType.class, names = {"BTREE", "RANGE"} )
    void recoverDatabaseWithConstraint( IndexType indexType ) throws Exception
    {
        GraphDatabaseService database = createDatabase();

        int numberOfNodes = 10;
        String property = "prop";
        Label label = Label.label( "myLabel" );

        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().constraintFor( label ).assertPropertyIsUnique( property ).withIndexType( indexType ).create();
            tx.commit();
        }
        awaitIndexesOnline( database );

        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction tx = database.beginTx() )
            {
                Node node = tx.createNode( label );

                node.setProperty( property, i );
                tx.commit();
            }
        }
        managementService.shutdown();
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        try
        {
            awaitIndexesOnline( recoveredDatabase );

            // let's verify that the constraint has recovered with all values
            // by trying to create duplicates of all nodes under the constraint
            for ( int i = 0; i < numberOfNodes; i++ )
            {
                int finalInt = i;
                assertThrows( ConstraintViolationException.class, () ->
                {
                    try ( Transaction tx = recoveredDatabase.beginTx() )
                    {
                        Node node = tx.createNode( label );

                        node.setProperty( property, finalInt );
                        tx.commit();
                    }
                } );
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithNodeIndexes() throws Throwable
    {
        GraphDatabaseService database = createDatabase();
        int numberOfNodes = 10;
        Label label = Label.label( "myLabel" );
        String property = "prop";
        String btreeIndex = "b-tree index";
        String rangeIndex = "range index";
        String textIndex = "text index";
        String fullTextIndex = "full text index";

        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().indexFor( label ).on( property ).withIndexType( IndexType.BTREE ).withName( btreeIndex ).create();
            transaction.schema().indexFor( label ).on( property ).withIndexType( IndexType.RANGE ).withName( rangeIndex ).create();
            transaction.schema().indexFor( label ).on( property ).withIndexType( IndexType.TEXT ).withName( textIndex ).create();
            transaction.schema().indexFor( label ).on( property ).withIndexType( IndexType.FULLTEXT ).withName( fullTextIndex ).create();
            transaction.commit();
        }
        awaitIndexesOnline( database );

        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction tx = database.beginTx() )
            {
                Node node = tx.createNode( label );

                node.setProperty( property, "value" + i );
                tx.commit();
            }
        }
        managementService.shutdown();
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        awaitIndexesOnline( recoveredDatabase );
        try ( InternalTransaction transaction = (InternalTransaction) recoveredDatabase.beginTx() )
        {
            verifyNodeIndexEntries( numberOfNodes, btreeIndex, transaction, allEntries() );
            verifyNodeIndexEntries( numberOfNodes, rangeIndex, transaction, allEntries() );
            verifyNodeIndexEntries( numberOfNodes, textIndex, transaction, allEntries() );
            verifyNodeIndexEntries( numberOfNodes, fullTextIndex, transaction, fulltextSearch( "*" ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithNodePointIndex() throws Throwable
    {
        GraphDatabaseService database = createDatabase();
        int numberOfNodes = 10;
        Label label = Label.label( "myLabel" );
        String property = "prop";
        String pointIndex = "point index";

        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().indexFor( label ).on( property ).withIndexType( IndexType.POINT ).withName( pointIndex ).create();
            transaction.commit();
        }
        awaitIndexesOnline( database );

        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction tx = database.beginTx() )
            {
                Node node = tx.createNode( label );

                node.setProperty( property, Values.pointValue( CoordinateReferenceSystem.Cartesian, i, -i ) );
                tx.commit();
            }
        }
        managementService.shutdown();
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        awaitIndexesOnline( recoveredDatabase );
        try ( InternalTransaction transaction = (InternalTransaction) recoveredDatabase.beginTx() )
        {
            verifyNodeIndexEntries( numberOfNodes, pointIndex, transaction, allEntries() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithRelationshipIndexes() throws Throwable
    {
        GraphDatabaseService database = createDatabase();
        int numberOfRelationships = 10;
        RelationshipType type = RelationshipType.withName( "TYPE" );
        String property = "prop";
        String btreeIndex = "b-tree index";
        String rangeIndex = "range index";
        String textIndex = "text index";
        String fullTextIndex = "full text index";

        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().indexFor( type ).on( property ).withIndexType( IndexType.BTREE ).withName( btreeIndex ).create();
            transaction.schema().indexFor( type ).on( property ).withIndexType( IndexType.RANGE ).withName( rangeIndex ).create();
            transaction.schema().indexFor( type ).on( property ).withIndexType( IndexType.TEXT ).withName( textIndex ).create();
            transaction.schema().indexFor( type ).on( property ).withIndexType( IndexType.FULLTEXT ).withName( fullTextIndex ).create();
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
                relationship.setProperty( property, "value" + i );
            }
            transaction.commit();
        }
        managementService.shutdown();
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        awaitIndexesOnline( recoveredDatabase );
        try ( InternalTransaction transaction = (InternalTransaction) recoveredDatabase.beginTx() )
        {
            verifyRelationshipIndexEntries( numberOfRelationships, btreeIndex, transaction, allEntries() );
            verifyRelationshipIndexEntries( numberOfRelationships, rangeIndex, transaction, allEntries() );
            verifyRelationshipIndexEntries( numberOfRelationships, textIndex, transaction, allEntries() );
            verifyRelationshipIndexEntries( numberOfRelationships, fullTextIndex, transaction, fulltextSearch( "*" ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void recoverDatabaseWithRelationshipPointIndex() throws Throwable
    {
        GraphDatabaseService database = createDatabase();
        int numberOfRelationships = 10;
        RelationshipType type = RelationshipType.withName( "TYPE" );
        String property = "prop";
        String pointIndex = "point index";

        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().indexFor( type ).on( property ).withIndexType( IndexType.POINT ).withName( pointIndex ).create();
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
                relationship.setProperty( property, Values.pointValue( CoordinateReferenceSystem.Cartesian, i, -i ) );
            }
            transaction.commit();
        }
        managementService.shutdown();
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

        recoverDatabase();

        GraphDatabaseAPI recoveredDatabase = createDatabase();
        awaitIndexesOnline( recoveredDatabase );
        try ( InternalTransaction transaction = (InternalTransaction) recoveredDatabase.beginTx() )
        {
            verifyRelationshipIndexEntries( numberOfRelationships, pointIndex, transaction, allEntries() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private void verifyRelationshipIndexEntries(
            int numberOfRelationships, String indexName, InternalTransaction transaction, PropertyIndexQuery query ) throws KernelException
    {
        KernelTransaction ktx = transaction.kernelTransaction();
        IndexDescriptor index = ktx.schemaRead().indexGetForName( indexName );
        IndexReadSession indexReadSession = ktx.dataRead().indexReadSession( index );
        int relationshipsInIndex = 0;
        try ( RelationshipValueIndexCursor cursor = ktx.cursors().allocateRelationshipValueIndexCursor( ktx.cursorContext(), ktx.memoryTracker() ) )
        {
            ktx.dataRead().relationshipIndexSeek( ktx.queryContext(), indexReadSession, cursor, unconstrained(), query );
            while ( cursor.next() )
            {
                relationshipsInIndex++;
            }
        }
        assertEquals( numberOfRelationships, relationshipsInIndex );
    }

    private void verifyNodeIndexEntries(
            int numberOfNodes, String indexName, InternalTransaction transaction, PropertyIndexQuery query ) throws KernelException
    {
        KernelTransaction ktx = transaction.kernelTransaction();
        IndexDescriptor index = ktx.schemaRead().indexGetForName( indexName );
        IndexReadSession indexReadSession = ktx.dataRead().indexReadSession( index );
        int nodesInIndex = 0;
        try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor( ktx.cursorContext(), ktx.memoryTracker() ) )
        {
            ktx.dataRead().nodeIndexSeek( ktx.queryContext(), indexReadSession, cursor, unconstrained(), query );
            while ( cursor.next() )
            {
                nodesInIndex++;
            }
        }
        assertEquals( numberOfNodes, nodesInIndex );
    }

    @Test
    void recoverDatabaseWithFirstTransactionLogFileWithoutShutdownCheckpoint() throws Throwable
    {
        GraphDatabaseService database = createDatabase();
        generateSomeData( database );
        managementService.shutdown();
        assertEquals( 1, countCheckPointsInTransactionLogs() );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

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

        LogFiles logFiles = buildLogFiles();
        Path[] txLogFiles = fileSystem.listFiles( logFiles.logFilesDirectory(), path -> path.getFileName().toString().startsWith( DEFAULT_NAME ) );
        txLogFiles = ArrayUtil.concat( txLogFiles, logFiles.getCheckpointFile().getDetachedCheckpointFiles() );
        Path databasesDirectory = databaseLayout.getNeo4jLayout().databasesDirectory();
        DatabaseLayout legacyLayout = Neo4jLayout.ofFlat( databasesDirectory ).databaseLayout( databaseLayout.getDatabaseName() );
        LegacyTransactionLogsLocator logsLocator = new LegacyTransactionLogsLocator( Config.defaults(), legacyLayout );
        Path transactionLogsDirectory = logsLocator.getTransactionLogsDirectory();
        assertNotNull( txLogFiles );
        assertTrue( txLogFiles.length > 0 );
        for ( Path logFile : txLogFiles )
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
            assertThat( logProvider.serialize() ).contains( txLogFiles[0].getFileName().toString() );
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

        assertEquals( -1,
                getRecord( pageCache, database.databaseLayout().metadataStore(), LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP, databaseLayout.getDatabaseName(),
                        NULL ) );

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

        assertEquals( 2, countTransactionLogFiles() );
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

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

        removeFileWithCheckpoint();

        assertEquals( 4, countTransactionLogFiles() );
        assertEquals( 0, countCheckPointsInTransactionLogs() );
        assertTrue( isRecoveryRequired( databaseLayout ) );

        startStopDatabase();
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

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

        removeFileWithCheckpoint();

        assertEquals( 4, countTransactionLogFiles() );
        assertEquals( 0, countCheckPointsInTransactionLogs() );
        assertTrue( isRecoveryRequired( databaseLayout ) );

        startStopDatabase();
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

        startStopDatabase();

        assertFalse( isRecoveryRequired( databaseLayout ) );
        // we will have 2 checkpoints here because offset in both of them will be the same
        // and 2 will be truncated instead since truncation is based on position
        // next start-stop cycle will have transaction between so we will have 3 checkpoints as expected.
        assertEquals( 2, countCheckPointsInTransactionLogs() );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );
        builder = null; // Reset log rotation threshold setting to avoid immediate rotation on `createSingleNode()`.

        GraphDatabaseService service = createDatabase( logThreshold * 2 ); // Bigger log, to avoid rotation.
        createSingleNode( service );
        this.managementService.shutdown();
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );
        startStopDatabase();

        assertFalse( isRecoveryRequired( databaseLayout ) );
        assertEquals( 3, countCheckPointsInTransactionLogs() );
    }

    @Test
    void recoverDatabaseWithoutOneIdFile() throws Throwable
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        RecordDatabaseLayout layout = RecordDatabaseLayout.cast( db.databaseLayout() );
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

        for ( Path idFile : layout.idFiles() )
        {
            fileSystem.deleteFileOrThrow( idFile );
        }
        assertTrue( isRecoveryRequired( layout ) );

        recoverDatabase();
        assertFalse( isRecoveryRequired( layout ) );

        for ( Path idFile : layout.idFiles() )
        {
            assertTrue( fileSystem.fileExists( idFile ) );
        }
    }

    @Test
    void failRecoveryWithMissingStoreFile() throws Exception
    {
        GraphDatabaseAPI database = createDatabase();
        generateSomeData( database );
        RecordDatabaseLayout layout = RecordDatabaseLayout.cast( database.databaseLayout() );
        managementService.shutdown();

        fileSystem.deleteFileOrThrow( layout.nodeStore() );

        GraphDatabaseAPI restartedDb = createDatabase();

        try
        {
            DatabaseStateService dbStateService = restartedDb.getDependencyResolver().resolveDependency( DatabaseStateService.class );

            var failure = dbStateService.causeOfFailure( restartedDb.databaseId() );
            assertTrue( failure.isPresent() );
            assertThat( failure.get().getCause() ).hasMessageContainingAll( "neostore.nodestore.db", "is(are) missing and recovery is not possible" );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void failRecoveryWithMissingStoreFileAndIdFile() throws Exception
    {
        GraphDatabaseAPI database = createDatabase();
        generateSomeData( database );
        RecordDatabaseLayout layout = RecordDatabaseLayout.cast( database.databaseLayout() );
        managementService.shutdown();

        // Recovery should not be attempted on any store with missing store files, even if other recoverable files are missing as well.
        fileSystem.deleteFileOrThrow( layout.nodeStore() );
        fileSystem.deleteFileOrThrow( layout.idLabelTokenStore() );

        GraphDatabaseAPI restartedDb = createDatabase();

        try
        {
            DatabaseStateService dbStateService = restartedDb.getDependencyResolver().resolveDependency( DatabaseStateService.class );

            var failure = dbStateService.causeOfFailure( restartedDb.databaseId() );
            assertTrue( failure.isPresent() );
            assertThat( failure.get().getCause() ).hasMessageContainingAll( "neostore.nodestore.db", "is(are) missing and recovery is not possible" );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void cancelRecoveryInTheMiddle() throws Throwable
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );
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
                try
                {
                    guardExtensionFactory.getProvidedGuardConsumer().globalGuard.stop();
                }
                catch ( Exception e )
                {
                    // do nothing
                }
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
        var service = new TestDatabaseManagementServiceBuilder( layout.getNeo4jLayout() )
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

    @Test
    void shouldForceRecoveryEvenThoughNotSeeminglyRequired() throws Exception
    {
        // given
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        RecordDatabaseLayout layout = RecordDatabaseLayout.cast( db.databaseLayout() );
        managementService.shutdown();
        assertFalse( isRecoveryRequired( layout ) );
        // Make an ID generator, say for the node store, dirty
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate(), "my db" );
        try ( IdGenerator idGenerator = idGeneratorFactory.open( pageCache, layout.idNodeStore(), RecordIdType.NODE, () -> 0L /*will not be used*/, 10_000,
                writable(), Config.defaults(), NULL, Sets.immutable.empty(), IdSlotDistribution.SINGLE_IDS ) )
        {
            // Merely opening a marker will make the backing GBPTree dirty
            idGenerator.marker( NULL ).close();
        }
        assertFalse( isRecoveryRequired( layout ) );
        assertTrue( idGeneratorIsDirty( layout.idNodeStore(), RecordIdType.NODE ) );

        // when
        MutableBoolean recoveryRunEvenThoughNoCommitsAfterLastCheckpoint = new MutableBoolean();
        RecoveryStartInformationProvider.Monitor monitor = new RecoveryStartInformationProvider.Monitor()
        {
            @Override
            public void noCommitsAfterLastCheckPoint( LogPosition logPosition )
            {
                recoveryRunEvenThoughNoCommitsAfterLastCheckpoint.setTrue();
            }
        };
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( monitor );
        Recovery.performRecovery( fileSystem, pageCache, EMPTY, Config.defaults(), layout, defaultStorageEngine(), true, NullLogProvider.getInstance(),
                monitors, Iterables.cast( Services.loadAll( ExtensionFactory.class ) ), Optional.empty(), null, INSTANCE, Clock.systemUTC(),
                RecoveryPredicate.ALL );

        // then
        assertFalse( idGeneratorIsDirty( layout.idNodeStore(), RecordIdType.NODE ) );
        assertTrue( recoveryRunEvenThoughNoCommitsAfterLastCheckpoint.booleanValue() );
    }

    @Test
    void keepCheckpointVersionOnMissingLogFilesWhenValueIsReasonable() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        removeTransactionLogs();

        assertTrue( isRecoveryRequired( layout ) );

        MetaDataStore.setRecord( pageCache, layout.metadataStore(), CHECKPOINT_LOG_VERSION, 18, layout.getDatabaseName(), NULL );

        recoverDatabase();
        assertFalse( isRecoveryRequired( layout ) );

        assertEquals( 18, MetaDataStore.getRecord( pageCache, layout.metadataStore(), CHECKPOINT_LOG_VERSION, layout.getDatabaseName(), NULL ) );
    }

    @Test
    void resetCheckpointVersionOnMissingLogFilesWhenValueIsDefinitelyWrong() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        removeTransactionLogs();

        assertTrue( isRecoveryRequired( layout ) );

        MetaDataStore.setRecord( pageCache, layout.metadataStore(), CHECKPOINT_LOG_VERSION, -42, layout.getDatabaseName(), NULL );

        recoverDatabase();
        assertFalse( isRecoveryRequired( layout ) );

        assertEquals( 0, MetaDataStore.getRecord( pageCache, layout.metadataStore(), CHECKPOINT_LOG_VERSION, layout.getDatabaseName(), NULL ) );
    }

    @Test
    void recoverySetsCheckpointLogVersionFieldNoCheckpointFiles() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        removeFileWithCheckpoint();

        assertTrue( isRecoveryRequired( layout ) );

        MetaDataStore.setRecord( pageCache, layout.metadataStore(), CHECKPOINT_LOG_VERSION, -5, layout.getDatabaseName(), NULL );

        recoverDatabase();
        assertFalse( isRecoveryRequired( layout ) );

        assertEquals( 0, MetaDataStore.getRecord( pageCache, layout.metadataStore(), CHECKPOINT_LOG_VERSION, layout.getDatabaseName(), NULL ) );
    }

    @Test
    void recoverySetsCheckpointLogVersionFieldSeveralCheckpointFiles() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );

        var checkpointFile = db.getDependencyResolver().resolveDependency( LogFiles.class ).getCheckpointFile();
        var appender = (DetachedCheckpointAppender) checkpointFile.getCheckpointAppender();
        appender.rotate();
        appender.checkPoint( LogCheckPointEvent.NULL, new LogPosition( 0, BASE_TX_LOG_BYTE_OFFSET ), Instant.now(), "test1" );
        appender.rotate();
        appender.checkPoint( LogCheckPointEvent.NULL, new LogPosition( 0, BASE_TX_LOG_BYTE_OFFSET ), Instant.now(), "test2" );
        appender.rotate();
        appender.checkPoint( LogCheckPointEvent.NULL, new LogPosition( 0, BASE_TX_LOG_BYTE_OFFSET ), Instant.now(), "test3" );

        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        removeFileWithCheckpoint();

        assertTrue( isRecoveryRequired( layout ) );

        MetaDataStore.setRecord( pageCache, layout.metadataStore(), CHECKPOINT_LOG_VERSION, -5, layout.getDatabaseName(), NULL );

        recoverDatabase();
        assertFalse( isRecoveryRequired( layout ) );

        assertEquals( 2, MetaDataStore.getRecord( pageCache, layout.metadataStore(), CHECKPOINT_LOG_VERSION, layout.getDatabaseName(), NULL ) );
    }

    @Test
    void recoverDatabaseWithAllTransactionsPredicate() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        long expectedLastTransactionId = getMetadataProvider( db ).getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue( isRecoveryRequired( layout ) );

        recoverDatabase( ALL );

        db = createDatabase();
        assertEquals( expectedLastTransactionId, getMetadataProvider( db ).getLastCommittedTransactionId() );
    }

    @Test
    void recoverDatabaseWithIdPredicateHigherToLastAvailable() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        long expectedLastTransactionId = getMetadataProvider( db ).getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue( isRecoveryRequired( layout ) );

        recoverDatabase( RecoveryCriteria.until( expectedLastTransactionId + 5 ) );

        db = createDatabase();
        assertEquals( expectedLastTransactionId, getMetadataProvider( db ).getLastCommittedTransactionId() );
    }

    @Test
    void recoverDatabaseWithIdPredicateLowerToLastAvailable() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        long originalLastCommitted = getMetadataProvider( db ).getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue( isRecoveryRequired( layout ) );

        long lastTransactionToBeApplied = originalLastCommitted - 5;
        recoverDatabase( RecoveryCriteria.until( lastTransactionToBeApplied ) );

        db = createDatabase();
        assertEquals( lastTransactionToBeApplied - 1, getMetadataProvider( db ).getLastCommittedTransactionId() );
    }

    @Test
    void recoverDatabaseWithDatePredicateHigherToLastAvailable() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();
        var metaDataStore = getMetadataProvider( db );
        long expectedLastCommitTimestamp = metaDataStore.getLastCommittedTransaction().commitTimestamp();
        long expectedLastTransactionId = metaDataStore.getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue( isRecoveryRequired( layout ) );

        recoverDatabase( RecoveryCriteria.until( Instant.ofEpochMilli( expectedLastCommitTimestamp + 1 ) ) );

        db = createDatabase();
        assertEquals( expectedLastTransactionId, getMetadataProvider( db ).getLastCommittedTransactionId() );
    }

    @Test
    void recoverDatabaseWithDatePredicateLowerToLastAvailable() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();

        var metaDataStore = getMetadataProvider( db );
        long expectedLastCommitTimestamp = metaDataStore.getLastCommittedTransaction().commitTimestamp();
        long expectedLastCommitted = metaDataStore.getLastCommittedTransactionId();

        fakeClock.forward( 4, MINUTES );

        generateSomeData( db );
        long originalLastCommitted = metaDataStore.getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue( isRecoveryRequired( layout ) );

        recoverDatabase( RecoveryCriteria.until( Instant.ofEpochMilli( expectedLastCommitTimestamp + 1 ) ) );

        db = createDatabase();
        long postRecoveryLastCommittedTxId = getMetadataProvider( db ).getLastCommittedTransactionId();
        assertEquals( expectedLastCommitted, postRecoveryLastCommittedTxId );
        assertNotEquals( originalLastCommitted, postRecoveryLastCommittedTxId );
    }

    @Test
    void recoverDatabaseWithIdPredicateWithNothingAfterLastCheckpoint() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();
        generateSomeData( db );
        long originalLastCommitted = getMetadataProvider( db ).getLastCommittedTransactionId();
        db.getDependencyResolver().resolveDependency( CheckPointerImpl.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
        generateSomeData( db );
        managementService.shutdown();

        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );
        assertTrue( isRecoveryRequired( layout ) );

        recoverDatabase( RecoveryCriteria.until( originalLastCommitted + 1 ) );

        db = createDatabase();
        assertEquals( originalLastCommitted, getMetadataProvider( db ).getLastCommittedTransactionId() );
    }

    @Test
    void earlyRecoveryTerminationOnTxIdCriteriaShouldPrintReason() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();
        generateSomeData( db );
        long originalLastCommitted = getMetadataProvider( db ).getLastCommittedTransactionId();
        managementService.shutdown();

        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );
        assertTrue( isRecoveryRequired( layout ) );

        long restoreUntilTxId = originalLastCommitted - 4;
        recoverDatabase( RecoveryCriteria.until( restoreUntilTxId ) );

        assertThat( logProvider ).containsMessages(
                "Partial database recovery based on provided criteria: transaction id should be < " + restoreUntilTxId + ". " +
                        "Last replayed transaction: transaction id: " + (restoreUntilTxId - 1) + ", time 1970-01-01 00:00:10.000+0000." );
        db = createDatabase();
        assertEquals( restoreUntilTxId - 1, getMetadataProvider( db ).getLastCommittedTransactionId() );
    }

    @Test
    void earlyRecoveryTerminationOnTxDateCriteriaShouldPrintReason() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        generateSomeData( db );
        DatabaseLayout layout = db.databaseLayout();

        var metaDataStore = getMetadataProvider( db );
        long expectedLastCommitTimestamp = metaDataStore.getLastCommittedTransaction().commitTimestamp();
        long expectedLastCommitted = metaDataStore.getLastCommittedTransactionId();

        fakeClock.forward( 10, MINUTES );

        generateSomeData( db );
        long originalLastCommitted = metaDataStore.getLastCommittedTransactionId();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue( isRecoveryRequired( layout ) );

        recoverDatabase( RecoveryCriteria.until( Instant.ofEpochMilli( expectedLastCommitTimestamp + 1 ) ) );

        assertThat( logProvider ).containsMessages(
                "Partial database recovery based on provided criteria: transaction date should be before 1970-01-01 00:00:10.001+0000. " +
                        "Last replayed transaction: transaction id: " + expectedLastCommitted + ", time 1970-01-01 00:00:10.000+0000." );

        db = createDatabase();
        long postRecoveryLastCommittedTxId = getMetadataProvider( db ).getLastCommittedTransactionId();
        assertEquals( expectedLastCommitted, postRecoveryLastCommittedTxId );
        assertNotEquals( originalLastCommitted, postRecoveryLastCommittedTxId );
    }

    @Test
    void failToReadTransactionOnIncorrectCriteria() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        removeFileWithCheckpoint();
        assertTrue( isRecoveryRequired( layout ) );

        assertThatThrownBy( () -> recoverDatabase( RecoveryCriteria.until( 2 ) ) )
                .hasCauseInstanceOf( RecoveryPredicateException.class )
                .getCause()
                .hasMessageContaining( "Partial recovery criteria can't be satisfied. " +
                        "No transaction after checkpoint matching to provided criteria found and fail " +
                        "to read transaction before checkpoint. Recovery criteria: transaction id should be < 2." );

        assertTrue( isRecoveryRequired( layout ) );
    }

    @Test
    void transactionBeforeCheckpointNotMatchingExpectedCriteria() throws Exception
    {
        GraphDatabaseAPI db = createDatabase();
        DatabaseLayout layout = db.databaseLayout();
        generateSomeData( db );
        db.getDependencyResolver().resolveDependency( CheckPointerImpl.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
        generateSomeData( db );
        managementService.shutdown();

        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( databaseLayout, fileSystem );

        assertTrue( isRecoveryRequired( layout ) );

        assertThatThrownBy( () -> recoverDatabase( RecoveryCriteria.until( 1 ) ) )
                .hasCauseInstanceOf( RecoveryPredicateException.class )
                .getCause().hasMessageContaining(
                        "Partial recovery criteria can't be satisfied. Transaction after and before " +
                                "checkpoint does not satisfy provided recovery criteria. Observed transaction id: 24, " +
                                "recovery criteria: transaction id should be < 1." );

        assertTrue( isRecoveryRequired( layout ) );
    }

    private boolean idGeneratorIsDirty( Path path, IdType idType ) throws IOException
    {
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate(), "my db" );
        try ( IdGenerator idGenerator = idGeneratorFactory.open( pageCache, path, idType, () -> 0L /*will not be used*/, 10_000, readOnly(),
                Config.defaults(), NULL, Sets.immutable.empty(), IdSlotDistribution.SINGLE_IDS ) )
        {
            MutableBoolean dirtyOnStartup = new MutableBoolean();
            InvocationHandler invocationHandler = ( proxy, method, args ) ->
            {
                if ( method.getName().equals( "dirtyOnStartup" ) )
                {
                    dirtyOnStartup.setTrue();
                }
                return null;
            };
            ReporterFactory reporterFactory = new ReporterFactory( invocationHandler );
            idGenerator.consistencyCheck( reporterFactory, NULL );
            return dirtyOnStartup.booleanValue();
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

    private static void createSingleNode( GraphDatabaseService service )
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
        recoverDatabase( EMPTY, ALL );
    }

    private void recoverDatabase( DatabaseTracers tracers ) throws Exception
    {
        recoverDatabase( tracers, ALL );
    }

    private void recoverDatabase( RecoveryCriteria recoveryCriteria ) throws Exception
    {
        recoverDatabase( EMPTY, recoveryCriteria );
    }

    void additionalConfiguration( Config config )
    {
        config.set( fail_on_missing_files, false );
    }

    TestDatabaseManagementServiceBuilder additionalConfiguration( TestDatabaseManagementServiceBuilder builder )
    {
        return builder;
    }

    private void recoverDatabase( DatabaseTracers databaseTracers, RecoveryCriteria recoveryCriteria ) throws Exception
    {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( new LoggingLogFileMonitor( logProvider.getLog( getClass() ) ) );
        Config config = Config.newBuilder().build();
        additionalConfiguration( config );
        assertTrue( isRecoveryRequired( databaseLayout, config ) );
        performRecovery( fileSystem, pageCache, databaseTracers, config, databaseLayout, defaultStorageEngine(), false, logProvider, monitors,
                Iterables.cast( Services.loadAll( ExtensionFactory.class ) ), Optional.empty(), RecoveryStartupChecker.EMPTY_CHECKER, INSTANCE, fakeClock,
                recoveryCriteria.toPredicate() );
        assertFalse( isRecoveryRequired( databaseLayout, config ) );
    }

    private boolean isRecoveryRequired( DatabaseLayout layout ) throws Exception
    {
        Config config = Config.newBuilder().build();
        additionalConfiguration( config );
        return isRecoveryRequired( layout, config );
    }

    private boolean isRecoveryRequired( DatabaseLayout layout, Config config ) throws Exception
    {
        return Recovery.isRecoveryRequired( fileSystem, layout, config, INSTANCE );
    }

    private int countCheckPointsInTransactionLogs() throws IOException
    {
        LogFiles logFiles = buildLogFiles();
        var checkpoints = logFiles.getCheckpointFile().reachableCheckpoints();
        return checkpoints.size();
    }

    private LogFiles buildLogFiles() throws IOException
    {
        return LogFilesBuilder
                .logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fileSystem )
                .withStorageEngineFactory( defaultStorageEngine() )
                .build();
    }

    private void removeTransactionLogs() throws IOException
    {
        LogFiles logFiles = buildLogFiles();
        for ( Path logFile : fileSystem.listFiles( logFiles.logFilesDirectory() ) )
        {
            fileSystem.deleteFile( logFile );
        }
    }

    private void removeFileWithCheckpoint() throws IOException
    {
        LogFiles logFiles = buildLogFiles();
        fileSystem.deleteFileOrThrow( logFiles.getCheckpointFile().getCurrentFile() );
    }

    private int countTransactionLogFiles() throws IOException
    {
        LogFiles logFiles = buildLogFiles();
        return logFiles.logFiles().length;
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
        return (GraphDatabaseAPI) managementService.database( databaseLayout.getDatabaseName() );
    }

    private void createBuilder( long logThreshold )
    {
        if ( builder == null )
        {
            logProvider = new AssertableLogProvider();
            fakeClock = Clocks.fakeClock( 10, SECONDS );
            builder = new TestDatabaseManagementServiceBuilder( neo4jLayout )
                    .setConfig( preallocate_logical_logs, false )
                    .setClock( fakeClock )
                    .setInternalLogProvider( logProvider )
                    .setConfig( logical_log_rotation_threshold, logThreshold );
            builder = additionalConfiguration( builder );
        }
    }

    private void startStopDatabaseWithForcedRecovery()
    {
        DatabaseManagementService forcedRecoveryManagementService = forcedRecoveryManagement();
        forcedRecoveryManagementService.shutdown();
    }

    private DatabaseManagementService forcedRecoveryManagement()
    {
        TestDatabaseManagementServiceBuilder serviceBuilder = new TestDatabaseManagementServiceBuilder( neo4jLayout )
                .setConfig( fail_on_missing_files, false );
        return additionalConfiguration( serviceBuilder ).build();
    }

    private static PageCache getDatabasePageCache( GraphDatabaseAPI databaseAPI )
    {
        return databaseAPI.getDependencyResolver().resolveDependency( PageCache.class );
    }

    private static MetadataProvider getMetadataProvider( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( MetadataProvider.class );
    }

    private void verifyRecoveryTimestampPresent( GraphDatabaseAPI databaseAPI ) throws IOException
    {
        GraphDatabaseAPI restartedDatabase = createDatabase();
        try
        {
            PageCache restartedCache = getDatabasePageCache( restartedDatabase );
            final long record = getRecord( restartedCache, databaseAPI.databaseLayout().metadataStore(), LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP,
                    databaseLayout.getDatabaseName(), NULL );
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
