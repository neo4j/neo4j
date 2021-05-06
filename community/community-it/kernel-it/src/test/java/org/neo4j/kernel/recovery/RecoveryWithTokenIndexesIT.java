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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.IndexingTestUtil;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_single_automatic_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
class RecoveryWithTokenIndexesIT
{
    @RegisterExtension
    static final PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;
    private Config config;

    private static final Label label = Label.label( "label" );
    private static final RelationshipType type = RelationshipType.withName( "type" );

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
            managementService = null;
        }
    }

    private static Stream<Arguments> arguments()
    {
        return Stream.of(
                Arguments.of( "indexes created during recovery", false ),
                Arguments.of( "indexes updated during recovery", true )
        );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "arguments" )
    void recoverDatabaseWithTokenIndexes( String name, boolean checkpointIndexes ) throws Throwable
    {
        config = Config.newBuilder().set( neo4j_home, testDirectory.homePath() )
                .set( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true ).build();

        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        GraphDatabaseService db = startDatabase( fs );
        IndexingTestUtil.assertOnlyDefaultTokenIndexesExists( db );

        if ( checkpointIndexes )
        {
            // Checkpoint to not make index creation part of the recovery.
            checkPoint( db );
        }

        int numberOfEntities = 10;
        for ( int i = 0; i < numberOfEntities; i++ )
        {
            createEntities( db );
        }

        // Don't flush/checkpoint before taking the snapshot, to make the indexes need to recover (clean crash generation)
        EphemeralFileSystemAbstraction crashedFs = fs.snapshot();
        managementService.shutdown();
        fs.close();

        try ( PageCache cache = pageCacheExtension.getPageCache( crashedFs ) )
        {
            DatabaseLayout layout = DatabaseLayout.of( config );
            recoverDatabase( layout, crashedFs, cache );
        }

        db = startDatabase( crashedFs );

        // Verify that the default token indexes still exist
        IndexingTestUtil.assertOnlyDefaultTokenIndexesExists( db );
        awaitIndexesOnline( db );

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( numberOfEntities, tx.findNodes( label ).stream().count() );
            assertEquals( numberOfEntities, tx.findRelationships( type ).stream().count() );
        }
    }

    @Test
    void recoverDatabaseWithInjectedTokenIndex() throws Throwable
    {
        // Starting an existing database on 4.3 or newer binaries should make the old label scan store
        // into a token index - but there is no schema rule for it in store until after upgrade transaction has been run
        // (there is just an injected indexRule in the cache).
        // This tests that recovery on a database with the injected version of the NLI keeps it through recovery.

        config = Config.newBuilder()
                .set( allow_single_automatic_upgrade, false )
                .set( allow_upgrade, true )
                .set( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true ).build();

        // Database with 10 nodes with label 'label' and 10 relationships with type 'type'.
        int numberOfEntities = 10;
        GraphDatabaseService database = createDatabaseOfOlderVersion( "4-2-data-10-nodes-rels.zip" );
        DatabaseLayout layout = ((GraphDatabaseAPI) database).databaseLayout();

        verifyInjectedNLIExistAndOnline( database );

        managementService.shutdown();
        // Remove check point from shutdown, store upgrade, and shutdown done when creating the 4.2 store.
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( layout, fileSystem );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( layout, fileSystem );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( layout, fileSystem );

        try ( PageCache pageCache = pageCacheExtension.getPageCache( fileSystem ) )
        {
            recoverDatabaseOfOlderVersion( layout, pageCache );
        }

        GraphDatabaseService recoveredDatabase = startDatabaseOfOlderVersion();

        // Verify that the injected NLI still exist
        verifyInjectedNLIExistAndOnline( recoveredDatabase );

        try ( Transaction tx = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfEntities, tx.findNodes( label ).stream().count() );
            assertEquals( numberOfEntities, tx.findRelationships( type ).stream().count() );
        }
    }

    @Test
    void recoverDatabaseWithPersistedTokenIndex() throws Throwable
    {
        // Starting an existing database on 4.3 or newer binaries should make the old label scan store
        // into a token index - but there is no schema rule for it in store until after upgrade transaction has been run
        // (there is just an injected indexRule in the cache).
        // This tests that recovery on a database with the persisted version of the NLI keeps it through recovery.

        config = Config.newBuilder()
                .set( allow_single_automatic_upgrade, true )
                .set( allow_upgrade, true )
                .set( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true ).build();

        // Database with 10 nodes with label 'label' and 10 relationships with type 'type'.
        int numberOfEntities = 10;
        GraphDatabaseService database = createDatabaseOfOlderVersion( "4-2-data-10-nodes-rels.zip" );
        DatabaseLayout layout = ((GraphDatabaseAPI) database).databaseLayout();

        verifyInjectedNLIExistAndOnline( database );

        // Do a write transaction to trigger upgrade transaction.
        try ( Transaction tx = database.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }

        // Injected index should now have been turned into a real one.
        IndexDescriptor persistedNLI = IndexDescriptor.NLI_PROTOTYPE.materialise( 1 );
        verifyIndexExistAndOnline( database, persistedNLI );

        managementService.shutdown();
        // Remove check point from shutdown, store upgrade, and shutdown done when creating the 4.2 store.
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( layout, fileSystem );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( layout, fileSystem );
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile( layout, fileSystem );

        try ( PageCache pageCache = pageCacheExtension.getPageCache( fileSystem ) )
        {
            recoverDatabaseOfOlderVersion( layout, pageCache );
        }

        GraphDatabaseService recoveredDatabase = startDatabaseOfOlderVersion();

        // Verify that the persisted version of the NLI still exist
        verifyIndexExistAndOnline( recoveredDatabase, persistedNLI );

        try ( Transaction tx = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfEntities, tx.findNodes( label ).stream().count() );
            assertEquals( numberOfEntities, tx.findRelationships( type ).stream().count() );
        }
    }

    private static void checkPoint( GraphDatabaseService db ) throws IOException
    {
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( CheckPointer.class )
                .forceCheckPoint( new SimpleTriggerInfo( "Manual trigger" ) );
    }

    private static void awaitIndexesOnline( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 10, MINUTES );
            transaction.commit();
        }
    }

    private void verifyInjectedNLIExistAndOnline( GraphDatabaseService db )
    {
        verifyIndexExistAndOnline( db, IndexDescriptor.INJECTED_NLI );
    }

    private void verifyIndexExistAndOnline( GraphDatabaseService db, IndexDescriptor index )
    {
        awaitIndexesOnline( db );
        try ( Transaction tx = db.beginTx() )
        {
            var indexes = StreamSupport.stream( tx.schema().getIndexes().spliterator(), false )
                    .map( IndexDefinitionImpl.class::cast )
                    .map( IndexDefinitionImpl::getIndexReference )
                    .collect( Collectors.toList() );
            assertThat( indexes ).hasSize( 1 );
            assertThat( indexes.get( 0 ) ).isEqualTo( index );
        }
    }

    private GraphDatabaseService createDatabaseOfOlderVersion( String databaseArchiveName ) throws IOException
    {
        Unzip.unzip( getClass(), databaseArchiveName, testDirectory.homePath() );

        return startDatabaseOfOlderVersion();
    }

    private GraphDatabaseService startDatabaseOfOlderVersion()
    {
        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( config )
                .build();
        return managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    private void recoverDatabaseOfOlderVersion( DatabaseLayout layout, PageCache pageCache ) throws Exception
    {
        recoverDatabase( layout, fileSystem, pageCache );
    }

    private GraphDatabaseService startDatabase( EphemeralFileSystemAbstraction fs )
    {
        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setFileSystem( fs )
                .impermanent()
                .setConfig( config )
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void recoverDatabase( DatabaseLayout layout, FileSystemAbstraction fs, PageCache cache ) throws Exception
    {
        assertTrue( Recovery.isRecoveryRequired( fs, layout, config, INSTANCE ) );
        performRecovery( fs, cache, DatabaseTracers.EMPTY, config, layout, INSTANCE );
        assertFalse( Recovery.isRecoveryRequired( fs, layout, config, INSTANCE ) );
    }

    private void createEntities( GraphDatabaseService service )
    {
        try ( Transaction transaction = service.beginTx() )
        {
            Node node = transaction.createNode( label );
            node.createRelationshipTo( node, type );
            transaction.commit();
        }
    }
}
