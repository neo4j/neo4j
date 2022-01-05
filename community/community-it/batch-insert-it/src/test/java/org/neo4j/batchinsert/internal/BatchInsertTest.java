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
package org.neo4j.batchinsert.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@PageCacheExtension
@Neo4jLayoutExtension
class BatchInsertTest
{
    private static final String INTERNAL_LOG_FILE = "debug.log";
    private static final RelationshipType[] relTypeArray = {
        RelTypes.REL_TYPE1, RelTypes.REL_TYPE2, RelTypes.REL_TYPE3,
        RelTypes.REL_TYPE4, RelTypes.REL_TYPE5 };

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private RecordDatabaseLayout databaseLayout;

    private DatabaseManagementService managementService;

    private static Stream<Arguments> params()
    {
        return Stream.of(
            arguments( 5 ),
            arguments( GraphDatabaseSettings.dense_node_threshold.defaultValue() )
        );
    }

    private enum RelTypes implements RelationshipType
    {
        BATCH_TEST,
        REL_TYPE1,
        REL_TYPE2,
        REL_TYPE3,
        REL_TYPE4,
        REL_TYPE5
    }

    @AfterEach
    void cleanup()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
            managementService = null;
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldUpdateStringArrayPropertiesOnNodesUsingBatchInserter1( int denseNodeThreshold ) throws Exception
    {
        // Given
        var inserter = newBatchInserter( denseNodeThreshold );

        String[] array1 = {"1"};
        String[] array2 = {"a"};

        long id1 = inserter.createNode();

        // When
        inserter.setNodeProperty( id1, "array", array1 );
        inserter.setNodeProperty( id1, "array", array2 );

        // Then
        var db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );

        try ( var tx = db.beginTx() )
        {
            assertThat( tx.getNodeById( id1 ).getProperty( "array" ) ).isEqualTo( array2 );
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testSimple( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
        long node1 = inserter.createNode();
        long node2 = inserter.createNode();
        long rel1 = inserter.createRelationship( node1, node2, withName( "test" ) );

        var db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );

        try ( var tx = db.beginTx() )
        {
            var n1 = tx.getNodeById( node1 );
            var n2 = tx.getNodeById( node2 );
            var rel = tx.getRelationshipById( rel1 );
            assertThat( rel.getStartNode().getId() ).isEqualTo( n1.getId() );
            assertThat( rel.getEndNode().getId() ).isEqualTo( n2.getId() );
            assertThat( rel.getType() ).isEqualTo( withName( "test" ) );
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testSetAndAddNodeProperties( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
        long tehNode = inserter.createNode();
        inserter.setNodeProperty( tehNode, "four", "four" );
        inserter.setNodeProperty( tehNode, "five", "five" );

        var db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );

        try ( var tx = db.beginTx() )
        {
            var node = tx.getNodeById( tehNode );
            assertThat( node.getProperty( "four" ) ).isEqualTo( "four" );
            assertThat( node.getProperty( "five" ) ).isEqualTo( "five" );
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void setSingleProperty( int denseNodeThreshold ) throws Exception
    {
        BatchInserter inserter = newBatchInserter( denseNodeThreshold );
        long nodeById = inserter.createNode();

        String value = "Something";
        String key = "name";
        inserter.setNodeProperty( nodeById, key, value );

        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );
        try ( var tx = db.beginTx() )
        {
            var node = tx.getNodeById( nodeById );
            assertThat( node.getProperty( key ) ).isEqualTo( value );
        }
        managementService.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testMore( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
        long startNode = inserter.createNode();
        long[] endNodes = new long[25];
        Set<Long> rels = new HashSet<>();
        for ( int i = 0; i < 25; i++ )
        {
            endNodes[i] = inserter.createNode();
            rels.add( inserter.createRelationship( startNode, endNodes[i], relTypeArray[i % 5] ) );
        }

        var db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );
        try ( var tx = db.beginTx() )
        {
            var node = tx.getNodeById( startNode );
            var nodeRels = node.getRelationships();
            for ( Relationship rel : nodeRels )
            {
                assertThat( rels.remove( rel.getId() ) ).isTrue();
            }
            assertThat( rels ).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void makeSureLoopsCanBeCreated( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );
        long startNode = inserter.createNode();
        long otherNode = inserter.createNode();
        long selfRelationship = inserter.createRelationship( startNode, startNode, relTypeArray[0] );
        long relationship = inserter.createRelationship( startNode, otherNode, relTypeArray[0] );

        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );

        try ( Transaction transaction = db.beginTx() )
        {
            Node realStartNode = transaction.getNodeById( startNode );
            Relationship realSelfRelationship = transaction.getRelationshipById( selfRelationship );
            Relationship realRelationship = transaction.getRelationshipById( relationship );
            assertEquals( realSelfRelationship,
                    realStartNode.getSingleRelationship( RelTypes.REL_TYPE1, Direction.INCOMING ) );
            assertEquals( asSet( realSelfRelationship, realRelationship ),
                    Iterables.asSet( realStartNode.getRelationships( Direction.OUTGOING ) ) );
            assertEquals( asSet( realSelfRelationship, realRelationship ),
                    Iterables.asSet( realStartNode.getRelationships() ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void createBatchNodeAndRelationshipsDeleteAllInEmbedded( int denseNodeThreshold ) throws Exception
    {
        /*
         *    ()--[REL_TYPE1]-->(node)--[BATCH_TEST]->()
         */

        var inserter = newBatchInserter( denseNodeThreshold );
        long nodeId = inserter.createNode();
        inserter.createRelationship( nodeId, inserter.createNode(),
                RelTypes.BATCH_TEST );
        inserter.createRelationship( inserter.createNode(), nodeId,
                                     RelTypes.REL_TYPE1 );

        // Delete node and all its relationships
        GraphDatabaseService db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
            }
            node.delete();
            tx.commit();
        }

        managementService.shutdown();
    }

    @Test
    void messagesLogGetsClosed() throws IOException
    {
        Config config = Config.newBuilder()
                .set( preallocate_logical_logs, false )
                .set( neo4j_home, testDirectory.homePath() )
                .build();
        BatchInserter inserter = BatchInserters.inserter( databaseLayout, fs, config );
        inserter.shutdown();
        Files.delete( databaseLayout.getNeo4jLayout().homeDirectory().resolve( INTERNAL_LOG_FILE ) );
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldRepopulatePreexistingIndexes( int denseNodeThreshold ) throws Throwable
    {
        // GIVEN
        var db = instantiateGraphDatabaseService( denseNodeThreshold );

        try ( var tx = db.beginTx() )
        {
            tx.schema().indexFor( withName( "Hacker" ) ).on( "handle" ).create();
            tx.commit();
        }

        try ( var tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            var rel = tx.createNode().createRelationshipTo( tx.createNode(), withName( "Hacker" ) );
            rel.setProperty( "handle", "Jakewins" );
            tx.commit();
        }

        managementService.shutdown();

        BatchInserter inserter = newBatchInserter( denseNodeThreshold );

        long boggle = inserter.createRelationship( inserter.createNode(), inserter.createNode(), withName( "Hacker" ) );
        inserter.setRelationshipProperty( boggle, "handle", "b0ggl3" );

        db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );
        try ( var tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
            assertThat( tx.findRelationships( withName( "Hacker" ) ).stream() ).hasSize( 2 );
            assertThat( tx.findRelationships( withName( "Hacker" ), "handle", "b0ggl3" ).stream().map( Entity::getId ) ).containsExactly( boggle );
        }
        managementService.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void propertiesCanBeReSetUsingBatchInserter( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        long nodeId = inserter.createNode();
        inserter.setNodeProperty( nodeId, "name", "YetAnotherOne" );
        inserter.setNodeProperty( nodeId, "additional", "something" );

        var db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );

        try ( var tx = db.beginTx() )
        {
            var node = tx.getNodeById( nodeId );
            assertThat( node.getProperty( "name" ) ).isEqualTo( "YetAnotherOne" );
            assertThat( node.getProperty( "additional" ) ).isEqualTo( "something" );
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void replaceWithBiggerPropertySpillsOverIntoNewPropertyRecord( int denseNodeThreshold ) throws Exception
    {
        // GIVEN
        var inserter = newBatchInserter( denseNodeThreshold );
        long id = inserter.createNode();
        inserter.setNodeProperty( id, "count", 1 );

        // WHEN
        inserter.setNodeProperty( id, "count", "something_bigger" );

        var db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );

        try ( var tx = db.beginTx() )
        {
            assertThat( tx.getNodeById( id ).getProperty( "count" ) ).isEqualTo( "something_bigger" );
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void mustSplitUpRelationshipChainsWhenCreatingDenseNodes( int denseNodeThreshold ) throws Exception
    {
        var inserter = newBatchInserter( denseNodeThreshold );

        long node1 = inserter.createNode();
        long node2 = inserter.createNode();

        for ( int i = 0; i < 1000; i++ )
        {
            for ( MyRelTypes relType : MyRelTypes.values() )
            {
                inserter.createRelationship( node1, node2, relType );
            }
        }

        NeoStores neoStores = getFlushedNeoStores( inserter );
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord record = nodeStore.newRecord();
        try ( var cursor = nodeStore.openPageCursorForReading( 0, NULL ) )
        {
            nodeStore.getRecordByCursor( node1, record, NORMAL, cursor );
        }
        assertTrue( record.isDense(), "Node " + record + " should have been dense" );
        inserter.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldBuildCorrectCountsStoreOnIncrementalImport( int denseNodeThreshold ) throws Exception
    {
        // given
        var type = withName( "Person" );
        for ( int r = 0; r < 3; r++ )
        {
            try ( var inserter = newBatchInserter( denseNodeThreshold ) )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    inserter.createRelationship( inserter.createNode(), inserter.createNode(), type );
                }
            }

            // then
            CursorContext cursorContext = NULL;
            try ( var countsStore = new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), fs, RecoveryCleanupWorkCollector.immediate(),
                    new CountsBuilder()
                    {
                        @Override
                        public void initialize( CountsAccessor.Updater updater, CursorContext cursorContext, MemoryTracker memoryTracker )
                        {
                            throw new UnsupportedOperationException( "Should not be required" );
                        }

                        @Override
                        public long lastCommittedTxId()
                        {
                            return TransactionIdStore.BASE_TX_ID;
                        }
                    }, readOnly(), PageCacheTracer.NULL, GBPTreeCountsStore.NO_MONITOR, databaseLayout.getDatabaseName(), 1000,
                    NullLogProvider.getInstance(), cursorContext ) )
            {
                countsStore.start( cursorContext, StoreCursors.NULL, INSTANCE );
                assertThat( countsStore.relationshipCount( ANY_LABEL, 0, ANY_LABEL, cursorContext ) ).isEqualTo( (r + 1) * 100L );
            }
        }
    }

    @Test
    void shouldIncrementDegreesOnUpdatingDenseNode() throws Exception
    {
        // given
        int denseNodeThreshold = 10;
        BatchInserter inserter = newBatchInserter( configurationBuilder()
                .set( dense_node_threshold, 10 )
                // Make it flush (and empty the record changes set) multiple times during this insertion
                .set( GraphDatabaseInternalSettings.batch_inserter_batch_size, 2 )
                .build() );
        long denseNode = inserter.createNode();

        // when
        for ( int i = 0; i < denseNodeThreshold * 2; i++ )
        {
            inserter.createRelationship( denseNode, inserter.createNode(), relTypeArray[0] );
        }

        // then
        GraphDatabaseAPI db = switchToEmbeddedGraphDatabaseService( inserter, denseNodeThreshold );
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( tx.getNodeById( denseNode ).getDegree( relTypeArray[0], Direction.OUTGOING ) ).isEqualTo( denseNodeThreshold * 2 );
        }
        managementService.shutdown();
    }

    private Config.Builder configurationBuilder()
    {
        return Config.newBuilder()
                .set( neo4j_home, testDirectory.absolutePath() )
                .set( preallocate_logical_logs, false );
    }

    private Config configuration( int denseNodeThreshold )
    {
        return configurationBuilder()
                .set( GraphDatabaseSettings.dense_node_threshold, denseNodeThreshold )
                .build();
    }

    private BatchInserter newBatchInserter( int denseNodeThreshold ) throws Exception
    {
        return newBatchInserter( configuration( denseNodeThreshold ) );
    }

    private BatchInserter newBatchInserter( Config config ) throws Exception
    {
        return BatchInserters.inserter( databaseLayout, fs, config );
    }

    private GraphDatabaseAPI switchToEmbeddedGraphDatabaseService( BatchInserter inserter, int denseNodeThreshold )
    {
        inserter.shutdown();
        return instantiateGraphDatabaseService( denseNodeThreshold );
    }

    private GraphDatabaseAPI instantiateGraphDatabaseService( int denseNodeThreshold )
    {
        TestDatabaseManagementServiceBuilder factory = new TestDatabaseManagementServiceBuilder( databaseLayout );
        factory.setFileSystem( fs );
        managementService = factory.impermanent()
                                   // Shouldn't be necessary to set dense node threshold since it's a stick config
                                   .setConfig( configuration( denseNodeThreshold ) ).build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static void forceFlush( BatchInserter inserter )
    {
        ((BatchInserterImpl)inserter).forceFlushChanges();
    }

    private static NeoStores getFlushedNeoStores( BatchInserter inserter )
    {
        forceFlush( inserter );
        return ((BatchInserterImpl) inserter).getNeoStores();
    }
}
