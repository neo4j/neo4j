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
package migration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory30;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.helpers.ArrayUtil.concat;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.test.Unzip.unzip;

@ExtendWith( TestDirectoryExtension.class )
class StartOldDbOnCurrentVersionAndCreateFusionIndexIT
{
    private static final String ZIP_FILE_3_5 = "3_5-db.zip";

    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";
    private DatabaseManagementService managementService;

    private enum Provider
    {
        // in order of appearance
        LUCENE_10( "Label1", "lucene-1.0", NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR ),
        FUSION_10( "Label2", "lucene+native-1.0", NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR ),
        FUSION_20( "Label3", "lucene+native-2.0", GenericNativeIndexProvider.DESCRIPTOR ),
        BTREE_10( "Label4", "native-btree-1.0", GenericNativeIndexProvider.DESCRIPTOR ),
        BTREE_10_40( "Label5", "native-btree-1.0", GenericNativeIndexProvider.DESCRIPTOR );

        private final Label label;
        @SuppressWarnings( {"unused", "FieldCanBeLocal"} )
        private final String originallyCreatedWithProvider;
        private final IndexProviderDescriptor descriptor;

        Provider( String labelName, String originallyCreatedWithProvider, IndexProviderDescriptor descriptor )
        {
            this.label = Label.label( labelName );
            this.originallyCreatedWithProvider = originallyCreatedWithProvider;
            this.descriptor = descriptor;
        }
    }

    private static final Provider DEFAULT_PROVIDER = Provider.BTREE_10_40;

    @Inject
    private TestDirectory directory;

    @Disabled( "Here as reference for how 3.5 db was created" )
    @Test
    void create3_5Database() throws Exception
    {
        File storeDir = tempStoreDirectory();
        DatabaseManagementServiceBuilder builder = new DatabaseManagementServiceBuilder( storeDir );

        createIndexDataAndShutdown( builder, "lucene-1.0", Provider.LUCENE_10.label );
        createIndexDataAndShutdown( builder, "lucene+native-1.0", Provider.FUSION_10.label );
        createIndexDataAndShutdown( builder, "lucene+native-2.0", Provider.FUSION_20.label );
        createIndexDataAndShutdown( builder, GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerName(), Provider.BTREE_10.label, db ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.execute( "CALL db.index.fulltext.createNodeIndex('fts1', ['Fts1'], ['prop1'] )" ).close();
                db.execute( "CALL db.index.fulltext.createNodeIndex('fts2', ['Fts2', 'Fts3'], ['prop1', 'prop2'] )" ).close();
                db.execute( "CALL db.index.fulltext.createNodeIndex('fts3', ['Fts4'], ['prop1'], {eventually_consistent: 'true'} )" ).close();
                db.execute( "CALL db.index.fulltext.createRelationshipIndex('fts4', ['FtsRel1', 'FtsRel2'], ['prop1', 'prop2'], " +
                        "{eventually_consistent: 'true'} )" ).close();
                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                Node node = db.createNode( Label.label( "Fts1" ), Label.label( "Fts2" ), Label.label( "Fts3" ), Label.label( "Fts4" ) );
                node.setProperty( "prop1", "a" );
                node.setProperty( "prop2", "a" );
                node.createRelationshipTo( node, RelationshipType.withName( "FtsRel1" ) ).setProperty( "prop1", "a" );
                node.createRelationshipTo( node, RelationshipType.withName( "FtsRel2" ) ).setProperty( "prop2", "a" );
                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                db.execute( "call db.index.fulltext.awaitEventuallyConsistentIndexRefresh" ).close();
                tx.success();
            }
        } );

        File zipFile = new File( storeDir.getParentFile(), storeDir.getName() + ".zip" );
        ZipUtils.zip( new DefaultFileSystemAbstraction(), storeDir, zipFile );
        System.out.println( "Db created in " + zipFile.getAbsolutePath() );
    }

    @Test
    void shouldOpen3_5DbAndCreateAndWorkWithSomeFusionIndexes() throws Exception
    {
        Provider highestProviderInOldVersion = Provider.BTREE_10;
        shouldOpenOldDbAndCreateAndWorkWithSomeFusionIndexes( ZIP_FILE_3_5, highestProviderInOldVersion );
    }

    private void shouldOpenOldDbAndCreateAndWorkWithSomeFusionIndexes( String zippedDbName, Provider highestProviderInOldVersion ) throws Exception
    {
        // given
        File targetDirectory = directory.databaseDir();
        unzip( getClass(), zippedDbName, targetDirectory );
        IndexRecoveryTracker indexRecoveryTracker = new IndexRecoveryTracker();
        // when
        File storeDir = directory.storeDir();
        managementService = setupDb( storeDir, indexRecoveryTracker );
        GraphDatabaseAPI db = getDefaultDatabase();
        // then
        Provider[] providers = providersUpToAndIncluding( highestProviderInOldVersion );
        Provider[] providersIncludingSubject = concat( providers, DEFAULT_PROVIDER );
        int fulltextIndexes = 4;
        int expectedNumberOfIndexes = fulltextIndexes + providers.length * 2;
        try
        {
            IndexRecoveryTracker nonMigratedNativeRecoveries = new IndexRecoveryTracker();
            filterOutNonMigratedNativeIndexes( db, indexRecoveryTracker, nonMigratedNativeRecoveries );
            int nonMigratedIndexCount = nonMigratedNativeRecoveries.initialStateMap.size();
            int migratedIndexCount = expectedNumberOfIndexes - nonMigratedIndexCount;

            // All indexes that has been migrated to other provider needs to be rebuilt:
            verifyInitialState( indexRecoveryTracker, migratedIndexCount, InternalIndexState.POPULATING );
            // All indexes that was already backed by Generic Native Index Provider do not need rebuilding, and start up as ONLINE:
            verifyInitialState( nonMigratedNativeRecoveries, nonMigratedIndexCount, InternalIndexState.ONLINE );

            // Wait for all populating indexes to finish, so we can verify their contents:
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 10, TimeUnit.MINUTES );
                tx.success();
            }

            // then
            for ( Provider provider : providers )
            {
                verifyIndexes( db, provider.label );
            }

            // when
            createIndexesAndData( db, DEFAULT_PROVIDER.label );

            // then
            verifyIndexes( db, DEFAULT_PROVIDER.label );

            // when
            for ( Provider provider : providersIncludingSubject )
            {
                additionalUpdates( db, provider.label );

                // then
                verifyAfterAdditionalUpdate( db, provider.label );
            }

            // then
            try ( Transaction tx = db.beginTx() )
            {
                IndexDefinition fts1 = db.schema().getIndexByName( "fts1" );

                Iterator<Label> fts1labels = fts1.getLabels().iterator();
                assertTrue( fts1labels.hasNext() );
                assertEquals( fts1labels.next().name(), "Fts1" );
                assertFalse( fts1labels.hasNext() );

                Iterator<String> fts1props = fts1.getPropertyKeys().iterator();
                assertTrue( fts1props.hasNext() );
                assertEquals( fts1props.next(), "prop1" );
                assertFalse( fts1props.hasNext() );

                IndexDefinition fts2 = db.schema().getIndexByName( "fts2" );

                Iterator<Label> fts2labels = fts2.getLabels().iterator();
                assertTrue( fts2labels.hasNext() );
                assertEquals( fts2labels.next().name(), "Fts2" );
                assertTrue( fts2labels.hasNext() );
                assertEquals( fts2labels.next().name(), "Fts3" );
                assertFalse( fts2labels.hasNext() );

                Iterator<String> fts2props = fts2.getPropertyKeys().iterator();
                assertTrue( fts2props.hasNext() );
                assertEquals( fts2props.next(), "prop1" );
                assertTrue( fts2props.hasNext() );
                assertEquals( fts2props.next(), "prop2" );
                assertFalse( fts2props.hasNext() );

                IndexDefinition fts3 = db.schema().getIndexByName( "fts3" );

                Iterator<Label> fts3labels = fts3.getLabels().iterator();
                assertTrue( fts3labels.hasNext() );
                assertEquals( fts3labels.next().name(), "Fts4" );
                assertFalse( fts3labels.hasNext() );

                Iterator<String> fts3props = fts3.getPropertyKeys().iterator();
                assertTrue( fts3props.hasNext() );
                assertEquals( fts3props.next(), "prop1" );
                assertFalse( fts3props.hasNext() );
                // TODO verify the index configuration of 'fts3' -- it is eventually consistent.

                IndexDefinition fts4 = db.schema().getIndexByName( "fts4" );

                Iterator<RelationshipType> fts4relTypes = fts4.getRelationshipTypes().iterator();
                assertTrue( fts4relTypes.hasNext() );
                assertEquals( fts4relTypes.next().name(), "FtsRel1" );
                assertTrue( fts4relTypes.hasNext() );
                assertEquals( fts4relTypes.next().name(), "FtsRel2" );
                assertFalse( fts4relTypes.hasNext() );

                Iterator<String> fts4props = fts4.getPropertyKeys().iterator();
                assertTrue( fts4props.hasNext() );
                assertEquals( fts4props.next(), "prop1" );
                assertTrue( fts4props.hasNext() );
                assertEquals( fts4props.next(), "prop2" );
                assertFalse( fts4props.hasNext() );
                // TODO verify the index configuration of 'fts3' -- it is eventually consistent.

                try ( var result = db.execute( "CALL db.index.fulltext.queryNodes( 'fts1', 'a' )" ).stream() )
                {
                    assertEquals( result.count(), 1L );
                }
                try ( var result = db.execute( "CALL db.index.fulltext.queryNodes( 'fts2', 'a' )" ).stream() )
                {
                    assertEquals( result.count(), 1L );
                }
                try ( var result = db.execute( "CALL db.index.fulltext.queryNodes( 'fts3', 'a' )" ).stream() )
                {
                    assertEquals( result.count(), 1L );
                }
                try ( var result = db.execute( "CALL db.index.fulltext.queryRelationships( 'fts4', 'a' )" ).stream() )
                {
                    assertEquals( result.count(), 2L );
                }

                tx.success();
            }

            // and finally
            for ( Provider provider : providersIncludingSubject )
            {
                verifyExpectedProvider( db, provider.label, provider.descriptor );
            }
        }
        finally
        {
            managementService.shutdown();
        }

        // when
        managementService = setupDb( storeDir, indexRecoveryTracker );
        try
        {
            // then
            verifyInitialState( indexRecoveryTracker, expectedNumberOfIndexes + 2, InternalIndexState.ONLINE );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private void filterOutNonMigratedNativeIndexes( GraphDatabaseAPI db, IndexRecoveryTracker indexRecoveryTracker,
            IndexRecoveryTracker nonMigratedIndexRecoveries )
    {
        String nonMigratedLabelName = Provider.BTREE_10.label.name();
        int nonMigratedLabelId = getLabelId( db, nonMigratedLabelName );
        indexRecoveryTracker.initialStateMap.entrySet().removeIf( entry ->
        {
            StoreIndexDescriptor indexDescriptor = entry.getKey();
            InternalIndexState internalIndexState = entry.getValue();
            if ( indexDescriptor.schema().getEntityTokenIds()[0] == nonMigratedLabelId )
            {
                nonMigratedIndexRecoveries.initialState( indexDescriptor, internalIndexState );
                return true;
            }
            return false;
        } );
    }

    private int getLabelId( GraphDatabaseAPI db, String labelName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            TokenRead tokenRead = getTokenRead( db );
            return tokenRead.nodeLabel( labelName );
        }
    }

    private TokenRead getTokenRead( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class )
                .getKernelTransactionBoundToThisThread( false )
                .tokenRead();
    }

    private Provider[] providersUpToAndIncluding( Provider provider )
    {
        return Stream.of( Provider.values() ).filter( p -> p.ordinal() <= provider.ordinal() ).toArray( Provider[]::new );
    }

    private DatabaseManagementService setupDb( File storeDir, IndexRecoveryTracker indexRecoveryTracker )
    {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( indexRecoveryTracker );
        return new DatabaseManagementServiceBuilder( storeDir )
                .setMonitors( monitors )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .build();
    }

    private GraphDatabaseAPI getDefaultDatabase()
    {
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void verifyInitialState( IndexRecoveryTracker indexRecoveryTracker, int expectedNumberOfIndexes, InternalIndexState expectedInitialState )
    {
        assertEquals( expectedNumberOfIndexes, indexRecoveryTracker.initialStateMap.size(), "exactly " + expectedNumberOfIndexes + " indexes " );
        for ( InternalIndexState actualInitialState : indexRecoveryTracker.initialStateMap.values() )
        {
            assertEquals( expectedInitialState, actualInitialState, "initial state is online, don't do recovery: " + indexRecoveryTracker.initialStateMap );
        }
    }

    private static void verifyExpectedProvider( GraphDatabaseAPI db, Label label, IndexProviderDescriptor expectedDescriptor )
            throws TransactionFailureException
    {
        try ( Transaction tx = db.beginTx();
              KernelTransaction kernelTransaction =
                      db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true ) )
        {
            TokenRead tokenRead = kernelTransaction.tokenRead();
            SchemaRead schemaRead = kernelTransaction.schemaRead();

            int labelId = tokenRead.nodeLabel( label.name() );
            int key1Id = tokenRead.propertyKey( KEY1 );
            int key2Id = tokenRead.propertyKey( KEY2 );

            IndexReference index = schemaRead.index( labelId, key1Id );
            assertIndexHasExpectedProvider( expectedDescriptor, index );
            index = schemaRead.index( labelId, key1Id, key2Id );
            assertIndexHasExpectedProvider( expectedDescriptor, index );
            tx.success();
        }
    }

    private static void assertIndexHasExpectedProvider( IndexProviderDescriptor expectedDescriptor, IndexReference index )
    {
        assertEquals( expectedDescriptor.getKey(), index.providerKey(), "same key" );
        assertEquals( expectedDescriptor.getVersion(), index.providerVersion(), "same version" );
    }

    private static void createIndexDataAndShutdown( DatabaseManagementServiceBuilder builder, String indexProvider, Label label )
    {
        createIndexDataAndShutdown( builder, indexProvider, label, db -> {} );
    }

    private static void createIndexDataAndShutdown( DatabaseManagementServiceBuilder builder, String indexProvider, Label label,
            Consumer<GraphDatabaseService> otherActions )
    {
        builder.setConfig( GraphDatabaseSettings.default_schema_provider, indexProvider );
        DatabaseManagementService dbms = builder.build();
        try
        {
            GraphDatabaseService db = dbms.database( DEFAULT_DATABASE_NAME );
            otherActions.accept( db );
            createIndexData( db, label );
        }
        finally
        {
            dbms.shutdown();
        }
    }

    private static void createIndexData( GraphDatabaseService db, Label label )
    {
        createIndexesAndData( db, label );
    }

    private static File tempStoreDirectory() throws IOException
    {
        File file = File.createTempFile( "create-db", "neo4j" );
        File storeDir = new File( file.getAbsoluteFile().getParentFile(), file.getName() );
        FileUtils.deleteFile( file );
        return storeDir;
    }

    private static void createIndexesAndData( GraphDatabaseService db, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( KEY1 ).create();
            db.schema().indexFor( label ).on( KEY1 ).on( KEY2 ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }

        createData( db, label );
    }

    private static void createData( GraphDatabaseService db, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                Node node = db.createNode( label );
                Object value = i % 2 == 0 ? i : String.valueOf( i );
                node.setProperty( KEY1, value );
                if ( i % 3 == 0 )
                {
                    node.setProperty( KEY2, value );
                }
            }
            tx.success();
        }
    }

    private static void createSpatialAndTemporalData( GraphDatabaseAPI db, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                Node node = db.createNode( label );
                Object value = i % 2 == 0 ?
                               Values.pointValue( CoordinateReferenceSystem.Cartesian, i, i ) :
                               DurationValue.duration( 0, 0, i, 0 );
                node.setProperty( KEY1, value );
                if ( i % 3 ==  0 )
                {
                    node.setProperty( KEY2, value );
                }
            }
            tx.success();
        }
    }

    private static void additionalUpdates( GraphDatabaseAPI db, Label label )
    {
        createData( db, label );
        createSpatialAndTemporalData( db, label );
    }

    private static void verifyIndexes( GraphDatabaseAPI db, Label label ) throws Exception
    {
        assertTrue( hasIndex( db, label, KEY1 ) );
        assertEquals( 100, countIndexedNodes( db, label, KEY1 ) );

        assertTrue( hasIndex( db, label, KEY1, KEY2 ) );
        assertEquals( 34, countIndexedNodes( db, label, KEY1, KEY2 ) );
    }

    private static void verifyAfterAdditionalUpdate( GraphDatabaseAPI db, Label label ) throws Exception
    {
        assertTrue( hasIndex( db, label, KEY1 ) );
        assertEquals( 300, countIndexedNodes( db, label, KEY1 ) );

        assertTrue( hasIndex( db, label, KEY1, KEY2 ) );
        assertEquals( 102, countIndexedNodes( db, label, KEY1, KEY2 ) );
    }

    private static int countIndexedNodes( GraphDatabaseAPI db, Label label, String... keys ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = db.getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .getKernelTransactionBoundToThisThread( true );

            TokenRead tokenRead = ktx.tokenRead();
            int labelId = tokenRead.nodeLabel( label.name() );
            int[] propertyKeyIds = new int[keys.length];
            for ( int i = 0; i < propertyKeyIds.length; i++ )
            {
                propertyKeyIds[i] = tokenRead.propertyKey( keys[i] );
            }
            IndexQuery[] predicates = new IndexQuery[propertyKeyIds.length];
            for ( int i = 0; i < propertyKeyIds.length; i++ )
            {
                predicates[i] = IndexQuery.exists( propertyKeyIds[i] );
            }
            IndexReference index = ktx.schemaRead().index( labelId, propertyKeyIds );
            IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
            try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                ktx.dataRead().nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, predicates );
                int count = 0;
                while ( cursor.next() )
                {
                    count++;
                }

                tx.success();
                return count;
            }
        }
    }

    private static boolean hasIndex( GraphDatabaseService db, Label label, String... keys )
    {
        try ( Transaction tx = db.beginTx() )
        {
            List<String> keyList = asList( keys );
            for ( IndexDefinition index : db.schema().getIndexes( label ) )
            {
                if ( asList( index.getPropertyKeys() ).equals( keyList ) )
                {
                    return true;
                }
            }
            tx.success();
        }
        return false;
    }

    private class IndexRecoveryTracker extends IndexingService.MonitorAdapter
    {
        Map<StoreIndexDescriptor,InternalIndexState> initialStateMap = new HashMap<>();

        @Override
        public void initialState( StoreIndexDescriptor descriptor, InternalIndexState state )
        {
            initialStateMap.put( descriptor, state );
        }

        public void reset()
        {
            initialStateMap = new HashMap<>();
        }
    }
}
