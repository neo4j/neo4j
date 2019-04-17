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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.LuceneIndexProviderFactory;
import org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory10;
import org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory20;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.helpers.ArrayUtil.concat;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.test.Unzip.unzip;

@ExtendWith( TestDirectoryExtension.class )
class StartOldDbOnCurrentVersionAndCreateFusionIndexIT
{
    private static final String ZIP_FILE_3_2 = "3_2-db.zip";
    private static final String ZIP_FILE_3_3 = "3_3-db.zip";
    private static final String ZIP_FILE_3_4 = "3_4-db.zip";

    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";

    private enum Provider
    {
        // in order of appearance
        LUCENE_10( "Label1", GraphDatabaseSettings.SchemaIndex.LUCENE10, LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR ),
        FUSION_10( "Label2", GraphDatabaseSettings.SchemaIndex.NATIVE10, NativeLuceneFusionIndexProviderFactory10.DESCRIPTOR ),
        FUSION_20( "Label3", GraphDatabaseSettings.SchemaIndex.NATIVE20, NativeLuceneFusionIndexProviderFactory20.DESCRIPTOR ),
        BTREE_10( "Label4", GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10, GenericNativeIndexProvider.DESCRIPTOR );

        private final Label label;
        private final GraphDatabaseSettings.SchemaIndex setting;
        private final IndexProviderDescriptor descriptor;

        Provider( String labelName, GraphDatabaseSettings.SchemaIndex setting, IndexProviderDescriptor descriptor )
        {
            this.label = Label.label( labelName );
            this.setting = setting;
            this.descriptor = descriptor;
        }
    }

    private static final Provider DEFAULT_PROVIDER = Provider.BTREE_10;

    @Inject
    private TestDirectory directory;

    @Disabled( "Here as reference for how 3.2 db was created" )
    @Test
    void create3_2Database() throws Exception
    {
        File storeDir = tempStoreDirectory();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        createIndexData( db, Provider.LUCENE_10.label );
        db.shutdown();

        File zipFile = new File( storeDir.getParentFile(), storeDir.getName() + ".zip" );
        ZipUtils.zip( new DefaultFileSystemAbstraction(), storeDir, zipFile );
        System.out.println( "Db created in " + zipFile.getAbsolutePath() );
    }

    @Disabled( "Here as reference for how 3.3 db was created" )
    @Test
    void create3_3Database() throws Exception
    {
        File storeDir = tempStoreDirectory();
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( storeDir );

        builder.setConfig( GraphDatabaseSettings.enable_native_schema_index, Settings.FALSE );
        GraphDatabaseService db = builder.newGraphDatabase();
        createIndexData( db, Provider.LUCENE_10.label );
        db.shutdown();

        builder.setConfig( GraphDatabaseSettings.enable_native_schema_index, Settings.TRUE );
        db = builder.newGraphDatabase();
        createIndexData( db, Provider.FUSION_10.label );
        db.shutdown();

        File zipFile = new File( storeDir.getParentFile(), storeDir.getName() + ".zip" );
        ZipUtils.zip( new DefaultFileSystemAbstraction(), storeDir, zipFile );
        System.out.println( "Db created in " + zipFile.getAbsolutePath() );
    }

    @Disabled( "Here as reference for how 3.4 db was created" )
    @Test
    void create3_4Database() throws Exception
    {
        File storeDir = tempStoreDirectory();
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( storeDir );

        createIndexDataAndShutdown( builder, GraphDatabaseSettings.SchemaIndex.LUCENE10.providerName(), Provider.LUCENE_10.label );
        createIndexDataAndShutdown( builder, GraphDatabaseSettings.SchemaIndex.NATIVE10.providerName(), Provider.FUSION_10.label );
        createIndexDataAndShutdown( builder, GraphDatabaseSettings.SchemaIndex.NATIVE20.providerName(), Provider.FUSION_20.label );

        File zipFile = new File( storeDir.getParentFile(), storeDir.getName() + ".zip" );
        ZipUtils.zip( new DefaultFileSystemAbstraction(), storeDir, zipFile );
        System.out.println( "Db created in " + zipFile.getAbsolutePath() );
    }

    @Test
    void shouldOpen3_2DbAndCreateAndWorkWithSomeFusionIndexes() throws Exception
    {
        shouldOpenOldDbAndCreateAndWorkWithSomeFusionIndexes( ZIP_FILE_3_2, Provider.LUCENE_10 );
    }

    @Test
    void shouldOpen3_3DbAndCreateAndWorkWithSomeFusionIndexes() throws Exception
    {
        shouldOpenOldDbAndCreateAndWorkWithSomeFusionIndexes( ZIP_FILE_3_3, Provider.FUSION_10 );
    }

    @Test
    void shouldOpen3_4DbAndCreateAndWorkWithSomeFusionIndexes() throws Exception
    {
        shouldOpenOldDbAndCreateAndWorkWithSomeFusionIndexes( ZIP_FILE_3_4, Provider.FUSION_20 );
    }

    private void shouldOpenOldDbAndCreateAndWorkWithSomeFusionIndexes( String zippedDbName, Provider highestProviderInOldVersion ) throws Exception
    {
        // given
        unzip( getClass(), zippedDbName, directory.databaseDir() );
        IndexRecoveryTracker indexRecoveryTracker = new IndexRecoveryTracker();
        // when
        GraphDatabaseAPI db = setupDb( directory.databaseDir(), indexRecoveryTracker );

        // then
        Provider[] providers = providersUpToAndIncluding( highestProviderInOldVersion );
        Provider[] providersIncludingSubject = concat( providers, DEFAULT_PROVIDER );
        int expectedNumberOfIndexes = providers.length * 2;
        try
        {
            verifyInitialState( indexRecoveryTracker, expectedNumberOfIndexes, InternalIndexState.ONLINE );

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

            // and finally
            for ( Provider provider : providersIncludingSubject )
            {
                verifyExpectedProvider( db, provider.label, provider.descriptor );
            }
        }
        finally
        {
            db.shutdown();
        }

        // when
        db = setupDb( directory.databaseDir(), indexRecoveryTracker );
        try
        {
            // then
            verifyInitialState( indexRecoveryTracker, expectedNumberOfIndexes + 2, InternalIndexState.ONLINE );
        }
        finally
        {
            db.shutdown();
        }
    }

    private Provider[] providersUpToAndIncluding( Provider provider )
    {
        return Stream.of( Provider.values() ).filter( p -> p.ordinal() <= provider.ordinal() ).toArray( Provider[]::new );
    }

    private GraphDatabaseAPI setupDb( File storeDir, IndexRecoveryTracker indexRecoveryTracker )
    {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( indexRecoveryTracker );
        return (GraphDatabaseAPI) new GraphDatabaseFactory()
                .setMonitors( monitors )
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .newGraphDatabase();
    }

    private void verifyInitialState( IndexRecoveryTracker indexRecoveryTracker, int expectedNumberOfIndexes, InternalIndexState expectedInitialState )
    {
        assertEquals( expectedNumberOfIndexes, indexRecoveryTracker.initialStateMap.size(), "exactly " + expectedNumberOfIndexes + " indexes " );
        for ( InternalIndexState actualInitialState : indexRecoveryTracker.initialStateMap.values() )
        {
            assertEquals( expectedInitialState, actualInitialState, "initial state is online, don't do recovery" );
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

    private static void createIndexDataAndShutdown( GraphDatabaseBuilder builder, String indexProvider, Label label )
    {
        createIndexDataAndShutdown( builder, indexProvider, label, db -> {} );
    }

    private static void createIndexDataAndShutdown( GraphDatabaseBuilder builder, String indexProvider, Label label,
            Consumer<GraphDatabaseService> otherActions )
    {
        builder.setConfig( GraphDatabaseSettings.default_schema_provider, indexProvider );
        GraphDatabaseService db = builder.newGraphDatabase();
        try
        {
            otherActions.accept( db );
            createIndexData( db, label );
        }
        finally
        {
            db.shutdown();
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
            NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor();
            ktx.dataRead().nodeIndexSeek( index, cursor, IndexOrder.NONE, false, predicates );
            int count = 0;
            while ( cursor.next() )
            {
                count++;
            }

            tx.success();
            return count;
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
        Map<IndexDescriptor,InternalIndexState> initialStateMap = new HashMap<>();

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
