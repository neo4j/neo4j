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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.impl.schema.LuceneIndexProviderFactory;
import org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory10;
import org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory20;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.count;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.test.Unzip.unzip;

public class StartOldDbOn3_4AndCreateFusionIndexIT
{
    private static final String ZIP_FILE_3_2 = "3_2-db.zip";
    private static final String ZIP_FILE_3_3 = "3_3-db.zip";

    private static final Label LABEL_LUCENE_10 = Label.label( "Label1" );
    private static final Label LABEL_FUSION_10 = Label.label( "Label2" );
    private static final Label LABEL_FUSION_20 = Label.label( "Label3" );
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";

    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    @Ignore( "Here as reference for how 3.2 db was created" )
    @Test
    public void create3_2Database() throws Exception
    {
        File storeDir = tempStoreDirectory();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        createIndexDataAndShutdown( db, LABEL_LUCENE_10 );
    }

    @Ignore( "Here as reference for how 3.3 db was created" )
    @Test
    public void create3_3Database() throws Exception
    {
        File storeDir = tempStoreDirectory();
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( storeDir );

        builder.setConfig( GraphDatabaseSettings.enable_native_schema_index, Settings.FALSE );
        GraphDatabaseService db = builder.newGraphDatabase();
        createIndexDataAndShutdown( db, LABEL_LUCENE_10 );

        builder.setConfig( GraphDatabaseSettings.enable_native_schema_index, Settings.TRUE );
        db = builder.newGraphDatabase();
        createIndexDataAndShutdown( db, LABEL_FUSION_10 );
        System.out.println( "Db created in " + storeDir.getAbsolutePath() );
    }

    @Test
    public void shouldOpen3_2DbAndCreateAndWorkWithSomeFusionIndexes() throws Exception
    {
        // given
        File storeDir = unzip( getClass(), ZIP_FILE_3_2, directory.absolutePath() );
        IndexRecoveryTracker indexRecoveryTracker = new IndexRecoveryTracker();

        // when
        GraphDatabaseAPI db = setupDb( storeDir, indexRecoveryTracker );

        // then
        verifyInitialState( indexRecoveryTracker, 2, InternalIndexState.ONLINE );
        try
        {
            // then
            verifyIndexes( db, LABEL_LUCENE_10 );

            // when
            createIndexesAndData( db, LABEL_FUSION_20 );

            // then
            verifyIndexes( db, LABEL_FUSION_20 );

            // when
            additionalUpdates( db, LABEL_LUCENE_10 );

            // then
            verifyAfterAdditionalUpdate( db, LABEL_LUCENE_10 );

            // when
            additionalUpdates( db, LABEL_FUSION_20 );

            // then
            verifyAfterAdditionalUpdate( db, LABEL_FUSION_20 );

            // and finally
            verifyExpectedProvider( db, LABEL_LUCENE_10, LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR );
            verifyExpectedProvider( db, LABEL_FUSION_20, NativeLuceneFusionIndexProviderFactory20.DESCRIPTOR );
        }
        finally
        {
            db.shutdown();
            indexRecoveryTracker.reset();
        }

        // when
        db = setupDb( storeDir, indexRecoveryTracker );
        try
        {
            // then
            verifyInitialState( indexRecoveryTracker, 4, InternalIndexState.ONLINE );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldOpen3_3DbAndCreateAndWorkWithSomeFusionIndexes() throws Exception
    {
        // given
        File storeDir = unzip( getClass(), ZIP_FILE_3_3, directory.absolutePath() );
        IndexRecoveryTracker indexRecoveryTracker = new IndexRecoveryTracker();

        // when
        GraphDatabaseAPI db = setupDb( storeDir, indexRecoveryTracker );

        // then
        verifyInitialState( indexRecoveryTracker, 4, InternalIndexState.ONLINE );
        try
        {
            // then
            verifyIndexes( db, LABEL_LUCENE_10 );
            verifyIndexes( db, LABEL_FUSION_10 );

            // when
            createIndexesAndData( db, LABEL_FUSION_20 );

            // then
            verifyIndexes( db, LABEL_FUSION_20 );

            // when
            additionalUpdates( db, LABEL_LUCENE_10 );

            // then
            verifyAfterAdditionalUpdate( db, LABEL_LUCENE_10 );

            // when
            additionalUpdates( db, LABEL_FUSION_10 );

            // then
            verifyAfterAdditionalUpdate( db, LABEL_FUSION_10 );

            // when
            additionalUpdates( db, LABEL_FUSION_20 );

            // then
            verifyAfterAdditionalUpdate( db, LABEL_FUSION_20 );

            // and finally
            verifyExpectedProvider( db, LABEL_LUCENE_10, LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR );
            verifyExpectedProvider( db, LABEL_FUSION_10, NativeLuceneFusionIndexProviderFactory10.DESCRIPTOR );
            verifyExpectedProvider( db, LABEL_FUSION_20, NativeLuceneFusionIndexProviderFactory20.DESCRIPTOR );
        }
        finally
        {
            db.shutdown();
        }

        // when
        db = setupDb( storeDir, indexRecoveryTracker );
        try
        {
            // then
            verifyInitialState( indexRecoveryTracker, 6, InternalIndexState.ONLINE );
        }
        finally
        {
            db.shutdown();
        }
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
        assertEquals( "exactly " + expectedNumberOfIndexes + " indexes ", expectedNumberOfIndexes, indexRecoveryTracker.initialStateMap.size() );
        for ( InternalIndexState actualInitialState : indexRecoveryTracker.initialStateMap.values() )
        {
            assertEquals( "initial state is online, don't do recovery", expectedInitialState, actualInitialState );
        }
    }

    private void verifyExpectedProvider( GraphDatabaseAPI db, Label label, IndexProvider.Descriptor expectedDescriptor ) throws TransactionFailureException
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

            CapableIndexReference index = schemaRead.index( labelId, key1Id );
            assertIndexHasExpectedProvider( expectedDescriptor, index );
            index = schemaRead.index( labelId, key1Id, key2Id );
            assertIndexHasExpectedProvider( expectedDescriptor, index );
            tx.success();
        }
    }

    private void assertIndexHasExpectedProvider( IndexProvider.Descriptor expectedDescriptor, CapableIndexReference index )
    {
        assertEquals( "same key", expectedDescriptor.getKey(), index.providerKey() );
        assertEquals( "same version", expectedDescriptor.getVersion(), index.providerVersion() );
    }

    private void createIndexDataAndShutdown( GraphDatabaseService db, Label label )
    {
        try
        {
            createIndexesAndData( db, label );
        }
        finally
        {
            db.shutdown();
        }
    }

    private File tempStoreDirectory() throws IOException
    {
        File file = File.createTempFile( "create-db", "neo4j" );
        File storeDir = new File( file.getAbsoluteFile().getParentFile(), file.getName() );
        FileUtils.deleteFile( file );
        return storeDir;
    }

    private void createIndexesAndData( GraphDatabaseService db, Label label )
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

    private void createData( GraphDatabaseService db, Label label )
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

    private void createSpatialAndTemporalData( GraphDatabaseAPI db, Label label )
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

    private void additionalUpdates( GraphDatabaseAPI db, Label label )
    {
        createData( db, label );
        createSpatialAndTemporalData( db, label );
    }

    private void verifyIndexes( GraphDatabaseAPI db, Label label ) throws Exception
    {
        assertTrue( hasIndex( db, label, KEY1 ) );
        assertEquals( 100, countIndexedNodes( db, label, KEY1 ) );

        assertTrue( hasIndex( db, label, KEY1, KEY2 ) );
        assertEquals( 34, countIndexedNodes( db, label, KEY1, KEY2 ) );
    }

    private void verifyAfterAdditionalUpdate( GraphDatabaseAPI db, Label label ) throws Exception
    {
        assertTrue( hasIndex( db, label, KEY1 ) );
        assertEquals( 300, countIndexedNodes( db, label, KEY1 ) );

        assertTrue( hasIndex( db, label, KEY1, KEY2 ) );
        assertEquals( 102, countIndexedNodes( db, label, KEY1, KEY2 ) );
    }

    private int countIndexedNodes( GraphDatabaseAPI db, Label label, String... keys ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = db.getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .getKernelTransactionBoundToThisThread( true );

            try ( Statement statement = ktx.acquireStatement() )
            {
                TokenRead tokenRead = ktx.tokenRead();
                int labelId = tokenRead.nodeLabel( label.name() );
                int[] propertyKeyIds = new int[keys.length];
                for ( int i = 0; i < keys.length; i++ )
                {
                    propertyKeyIds[i] = tokenRead.propertyKey( keys[i] );
                }

                CapableIndexReference index = ktx.schemaRead().index( labelId, propertyKeyIds );

                // wait for index to come online
                db.schema().awaitIndexesOnline( 5, TimeUnit.SECONDS );

                int count;
                StorageStatement storeStatement = ((KernelStatement) statement).getStoreStatement();
                IndexReader reader = storeStatement.getIndexReader( DefaultIndexReference.toDescriptor( index ) );
                IndexQuery[] predicates = new IndexQuery[propertyKeyIds.length];
                for ( int i = 0; i < propertyKeyIds.length; i++ )
                {
                    predicates[i] = IndexQuery.exists( propertyKeyIds[i] );
                }
                count = count( reader.query( predicates ) );

                tx.success();
                return count;
            }
        }
    }

    private boolean hasIndex( GraphDatabaseService db, Label label, String... keys )
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
        Map<SchemaIndexDescriptor,InternalIndexState> initialStateMap = new HashMap<>();

        @Override
        public void initialState( SchemaIndexDescriptor descriptor, InternalIndexState state )
        {
            initialStateMap.put( descriptor, state );
        }

        public void reset()
        {
            initialStateMap = new HashMap<>();
        }
    }
}
