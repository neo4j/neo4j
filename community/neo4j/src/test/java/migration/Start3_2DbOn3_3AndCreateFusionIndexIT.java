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
package migration;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static java.util.Arrays.asList;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.count;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.test.Unzip.unzip;

public class Start3_2DbOn3_3AndCreateFusionIndexIT
{
    private static final String ZIP_FILE = "3_2-db.zip";

    private static final Label LABEL1 = Label.label( "Label1" );
    private static final Label LABEL2 = Label.label( "Label2" );
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
        try
        {
            createIndexesAndData( db, LABEL1 );
        }
        finally
        {
            db.shutdown();
        }
        System.out.println( "Db created in " + storeDir.getAbsolutePath() );
    }

    @Test
    public void shouldOpen3_2DbAndCreateAndWorkWithSomeFusionIndexes() throws Exception
    {
        // given
        File storeDir = unzip( getClass(), ZIP_FILE, directory.absolutePath() );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            verifyIndexes( db, LABEL1 );

            // when
            createIndexesAndData( db, LABEL2 );

            // then
            verifyIndexes( db, LABEL1 );
            verifyIndexes( db, LABEL2 );
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

    private void verifyIndexes( GraphDatabaseAPI db, Label label ) throws Exception
    {
        // There should be an index for the label and KEY1 containing 100 nodes
        assertTrue( hasIndex( db, label, KEY1 ) );
        assertEquals( 100, countIndexedNodes( db, label, KEY1 ) );

        // There should be an index for the label and KEY1+KEY2 containing 34 nodes
        assertTrue( hasIndex( db, label, KEY1, KEY2 ) );
        assertEquals( 34, countIndexedNodes( db, label, KEY1, KEY2 ) );
    }

    private int countIndexedNodes( GraphDatabaseAPI db, Label label, String... keys ) throws Exception
    {
        try ( Transaction tx = db.beginTx();
              KernelTransaction ktx = db.getDependencyResolver()
                      .resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true );
              Statement statement = ktx.acquireStatement() )
        {
            ReadOperations read = statement.readOperations();
            int labelId = read.labelGetForName( label.name() );
            int[] propertyKeyIds = new int[keys.length];
            for ( int i = 0; i < keys.length; i++ )
            {
                propertyKeyIds[i] = read.propertyKeyGetForName( keys[i] );
            }

            IndexDescriptor index = read.indexGetForSchema( new LabelSchemaDescriptor( labelId, propertyKeyIds ) );
            int count;
            StorageStatement storeStatement = ((KernelStatement)statement).getStoreStatement();
            IndexReader reader = storeStatement.getIndexReader( index );
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
}
