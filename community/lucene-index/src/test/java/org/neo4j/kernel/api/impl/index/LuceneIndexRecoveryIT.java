/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

public class LuceneIndexRecoveryIT
{
    @Test
    public void addShouldBeIdempotentWhenDoingRecovery() throws Exception
    {
        // Given
        startDb(createLuceneIndexFactory());
        Label myLabel = label( "MyLabel" );

        createIndex( myLabel, true );

        createNode( myLabel, 12 );
        assertEquals( 1, doIndexLookup( myLabel, 12 ).size() );

        // And Given
        killDb();

        // When
        startDb( createLuceneIndexFactory() );

        // Then
        assertEquals( 1, doIndexLookup( myLabel, 12 ).size() );
    }

    @Test
    public void changeShouldBeIdempotentWhenDoingRecovery() throws Exception
    {
        // Given
        startDb(createLuceneIndexFactory());
        Label myLabel = label( "MyLabel" );
        createIndex( myLabel, true );
        long node = createNode( myLabel, 12 );
        rotateLogs();
        
        updateNode( node, 13 );

        // And Given
        killDb();

        // When
        startDb( createLuceneIndexFactory() );

        // Then
        assertEquals( 0, doIndexLookup( myLabel, 12 ).size() );
        assertEquals( 1, doIndexLookup( myLabel, 13 ).size() );
    }
    
    @Test
    public void removeShouldBeIdempotentWhenDoingRecovery() throws Exception
    {
        // Given
        startDb(createLuceneIndexFactory());
        Label myLabel = label( "MyLabel" );
        createIndex( myLabel, true );
        long node = createNode( myLabel, 12 );
        rotateLogs();
        
        deleteNode( node );

        // And Given
        killDb();

        // When
        startDb( createLuceneIndexFactory() );

        // Then
        assertEquals( 0, doIndexLookup( myLabel, 12 ).size() );
    }
    
    private void deleteNode( long node )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.getNodeById( node ).delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void shouldNotAddTwiceDuringRecoveryIfCrashedDuringPopulation() throws Exception
    {
        // Given
        startDb(createPopulationBlockingIndexFactory());
        Label myLabel = label( "MyLabel" );

        createIndex( myLabel, false );
        long nodeId = createNode( myLabel, 12 );
        assertEquals( 1, doIndexLookup( myLabel, 12 ).size() );

        // And Given
        killDb();

        // When
        startDb( createLuceneIndexFactory() );

        IndexDefinition indexDefinition = db.schema().getIndexes().iterator().next();
        db.schema().awaitIndexOnline( indexDefinition, 2l, TimeUnit.SECONDS );

        // Then
        assertEquals( 12, db.getNodeById( nodeId ).getProperty( NUM_BANANAS_KEY ) );
        assertEquals( 1, doIndexLookup( myLabel, 12 ).size() );
    }

    @Test
    public void shouldNotUpdateTwiceDuringRecovery() throws Exception
    {
        // Given
        startDb(createLuceneIndexFactory());
        Label myLabel = label( "MyLabel" );

        createIndex( myLabel, true );

        long nodeId = createNode( myLabel, 12 );
        updateNode(nodeId, 14);

        // And Given
        killDb();

        // When
        startDb( createLuceneIndexFactory() );

        // Then
        assertEquals( 0, doIndexLookup( myLabel, 12 ).size() );
        assertEquals( 1, doIndexLookup( myLabel, 14 ).size() );
    }

    private GraphDatabaseAPI db;
    private DirectoryFactory directoryFactory;
    private final DirectoryFactory ignoreCloseDirectoryFactory = new DirectoryFactory()
    {
        @Override
        public Directory open( File dir ) throws IOException
        {
            return directoryFactory.open( dir );
        }

        @Override
        public void close()
        {
        }
    };

    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private final String NUM_BANANAS_KEY = "number_of_bananas_owned";


    private void startDb(KernelExtensionFactory<?> indexProviderFactory)
    {
       if ( db != null )
           db.shutdown();

       TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
       factory.setFileSystem( fs.get() );
       factory.setKernelExtensions( Arrays.<KernelExtensionFactory<?>>asList( indexProviderFactory ) );
       db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
    }

    private void killDb()
    {
       if ( db != null )
       {
           fs.snapshot( new Runnable()
           {
               @Override
               public void run()
               {
                   db.shutdown();
                   db = null;
               }
           } );
       }
    }

    @Before
    public void before()
    {
        directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    }

    @After
    public void after()
    {
       if ( db != null )
           db.shutdown();
       directoryFactory.close();
    }

    private void rotateLogs()
    {
       db.getXaDataSourceManager().rotateLogicalLogs();
    }

    private void createIndex( Label label, boolean wait )
    {
       Transaction tx = db.beginTx();
       IndexDefinition definition = db.schema().indexCreator( label ).on( NUM_BANANAS_KEY ).create();
       tx.success();
       tx.finish();
       if (wait)
         db.schema().awaitIndexOnline( definition, 10, TimeUnit.SECONDS );
    }

    private Set<Node> doIndexLookup( Label myLabel, Object value )
    {
        Transaction tx = db.beginTx();
        Iterable<Node> iter = db.findNodesByLabelAndProperty( myLabel, NUM_BANANAS_KEY, value );
        Set<Node> nodes = asUniqueSet( iter );
        tx.success();
        tx.finish();
        return nodes;
    }

    private long createNode( Label label, int number )
           throws PropertyKeyNotFoundException, LabelNotFoundKernelException
    {
       Transaction tx = db.beginTx();
       try
       {
           Node node = db.createNode( label );
           node.setProperty( NUM_BANANAS_KEY, number );
           tx.success();
           return node.getId();
       }
       finally
       {
           tx.finish();
       }
    }

    private void updateNode( long nodeId, int value )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( NUM_BANANAS_KEY, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    // Creates a lucene index factory with the shared in-memory directory
    private KernelExtensionFactory<?> createLuceneIndexFactory() {
        return new KernelExtensionFactory<LuceneSchemaIndexProviderFactory.Dependencies>("my-index")
        {
            @Override
            public Lifecycle newKernelExtension( LuceneSchemaIndexProviderFactory.Dependencies dependencies ) throws
                    Throwable
            {
                return new LuceneSchemaIndexProvider( ignoreCloseDirectoryFactory, dependencies.getConfig() );
            }
        };
    }

    // Creates a lucene index factory with the shared in-memory directory, which waits for a latch on population
    private KernelExtensionFactory<?> createPopulationBlockingIndexFactory()
    {
        return new KernelExtensionFactory<LuceneSchemaIndexProviderFactory.Dependencies>("my-index") {

            @Override
            public Lifecycle newKernelExtension( LuceneSchemaIndexProviderFactory.Dependencies dependencies ) throws
                    Throwable
            {
                final LuceneSchemaIndexProvider delegate = new LuceneSchemaIndexProvider( ignoreCloseDirectoryFactory, dependencies.getConfig() );
                return new SchemaIndexProvider( 0 ) {
                    @Override
                    public IndexPopulator getPopulator( long indexId )
                    {
                        final IndexPopulator populator = delegate.getPopulator( indexId );
                        return new IndexPopulator()
                        {
                            @Override
                            public void create() throws IOException
                            {
                                populator.create();
                            }

                            @Override
                            public void drop() throws IOException
                            {
                                populator.drop();
                            }

                            @Override
                            public void add( long nodeId, Object propertyValue )
                            {
                                populator.add( nodeId, propertyValue );
                            }

                            @Override
                            public void update( Iterable<NodePropertyUpdate> updates )
                            {
                                populator.update( updates );
                            }

                            @Override
                            public void close( boolean populationCompletedSuccessfully ) throws IOException
                            {
                                populator.close( false );
                            }
                        };
                    }

                    @Override
                    public IndexAccessor getOnlineAccessor( long indexId )
                    {
                        return delegate.getOnlineAccessor( indexId );
                    }

                    @Override
                    public InternalIndexState getInitialState( long indexId )
                    {
                        return delegate.getInitialState( indexId );
                    }
                };
            }
        };
    }
}
