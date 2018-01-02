/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipOutputStream;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;

public class LuceneIndexRecoveryIT
{
    private final static Label myLabel = label( "MyLabel" );
    @Rule
    public final AssertableLogProvider log = new AssertableLogProvider( true );

    @Test
    public void addShouldBeIdempotentWhenDoingRecovery() throws Exception
    {
        // Given
        startDb( createLuceneIndexFactory() );

        IndexDefinition index = createIndex( myLabel );
        waitForIndex( index );

        long nodeId = createNode( myLabel, 12 );
        try(Transaction tx = db.beginTx())
        {
            assertNotNull( db.getNodeById( nodeId ) );
        }
        assertEquals( 1, doIndexLookup( myLabel, 12 ).size() );

        // And Given
        killDb();

        // When
        startDb( createLuceneIndexFactory() );

        // Then
        try(Transaction tx = db.beginTx())
        {
            assertNotNull( db.getNodeById( nodeId ) );
        }
        assertEquals( 1, doIndexLookup( myLabel, 12 ).size() );
    }

    @Test
    public void changeShouldBeIdempotentWhenDoingRecovery() throws Exception
    {
        // Given
        startDb( createLuceneIndexFactory() );

        IndexDefinition indexDefinition = createIndex( myLabel );
        waitForIndex( indexDefinition );

        long node = createNode( myLabel, 12 );
        rotateLogsAndCheckPoint();

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
        startDb( createLuceneIndexFactory() );

        IndexDefinition indexDefinition = createIndex( myLabel );
        waitForIndex( indexDefinition );

        long node = createNode( myLabel, 12 );
        rotateLogsAndCheckPoint();

        deleteNode( node );

        // And Given
        killDb();

        // When
        startDb( createLuceneIndexFactory() );

        // Then
        assertEquals( 0, doIndexLookup( myLabel, 12 ).size() );
    }

    @Test
    public void shouldNotAddTwiceDuringRecoveryIfCrashedDuringPopulation() throws Exception
    {
        // Given
        startDb( createAlwaysInitiallyPopulatingLuceneIndexFactory() );

        IndexDefinition indexDefinition = createIndex( myLabel );
        waitForIndex( indexDefinition );

        long nodeId = createNode( myLabel, 12 );
        assertEquals( 1, doIndexLookup( myLabel, 12 ).size() );

        // And Given
        killDb();

        // When
        startDb( createAlwaysInitiallyPopulatingLuceneIndexFactory() );

        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().getIndexes().iterator().next();
            waitForIndex( index );

            // Then
            assertEquals( 12, db.getNodeById( nodeId ).getProperty( NUM_BANANAS_KEY ) );
            assertEquals( 1, doIndexLookup( myLabel, 12 ).size() );
        }
    }

    @Test
    public void shouldNotUpdateTwiceDuringRecovery() throws Exception
    {
        // Given
        startDb( createLuceneIndexFactory() );

        IndexDefinition indexDefinition = createIndex( myLabel );
        waitForIndex( indexDefinition );

        long nodeId = createNode( myLabel, 12 );
        updateNode( nodeId, 14 );

        // And Given
        killDb();

        // When
        startDb( createLuceneIndexFactory() );

        // Then
        assertEquals( 0, doIndexLookup( myLabel, 12 ).size() );
        assertEquals( 1, doIndexLookup( myLabel, 14 ).size() );
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
        {
            db.shutdown();
        }
        directoryFactory.close();
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

        @Override
        public void dumpToZip( ZipOutputStream zip, byte[] scratchPad ) throws IOException
        {
            directoryFactory.dumpToZip( zip, scratchPad );
        }
    };

    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private final String NUM_BANANAS_KEY = "number_of_bananas_owned";

    private void startDb( KernelExtensionFactory<?> indexProviderFactory )
    {
       if ( db != null )
    {
        db.shutdown();
    }

       TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
       factory.setFileSystem( fs.get() );
       factory.setInternalLogProvider( log );
       factory.addKernelExtensions( Arrays.<KernelExtensionFactory<?>>asList( indexProviderFactory ) );
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

    private void rotateLogsAndCheckPoint() throws IOException
    {
        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" )
        );
    }

    private IndexDefinition createIndex( Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition definition = db.schema().indexFor( label ).on( NUM_BANANAS_KEY ).create();
            tx.success();
            return definition;
        }
    }

    private void waitForIndex( IndexDefinition definition )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( definition, 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private Set<Node> doIndexLookup( Label myLabel, Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<Node> iter = db.findNodes( myLabel, NUM_BANANAS_KEY, value );
            Set<Node> nodes = asUniqueSet( iter );
            tx.success();
            return nodes;
        }
    }

    private long createNode( Label label, int number )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( NUM_BANANAS_KEY, number );
            tx.success();
            return node.getId();
        }
    }

    private void updateNode( long nodeId, int value )
    {

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( NUM_BANANAS_KEY, value );
            tx.success();
        }
    }

    private void deleteNode( long node )
    {

        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( node ).delete();
            tx.success();
        }
    }

    // Creates a lucene index factory with the shared in-memory directory
    private KernelExtensionFactory<?> createAlwaysInitiallyPopulatingLuceneIndexFactory()
    {
        return new KernelExtensionFactory<LuceneSchemaIndexProviderFactory.Dependencies>(
                LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR.getKey() )
        {
            @Override
            public Lifecycle newInstance( KernelContext context, LuceneSchemaIndexProviderFactory.Dependencies dependencies )
                    throws Throwable
            {
                return new LuceneSchemaIndexProvider( fs.get(), ignoreCloseDirectoryFactory, context.storeDir(),
                        dependencies.getLogging().getInternalLogProvider(), dependencies.getConfig(), OperationalMode.single )
                {
                    @Override
                    public InternalIndexState getInitialState( long indexId )
                    {
                        return InternalIndexState.POPULATING;
                    }
                };
            }
        };
    }

    // Creates a lucene index factory with the shared in-memory directory
    private KernelExtensionFactory<?> createLuceneIndexFactory()
    {
        return new KernelExtensionFactory<LuceneSchemaIndexProviderFactory.Dependencies>(
                LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR.getKey() )
        {
            @Override
            public Lifecycle newInstance( KernelContext context, LuceneSchemaIndexProviderFactory.Dependencies dependencies )
                    throws Throwable
            {
                return new LuceneSchemaIndexProvider( fs.get(), ignoreCloseDirectoryFactory, context.storeDir(),
                        dependencies.getLogging().getInternalLogProvider(), dependencies.getConfig(), OperationalMode.single )
                {
                    @Override
                    public int compareTo( SchemaIndexProvider o )
                    {
                        return 1;
                    }
                };
            }
        };
    }
}
