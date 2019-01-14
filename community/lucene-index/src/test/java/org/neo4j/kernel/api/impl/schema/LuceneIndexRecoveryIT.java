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
package org.neo4j.kernel.api.impl.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.asUniqueSet;
import static org.neo4j.kernel.api.impl.schema.LuceneIndexProvider.defaultDirectoryStructure;

public class LuceneIndexRecoveryIT
{
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private final String NUM_BANANAS_KEY = "number_of_bananas_owned";
    private static final Label myLabel = label( "MyLabel" );
    private GraphDatabaseAPI db;
    private DirectoryFactory directoryFactory;

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

    @Test
    public void addShouldBeIdempotentWhenDoingRecovery() throws Exception
    {
        // Given
        startDb( createLuceneIndexFactory() );

        IndexDefinition index = createIndex( myLabel );
        waitForIndex( index );

        long nodeId = createNode( myLabel, 12 );
        try ( Transaction ignored = db.beginTx() )
        {
            assertNotNull( db.getNodeById( nodeId ) );
        }
        assertEquals( 1, doIndexLookup( myLabel, 12 ).size() );

        // And Given
        killDb();

        // When
        startDb( createLuceneIndexFactory() );

        // Then
        try ( Transaction ignored = db.beginTx() )
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

        try ( Transaction ignored = db.beginTx() )
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

    private void startDb( KernelExtensionFactory<?> indexProviderFactory )
    {
        if ( db != null )
        {
            db.shutdown();
        }

        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
        factory.setKernelExtensions( Collections.singletonList( indexProviderFactory ) );
        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
    }

    private void killDb() throws Exception
    {
        if ( db != null )
        {
            fs.snapshot( () ->
            {
                db.shutdown();
                db = null;
            } );
        }
    }

    private void rotateLogsAndCheckPoint() throws IOException
    {
        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" ) );
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
    private KernelExtensionFactory<LuceneIndexProviderFactory.Dependencies> createAlwaysInitiallyPopulatingLuceneIndexFactory()
    {
        return new KernelExtensionFactory<LuceneIndexProviderFactory.Dependencies>(
                LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR.getKey() )
        {
            @Override
            public Lifecycle newInstance( KernelContext context, LuceneIndexProviderFactory.Dependencies dependencies )
            {
                return new LuceneIndexProvider( fs.get(), directoryFactory, defaultDirectoryStructure( context.storeDir() ),
                        IndexProvider.Monitor.EMPTY, dependencies.getConfig(), context.databaseInfo().operationalMode )
                {
                    @Override
                    public InternalIndexState getInitialState( long indexId, SchemaIndexDescriptor descriptor )
                    {
                        return InternalIndexState.POPULATING;
                    }
                };
            }
        };
    }

    // Creates a lucene index factory with the shared in-memory directory
    private KernelExtensionFactory<LuceneIndexProviderFactory.Dependencies> createLuceneIndexFactory()
    {
        return new KernelExtensionFactory<LuceneIndexProviderFactory.Dependencies>(
                LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR.getKey() )
        {

            @Override
            public Lifecycle newInstance( KernelContext context, LuceneIndexProviderFactory.Dependencies dependencies )
            {
                return new LuceneIndexProvider( fs.get(), directoryFactory, defaultDirectoryStructure( context.storeDir() ),
                        IndexProvider.Monitor.EMPTY, dependencies.getConfig(), context.databaseInfo().operationalMode )
                {
                    @Override
                    public int compareTo( IndexProvider o )
                    {
                        return 1;
                    }
                };
            }
        };
    }
}
