/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.index.internal.gbptree.TreeNodeDynamicSize;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.schema.LayoutTestUtil;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.TestLabels.LABEL_ONE;

@RunWith( Parameterized.class )
public class StringLengthIndexValidationIT
{
    @Parameterized.Parameters( name = "{0}" )
    public static GraphDatabaseSettings.SchemaIndex[] parameters()
    {
        return new GraphDatabaseSettings.SchemaIndex[]{
                GraphDatabaseSettings.SchemaIndex.NATIVE20,
                GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10
        };
    }

    @Parameterized.Parameter()
    public static GraphDatabaseSettings.SchemaIndex schemaIndex;

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private static final String propKey = "largeString";
    private static final int keySizeLimit = TreeNodeDynamicSize.keyValueSizeCapFromPageSize( PageCache.PAGE_SIZE );
    private static final AtomicBoolean trapPopulation = new AtomicBoolean();
    private static final Barrier.Control populationScanFinished = new Barrier.Control();
    private GraphDatabaseService db;

    @Before
    public void setup()
    {
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        Monitors monitors = new Monitors();
        IndexingService.MonitorAdapter trappingMonitor = new IndexingService.MonitorAdapter()
        {
            @Override
            public void indexPopulationScanComplete()
            {
                if ( trapPopulation.get() )
                {
                    populationScanFinished.reached();
                }
            }
        };
        monitors.addMonitorListener( trappingMonitor );
        factory.setMonitors( monitors );
        db = factory.newEmbeddedDatabaseBuilder( testDirectory.storeDir() )
                .setConfig( GraphDatabaseSettings.default_schema_provider, schemaIndex.providerName() )
                .newGraphDatabase();
    }

    @After
    public void tearDown()
    {
        db.shutdown();
    }

    @Test
    public void shouldSuccessfullyWriteAndReadWithinIndexKeySizeLimit()
    {
        createIndex();
        String propValue = getString( keySizeLimit );
        long expectedNodeId;

        // Write
        expectedNodeId = createNode( propValue );

        // Read
        assertReadNode( propValue, expectedNodeId );
    }

    @Test
    public void shouldSuccessfullyPopulateIndexWithinIndexKeySizeLimit()
    {
        String propValue = getString( keySizeLimit );
        long expectedNodeId;

        // Write
        expectedNodeId = createNode( propValue );

        // Populate
        createIndex();

        // Read
        assertReadNode( propValue, expectedNodeId );
    }

    @Test
    public void txMustFailIfExceedingIndexKeySizeLimit()
    {
        createIndex();

        // Write
        try ( Transaction tx = db.beginTx() )
        {
            String propValue = getString( keySizeLimit + 1 );
            db.createNode( LABEL_ONE ).setProperty( propKey, propValue );
            tx.success();
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), containsString( "Please see index documentation for limitations." ) );
        }
    }

    @Test
    public void indexPopulationMustFailIfExceedingIndexKeySizeLimit()
    {
        // Write
        String propValue = getString( keySizeLimit + 1 );
        createNode( propValue );

        // Create index should be fine
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( propKey ).create();
            tx.success();
        }
        assertIndexFailToComeOnline();
        assertIndexInFailedState();
    }

    @Test
    public void externalUpdatesMustNotFailIndexPopulationIfWithinIndexKeySizeLimit() throws InterruptedException
    {
        trapPopulation.set( true );

        // Create index should be fine
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( propKey ).create();
            tx.success();
        }

        // Wait for index population to start
        populationScanFinished.await();

        // External update to index while population has not yet finished
        String propValue = getString( keySizeLimit );
        long nodeId = createNode( propValue );

        // Continue index population
        populationScanFinished.release();

        // Waiting for it to come online should succeed
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }

        assertReadNode( propValue, nodeId );
    }

    @Test
    public void externalUpdatesMustFailIndexPopulationIfExceedingIndexKeySizeLimit() throws InterruptedException
    {
        trapPopulation.set( true );

        // Create index should be fine
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( propKey ).create();
            tx.success();
        }

        // Wait for index population to start
        populationScanFinished.await();

        // External update to index while population has not yet finished
        String propValue = getString( keySizeLimit + 1 );
        createNode( propValue );

        // Continue index population
        populationScanFinished.release();

        assertIndexFailToComeOnline();
        assertIndexInFailedState();
    }

    public void assertIndexFailToComeOnline()
    {
        // Waiting for it to come online should fail
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(), Matchers.allOf(
                    containsString(
                            format( "Index IndexDefinition[label:LABEL_ONE on:largeString] " +
                                    "(IndexRule[id=1, descriptor=Index( GENERAL, :label[0](property[0]) ), provider={key=%s, version=%s}]) " +
                                    "entered a FAILED state.", schemaIndex.providerKey(), schemaIndex.providerVersion() ) ),
                    containsString( "Failed while trying to write to index, targetIndex=Index( GENERAL, :LABEL_ONE(largeString) ), nodeId=0" )
            ) );
        }
    }

    public void assertIndexInFailedState()
    {
        // Index should be in failed state
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<IndexDefinition> iterator = db.schema().getIndexes( LABEL_ONE ).iterator();
            assertTrue( iterator.hasNext() );
            IndexDefinition next = iterator.next();
            assertEquals( "state is FAILED", Schema.IndexState.FAILED, db.schema().getIndexState( next ) );
            assertThat( db.schema().getIndexFailure( next ),
                    Matchers.allOf(
                            containsString( "Index key-value size it to large. Please see index documentation for limitations." ),
                            containsString( "Failed while trying to write to index, targetIndex=Index( GENERAL, :LABEL_ONE(largeString) ), nodeId=0" )
                    ) );
            tx.success();
        }
    }

    // Each char in string need to fit in one byte
    private String getString( int keySize )
    {
        return LayoutTestUtil.generateStringResultingInSizeForIndexProvider( keySize, schemaIndex );
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( propKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private long createNode( String propValue )
    {
        long expectedNodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( LABEL_ONE );
            node.setProperty( propKey, propValue );
            expectedNodeId = node.getId();
            tx.success();
        }
        return expectedNodeId;
    }

    private void assertReadNode( String propValue, long expectedNodeId )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.findNode( LABEL_ONE, propKey, propValue );
            assertNotNull( node );
            assertEquals( "node id", expectedNodeId, node.getId() );
            tx.success();
        }
    }
}
