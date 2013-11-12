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
package org.neo4j.graphdb;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class NodeMergerAcceptanceTest
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( NodeMergerAcceptanceTest.class );

    private GraphDatabaseService graph;

    private Label label = DynamicLabel.label( "Person" );

    @Before
    public void before()
    {
        this.graph = dbRule.getGraphDatabaseService();
    }

    @Test
    public void shouldCreateNewNodeForLabeledNode() throws Exception
    {
        // given
        try ( Transaction ignored = graph.beginTx() )
        {
            Merger<Node> nodeCreator = graph.getOrCreateNode().withProperty( "name", "Ben" );

            // when
            MergeResult<Node> result = nodeCreator.merge();

            // then
            assertTrue( result.containsNewlyCreated() );
            assertEquals( "Ben", result.single().getProperty( "name" ) );
        }
    }

    @Test
    public void shouldCreateNewNodeForIndexedNode() throws Exception
    {
        // given
        createIndex();

        try ( Transaction ignored = graph.beginTx() )
        {
            Merger<Node> nodeCreator = graph.getOrCreateNode( label ).withProperty( "name", "Ben" );

            // when
            MergeResult<Node> result = nodeCreator.merge();
            // then
            assertTrue( result.containsNewlyCreated() );
            assertEquals( "Ben", result.single().getProperty( "name" ) );
        }
    }

    @Test
    public void shouldRetrieveAllLabeledNodesWhenNotUsingConstraintsAndIndexes() throws Exception
    {
        // given
        Node node1, node2;
        try ( Transaction tx = graph.beginTx() )
        {
            node1 = graph.createNode( label );
            node1.setProperty( "id", 1 );

            node2 = graph.createNode( label );
            node2.setProperty( "id", 1 );
            tx.success();
        }

        try ( Transaction ignored = graph.beginTx() )
        {
            Merger<Node> nodeCreator = graph.getOrCreateNode( label ).withProperty( "id", 1 );

            // when
            MergeResult<Node> result = nodeCreator.merge();

            // then
            assertFalse( result.containsNewlyCreated() );
            assertEquals( asSet( node1, node2 ), asSet( result ) );
        }
    }

    @Test
    public void shouldRetrieveAllLabeledNodesWhenUsingIndexesOnly() throws Exception
    {
        // given
        createIndex();

        Node node1, node2;
        try ( Transaction tx = graph.beginTx() )
        {
            node1 = graph.createNode( label );
            node1.setProperty( "name", "Stefan" );

            node2 = graph.createNode( label );
            node2.setProperty( "name", "Stefan" );
            tx.success();
        }

        try ( Transaction ignored = graph.beginTx() )
        {
            Merger<Node> nodeCreator = graph.getOrCreateNode( label ).withProperty( "name", "Stefan" );

            // when
            MergeResult<Node> result = nodeCreator.merge();

            // then
            assertFalse( result.containsNewlyCreated() );
            assertEquals( asSet( node1, node2 ), asSet( result ) );
        }
    }

    @Test
    public void shouldRetrieveAllLabeledNodesWhenNotRequiringLabels() throws Exception
    {
        // given
        createIndex();

        Node node1, node2;
        try ( Transaction tx = graph.beginTx() )
        {
            node1 = graph.createNode();
            node1.setProperty( "name", "Stefan" );

            node2 = graph.createNode( label );
            node2.setProperty( "name", "Stefan" );
            tx.success();
        }

        try ( Transaction ignored = graph.beginTx() )
        {
            Merger<Node> nodeCreator = graph.getOrCreateNode().withProperty( "name", "Stefan" );

            // when
            MergeResult<Node> result = nodeCreator.merge();

            // then
            assertFalse( result.containsNewlyCreated() );
            assertEquals( asSet( node1, node2 ), asSet( result ) );
        }
    }

    @Test
    public void shouldCreateNewNodesForUniqueConstraint() throws Exception
    {
        // given
        createConstraint();

        try ( Transaction ignored = graph.beginTx() )
        {
            Merger<Node> nodeCreator = graph.getOrCreateNode( label ).withProperty( "name", "Ben" );

            // when
            MergeResult<Node> result = nodeCreator.merge();

            // then
            assertTrue( result.containsNewlyCreated() );
            assertEquals( "Ben", result.single().getProperty( "name" ) );
        }
    }

    @Test
    public void shouldNotRecreateExistingNodeForUniqueConstraint() throws Exception
    {
        // given
        createConstraint();

        try ( Transaction ignored = graph.beginTx() )
        {
            Merger<Node> nodeCreator = graph.getOrCreateNode( label ).withProperty( "name", "Ben" );

            // when
            MergeResult<Node> result1 = nodeCreator.merge();
            Node node1 = result1.single();

            MergeResult<Node> result2 = nodeCreator.merge();
            Node node2 = result2.single();

            // then
            assertFalse( result2.containsNewlyCreated() );
            assertEquals( node1.getId(), node2.getId() );
        }
    }

    @Test
    public void shouldBlockConflictingTransactionForUniqueConstraint() throws Exception
    {
        // given
        createConstraint();

        final DoubleLatch bothThreads = new DoubleLatch( 2 );
        final CountDownLatch releaseUniqueLock = new CountDownLatch( 1 );

        Thread blocker = new Thread()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = graph.beginTx() )
                {
                    Merger<Node> nodeCreator = graph.getOrCreateNode( label ).withProperty( "name", "Ben" );
                    MergeResult<Node> result = nodeCreator.merge();
                    assertTrue( result.containsNewlyCreated() );
                    result.single().setProperty( "blocker", true );

                    // trigger blocked after holding the lock
                    bothThreads.start();

                    // keep holding the lock
                    try
                    {
                        releaseUniqueLock.await();
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }
                    tx.success();
                }
                bothThreads.finish();
            }
        };

        Thread blocked = new Thread() {

            @Override
            public void run()
            {
                try ( Transaction tx = graph.beginTx() )
                {
                    bothThreads.start();
                    Merger<Node> nodeCreator = graph.getOrCreateNode( label ).withProperty( "name", "Ben" );
                    MergeResult<Node> result = nodeCreator.merge();
                    assertFalse( result.containsNewlyCreated() );
                    result.single().setProperty( "blocked", true );
                    tx.success();
                }
                bothThreads.finish();
            }
        };

        // when blocker holds the unique lock
        blocker.start();
        blocked.start();
        bothThreads.awaitStart();

        // then blocked will eventually enter WAITING state
        waitUntilWaiting( blocked, 5000L );

        // when blocker releases the unique lock
        releaseUniqueLock.countDown();
        bothThreads.awaitFinish();

        // then the object has been updated twice
        try ( Transaction tx = graph.beginTx() )
        {
            Merger<Node> nodeCreator = graph.getOrCreateNode( label ).withProperty( "name", "Ben" );
            MergeResult<Node> result = nodeCreator.merge();
            assertFalse( result.containsNewlyCreated() );
            Node singleNode = result.single();
            assertTrue( singleNode.hasProperty( "blocker" ) );
            assertTrue( singleNode.hasProperty( "blocked" ) );
            tx.success();
        }
    }

    @Test
    public void shouldNotDeadlockWhenCreatingUniqueNodesInParallel() throws Exception
    {
        // give
        createConstraint();
        ExecutorService thread1 = Executors.newSingleThreadExecutor(), thread2 = Executors.newSingleThreadExecutor();
        final CountDownLatch latch1 = new CountDownLatch( 1 ), latch2 = new CountDownLatch( 1 );

        try
        {
            // when
            Future<Node> node1 = thread1.submit( new Callable<Node>()
            {
                @Override
                public Node call() throws Exception
                {
                    try ( Transaction tx = graph.beginTx() )
                    {
                        Merger<Node> merger = graph.getOrCreateNode( label ).withProperty( "name", "value" );
                        latch2.countDown();
                        await( latch1 );
                        Node result = merger.merge().single();
                        tx.success();
                        return result;
                    }
                }
            } );

            Future<Node> node2 = thread2.submit( new Callable<Node>()
            {
                @Override
                public Node call() throws Exception
                {
                    try ( Transaction tx = graph.beginTx() )
                    {
                        await( latch2 );
                        latch1.countDown();
                        Merger<Node> merger = graph.getOrCreateNode( label ).withProperty( "name", "value" );
                        Node result = merger.merge().single();
                        tx.success();
                        return result;
                    }
                }
            } );

            // then
            assertEquals( node1.get(), node2.get() );
        }
        finally
        {
            thread1.shutdown();
            thread2.shutdown();
        }
    }

    private static void await( CountDownLatch latch )
    {
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void waitUntilWaiting( Thread thread, long durationInMillis )
    {
        long endTime = System.currentTimeMillis() + durationInMillis;
        while ( System.currentTimeMillis() < endTime )
        {
            if ( Thread.State.WAITING == thread.getState() )
            {
                return;
            }
            Thread.yield();
        }

        throw new IllegalStateException( "Excepted thread to enter WAITING state" );
    }

    private IndexDefinition createIndex()
    {
        IndexDefinition index;

        try ( Transaction tx = graph.beginTx() )
        {
            index = graph.schema().indexFor( label ).on( "name" ).create();
            tx.success();
        }

        try ( Transaction tx = graph.beginTx() )
        {
            graph.schema().awaitIndexOnline( index, 10, TimeUnit.SECONDS );
            tx.success();
        }

        return index;
    }

    private ConstraintDefinition createConstraint()
    {
        try ( Transaction tx = graph.beginTx() )
        {
            ConstraintDefinition constraint =
                    graph.schema().constraintFor( label ).assertPropertyIsUnique( "name" ).create();
            tx.success();
            return constraint;
        }
    }
}
