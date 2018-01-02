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
package org.neo4j.kernel.impl.core;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.relationship_grab_size;
import static org.neo4j.helpers.collection.IteratorUtil.count;

/**
 * This isn't a deterministic test, but instead tries to trigger a race condition
 * for a couple of seconds. The original issues is mostly seen immediately, but after
 * a fix is in this test will take the full amount of seconds unfortunately.
 */
public class TestConcurrentRelationshipChainLoadingIssue
{
    private final int relCount = 2;

    @Test
    public void tryToTriggerRelationshipLoadingStoppingMidWay() throws Throwable
    {
        tryToTriggerRelationshipLoadingStoppingMidWay( 50 );
    }

    @Test
    public void tryToTriggerRelationshipLoadingStoppingMidWayForDenseNodeRepresentation() throws Throwable
    {
        tryToTriggerRelationshipLoadingStoppingMidWay( 1 );
    }

    private void tryToTriggerRelationshipLoadingStoppingMidWay( int denseNodeThreshold ) throws Throwable
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( relationship_grab_size, "" + relCount/2 )
                .setConfig( dense_node_threshold, "" + denseNodeThreshold )
                .newGraphDatabase();
        Node node = createNodeWithRelationships( db );

        checkStateToHelpDiagnoseFlakeyTest( db, node );

        long end = currentTimeMillis() + SECONDS.toMillis( 5 );
        int iterations = 0;
        while ( currentTimeMillis() < end )
        {
            tryOnce( db, node, iterations++ );
        }

        db.shutdown();
    }

    private void checkStateToHelpDiagnoseFlakeyTest( GraphDatabaseAPI db, Node node )
    {
        loadNode( db, node );
        // TODO clear cache here
        loadNode( db, node );
    }

    private void loadNode( GraphDatabaseAPI db, Node node )
    {
        try (Transaction ignored = db.beginTx()) {
            count( node.getRelationships() );
        }
    }

    private void awaitStartSignalAndRandomTimeLonger( final CountDownLatch startSignal )
    {
        try
        {
            startSignal.await();
            idleLoop( (int) (System.currentTimeMillis() % 100000) );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void tryOnce( final GraphDatabaseAPI db, final Node node, int iterations ) throws Throwable
    {
        ExecutorService executor = newCachedThreadPool();
        final CountDownLatch startSignal = new CountDownLatch( 1 );
        int threads = getRuntime().availableProcessors();
        final List<Throwable> errors = Collections.synchronizedList( new ArrayList<Throwable>() );
        for ( int i = 0; i < threads; i++ )
        {
            executor.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    awaitStartSignalAndRandomTimeLonger( startSignal );
                    try ( Transaction transaction = db.beginTx() )
                    {
                        assertEquals( relCount, count( node.getRelationships() ) );
                    }
                    catch ( Throwable e )
                    {
                        errors.add( e );
                    }
                }
            } );
        }
        startSignal.countDown();
        executor.shutdown();
        executor.awaitTermination( 10, SECONDS );

        if ( !errors.isEmpty() )
        {
            Exception exception = new Exception(
                    format("Exception(s) after %s iterations with %s threads", iterations, threads));
            for ( Throwable error : errors )
            {
                exception.addSuppressed( error );
            }
            throw exception;
        }
    }

    private static int idleLoop( int l )
    {
        // Use atomic integer to disable the JVM from rewriting this loop to simple addition.
        AtomicInteger i = new AtomicInteger( 0 );
        for ( int j = 0; j < l; j++ )
        {
            i.incrementAndGet();
        }
        return i.get();
    }

    private Node createNodeWithRelationships( GraphDatabaseAPI db )
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int i = 0; i < relCount / 2; i++ )
            {
                node.createRelationshipTo( node, MyRelTypes.TEST );
            }
            for ( int i = 0; i < relCount / 2; i++ )
            {
                node.createRelationshipTo( node, MyRelTypes.TEST2 );
            }
            tx.success();
            return node;
        }
    }
}
