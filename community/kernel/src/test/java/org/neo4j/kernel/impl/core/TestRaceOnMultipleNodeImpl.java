/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;

@Ignore("Jake: This assumes completely fair locking, which is not guaranteed, discuss what to do here before merging.")
public class TestRaceOnMultipleNodeImpl
{
    @Test
    public void concurrentRemoveProperty() throws Exception
    { // ASSUMPTION: locking is fair, first one to wait is first one to get the lock
        final Node root =  tx( new Callable<Node>() {
            @Override
            public Node call() throws Exception
            {
                return graphdb.createNode();
            }
        });
        final Node original = tx( new Callable<Node>()
        { // setup: create the node with the property that we will remove
            @Override
            public Node call() throws Exception
            {
                Node node = graphdb.createNode();
                node.setProperty( "key", "original" );
                return node;
            }
        } );
        // set up a wait chain: remover <- blocker <- offender
        final CountDownLatch removerSetUp = latch(), waitChainSetUp = latch();
        txThread( "remover", new Runnable()
        {
            @Override
            public void run()
            { // remove the property then wait until the entire wait chain is set up
                original.removeProperty( "key" );
                removerSetUp.countDown();
                await( waitChainSetUp );
            }
        } );
        await( removerSetUp );
        clearCaches(); // mess things up by giving threads different NodeImpl instances
        awaitWaitingState( txThread( "blocker", new Runnable()
        {
            @Override
            public void run()
            {
                original.removeProperty( "not existing" ); // block to make sure that we wait until "remover" is done
                root.setProperty( "key", "root" ); // reuse property record with same key for different node
            }
        }));
        clearCaches(); // mess things up by giving threads different NodeImpl instances
        final AtomicBoolean precondition = new AtomicBoolean( false ); // just used as a mutable Boolean
        final CountDownLatch readyToBlockOnLock = latch(), done = latch();
        Thread offender = thread( "offender", new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    tx( new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            // populate the NodeImpl object to make sure that we don't go to disk after acquiring lock
                            precondition.set( "original".equals( original.getProperty( "key" ) ) );
                            // this prevents false positives in awaitWaitingState( offender )
                            readyToBlockOnLock.countDown();
                            // this will block on the lock since there are two other threads ahead of us
                            original.removeProperty( "key" );
                        }
                    } );
                }
                finally
                {
                    done.countDown();
                }
            }
        } );
        await( readyToBlockOnLock ); // wait until no other locks are in the way
        awaitWaitingState( offender ); // wait until "offender" has started waiting on the lock
        clearCaches(); // clear the caches so that the NodeImpl will not get updated
        waitChainSetUp.countDown(); // allow the transactions to start
        await( done );
        clearCaches(); // to make sure that we do verification on the persistent state in the db
        // verify
        assertThat( root, inTx( graphdb, hasProperty( "key" )  ) );
        assertTrue( "invalid precondition", precondition.get() );
    }

    @Test
    public void concurrentSetProperty() throws Exception
    { // ASSUMPTION: locking is fair, first one to wait is first one to get the lock
        final Node root = tx( new Callable<Node>()
        {
            @Override
            public Node call() throws Exception
            {
                return graphdb.createNode();
            }
        } );
        tx( new Runnable()
        {
            @Override
            public void run()
            {
                root.setProperty( "tx", "main" );
                root.setProperty( "a", 1 );
                root.setProperty( "b", 2 );
                root.setProperty( "c", 3 );
                root.setProperty( "d", 4 );
            }
        } );
        final CountDownLatch writerSetUp = latch(), waitChainSetUp = latch();
        txThread( "writer", new Runnable()
        {
            @Override
            public void run()
            {
                root.setProperty( "e", 5 );
                writerSetUp.countDown();
                await( waitChainSetUp );
                root.setProperty( "tx", "writer" );
            }
        } );
        await( writerSetUp );
        awaitWaitingState( txThread( "remover", new Runnable()
        {
            @Override
            public void run()
            {
                root.removeProperty( "tx" );
            }
        } ) );
        clearCaches();
        final AtomicBoolean precondition = new AtomicBoolean( false ); // just used as a mutable Boolean
        final CountDownLatch offenderSetUp = latch(), done = latch();
        Thread offender = thread( "offender", new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    tx( new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            for ( @SuppressWarnings("unused")String key : root.getPropertyKeys() )
                            {
                                precondition.set( true );
                            }
                            offenderSetUp.countDown();
                            root.setProperty( "tx", "offender" );
                        }
                    } );
                }
                finally
                {
                    done.countDown();
                }
            }
        } );
        await( offenderSetUp );
        awaitWaitingState( offender );
        clearCaches();
        waitChainSetUp.countDown();
        await( done );
        clearCaches();
        assertThat( root, inTx( graphdb, hasProperty( "tx" ).withValue( "offender" )  ) );
        assertTrue( "node should not have any properties when entering second tx", precondition.get() );
    }


    private CountDownLatch latch()
    {
        return new CountDownLatch( 1 );
    }

    private static void await( CountDownLatch latch )
    {
        for ( ;; )
        {
            try
            {
                latch.await();
                return;
            }
            catch ( InterruptedException e )
            {
                // ignore
            }
        }
    }

    private void clearCaches()
    {
        graphdb.getDependencyResolver().resolveDependency( Caches.class ).clear();
    }

    private static Thread thread( String name, Runnable task )
    {
        Thread thread = new Thread( task, name );
        thread.start();
        return thread;
    }

    private static void awaitWaitingState( Thread thread )
    {
        for ( ;/*ever*/; )
        {
            switch ( thread.getState() )
            {
            case WAITING:
            case TIMED_WAITING:
                return;
            case TERMINATED:
                throw new IllegalStateException( "thread terminated" );
            default:
                try
                {
                    Thread.sleep( 1 );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
            }
        }
    }

    private Thread txThread(String name, final Runnable task)
    {
        return thread( name, new Runnable()
        {
            @Override
            public void run()
            {
                tx( task );
            }
        } );
    }

    private void tx( Runnable task )
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            task.run();

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private <R> R tx( Callable<R> task ) throws Exception
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            R result = task.call();

            tx.success();

            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    private GraphDatabaseAPI graphdb;

    @Before
    public void startDb()
    {
        graphdb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @After
    public void shutdownDb()
    {
        try
        {
            if ( graphdb != null )
            {
                graphdb.shutdown();
            }
        }
        finally
        {
            graphdb = null;
        }
    }
}
