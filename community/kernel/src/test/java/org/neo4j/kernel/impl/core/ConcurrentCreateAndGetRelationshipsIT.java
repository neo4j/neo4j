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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.IteratorUtil.count;

/**
 * Ensures the absence of an issue where iterating through a {@link RelationshipIterator} would result in
 * {@link ArrayIndexOutOfBoundsException} due to incrementing an array index too eagerly so that a consecutive
 * call to {@link RelationshipIterator#next()} would try to get the internal type iterator with a too high index.
 * 
 * This test is probabilistic in trying to produce the issue. There's a chance this test will be unsuccessful in
 * reproducing the issue (test being successful where it should have failed), but it will never randomly fail
 * where it should have been successful. After the point where the issue has been fixed this test will use
 * the full 0.5 seconds to try to reproduce it.
 * 
 */
public class ConcurrentCreateAndGetRelationshipsIT
{
    @Test
    public void tryToReproduceTheIssue() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        CountDownLatch startSignal = new CountDownLatch( 1 );
        AtomicBoolean stopSignal = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<Exception>();
        Node parentNode = createNode( db );
        Collection<Worker> workers = createWorkers( db, startSignal, stopSignal, failure, parentNode );
        
        // WHEN
        startSignal.countDown();
        sleep( 500 );
        stopSignal.set( true );
        awaitWorkersToEnd( workers );
        
        // THEN
        if ( failure.get() != null )
        {
            throw new Exception( "A worker failed", failure.get() );
        }
    }

    private void awaitWorkersToEnd( Collection<Worker> workers ) throws InterruptedException
    {
        for ( Worker worker : workers )
        {
            worker.join();
        }
    }

    private Collection<Worker> createWorkers( GraphDatabaseService db, CountDownLatch startSignal,
            AtomicBoolean stopSignal, AtomicReference<Exception> failure, Node parentNode )
    {
        Collection<Worker> workers = new ArrayList<Worker>();
        for ( int i = 0; i < 2; i++ )
        {
            workers.add( newWorker( db, startSignal, stopSignal, failure, parentNode ) );
        }
        return workers;
    }

    private Worker newWorker( GraphDatabaseService db, CountDownLatch startSignal, AtomicBoolean stopSignal,
            AtomicReference<Exception> failure, Node parentNode )
    {
        Worker worker = new Worker( db, startSignal, stopSignal, failure, parentNode );
        worker.start();
        return worker;
    }

    private Node createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            tx.success();
            return node;
        }
    }

    public final @Rule ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
    private static final RelationshipType RELTYPE = MyRelTypes.TEST;

    private static class Worker extends Thread
    {
        private final GraphDatabaseService db;
        private final CountDownLatch startSignal;
        private final AtomicReference<Exception> failure;
        private final Node parentNode;
        private final AtomicBoolean stopSignal;

        public Worker( GraphDatabaseService db, CountDownLatch startSignal, AtomicBoolean stopSignal,
                AtomicReference<Exception> failure, Node parentNode )
        {
            this.db = db;
            this.startSignal = startSignal;
            this.stopSignal = stopSignal;
            this.failure = failure;
            this.parentNode = parentNode;
        }

        @Override
        public void run()
        {
            awaitStartSignal();
            while ( failure.get() == null && !stopSignal.get() )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    // ArrayIndexOutOfBoundsException happens here
                    count( parentNode.getRelationships( RELTYPE, OUTGOING ) );
                    
                    parentNode.createRelationshipTo( db.createNode(), RELTYPE );
                    tx.success();
                }
                catch ( Exception e )
                {
                    failure.compareAndSet( null, e );
                }
            }
        }

        private void awaitStartSignal()
        {
            try
            {
                startSignal.await( 10, SECONDS );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
