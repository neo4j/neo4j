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
package org.neo4j.graphdb;

import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class GraphDatabaseServiceTest
{
    @Test
    public void givenShutdownDatabaseWhenBeginTxThenExceptionIsThrown() throws Exception
    {
        // Given
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        db.shutdown();

        // When
        try
        {
            db.beginTx();
            fail();
        }
        catch ( Exception e )
        {
            // Then
            assertThat( e.getClass().getName(), CoreMatchers.equalTo( TransactionFailureException.class.getName() ) );
        }
    }

    @Test
    public void givenDatabaseAndStartedTxWhenShutdownThenWaitForTxToFinish() throws Exception
    {
        // Given
        final GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // When
        final CountDownLatch started = new CountDownLatch( 1 );
        Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    started.countDown();

                    try
                    {
                        Thread.sleep( 2000 );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }

                    db.createNode();
                    tx.success();
                }
                catch ( Throwable e )
                {
                    e.printStackTrace();
                }
            }
        } );

        started.await();
        db.shutdown();
    }

    @Test
    public void terminateTransactionThrowsExceptionOnNextOperation() throws Exception
    {
        // Given
        final GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            tx.terminate();
            try
            {
                db.createNode();
                fail( "Failed to throw TransactionTerminateException" );
            }
            catch ( TransactionTerminatedException ignored )
            {
            }
        }

        db.shutdown();
    }

    @Test
    public void terminateNestedTransactionThrowsExceptionOnNextOperation() throws Exception
    {
        // Given
        final GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            try ( Transaction nested = db.beginTx() )
            {
                tx.terminate();
            }
            try
            {
                db.createNode();
                fail( "Failed to throw TransactionTerminateException" );
            }
            catch ( TransactionTerminatedException ignored )
            {
            }
        }

        db.shutdown();
    }

    @Test
    public void terminateNestedTransactionThrowsExceptionOnNextNestedOperation() throws Exception
    {
        // Given
        final GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            try ( Transaction nested = db.beginTx() )
            {
                tx.terminate();
                try
                {
                    db.createNode();
                    fail( "Failed to throw TransactionTerminateException" );
                }
                catch ( TransactionTerminatedException ignored )
                {
                }
            }
        }

        db.shutdown();
    }

    @Test
    public void givenDatabaseAndStartedTxWhenShutdownAndStartNewTxThenBeginTxTimesOut() throws Exception
    {
        // Given
        final GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // When
        final CountDownLatch shutdown = new CountDownLatch( 1 );
        final AtomicReference result = new AtomicReference();
        Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    shutdown.countDown();

                    try
                    {
                        Thread.sleep( 2000 );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }

                    db.createNode();
                    tx.success();

                    Executors.newSingleThreadExecutor().submit( new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            try
                            {
                                db.beginTx();
                                result.set( Boolean.TRUE );
                            }
                            catch ( Exception e )
                            {
                                result.set( e );
                            }

                            synchronized ( result )
                            {
                                result.notifyAll();
                            }
                        }
                    } );
                }
                catch ( Throwable e )
                {
                    e.printStackTrace();
                }
            }
        } );

        shutdown.await();
        db.shutdown();

        while ( result.get() == null )
        {
            synchronized ( result )
            {
                result.wait( 100 );
            }
        }

        assertThat( result.get().getClass(), CoreMatchers.<Object>equalTo( TransactionFailureException.class ) );
    }

    @Test
    public void shouldLetDetectedDeadlocksDuringCommitBeThrownInTheirOriginalForm() throws Exception
    {
        // GIVEN a database with a couple of entities:
        // (n1) --> (r1) --> (r2) --> (r3)
        // (n2)
        GraphDatabaseService db = cleanup.add( new TestGraphDatabaseFactory().newImpermanentDatabase() );
        Node n1 = createNode( db );
        Node n2 = createNode( db );
        Relationship r3 = createRelationship( n1 );
        Relationship r2 = createRelationship( n1 );
        Relationship r1 = createRelationship( n1 );

        // WHEN creating a deadlock scenario where the final deadlock would have happened due to locks
        //      acquired during linkage of relationship records
        //
        // (r1) <-- (t1)
        //   |       ^
        //   v       |
        // (t2) --> (n2)
        OtherThreadExecutor<Void> t2 = cleanup.add( new OtherThreadExecutor<Void>( "T2", null ) );
        Transaction t1Tx = db.beginTx();
        Transaction t2Tx = t2.execute( beginTx( db ) );
        // (t1) <-- (n2)
        n2.setProperty( "locked", "indeed" );
        // (t2) <-- (r1)
        t2.execute( setProperty( r1, "locked", "absolutely" ) );
        // (t2) --> (n2)
        Future<Object> t2n2Wait = t2.executeDontWait( setProperty( n2, "locked", "In my dreams" ) );
        t2.waitUntilWaiting();
        // (t1) --> (r1) although delayed until commit, this is accomplished by deleting an adjacent
        //               relationship so that its surrounding relationships are locked at commit time.
        r2.delete();
        t1Tx.success();
        try
        {
            t1Tx.close();
            fail( "Should throw exception about deadlock" );
        }
        catch ( Exception e )
        {
            assertEquals( DeadlockDetectedException.class, e.getClass() );
        }
        finally
        {
            t2n2Wait.get();
            t2.execute( close( t2Tx ) );
            t2.close();
        }
    }

    /**
     * GitHub issue #5996
     */
    @Test
    public void terminationOfClosedTransactionDoesNotInfluenceNextTransaction()
    {
        GraphDatabaseService db = cleanup.add( new TestGraphDatabaseFactory().newImpermanentDatabase() );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        Transaction transaction = db.beginTx();
        try ( Transaction tx = transaction )
        {
            db.createNode();
            tx.success();
        }
        transaction.terminate();

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 2, Iterables.count( db.getAllNodes() ) );
            tx.success();
        }
    }

    private WorkerCommand<Void, Transaction> beginTx( final GraphDatabaseService db )
    {
        return new WorkerCommand<Void, Transaction>()
        {
            @Override
            public Transaction doWork( Void state ) throws Exception
            {
                return db.beginTx();
            }
        };
    }

    private WorkerCommand<Void, Object> setProperty( final PropertyContainer entity,
            final String key, final String value )
    {
        return new WorkerCommand<Void, Object>()
        {
            @Override
            public Object doWork( Void state ) throws Exception
            {
                entity.setProperty( key, value );
                return null;
            }
        };
    }

    private WorkerCommand<Void, Void> close( final Transaction tx )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                tx.close();
                return null;
            }
        };
    }

    private Relationship createRelationship( Node node )
    {
        try ( Transaction tx = node.getGraphDatabase().beginTx() )
        {
            Relationship rel = node.createRelationshipTo( node, MyRelTypes.TEST );
            tx.success();
            return rel;
        }
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

    public final @Rule CleanupRule cleanup = new CleanupRule();
}
