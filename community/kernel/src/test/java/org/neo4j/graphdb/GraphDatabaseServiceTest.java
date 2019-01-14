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
package org.neo4j.graphdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class GraphDatabaseServiceTest
{
    @ClassRule
    public static final DatabaseRule globalDb = new ImpermanentDatabaseRule();

    private final ExpectedException exception = ExpectedException.none();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final OtherThreadRule<Void> t2 = new OtherThreadRule<>( "T2-" + getClass().getName() );
    private final OtherThreadRule<Void> t3 = new OtherThreadRule<>( "T3-" + getClass().getName() );

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( exception ).around( t2 ).around( t3 );

    @Test
    public void givenShutdownDatabaseWhenBeginTxThenExceptionIsThrown()
    {
        // Given
        GraphDatabaseService db = getTemporaryDatabase();

        db.shutdown();

        // Expect
        exception.expect( DatabaseShutdownException.class );

        // When
        db.beginTx();
    }

    @Test
    public void givenDatabaseAndStartedTxWhenShutdownThenWaitForTxToFinish() throws Exception
    {
        // Given
        final GraphDatabaseService db = getTemporaryDatabase();

        // When
        Barrier.Control barrier = new Barrier.Control();
        Future<Object> txFuture = t2.execute( state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                barrier.reached();
                db.createNode();
                tx.success();
            }
            return null;
        } );

        // i.e. wait for transaction to start
        barrier.await();

        // now there's a transaction open, blocked on continueTxSignal
        Future<Object> shutdownFuture = t3.execute( state ->
        {
            db.shutdown();
            return null;
        } );
        t3.get().waitUntilWaiting( location -> location.isAt( DatabaseAvailability.class, "stop" ) );
        barrier.release();
        try
        {
            txFuture.get();
        }
        catch ( ExecutionException e )
        {
            // expected
        }
        shutdownFuture.get();
    }

    @Test
    public void terminateTransactionThrowsExceptionOnNextOperation()
    {
        // Given
        final GraphDatabaseService db = globalDb;

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
    }

    @Test
    public void terminateNestedTransactionThrowsExceptionOnNextOperation()
    {
        // Given
        final GraphDatabaseService db = globalDb;

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
    }

    @Test
    public void terminateNestedTransactionThrowsExceptionOnNextNestedOperation()
    {
        // Given
        final GraphDatabaseService db = globalDb;

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
    }

    @Test
    public void terminateNestedTransactionThrowsExceptionOnNextNestedOperationMultiThreadedVersion() throws Exception
    {
        // Given
        final GraphDatabaseService db = getTemporaryDatabase();
        try
        {
            // When
            final CountDownLatch txSet = new CountDownLatch( 1 );
            final CountDownLatch terminated = new CountDownLatch( 1 );
            final Transaction[] outer = {null};
            final Exception[] threadFail = {null};

            Thread worker = new Thread( () ->
            {
                try ( Transaction inner = db.beginTx() )
                {
                    outer[0] = inner;
                    txSet.countDown();
                    terminated.await();
                    db.createNode();
                    fail( "should have failed earlier" );
                }
                catch ( Exception e )
                {
                    threadFail[0] = e;
                }
            } );
            worker.start();
            txSet.await();
            outer[0].terminate();
            terminated.countDown();
            worker.join();
            assertThat( threadFail[0], instanceOf( TransactionTerminatedException.class ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void terminateNestedTransactionThrowsExceptionOnNextNestedOperationMultiThreadedVersionWithNestedTx()
            throws Exception
    {
        // Given
        final GraphDatabaseService db = getTemporaryDatabase();
        try
        {
            // When
            final CountDownLatch txSet = new CountDownLatch( 1 );
            final CountDownLatch terminated = new CountDownLatch( 1 );
            final Transaction[] outer = {null};
            final Exception[] threadFail = {null};

            Thread worker = new Thread( () ->
            {
                Transaction transaction = db.beginTx();
                try ( Transaction inner = db.beginTx() )
                {
                    outer[0] = inner;
                    txSet.countDown();
                    terminated.await();
                    db.createNode();
                    fail( "should have failed earlier" );
                }
                catch ( Exception e )
                {
                    threadFail[0] = e;
                }
                finally
                {
                    transaction.close();
                }
            } );
            worker.start();
            txSet.await();
            outer[0].terminate();
            terminated.countDown();
            worker.join();
            assertThat( threadFail[0], instanceOf( TransactionTerminatedException.class ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void givenDatabaseAndStartedTxWhenShutdownAndStartNewTxThenBeginTxTimesOut() throws Exception
    {
        // Given
        GraphDatabaseService db = getTemporaryDatabase();

        // When
        Barrier.Control barrier = new Barrier.Control();
        t2.execute( state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                barrier.reached(); // <-- this triggers t3 to start a db.shutdown()
            }
            return null;
        } );

        barrier.await();
        Future<Object> shutdownFuture = t3.execute( state ->
        {
            db.shutdown();
            return null;
        } );
        t3.get().waitUntilWaiting( location -> location.isAt( DatabaseAvailability.class, "stop" ) );
        barrier.release(); // <-- this triggers t2 to continue its transaction
        shutdownFuture.get();

        try
        {
            db.beginTx();
            fail( "Should fail" );
        }
        catch ( DatabaseShutdownException e )
        {
            //THEN good
        }
    }

    @Test
    public void shouldLetDetectedDeadlocksDuringCommitBeThrownInTheirOriginalForm() throws Exception
    {
        // GIVEN a database with a couple of entities:
        // (n1) --> (r1) --> (r2) --> (r3)
        // (n2)
        GraphDatabaseService db = globalDb;
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
        Transaction t1Tx = db.beginTx();
        Transaction t2Tx = t2.execute( beginTx( db ) ).get();
        // (t1) <-- (n2)
        n2.setProperty( "locked", "indeed" );
        // (t2) <-- (r1)
        t2.execute( setProperty( r1, "locked", "absolutely" ) ).get();
        // (t2) --> (n2)
        Future<Object> t2n2Wait = t2.execute( setProperty( n2, "locked", "In my dreams" ) );
        t2.get().waitUntilWaiting();
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
            t2.execute( close( t2Tx ) ).get();
        }
    }

    /**
     * GitHub issue #5996
     */
    @Test
    public void terminationOfClosedTransactionDoesNotInfluenceNextTransaction()
    {
        GraphDatabaseService db = globalDb;

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
            assertThat( db.getAllNodes(), is( iterableWithSize( 2 ) ) );
            tx.success();
        }
    }

    private WorkerCommand<Void, Transaction> beginTx( final GraphDatabaseService db )
    {
        return state -> db.beginTx();
    }

    private WorkerCommand<Void, Object> setProperty( final PropertyContainer entity,
            final String key, final String value )
    {
        return state ->
        {
            entity.setProperty( key, value );
            return null;
        };
    }

    private WorkerCommand<Void, Void> close( final Transaction tx )
    {
        return state ->
        {
            tx.close();
            return null;
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

    private GraphDatabaseService getTemporaryDatabase()
    {
        return new TestGraphDatabaseFactory().newImpermanentDatabase( testDirectory.directory( "impermanent" ) );
    }
}
