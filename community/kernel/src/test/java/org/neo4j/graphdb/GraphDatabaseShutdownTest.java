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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.locking.LockCountVisitor;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockClient;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.Exceptions.rootCause;

public class GraphDatabaseShutdownTest
{
    private GraphDatabaseAPI db;

    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>( "T2" );
    @Rule
    public final OtherThreadRule<Void> t3 = new OtherThreadRule<>( "T3" );

    @Before
    public void setUp()
    {
        db = newDb();
    }

    @After
    public void tearDown()
    {
        db.shutdown();
    }

    @Test
    public void transactionShouldReleaseLocksWhenGraphDbIsBeingShutdown()
    {
        // GIVEN
        final Locks locks = db.getDependencyResolver().resolveDependency( Locks.class );
        assertEquals( 0, lockCount( locks ) );
        Exception exceptionThrownByTxClose = null;

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            tx.acquireWriteLock( node );
            assertEquals( 1, lockCount( locks ) );

            db.shutdown();

            db.createNode();
            tx.success();
        }
        catch ( Exception e )
        {
            exceptionThrownByTxClose = e;
        }

        // THEN
        assertThat( exceptionThrownByTxClose, instanceOf( DatabaseShutdownException.class ) );
        assertFalse( db.isAvailable( 1 ) );
        assertEquals( 0, lockCount( locks ) );
    }

    @Test
    public void shouldBeAbleToShutdownWhenThereAreTransactionsWaitingForLocks() throws Exception
    {
        // GIVEN
        final Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            tx.success();
        }

        final CountDownLatch nodeLockedLatch = new CountDownLatch( 1 );
        final CountDownLatch shutdownCalled = new CountDownLatch( 1 );

        // WHEN
        // one thread locks previously created node and initiates graph db shutdown
        Future<Void> shutdownFuture = t2.execute( state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                node.addLabel( label( "ABC" ) );
                nodeLockedLatch.countDown();

                // Wait for T3 to start waiting for this node write lock
                t3.get().waitUntilWaiting( details -> details.isAt( CommunityLockClient.class, "acquireExclusive" ) );

                db.shutdown();

                shutdownCalled.countDown();
                tx.success();
            }
            return null;
        } );

        // other thread tries to lock the same node while it has been locked and graph db is being shutdown
        Future<Void> secondTxResult = t3.execute( state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                nodeLockedLatch.await();

                // T2 awaits this thread to get into a waiting state for this node write lock
                node.addLabel( label( "DEF" ) );

                shutdownCalled.await();
                tx.success();
            }
            return null;
        } );

        // start waiting when the trap has been triggered
        try
        {
            secondTxResult.get( 60, SECONDS );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( rootCause( e ), instanceOf( TransactionTerminatedException.class ) );
        }
        try
        {
            shutdownFuture.get();
            fail( "Should thrown exception since transaction should be canceled." );
        }
        catch ( Exception e )
        {
            assertThat( rootCause( e ), instanceOf( TransactionTerminatedException.class ) );
        }
    }

    private static int lockCount( Locks locks )
    {
        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        return lockCountVisitor.getLockCount();
    }

    private GraphDatabaseAPI newDb()
    {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.shutdown_transaction_end_timeout, "1s" )
                .newGraphDatabase();
    }
}
