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

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.locking.LockCountVisitor;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.Exceptions.rootCause;

public class GraphDatabaseShutdownTest
{
    @Test
    public void transactionShouldReleaseLocksWhenGraphDbIsBeingShutdown() throws Exception
    {
        // GIVEN
        final GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
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
        final GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        final Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            tx.success();
        }

        final CountDownLatch nodeLockedLatch = new CountDownLatch( 1 );

        // WHEN
        // one thread locks previously create node and initiates graph db shutdown
        newSingleThreadExecutor().submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    node.addLabel( label( "ABC" ) );
                    nodeLockedLatch.countDown();
                    Thread.sleep( 1_000 ); // Let the second thread attempt to lock same node
                    db.shutdown();
                    tx.success();
                }
                return null;
            }
        } );

        // other thread tries to lock the same node while it has been locked and graph db is being shutdown
        Future<Void> secondTxResult = newSingleThreadExecutor().submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    nodeLockedLatch.await();
                    node.addLabel( label( "DEF" ) );
                    tx.success();
                }
                return null;
            }
        } );

        // THEN
        // tx in second thread should fail in reasonable time
        try
        {
            secondTxResult.get( 60, SECONDS );
            fail( "Exception expected" );
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
}
