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

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.impl.locking.LockCountVisitor;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockClient;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.OtherThreadRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;

public class GraphDatabaseShutdownTest
{
    private GraphDatabaseAPI db;

    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>( "T2" );
    @Rule
    public final OtherThreadRule<Void> t3 = new OtherThreadRule<>( "T3" );
    private DatabaseManagementService managementService;

    @Before
    public void setUp()
    {
        db = newDb();
    }

    @After
    public void tearDown()
    {
        managementService.shutdown();
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
            Node node = tx.createNode();
            tx.acquireWriteLock( node );
            assertThat( lockCount( locks ) ).isGreaterThanOrEqualTo( 1 );

            managementService.shutdown();

            tx.createNode();
            tx.commit();
        }
        catch ( Exception e )
        {
            exceptionThrownByTxClose = e;
        }

        // THEN
        assertThat( exceptionThrownByTxClose ).isInstanceOf( TransactionTerminatedException.class );
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
            node = tx.createNode();
            tx.commit();
        }

        final CountDownLatch nodeLockedLatch = new CountDownLatch( 1 );
        final CountDownLatch shutdownCalled = new CountDownLatch( 1 );

        // WHEN
        // one thread locks previously created node and initiates graph db shutdown
        Future<Void> shutdownFuture = t2.execute( state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.getNodeById( node.getId() ).addLabel( label( "ABC" ) );
                nodeLockedLatch.countDown();

                // Wait for T3 to start waiting for this node write lock
                t3.get().waitUntilWaiting( details -> details.isAt( CommunityLockClient.class, "acquireExclusive" ) );

                managementService.shutdown();

                shutdownCalled.countDown();
                tx.commit();
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
                tx.getNodeById( node.getId() ).addLabel( label( "DEF" ) );

                shutdownCalled.await();
                tx.commit();
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
            assertThat( getRootCause( e ) ).isInstanceOf( TransactionTerminatedException.class );
        }
        try
        {
            shutdownFuture.get();
            fail( "Should thrown exception since transaction should be canceled." );
        }
        catch ( Exception e )
        {
            assertThat( getRootCause( e ) ).isInstanceOf( TransactionTerminatedException.class );
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
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }
}
