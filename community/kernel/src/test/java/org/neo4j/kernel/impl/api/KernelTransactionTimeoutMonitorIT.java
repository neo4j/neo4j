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
package org.neo4j.kernel.impl.api;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KernelTransactionTimeoutMonitorIT
{
    @Rule
    public DatabaseRule database = createDatabaseRule();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final int NODE_ID = 0;
    private ExecutorService executor;

    protected DatabaseRule createDatabaseRule()
    {
        return new EmbeddedDatabaseRule()
                .withSetting( GraphDatabaseSettings.transaction_monitor_check_interval, "100ms" );
    }

    @Before
    public void setUp()
    {
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown()
    {
        executor.shutdown();
    }

    @Test( timeout = 30_000 )
    public void terminateExpiredTransaction() throws Exception
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
        }

        expectedException.expectMessage( "The transaction has been terminated." );

        try ( Transaction transaction = database.beginTx() )
        {
            Node nodeById = database.getNodeById( NODE_ID );
            nodeById.setProperty( "a", "b" );
            executor.submit( startAnotherTransaction() ).get();
        }
    }

    @Test( timeout = 30_000 )
    public void terminatingTransactionMustEagerlyReleaseTheirLocks() throws Exception
    {
        AtomicBoolean nodeLockAcquired = new AtomicBoolean();
        AtomicBoolean lockerDone = new AtomicBoolean();
        BinaryLatch lockerPause = new BinaryLatch();
        long nodeId;
        try ( Transaction tx = database.beginTx() )
        {
            nodeId = database.createNode().getId();
            tx.success();
        }
        Future<?> locker = executor.submit( () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                Node node = database.getNodeById( nodeId );
                tx.acquireReadLock( node );
                nodeLockAcquired.set( true );
                lockerPause.await();
            }
            lockerDone.set( true );
        } );

        boolean proceed;
        do
        {
            proceed = nodeLockAcquired.get();
        }
        while ( !proceed );

        terminateOngoingTransaction();

        assertFalse( lockerDone.get() ); // but the thread should still be blocked on the latch
        // Yet we should be able to proceed and grab the locks they once held
        try ( Transaction tx = database.beginTx() )
        {
            // Write-locking is only possible if their shared lock was released
            tx.acquireWriteLock( database.getNodeById( nodeId ) );
            tx.success();
        }
        // No exception from our lock client being stopped (e.g. we ended up blocked for too long) or from timeout
        lockerPause.release();
        locker.get();
        assertTrue( lockerDone.get() );
    }

    private void terminateOngoingTransaction()
    {
        Set<KernelTransactionHandle> kernelTransactionHandles =
                database.resolveDependency( KernelTransactions.class ).activeTransactions();
        assertThat( kernelTransactionHandles, hasSize( 1 ) );
        for ( KernelTransactionHandle kernelTransactionHandle : kernelTransactionHandles )
        {
            kernelTransactionHandle.markForTermination( Status.Transaction.Terminated );
        }
    }

    private Runnable startAnotherTransaction()
    {
        return () ->
        {
            try ( InternalTransaction ignored = database
                    .beginTransaction( KernelTransaction.Type.implicit, LoginContext.AUTH_DISABLED, 1,
                            TimeUnit.SECONDS ) )
            {
                Node node = database.getNodeById( NODE_ID );
                node.setProperty( "c", "d" );
            }
        };
    }
}
