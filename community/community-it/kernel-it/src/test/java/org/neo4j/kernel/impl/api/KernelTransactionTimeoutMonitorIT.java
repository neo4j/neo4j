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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.lock_manager;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_monitor_check_interval;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;

@DbmsExtension( configurationCallback = "configure" )
public class KernelTransactionTimeoutMonitorIT
{
    @Inject
    private GraphDatabaseAPI database;

    private static final int NODE_ID = 0;
    private ExecutorService executor;

    @ExtensionCallback
    protected void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( lock_manager, "community");
        builder.setConfig( transaction_monitor_check_interval, Duration.ofMillis( 100 ) );
    }

    @BeforeEach
    void setUp()
    {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown()
    {
        executor.shutdown();
    }

    @Test
    @Timeout( 30 )
    void terminatingTransactionMustEagerlyReleaseTheirLocks() throws Exception
    {
        AtomicBoolean nodeLockAcquired = new AtomicBoolean();
        AtomicBoolean lockerDone = new AtomicBoolean();
        BinaryLatch lockerPause = new BinaryLatch();
        long nodeId;
        try ( Transaction tx = database.beginTx() )
        {
            nodeId = tx.createNode().getId();
            tx.commit();
        }
        Future<?> locker = executor.submit( () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                Node node = tx.getNodeById( nodeId );
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
            tx.acquireWriteLock( tx.getNodeById( nodeId ) );
            tx.commit();
        }
        // No exception from our lock client being stopped (e.g. we ended up blocked for too long) or from timeout
        lockerPause.release();
        locker.get();
        assertTrue( lockerDone.get() );
    }

    @Timeout( 30 )
    @Test
    void terminateExpiredTransaction() throws Exception
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.createNode();
            transaction.commit();
        }

        Exception exception = assertThrows( Exception.class, () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node nodeById = transaction.getNodeById( NODE_ID );
                nodeById.setProperty( "a", "b" );
                executor.submit( startAnotherTransaction() ).get();
            }
        } );
        assertThat( exception.getMessage() ).contains( "The transaction has been terminated." );
    }

    private void terminateOngoingTransaction()
    {
        Set<KernelTransactionHandle> kernelTransactionHandles =
                database.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
        assertThat( kernelTransactionHandles ).hasSize( 1 );
        for ( KernelTransactionHandle kernelTransactionHandle : kernelTransactionHandles )
        {
            kernelTransactionHandle.markForTermination( Status.Transaction.Terminated );
        }
    }

    private Runnable startAnotherTransaction()
    {
        return () ->
        {
            try ( InternalTransaction tx = database
                    .beginTransaction( KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED, EMBEDDED_CONNECTION, 1, TimeUnit.SECONDS ) )
            {
                Node node = tx.getNodeById( NODE_ID );
                node.setProperty( "c", "d" );
            }
        };
    }
}
