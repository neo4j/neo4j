/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KernelTransactionsIT
{

    public ExpectedException exception = ExpectedException.none();
    public EmbeddedDatabaseRule database = new EmbeddedDatabaseRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( database ).around( exception );

    private ExecutorService executorService;

    @Before
    public void setUp()
    {
        executorService = Executors.newCachedThreadPool();
    }

    @After
    public void tearDown()
    {
        executorService.shutdown();
    }

    @Test
    public void terminationOfRunningTransaction() throws Exception
    {
        KernelTransactions kernelTransactions = database.getDependencyResolver()
                .resolveDependency( KernelTransactions.class );

        CountDownLatch latch = new CountDownLatch( 1 );
        exception.expect( new RootCauseMatcher<>( TransactionTerminatedException.class,
                "The transaction has been terminated." ) );
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            executorService.submit( () ->
            {
                Set<KernelTransactionHandle> transactions = kernelTransactions.activeTransactions();
                assertThat( "Contain one single active transaction", transactions, hasSize( 1 ) );
                transactions.forEach( tx -> tx.markForTermination( Status.Transaction.TransactionTerminated ) );
                latch.countDown();
            } );
            latch.await();
            transaction.success();
        }

        assertThat( "Transactions have been terminated", kernelTransactions.activeTransactions(), empty() );
        database.shutdown();
    }

    @Test
    public void resetTerminationReasonOnTransactionInit()
            throws TransactionFailureException, ExecutionException, InterruptedException
    {
        KernelTransactions transactions = database.getDependencyResolver().resolveDependency( KernelTransactions.class );
        KernelTransactionImplementation transaction = (KernelTransactionImplementation) transactions
                .newInstance( KernelTransaction.Type.implicit, SecurityContext.AUTH_DISABLED, 10L );
        transaction.markForTermination( Status.Transaction.Terminated );

        executorService.submit( () ->
        {
            assertTrue( transaction.getReasonIfTerminated().isPresent() );
            transaction.initialize( 1L, 2L, new SimpleStatementLocks( new NoOpClient() ),
                    KernelTransaction.Type.explicit, SecurityContext.AUTH_DISABLED, 3L );
            assertFalse( transaction.getReasonIfTerminated().isPresent() );
            return null;
        } ).get();
    }

    @Test
    public void shutdownWhileRunningTransaction()
    {
        exception.expect( new RootCauseMatcher<>( TransactionTerminatedException.class,
                "The transaction has been terminated. Retry your operation in a " +
                        "new transaction, and you should see a successful result. " +
                        "The database is not currently available to serve your request, " +
                        "refer to the database logs for more details. Retrying your request at a later time may succeed." ) );

        try ( Transaction ignored = database.beginTx() )
        {
            database.createNode();
            database.shutdown();
        }
    }

    @Test
    public void shutdownDatabaseWhileHaveActiveTransactionRunning() throws InterruptedException
    {
        exception.expect( new RootCauseMatcher<>( TransactionTerminatedException.class,
                "The transaction has been terminated. Retry your operation in a " +
                        "new transaction, and you should see a successful result. " +
                        "The database is not currently available to serve your request, " +
                        "refer to the database logs for more details. Retrying your request at a later time may succeed." ) );

        CountDownLatch latch = new CountDownLatch( 1 );
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
            executorService.submit( () ->
            {
                database.shutdown();
                latch.countDown();
            } );
            latch.await();
        }
    }

    @Test
    public void shutdownWithHaveActiveTerminatedTransactionRunning() throws InterruptedException
    {
        exception.expect( new RootCauseMatcher<>( TransactionTerminatedException.class,
                "The transaction has been terminated. Retry your operation in a " +
                        "new transaction, and you should see a successful result. " +
                        "The database is not currently available to serve your request, " +
                        "refer to the database logs for more details. Retrying your request at a later time may succeed." ) );

        CountDownLatch latch = new CountDownLatch( 1 );
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.terminate();
            executorService.submit( () ->
            {
                database.shutdown();
                latch.countDown();
            } );
            latch.await();
        }
    }

    @Test
    public void terminateTransactionFromAnotherThread() throws InterruptedException
    {
        exception.expect( TransactionTerminatedException.class );
        exception.expectMessage( "The transaction has been terminated. Retry your operation in a new transaction, " +
                "and you should see a successful result. Explicitly terminated by the user." );

        CountDownLatch latch = new CountDownLatch( 1 );
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            executorService.submit( () ->
            {
                transaction.terminate();
                latch.countDown();
            } );
            latch.await();
            database.createNode();
        }

        database.shutdown();
    }

    @Test
    public void terminateTransactionFromAnotherThreadByHandle() throws InterruptedException
    {
        exception.expect( TransactionTerminatedException.class );
        exception.expectMessage( "The transaction has been terminated. Retry your operation in a new transaction, " +
                "and you should see a successful result. The request referred to a transaction that does not exist." );

        CountDownLatch latch = new CountDownLatch( 1 );
        try ( Transaction ignored = database.beginTx() )
        {
            database.createNode();
            executorService.submit( () ->
            {
                DependencyResolver dependencyResolver = database.getDependencyResolver();
                KernelTransactions transactions = dependencyResolver.resolveDependency( KernelTransactions.class );
                Set<KernelTransactionHandle> kernelTransactionHandles = transactions.activeTransactions();
                kernelTransactionHandles.forEach( tx -> tx.markForTermination( Status.Transaction.TransactionNotFound ) );
                latch.countDown();
            } );
            latch.await();
            database.createNode();
        }

        database.shutdown();
    }

    @Test
    public void shutdownRunningTransactionsOnDispose()
    {
        KernelTransactions transactions =
                database.getDependencyResolver().resolveDependency( KernelTransactions.class );
        try ( Transaction ignored = database.beginTx() )
        {
            database.createNode();

            database.shutdown();
        }
        catch ( Exception ignored )
        {
            // nothing
        }
        Set<KernelTransactionImplementation> allTransactions = transactions.getAllTransactions();
        assertThat( "We should have one transaction that was open and how is shutdown.",
                allTransactions, Matchers.hasSize( 1 ) );
        KernelTransactionImplementation shutdownTransaction = allTransactions.iterator().next();
        assertEquals( Status.General.DatabaseUnavailable, shutdownTransaction.getReasonIfTerminated().get() );
        assertTrue( "Transaction state should be shutdown.", shutdownTransaction.isShutdown() );
    }

    @Test
    public void waitClosingTransactionOnShutdown() throws TransactionFailureException, InterruptedException
    {
        Kernel kernel = database.getDependencyResolver().resolveDependency( Kernel.class );
        KernelTransactions transactions =
                database.getDependencyResolver().resolveDependency( KernelTransactions.class );

        KernelTransactionImplementation kernelTransaction = (KernelTransactionImplementation) kernel
                .newTransaction(KernelTransaction.Type.implicit, SecurityContext.AUTH_DISABLED, 10000L );

        CountDownLatch shutdownLatch = new CountDownLatch( 1 );
        database.registerKernelEventHandler( new ShutdownEventHandler( kernelTransaction ) );
        database.registerTransactionEventHandler( new ShutdownTransactionEventHandler( shutdownLatch ) );

        kernelTransaction.txState().nodeDoCreate( 2 );
        kernelTransaction.success();
        kernelTransaction.close();

        shutdownLatch.await();

        Set<KernelTransactionImplementation> allTransactions = transactions.getAllTransactions();
        assertThat( "No transactions, everything should be closed during shutdown.", allTransactions, empty() );
    }

    private static class ShutdownEventHandler implements KernelEventHandler
    {
        private final KernelTransactionImplementation kernelTransaction;

        ShutdownEventHandler( KernelTransactionImplementation kernelTransaction )
        {
            this.kernelTransaction = kernelTransaction;
        }

        @Override
        public void beforeShutdown()
        {
            assertTrue( "Transaction should be already closed.", kernelTransaction.isClosed() );
        }

        @Override
        public void kernelPanic( ErrorState error )
        {

        }

        @Override
        public Object getResource()
        {
            return null;
        }

        @Override
        public ExecutionOrder orderComparedTo( KernelEventHandler other )
        {
            return null;
        }
    }

    private class ShutdownTransactionEventHandler extends TransactionEventHandler.Adapter<Object>
    {
        private final CountDownLatch shutdownLatch;

        ShutdownTransactionEventHandler( CountDownLatch shutdownLatch )
        {
            this.shutdownLatch = shutdownLatch;
        }

        @Override
        public Object beforeCommit( TransactionData data ) throws Exception
        {
            executorService.submit(() -> {
                database.shutdown();
                shutdownLatch.countDown();
            } );
            return null;
        }
    }
}
