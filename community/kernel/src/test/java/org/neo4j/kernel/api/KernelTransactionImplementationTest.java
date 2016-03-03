/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.test.DoubleLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith( Parameterized.class )
public class KernelTransactionImplementationTest extends KernelTransactionTestBase
{
    @Parameterized.Parameter( 0 )
    public ThrowingConsumer<KernelTransaction,Exception> transactionConsumer;

    @Parameterized.Parameter( 1 )
    public boolean isWriteTx;

    @Parameterized.Parameter( 2 )
    public String ignored; // to make JUnit happy...

    @Parameterized.Parameters( name = "{2}" )
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(
                new Object[]{(ThrowingConsumer<KernelTransaction,Exception>) (tx) -> {}, false, "read"},
                new Object[]{(ThrowingConsumer<KernelTransaction,Exception>) kernelTransaction -> {
                    KernelStatement statement = (KernelStatement) kernelTransaction.acquireStatement();
                    statement.txState().nodeDoCreate( 42 );
                }, true, "write"}
        );
    }

    @Test
    public void shouldCommitSuccessfulTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction( accessMode() ) )
        {
            // WHEN
            transactionConsumer.accept( transaction );
            transaction.success();
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( true, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    private AccessMode accessMode()
    {
        return isWriteTx ? AccessMode.Static.WRITE : AccessMode.Static.READ;
    }

    @Test
    public void shouldRollbackUnsuccessfulTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction( accessMode() ) )
        {
            // WHEN
            transactionConsumer.accept( transaction );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldRollbackFailedTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction( accessMode() ) )
        {
            // WHEN
            transactionConsumer.accept( transaction );
            transaction.failure();
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldRollbackAndThrowOnFailedAndSuccess() throws Exception
    {
        // GIVEN
        boolean exceptionReceived = false;
        try ( KernelTransaction transaction = newTransaction( accessMode() ) )
        {
            // WHEN
            transactionConsumer.accept( transaction );
            transaction.failure();
            transaction.success();
        }
        catch ( TransactionFailureException e )
        {
            // Expected.
            exceptionReceived = true;
        }

        // THEN
        assertTrue( exceptionReceived );
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldRollbackOnClosingTerminatedTransaction() throws Exception
    {
        // GIVEN
        boolean exceptionReceived = false;
        try ( KernelTransaction transaction = newTransaction( accessMode() ) )
        {
            // WHEN
            transactionConsumer.accept( transaction );
            transaction.success();
            transaction.markForTermination();
        }
        catch ( TransactionFailureException e )
        {
            // Expected.
            exceptionReceived = true;
        }

        // THEN
        assertTrue( exceptionReceived );

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldRollbackOnClosingSuccessfulButTerminatedTransaction() throws Exception
    {
        try ( KernelTransaction transaction = newTransaction( accessMode() ) )
        {
            // WHEN
            transactionConsumer.accept( transaction );
            transaction.markForTermination();
            assertTrue( transaction.shouldBeTerminated() );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldRollbackOnClosingTerminatedButSuccessfulTransaction() throws Exception
    {
        // GIVEN
        boolean exceptionReceived = false;
        try ( KernelTransaction transaction = newTransaction( accessMode() ) )
        {
            // WHEN
            transactionConsumer.accept( transaction );
            transaction.markForTermination();
            transaction.success();
            assertTrue( transaction.shouldBeTerminated() );
        }
        catch ( TransactionFailureException e )
        {
            // Expected.
            exceptionReceived = true;
        }

        // THEN
        assertTrue( exceptionReceived );
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldNotDowngradeFailureState() throws Exception
    {
        try ( KernelTransaction transaction = newTransaction( accessMode() ) )
        {
            // WHEN
            transactionConsumer.accept( transaction );
            transaction.markForTermination();
            transaction.failure();
            assertTrue( transaction.shouldBeTerminated() );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldIgnoreTerminateAfterCommit() throws Exception
    {
        KernelTransaction transaction = newTransaction( accessMode() );
        transactionConsumer.accept( transaction );
        transaction.success();
        transaction.close();
        transaction.markForTermination();

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( true, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldIgnoreTerminateAfterRollback() throws Exception
    {
        KernelTransaction transaction = newTransaction( accessMode() );
        transactionConsumer.accept( transaction );
        transaction.close();
        transaction.markForTermination();

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test( expected = TransactionFailureException.class )
    public void shouldThrowOnTerminationInCommit() throws Exception
    {
        KernelTransaction transaction = newTransaction( accessMode() );
        transactionConsumer.accept( transaction );
        transaction.success();
        transaction.markForTermination();
        transaction.close();
    }

    @Test
    public void shouldIgnoreTerminationDuringRollback() throws Exception
    {
        KernelTransaction transaction = newTransaction( accessMode() );
        transactionConsumer.accept( transaction );
        transaction.markForTermination();
        transaction.close();

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    class ChildException
    {
        public Exception exception = null;
    }

    @Test
    public void shouldAllowTerminatingFromADifferentThread() throws Exception
    {
        // GIVEN
        final ChildException childException = new ChildException();
        final DoubleLatch latch = new DoubleLatch( 1 );
        final KernelTransaction transaction = newTransaction( accessMode() );
        transactionConsumer.accept( transaction );

        Thread thread = new Thread( () -> {
            try
            {
                latch.awaitStart();
                transaction.markForTermination();
                latch.finish();
            }
            catch ( Exception e )
            {
                childException.exception = e;
            }
        } );

        // WHEN
        thread.start();
        transaction.success();
        latch.startAndAwaitFinish();

        if ( childException.exception != null )
        {
            throw childException.exception;
        }

        boolean exceptionReceived = false;
        try
        {
            transaction.close();
        }
        catch ( TransactionFailureException e )
        {
            // Expected.
            exceptionReceived = true;
        }

        // THEN
        assertTrue( exceptionReceived );
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldUseStartTimeAndTxIdFromWhenStartingTxAsHeader() throws Exception
    {
        // GIVEN a transaction starting at one point in time
        long startingTime = clock.currentTimeMillis();
        when( legacyIndexState.hasChanges() ).thenReturn( true );
        doAnswer( invocation -> {
            Collection<StorageCommand> commands = invocation.getArgumentAt( 0, Collection.class );
            commands.add( mock( Command.class ) );
            return null;
        } ).when( storageEngine ).createCommands(
                any( Collection.class ),
                any( TransactionState.class ),
                any( StorageStatement.class ),
                any( ResourceLocker.class ),
                anyLong() );

        try ( KernelTransactionImplementation transaction = newTransaction( accessMode() ) )
        {
            transaction.initialize( 5L, mock( Locks.Client.class ), KernelTransaction.Type.implicit, AccessMode.Static.FULL );
            try ( KernelStatement statement = transaction.acquireStatement() )
            {
                statement.legacyIndexTxState(); // which will pull it from the supplier and the mocking above
                                                // will have it say that it has changes.
            }
            // WHEN committing it at a later point
            clock.forward( 5, MILLISECONDS );
            // ...and simulating some other transaction being committed
            when( metaDataStore.getLastCommittedTransactionId() ).thenReturn( 7L );
            transaction.success();
        }

        // THEN start time and last tx when started should have been taken from when the transaction started
        assertEquals( 5L, commitProcess.transaction.getLatestCommittedTxWhenStarted() );
        assertEquals( startingTime, commitProcess.transaction.getTimeStarted() );
        assertEquals( startingTime + 5, commitProcess.transaction.getTimeCommitted() );
    }

    @Test
    public void successfulTxShouldNotifyKernelTransactionsThatItIsClosed() throws TransactionFailureException
    {
        KernelTransactionImplementation tx = newTransaction( accessMode() );

        tx.success();
        tx.close();

        verify( txPool ).release( tx );
    }

    @Test
    public void failedTxShouldNotifyKernelTransactionsThatItIsClosed() throws TransactionFailureException
    {
        KernelTransactionImplementation tx = newTransaction( accessMode() );

        tx.failure();
        tx.close();

        verify( txPool ).release( tx );
    }

    private void verifyExtraInteractionWithTheMonitor( TransactionMonitor transactionMonitor, boolean isWriteTx )
    {
        if ( isWriteTx )
        {
            verify( this.transactionMonitor, times( 1 ) ).upgradeToWriteTransaction();
        }
        verifyNoMoreInteractions( transactionMonitor );
    }
}
