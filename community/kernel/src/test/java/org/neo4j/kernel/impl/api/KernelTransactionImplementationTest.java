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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.test.DoubleLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

@RunWith( Parameterized.class )
public class KernelTransactionImplementationTest extends KernelTransactionTestBase
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Parameterized.Parameter()
    public Consumer<KernelTransaction> transactionInitializer;

    @Parameterized.Parameter( 1 )
    public boolean isWriteTx;

    @Parameterized.Parameter( 2 )
    public String ignored; // to make JUnit happy...

    @Parameterized.Parameters( name = "{2}" )
    public static Collection<Object[]> parameters()
    {
        Consumer<KernelTransaction> readTxInitializer = tx ->
        {
        };
        Consumer<KernelTransaction> writeTxInitializer = tx ->
        {
            try ( KernelStatement statement = (KernelStatement) tx.acquireStatement() )
            {
                statement.txState().nodeDoCreate( 42 );
            }
        };
        return Arrays.asList(
                new Object[]{readTxInitializer, false, "read"},
                new Object[]{writeTxInitializer, true, "write"}
        );
    }

    @Test
    public void shouldCommitSuccessfulTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction( securityContext() ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
            transaction.success();
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( true, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldRollbackUnsuccessfulTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction( securityContext() ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldRollbackFailedTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction( securityContext() ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
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
        try ( KernelTransaction transaction = newTransaction( securityContext() ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
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
        KernelTransaction transaction = newTransaction( securityContext() );

        transactionInitializer.accept( transaction );
        transaction.success();
        transaction.markForTermination( Status.General.UnknownError );

        try
        {
            // WHEN
            transaction.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransactionTerminatedException.class ) );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldRollbackOnClosingSuccessfulButTerminatedTransaction() throws Exception
    {
        try ( KernelTransaction transaction = newTransaction( securityContext() ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
            transaction.markForTermination( Status.General.UnknownError );
            assertEquals( Status.General.UnknownError, transaction.getReasonIfTerminated().get() );
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
        KernelTransaction transaction = newTransaction( securityContext() );

        transactionInitializer.accept( transaction );
        transaction.markForTermination( Status.General.UnknownError );
        transaction.success();
        assertEquals( Status.General.UnknownError, transaction.getReasonIfTerminated().get() );

        try
        {
            // WHEN
            transaction.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransactionTerminatedException.class ) );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldNotDowngradeFailureState() throws Exception
    {
        try ( KernelTransaction transaction = newTransaction( securityContext() ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
            transaction.markForTermination( Status.General.UnknownError );
            transaction.failure();
            assertEquals( Status.General.UnknownError, transaction.getReasonIfTerminated().get() );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldIgnoreTerminateAfterCommit() throws Exception
    {
        KernelTransaction transaction = newTransaction( securityContext() );
        transactionInitializer.accept( transaction );
        transaction.success();
        transaction.close();
        transaction.markForTermination( Status.General.UnknownError );

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( true, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldIgnoreTerminateAfterRollback() throws Exception
    {
        KernelTransaction transaction = newTransaction( securityContext() );
        transactionInitializer.accept( transaction );
        transaction.close();
        transaction.markForTermination( Status.General.UnknownError );

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test( expected = TransactionTerminatedException.class )
    public void shouldThrowOnTerminationInCommit() throws Exception
    {
        KernelTransaction transaction = newTransaction( securityContext() );
        transactionInitializer.accept( transaction );
        transaction.success();
        transaction.markForTermination( Status.General.UnknownError );

        transaction.close();
    }

    @Test
    public void shouldIgnoreTerminationDuringRollback() throws Exception
    {
        KernelTransaction transaction = newTransaction( securityContext() );
        transactionInitializer.accept( transaction );
        transaction.markForTermination( Status.General.UnknownError );
        transaction.close();

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @Test
    public void shouldAllowTerminatingFromADifferentThread() throws Exception
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch( 1 );
        final KernelTransaction transaction = newTransaction( securityContext() );
        transactionInitializer.accept( transaction );

        Future<?> terminationFuture = Executors.newSingleThreadExecutor().submit( () ->
        {
            latch.waitForAllToStart();
            transaction.markForTermination( Status.General.UnknownError );
            latch.finish();
        } );

        // WHEN
        transaction.success();
        latch.startAndWaitForAllToStartAndFinish();

        assertNull( terminationFuture.get( 1, TimeUnit.MINUTES ) );

        try
        {
            transaction.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransactionTerminatedException.class ) );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldUseStartTimeAndTxIdFromWhenStartingTxAsHeader() throws Exception
    {
        // GIVEN a transaction starting at one point in time
        long startingTime = clock.millis();
        when( explicitIndexState.hasChanges() ).thenReturn( true );
        doAnswer( invocation ->
        {
            @SuppressWarnings( "unchecked" )
            Collection<StorageCommand> commands = invocation.getArgumentAt( 0, Collection.class );
            commands.add( mock( Command.class ) );
            return null;
        } ).when( storageEngine ).createCommands(
                any( Collection.class ),
                any( TransactionState.class ),
                any( StorageStatement.class ),
                any( ResourceLocker.class ),
                anyLong() );

        try ( KernelTransactionImplementation transaction = newTransaction( securityContext() ) )
        {
            SimpleStatementLocks statementLocks = new SimpleStatementLocks( mock( Locks.Client.class ) );
            transaction.initialize( 5L, BASE_TX_COMMIT_TIMESTAMP, statementLocks, KernelTransaction.Type.implicit,
                    AUTH_DISABLED, 0L );
            try ( KernelStatement statement = transaction.acquireStatement() )
            {
                statement.explicitIndexTxState(); // which will pull it from the supplier and the mocking above
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
        KernelTransactionImplementation tx = newTransaction( securityContext() );

        tx.success();
        tx.close();

        verify( txPool ).release( tx );
    }

    @Test
    public void failedTxShouldNotifyKernelTransactionsThatItIsClosed() throws TransactionFailureException
    {
        KernelTransactionImplementation tx = newTransaction( securityContext() );

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

    @Test
    public void shouldIncrementReuseCounterOnReuse() throws Exception
    {
        // GIVEN
        KernelTransactionImplementation transaction = newTransaction( securityContext() );
        int reuseCount = transaction.getReuseCount();

        // WHEN
        transaction.close();
        SimpleStatementLocks statementLocks = new SimpleStatementLocks( new NoOpClient() );
        transaction.initialize( 1, BASE_TX_COMMIT_TIMESTAMP, statementLocks, KernelTransaction.Type.implicit,
                securityContext(), 0L );

        // THEN
        assertEquals( reuseCount + 1, transaction.getReuseCount() );
    }

    @Test
    public void markForTerminationNotInitializedTransaction()
    {
        KernelTransactionImplementation tx = newNotInitializedTransaction();
        tx.markForTermination( Status.General.UnknownError );

        assertEquals( Status.General.UnknownError, tx.getReasonIfTerminated().get() );
    }

    @Test
    public void markForTerminationInitializedTransaction()
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( securityContext(), locksClient );

        tx.markForTermination( Status.General.UnknownError );

        assertEquals( Status.General.UnknownError, tx.getReasonIfTerminated().get() );
        verify( locksClient ).stop();
    }

    @Test
    public void markForTerminationTerminatedTransaction()
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( securityContext(), locksClient );
        transactionInitializer.accept( tx );

        tx.markForTermination( Status.Transaction.Terminated );
        tx.markForTermination( Status.Transaction.Outdated );
        tx.markForTermination( Status.Transaction.LockClientStopped );

        assertEquals( Status.Transaction.Terminated, tx.getReasonIfTerminated().get() );
        verify( locksClient ).stop();
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
    }

    @Test
    public void terminatedTxMarkedNeitherSuccessNorFailureClosesWithoutThrowing() throws TransactionFailureException
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( securityContext(), locksClient );
        transactionInitializer.accept( tx );
        tx.markForTermination( Status.General.UnknownError );

        tx.close();

        verify( locksClient ).stop();
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
    }

    @Test
    public void terminatedTxMarkedForSuccessThrowsOnClose()
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( securityContext(), locksClient );
        transactionInitializer.accept( tx );
        tx.success();
        tx.markForTermination( Status.General.UnknownError );

        try
        {
            tx.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransactionTerminatedException.class ) );
        }
    }

    @Test
    public void terminatedTxMarkedForFailureClosesWithoutThrowing() throws TransactionFailureException
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( securityContext(), locksClient );
        transactionInitializer.accept( tx );
        tx.failure();
        tx.markForTermination( Status.General.UnknownError );

        tx.close();

        verify( locksClient ).stop();
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
    }

    @Test
    public void terminatedTxMarkedForBothSuccessAndFailureThrowsOnClose()
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( securityContext(), locksClient );
        transactionInitializer.accept( tx );
        tx.success();
        tx.failure();
        tx.markForTermination( Status.General.UnknownError );

        try
        {
            tx.close();
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransactionTerminatedException.class ) );
        }
    }

    @Test
    public void txMarkedForBothSuccessAndFailureThrowsOnClose()
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( securityContext(), locksClient );
        tx.success();
        tx.failure();

        try
        {
            tx.close();
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransactionFailureException.class ) );
        }
    }

    @Test
    public void initializedTransactionShouldHaveNoTerminationReason() throws Exception
    {
        KernelTransactionImplementation tx = newTransaction( securityContext() );
        assertFalse( tx.getReasonIfTerminated().isPresent() );
    }

    @Test
    public void shouldReportCorrectTerminationReason() throws Exception
    {
        Status status = Status.Transaction.Terminated;
        KernelTransactionImplementation tx = newTransaction( securityContext() );
        tx.markForTermination( status );
        assertSame( status, tx.getReasonIfTerminated().get() );
    }

    @Test
    public void closedTransactionShouldHaveNoTerminationReason() throws Exception
    {
        KernelTransactionImplementation tx = newTransaction( securityContext() );
        tx.markForTermination( Status.Transaction.Terminated );
        tx.close();
        assertFalse( tx.getReasonIfTerminated().isPresent() );
    }

    @Test
    public void shouldCallCloseListenerOnCloseWhenCommitting() throws Exception
    {
        // given
        AtomicLong closeTxId = new AtomicLong( Long.MIN_VALUE );
        KernelTransactionImplementation tx = newTransaction( securityContext() );
        tx.registerCloseListener( closeTxId::set );

        // when
        if ( isWriteTx )
        {
            tx.upgradeToDataWrites();
            tx.txState().nodeDoCreate( 42L );
        }
        tx.success();
        tx.close();

        // then
        assertThat( closeTxId.get(), isWriteTx ? greaterThan( BASE_TX_ID ) : equalTo( 0L ) );
    }

    @Test
    public void shouldCallCloseListenerOnCloseWhenRollingBack() throws Exception
    {
        // given
        AtomicLong closeTxId = new AtomicLong( Long.MIN_VALUE );
        KernelTransactionImplementation tx = newTransaction( securityContext() );
        tx.registerCloseListener( closeTxId::set );

        // when
        tx.failure();
        tx.close();

        // then
        assertEquals( -1L, closeTxId.get() );
    }

    @Test
    public void transactionWithCustomTimeout()
    {
        long transactionTimeout = 5L;
        KernelTransactionImplementation transaction = newTransaction( transactionTimeout );
        assertEquals( "Transaction should have custom configured timeout.", transactionTimeout, transaction.timeout() );
    }

    @Test
    public void transactionStartTime()
    {
        long startTime = clock.forward( 5, TimeUnit.MINUTES ).millis();
        KernelTransactionImplementation transaction = newTransaction( AUTH_DISABLED );
        assertEquals( "Transaction start time should be the same as clock time.", startTime, transaction.startTime() );
    }

    @Test
    public void markForTerminationWithCorrectReuseCount() throws Exception
    {
        int reuseCount = 10;
        Status.Transaction terminationReason = Status.Transaction.Terminated;

        KernelTransactionImplementation tx = newNotInitializedTransaction( );
        initializeAndClose( tx, reuseCount );

        Locks.Client locksClient = mock( Locks.Client.class );
        SimpleStatementLocks statementLocks = new SimpleStatementLocks( locksClient );
        tx.initialize( 42, 42, statementLocks, KernelTransaction.Type.implicit, securityContext(), 0L );

        assertTrue( tx.markForTermination( reuseCount, terminationReason ) );

        assertEquals( terminationReason, tx.getReasonIfTerminated().get() );
        verify( locksClient ).stop();
    }

    @Test
    public void markForTerminationWithIncorrectReuseCount() throws Exception
    {
        int reuseCount = 13;
        int nextReuseCount = reuseCount + 2;
        Status.Transaction terminationReason = Status.Transaction.Terminated;

        KernelTransactionImplementation tx = newNotInitializedTransaction( );
        initializeAndClose( tx, reuseCount );

        Locks.Client locksClient = mock( Locks.Client.class );
        SimpleStatementLocks statementLocks = new SimpleStatementLocks( locksClient );
        tx.initialize( 42, 42, statementLocks, KernelTransaction.Type.implicit, securityContext(), 0L );

        assertFalse( tx.markForTermination( nextReuseCount, terminationReason ) );

        assertFalse( tx.getReasonIfTerminated().isPresent() );
        verify( locksClient, never() ).stop();
    }

    @Test
    public void closeClosedTransactionIsNotAllowed() throws TransactionFailureException
    {
        KernelTransactionImplementation transaction = newTransaction( 1000 );
        transaction.close();

        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage( "This transaction has already been completed." );
        transaction.close();
    }

    private SecurityContext securityContext()
    {
        return isWriteTx ? AnonymousContext.write() : AnonymousContext.read();
    }

    private void initializeAndClose( KernelTransactionImplementation tx, int times ) throws Exception
    {
        for ( int i = 0; i < times; i++ )
        {
            SimpleStatementLocks statementLocks = new SimpleStatementLocks( new NoOpClient() );
            tx.initialize( i + 10, i + 10, statementLocks, KernelTransaction.Type.implicit, securityContext(), 0L );
            tx.close();
        }
    }
}
