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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
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
                new Object[]{readTxInitializer, false, "readOperationsInNewTransaction"},
                new Object[]{writeTxInitializer, true, "write"}
        );
    }

    @Test
    public void shouldCommitSuccessfulTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction( loginContext() ) )
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
        try ( KernelTransaction transaction = newTransaction( loginContext() ) )
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
        try ( KernelTransaction transaction = newTransaction( loginContext() ) )
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
    public void shouldRollbackAndThrowOnFailedAndSuccess()
    {
        // GIVEN
        boolean exceptionReceived = false;
        try ( KernelTransaction transaction = newTransaction( loginContext() ) )
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
    public void shouldRollbackOnClosingTerminatedTransaction()
    {
        // GIVEN
        KernelTransaction transaction = newTransaction( loginContext() );

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
        try ( KernelTransaction transaction = newTransaction( loginContext() ) )
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
    public void shouldRollbackOnClosingTerminatedButSuccessfulTransaction()
    {
        // GIVEN
        KernelTransaction transaction = newTransaction( loginContext() );

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
        try ( KernelTransaction transaction = newTransaction( loginContext() ) )
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
        KernelTransaction transaction = newTransaction( loginContext() );
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
        KernelTransaction transaction = newTransaction( loginContext() );
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
        KernelTransaction transaction = newTransaction( loginContext() );
        transactionInitializer.accept( transaction );
        transaction.success();
        transaction.markForTermination( Status.General.UnknownError );

        transaction.close();
    }

    @Test
    public void shouldIgnoreTerminationDuringRollback() throws Exception
    {
        KernelTransaction transaction = newTransaction( loginContext() );
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
        final KernelTransaction transaction = newTransaction( loginContext() );
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
            Collection<StorageCommand> commands = invocation.getArgument( 0 );
            commands.add( mock( Command.class ) );
            return null;
        } ).when( storageEngine ).createCommands(
                any( Collection.class ),
                any( TransactionState.class ),
                any( StorageStatement.class ),
                any( ResourceLocker.class ),
                anyLong() );

        try ( KernelTransactionImplementation transaction = newTransaction( loginContext() ) )
        {
            SimpleStatementLocks statementLocks = new SimpleStatementLocks( mock( Locks.Client.class ) );
            transaction.initialize( 5L, BASE_TX_COMMIT_TIMESTAMP, statementLocks, KernelTransaction.Type.implicit,
                    SecurityContext.AUTH_DISABLED, 0L, 1L );
            transaction.txState();
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
        KernelTransactionImplementation tx = newTransaction( loginContext() );

        tx.success();
        tx.close();

        verify( txPool ).release( tx );
    }

    @Test
    public void failedTxShouldNotifyKernelTransactionsThatItIsClosed() throws TransactionFailureException
    {
        KernelTransactionImplementation tx = newTransaction( loginContext() );

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
        KernelTransactionImplementation transaction = newTransaction( loginContext() );
        int reuseCount = transaction.getReuseCount();

        // WHEN
        transaction.close();
        SimpleStatementLocks statementLocks = new SimpleStatementLocks( new NoOpClient() );
        transaction.initialize( 1, BASE_TX_COMMIT_TIMESTAMP, statementLocks, KernelTransaction.Type.implicit,
                loginContext().authorize( s -> -1 ), 0L, 1L );

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
        KernelTransactionImplementation tx = newTransaction( loginContext(), locksClient );

        tx.markForTermination( Status.General.UnknownError );

        assertEquals( Status.General.UnknownError, tx.getReasonIfTerminated().get() );
        verify( locksClient ).stop();
    }

    @Test
    public void markForTerminationTerminatedTransaction()
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( loginContext(), locksClient );
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
        KernelTransactionImplementation tx = newTransaction( loginContext(), locksClient );
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
        KernelTransactionImplementation tx = newTransaction( loginContext(), locksClient );
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
        KernelTransactionImplementation tx = newTransaction( loginContext(), locksClient );
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
        KernelTransactionImplementation tx = newTransaction( loginContext(), locksClient );
        transactionInitializer.accept( tx );
        tx.success();
        tx.failure();
        tx.markForTermination( Status.General.UnknownError );

        try
        {
            tx.close();
            fail();
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
        KernelTransactionImplementation tx = newTransaction( loginContext(), locksClient );
        tx.success();
        tx.failure();

        try
        {
            tx.close();
            fail();
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransactionFailureException.class ) );
        }
    }

    @Test
    public void initializedTransactionShouldHaveNoTerminationReason()
    {
        KernelTransactionImplementation tx = newTransaction( loginContext() );
        assertFalse( tx.getReasonIfTerminated().isPresent() );
    }

    @Test
    public void shouldReportCorrectTerminationReason()
    {
        Status status = Status.Transaction.Terminated;
        KernelTransactionImplementation tx = newTransaction( loginContext() );
        tx.markForTermination( status );
        assertSame( status, tx.getReasonIfTerminated().get() );
    }

    @Test
    public void closedTransactionShouldHaveNoTerminationReason() throws Exception
    {
        KernelTransactionImplementation tx = newTransaction( loginContext() );
        tx.markForTermination( Status.Transaction.Terminated );
        tx.close();
        assertFalse( tx.getReasonIfTerminated().isPresent() );
    }

    @Test
    public void shouldCallCloseListenerOnCloseWhenCommitting() throws Exception
    {
        // given
        AtomicLong closeTxId = new AtomicLong( Long.MIN_VALUE );
        KernelTransactionImplementation tx = newTransaction( loginContext() );
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
        KernelTransactionImplementation tx = newTransaction( loginContext() );
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
        tx.initialize( 42, 42, statementLocks, KernelTransaction.Type.implicit, loginContext().authorize( s -> -1 ), 0L, 0L );

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
        tx.initialize( 42, 42, statementLocks, KernelTransaction.Type.implicit,
                loginContext().authorize( s -> -1 ), 0L, 0L );

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

    @Test
    public void resetTransactionStatisticsOnRelease() throws TransactionFailureException
    {
        KernelTransactionImplementation transaction = newTransaction( 1000 );
        transaction.getStatistics().addWaitingTime( 1 );
        transaction.getStatistics().addWaitingTime( 1 );
        assertEquals( 2, transaction.getStatistics().getWaitingTimeNanos( 0 ) );
        transaction.close();
        assertEquals( 0, transaction.getStatistics().getWaitingTimeNanos( 0 ) );
    }

    @Test
    public void reportTransactionStatistics()
    {
        KernelTransactionImplementation transaction = newTransaction( 100 );
        KernelTransactionImplementation.Statistics statistics =
                new KernelTransactionImplementation.Statistics( transaction, new AtomicReference<>( new ThreadBasedCpuClock() ),
                        new AtomicReference<>( new ThreadBasedAllocation() ) );
        PredictablePageCursorTracer tracer = new PredictablePageCursorTracer();
        statistics.init( 2, tracer );

        assertEquals( 2, statistics.cpuTimeMillis() );
        assertEquals( 2, statistics.heapAllocatedBytes() );
        assertEquals( 1, statistics.totalTransactionPageCacheFaults() );
        assertEquals( 4, statistics.totalTransactionPageCacheHits() );
        statistics.addWaitingTime( 1 );
        assertEquals( 1, statistics.getWaitingTimeNanos( 0 ) );

        statistics.reset();

        statistics.init( 4, tracer );
        assertEquals( 4, statistics.cpuTimeMillis() );
        assertEquals( 4, statistics.heapAllocatedBytes() );
        assertEquals( 2, statistics.totalTransactionPageCacheFaults() );
        assertEquals( 6, statistics.totalTransactionPageCacheHits() );
        assertEquals( 0, statistics.getWaitingTimeNanos( 0 ) );
    }

    private LoginContext loginContext()
    {
        return isWriteTx ? AnonymousContext.write() : AnonymousContext.read();
    }

    private void initializeAndClose( KernelTransactionImplementation tx, int times ) throws Exception
    {
        for ( int i = 0; i < times; i++ )
        {
            SimpleStatementLocks statementLocks = new SimpleStatementLocks( new NoOpClient() );
            tx.initialize( i + 10, i + 10, statementLocks, KernelTransaction.Type.implicit, loginContext().authorize( s -> -1 ), 0L, 0L );
            tx.close();
        }
    }

    private static class ThreadBasedCpuClock extends CpuClock
    {
        private long iteration;
        @Override
        public long cpuTimeNanos( long threadId )
        {
            iteration++;
            return MILLISECONDS.toNanos( iteration * threadId );
        }
    }

    private static class ThreadBasedAllocation extends HeapAllocation
    {
        private long iteration;
        @Override
        public long allocatedBytes( long threadId )
        {
            iteration++;
            return iteration * threadId;
        }
    }

    private static class PredictablePageCursorTracer extends DefaultPageCursorTracer
    {
        private long iteration = 1;

        @Override
        public long accumulatedHits()
        {
            iteration++;
            return iteration * 2;
        }

        @Override
        public long accumulatedFaults()
        {
            return iteration;
        }
    }
}
