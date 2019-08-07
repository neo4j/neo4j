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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.DoubleLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

class KernelTransactionImplementationTest extends KernelTransactionTestBase
{
    private static Stream<Arguments> parameters()
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
        return Stream.of(
            arguments( "readOperationsInNewTransaction", false, readTxInitializer ),
            arguments( "write", true, writeTxInitializer )
        );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void changeTransactionTracingWithoutRestart( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        try ( KernelTransactionImplementation transaction = newTransaction( loginContext( isWriteTx ) ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
            assertSame( TransactionInitializationTrace.NONE, transaction.getInitializationTrace() );
            transaction.success();
        }
        config.setDynamic( GraphDatabaseSettings.transaction_tracing_level, GraphDatabaseSettings.TransactionTracingLevel.ALL, getClass().getSimpleName() );
        try ( KernelTransactionImplementation transaction = newTransaction( loginContext( isWriteTx ) ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
            assertNotSame( TransactionInitializationTrace.NONE, transaction.getInitializationTrace() );
            transaction.success();
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void emptyMetadataReturnedWhenMetadataIsNotSet( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        try ( KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) ) )
        {
            Map<String, Object> metaData = transaction.getMetaData();
            assertTrue( metaData.isEmpty() );
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void accessSpecifiedTransactionMetadata( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        try ( KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) ) )
        {
            Map<String, Object> externalMetadata = map( "Robot", "Bender", "Human", "Fry" );
            transaction.setMetaData( externalMetadata );
            Map<String, Object> transactionMetadata = transaction.getMetaData();
            assertFalse( transactionMetadata.isEmpty() );
            assertEquals( "Bender", transactionMetadata.get( "Robot" ) );
            assertEquals( "Fry", transactionMetadata.get( "Human" ) );
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldCommitSuccessfulTransaction( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
            transaction.commit();
        }

        // THEN
        verify( transactionMonitor ).transactionFinished( true, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldRollbackUnsuccessfulTransaction( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
        }

        // THEN
        verify( transactionMonitor ).transactionFinished( false, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldRollbackFailedTransaction( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
            transaction.rollback();
        }

        // THEN
        verify( transactionMonitor ).transactionFinished( false, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldRollbackAndThrowOnFailedAndSuccess( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
    {
        // GIVEN
        assertThrows( IllegalStateException.class, () ->
        {
            try ( KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) ) )
            {
                // WHEN
                transactionInitializer.accept( transaction );
                transaction.rollback();
                transaction.commit();
            }
        });
        verify( transactionMonitor ).transactionFinished( false, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldRollbackOnClosingTerminatedTransaction( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
            throws TransactionFailureException
    {
        // GIVEN
        KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) );

        transactionInitializer.accept( transaction );
        transaction.markForTermination( Status.General.UnknownError );

        assertThrows( TransactionTerminatedException.class, transaction::commit );

        // THEN
        verify( transactionMonitor ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldRollbackOnClosingSuccessfulButTerminatedTransaction( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
        throws Exception
    {
        try ( KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
            transaction.markForTermination( Status.General.UnknownError );
            assertEquals( Status.General.UnknownError, transaction.getReasonIfTerminated().get() );
        }

        // THEN
        verify( transactionMonitor ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldRollbackOnClosingTerminatedButSuccessfulTransaction( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
            throws TransactionFailureException
    {
        // GIVEN
        KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) );

        transactionInitializer.accept( transaction );
        transaction.markForTermination( Status.General.UnknownError );
        assertEquals( Status.General.UnknownError, transaction.getReasonIfTerminated().get() );

        assertThrows( TransactionTerminatedException.class, transaction::commit );

        // THEN
        verify( transactionMonitor ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldNotDowngradeFailureState( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        try ( KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) ) )
        {
            // WHEN
            transactionInitializer.accept( transaction );
            transaction.markForTermination( Status.General.UnknownError );
            assertEquals( Status.General.UnknownError, transaction.getReasonIfTerminated().get() );
        }

        // THEN
        verify( transactionMonitor ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldIgnoreTerminateAfterCommit( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) );
        transactionInitializer.accept( transaction );
        transaction.commit();
        transaction.markForTermination( Status.General.UnknownError );

        // THEN
        verify( transactionMonitor ).transactionFinished( true, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldIgnoreTerminateAfterRollback( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) );
        transactionInitializer.accept( transaction );
        transaction.close();
        transaction.markForTermination( Status.General.UnknownError );

        // THEN
        verify( transactionMonitor ).transactionFinished( false, isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldThrowOnTerminationInCommit( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) );
        transactionInitializer.accept( transaction );
        transaction.markForTermination( Status.General.UnknownError );

        assertThrows( TransactionTerminatedException.class, transaction::commit );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldIgnoreTerminationDuringRollback( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) );
        transactionInitializer.accept( transaction );
        transaction.markForTermination( Status.General.UnknownError );
        transaction.close();

        // THEN
        verify( transactionMonitor ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldAllowTerminatingFromADifferentThread( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch( 1 );
        final KernelTransaction transaction = newTransaction( loginContext( isWriteTx ) );
        transactionInitializer.accept( transaction );

        var executorService = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> terminationFuture = executorService.submit( () ->
            {
                latch.waitForAllToStart();
                transaction.markForTermination( Status.General.UnknownError );
                latch.finish();
            } );

            // WHEN
            latch.startAndWaitForAllToStartAndFinish();

            assertNull( terminationFuture.get( 1, TimeUnit.MINUTES ) );
            assertThrows( TransactionTerminatedException.class, transaction::commit );
        }
        finally
        {
            executorService.shutdownNow();
        }

        // THEN
        verify( transactionMonitor ).transactionFinished( false, isWriteTx );
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
        verifyExtraInteractionWithTheMonitor( transactionMonitor, isWriteTx );
    }

    @SuppressWarnings( "unchecked" )
    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldUseStartTimeAndTxIdFromWhenStartingTxAsHeader( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
        throws Exception
    {
        // GIVEN a transaction starting at one point in time
        long startingTime = clock.millis();
        doAnswer( invocation ->
        {
            @SuppressWarnings( "unchecked" )
            Collection<StorageCommand> commands = invocation.getArgument( 0 );
            commands.add( mock( StorageCommand.class ) );
            return null;
        } ).when( storageEngine ).createCommands(
            any( Collection.class ),
            any( TransactionState.class ),
            any( StorageReader.class ),
            any( CommandCreationContext.class ),
            any( ResourceLocker.class ),
            anyLong(), any( TxStateVisitor.Decorator.class ) );

        try ( KernelTransactionImplementation transaction = newTransaction( loginContext( isWriteTx ) ) )
        {
            SimpleStatementLocks statementLocks = new SimpleStatementLocks( mock( Locks.Client.class ) );
            transaction.initialize( 5L, BASE_TX_COMMIT_TIMESTAMP, statementLocks, KernelTransaction.Type.implicit,
                SecurityContext.AUTH_DISABLED, 0L, 1L, EMBEDDED_CONNECTION );
            transaction.txState().nodeDoCreate( 1L );
            // WHEN committing it at a later point
            clock.forward( 5, MILLISECONDS );
            // ...and simulating some other transaction being committed
            when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 7L );
            transaction.success();
        }

        // THEN start time and last tx when started should have been taken from when the transaction started
        assertEquals( 5L, getObservedFirstTransaction().getLatestCommittedTxWhenStarted() );
        assertEquals( startingTime, getObservedFirstTransaction().getTimeStarted() );
        assertEquals( startingTime + 5, getObservedFirstTransaction().getTimeCommitted() );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void successfulTxShouldNotifyKernelTransactionsThatItIsClosed( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
        throws Exception
    {
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ) );

        tx.success();
        tx.close();

        verify( txPool ).release( tx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void failedTxShouldNotifyKernelTransactionsThatItIsClosed( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
        throws Exception
    {
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ) );

        tx.failure();
        tx.close();

        verify( txPool ).release( tx );
    }

    private void verifyExtraInteractionWithTheMonitor( TransactionMonitor transactionMonitor, boolean isWriteTx )
    {
        if ( isWriteTx )
        {
            verify( this.transactionMonitor ).upgradeToWriteTransaction();
        }
        verifyNoMoreInteractions( transactionMonitor );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldIncrementReuseCounterOnReuse( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        // GIVEN
        KernelTransactionImplementation transaction = newTransaction( loginContext( isWriteTx ) );
        int reuseCount = transaction.getReuseCount();

        // WHEN
        transaction.close();
        SimpleStatementLocks statementLocks = new SimpleStatementLocks( new NoOpClient() );
        transaction.initialize( 1, BASE_TX_COMMIT_TIMESTAMP, statementLocks, KernelTransaction.Type.implicit,
            loginContext( isWriteTx ).authorize( LoginContext.IdLookup.EMPTY, GraphDatabaseSettings.DEFAULT_DATABASE_NAME ), 0L, 1L, EMBEDDED_CONNECTION );

        // THEN
        assertEquals( reuseCount + 1, transaction.getReuseCount() );
    }

    @Test
    void markForTerminationNotInitializedTransaction()
    {
        KernelTransactionImplementation tx = newNotInitializedTransaction();
        tx.markForTermination( Status.General.UnknownError );

        assertEquals( Status.General.UnknownError, tx.getReasonIfTerminated().get() );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void markForTerminationInitializedTransaction( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ), locksClient );

        tx.markForTermination( Status.General.UnknownError );

        assertEquals( Status.General.UnknownError, tx.getReasonIfTerminated().get() );
        verify( locksClient ).stop();
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void markForTerminationTerminatedTransaction( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ), locksClient );
        transactionInitializer.accept( tx );

        tx.markForTermination( Status.Transaction.Terminated );
        tx.markForTermination( Status.Transaction.Outdated );
        tx.markForTermination( Status.Transaction.LockClientStopped );

        assertEquals( Status.Transaction.Terminated, tx.getReasonIfTerminated().get() );
        verify( locksClient ).stop();
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void terminatedTxMarkedNeitherSuccessNorFailureClosesWithoutThrowing( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
        throws Exception
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ), locksClient );
        transactionInitializer.accept( tx );
        tx.markForTermination( Status.General.UnknownError );

        tx.close();

        verify( locksClient ).stop();
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void terminatedTxMarkedForSuccessThrowsOnClose( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ), locksClient );
        transactionInitializer.accept( tx );
        tx.success();
        tx.markForTermination( Status.General.UnknownError );

        assertThrows( TransactionTerminatedException.class, tx::close );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void terminatedTxMarkedForFailureClosesWithoutThrowing( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
        throws Exception
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ), locksClient );
        transactionInitializer.accept( tx );
        tx.failure();
        tx.markForTermination( Status.General.UnknownError );

        tx.close();

        verify( locksClient ).stop();
        verify( transactionMonitor ).transactionTerminated( isWriteTx );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void terminatedTxMarkedForBothSuccessAndFailureThrowsOnClose( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ), locksClient );
        transactionInitializer.accept( tx );
        tx.success();
        tx.failure();
        tx.markForTermination( Status.General.UnknownError );

        assertThrows( TransactionTerminatedException.class, tx::close );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void txMarkedForBothSuccessAndFailureThrowsOnClose( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
    {
        Locks.Client locksClient = mock( Locks.Client.class );
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ), locksClient );
        tx.success();
        tx.failure();

        assertThrows( TransactionFailureException.class, tx::close );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void initializedTransactionShouldHaveNoTerminationReason( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
    {
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ) );
        assertFalse( tx.getReasonIfTerminated().isPresent() );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldReportCorrectTerminationReason( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer )
    {
        Status status = Status.Transaction.Terminated;
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ) );
        tx.markForTermination( status );
        assertSame( status, tx.getReasonIfTerminated().get() );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void closedTransactionShouldHaveNoTerminationReason( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ) );
        tx.markForTermination( Status.Transaction.Terminated );
        tx.close();
        assertFalse( tx.getReasonIfTerminated().isPresent() );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldCallCloseListenerOnCloseWhenCommitting( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        // given
        AtomicLong closeTxId = new AtomicLong( Long.MIN_VALUE );
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ) );
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

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldCallCloseListenerOnCloseWhenRollingBack( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        // given
        AtomicLong closeTxId = new AtomicLong( Long.MIN_VALUE );
        KernelTransactionImplementation tx = newTransaction( loginContext( isWriteTx ) );
        tx.registerCloseListener( closeTxId::set );

        // when
        tx.failure();
        tx.close();

        // then
        assertEquals( -1L, closeTxId.get() );
    }

    @Test
    void transactionWithCustomTimeout()
    {
        long transactionTimeout = 5L;
        KernelTransactionImplementation transaction = newTransaction( transactionTimeout );
        assertEquals( transactionTimeout, transaction.timeout(), "Transaction should have custom configured timeout." );
    }

    @Test
    void transactionStartTime()
    {
        long startTime = clock.forward( 5, TimeUnit.MINUTES ).millis();
        KernelTransactionImplementation transaction = newTransaction( AUTH_DISABLED );
        assertEquals( startTime, transaction.startTime(), "Transaction start time should be the same as clock time." );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void markForTerminationWithCorrectReuseCount( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        int reuseCount = 10;
        Status.Transaction terminationReason = Status.Transaction.Terminated;

        KernelTransactionImplementation tx = newNotInitializedTransaction();
        initializeAndClose( tx, reuseCount, isWriteTx );

        Locks.Client locksClient = mock( Locks.Client.class );
        SimpleStatementLocks statementLocks = new SimpleStatementLocks( locksClient );
        tx.initialize( 42, 42, statementLocks, KernelTransaction.Type.implicit,
            loginContext( isWriteTx ).authorize( LoginContext.IdLookup.EMPTY, GraphDatabaseSettings.DEFAULT_DATABASE_NAME ), 0L, 0L, EMBEDDED_CONNECTION );

        assertTrue( tx.markForTermination( reuseCount, terminationReason ) );

        assertEquals( terminationReason, tx.getReasonIfTerminated().get() );
        verify( locksClient ).stop();
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void markForTerminationWithIncorrectReuseCount( String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer ) throws Exception
    {
        int reuseCount = 13;
        int nextReuseCount = reuseCount + 2;
        Status.Transaction terminationReason = Status.Transaction.Terminated;

        KernelTransactionImplementation tx = newNotInitializedTransaction();
        initializeAndClose( tx, reuseCount, isWriteTx );

        Locks.Client locksClient = mock( Locks.Client.class );
        SimpleStatementLocks statementLocks = new SimpleStatementLocks( locksClient );
        tx.initialize( 42, 42, statementLocks, KernelTransaction.Type.implicit,
            loginContext( isWriteTx ).authorize( LoginContext.IdLookup.EMPTY, GraphDatabaseSettings.DEFAULT_DATABASE_NAME ), 0L, 0L, EMBEDDED_CONNECTION );

        assertFalse( tx.markForTermination( nextReuseCount, terminationReason ) );

        assertFalse( tx.getReasonIfTerminated().isPresent() );
        verify( locksClient, never() ).stop();
    }

    @Test
    void resetTransactionStatisticsOnRelease() throws TransactionFailureException
    {
        KernelTransactionImplementation transaction = newTransaction( 1000 );
        transaction.getStatistics().addWaitingTime( 1 );
        transaction.getStatistics().addWaitingTime( 1 );
        assertEquals( 2, transaction.getStatistics().getWaitingTimeNanos( 0 ) );
        transaction.close();
        assertEquals( 0, transaction.getStatistics().getWaitingTimeNanos( 0 ) );
    }

    @Test
    void reportTransactionStatistics()
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

    private LoginContext loginContext( boolean isWriteTx )
    {
        return isWriteTx ? AnonymousContext.write() : AnonymousContext.read();
    }

    private void initializeAndClose( KernelTransactionImplementation tx, int times, boolean isWriteTx ) throws Exception
    {
        for ( int i = 0; i < times; i++ )
        {
            SimpleStatementLocks statementLocks = new SimpleStatementLocks( new NoOpClient() );
            tx.initialize( i + 10, i + 10, statementLocks, KernelTransaction.Type.implicit,
                loginContext( isWriteTx ).authorize( LoginContext.IdLookup.EMPTY, GraphDatabaseSettings.DEFAULT_DATABASE_NAME ), 0L, 0L, EMBEDDED_CONNECTION );
            tx.close();
        }
    }

    private TransactionRepresentation getObservedFirstTransaction()
    {
        return commitProcess.transactions.get( 0 );
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
