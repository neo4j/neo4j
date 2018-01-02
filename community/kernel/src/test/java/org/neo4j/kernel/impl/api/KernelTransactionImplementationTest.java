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
package org.neo4j.kernel.impl.api;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.pool.Pool;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpLocks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.test.DoubleLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

public class KernelTransactionImplementationTest
{
    @Test
    public void shouldCommitSuccessfulTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newInitializedTransaction() )
        {
            // WHEN
            transaction.success();
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( true );
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldRollbackUnsuccessfulTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newInitializedTransaction() )
        {
            // WHEN
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldRollbackFailedTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newInitializedTransaction() )
        {
            // WHEN
            transaction.failure();
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldRollbackAndThrowOnFailedAndSuccess() throws Exception
    {
        // GIVEN
        boolean exceptionReceived = false;
        try ( KernelTransaction transaction = newInitializedTransaction() )
        {
            // WHEN
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
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldRollbackOnClosingTerminatedTransaction() throws Exception
    {
        // GIVEN
        KernelTransaction transaction = newInitializedTransaction();
        transaction.success();
        transaction.markForTermination( Status.General.UnknownFailure );

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
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldRollbackOnClosingSuccessfulButTerminatedTransaction() throws Exception
    {
        try ( KernelTransaction transaction = newInitializedTransaction() )
        {
            // WHEN
            transaction.markForTermination( Status.General.UnknownFailure );
            assertEquals( Status.General.UnknownFailure, transaction.getReasonIfTerminated() );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldRollbackOnClosingTerminatedButSuccessfulTransaction() throws Exception
    {
        // GIVEN
        KernelTransaction transaction = newInitializedTransaction();
        transaction.markForTermination( Status.General.UnknownFailure );
        transaction.success();

        assertEquals( Status.General.UnknownFailure, transaction.getReasonIfTerminated() );

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
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldNotDowngradeFailureState() throws Exception
    {
        try ( KernelTransaction transaction = newInitializedTransaction() )
        {
            // WHEN
            transaction.markForTermination( Status.General.UnknownFailure );
            transaction.failure();
            assertEquals( Status.General.UnknownFailure, transaction.getReasonIfTerminated() );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldIgnoreTerminateAfterCommit() throws Exception
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        KernelTransaction transaction = newInitializedTransaction( true, locks );
        transaction.success();
        transaction.close();
        transaction.markForTermination( Status.General.UnknownFailure );

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( true );
        verify( transactionMonitor, never() ).transactionTerminated();
        verify( client, never() ).stop();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldIgnoreTerminateAfterRollback() throws Exception
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        KernelTransaction transaction = newInitializedTransaction( true, locks );
        transaction.close();
        transaction.markForTermination( Status.General.UnknownFailure );

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, never() ).transactionTerminated();
        verify( client, never() ).stop();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test(expected = TransactionTerminatedException.class)
    public void shouldThrowOnTerminationInCommit() throws Exception
    {
        KernelTransaction transaction = newInitializedTransaction();
        transaction.success();
        transaction.markForTermination( Status.General.UnknownFailure );

        transaction.close();
    }

    @Test
    public void shouldIgnoreTerminationDuringRollback() throws Exception
    {
        KernelTransaction transaction = newInitializedTransaction();
        transaction.markForTermination( Status.General.UnknownFailure );
        transaction.close();

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldAllowTerminatingFromADifferentThread() throws Exception
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch( 1 );
        final KernelTransaction transaction = newInitializedTransaction();

        Future<?> terminationFuture = Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                latch.awaitStart();
                transaction.markForTermination( Status.General.UnknownFailure );
                latch.finish();
            }
        } );

        // WHEN
        transaction.success();
        latch.startAndAwaitFinish();

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
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldUseStartTimeAndTxIdFromWhenStartingTxAsHeader() throws Exception
    {
        // GIVEN a transaction starting at one point in time
        long startingTime = clock.currentTimeMillis();
        when( recordState.hasChanges() ).thenReturn( true );
        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                Collection<Command> commands = (Collection<Command>) invocationOnMock.getArguments()[0];
                commands.add( new Command.NodeCommand() );
                return null;
            }
        } ).when( recordState ).extractCommands( anyListOf( Command.class ) );
        try ( KernelTransactionImplementation transaction = newInitializedTransaction() )
        {
            transaction.initialize( 5L, BASE_TX_COMMIT_TIMESTAMP );

            // WHEN committing it at a later point
            clock.forward( 5, MILLISECONDS );
            // ...and simulating some other transaction being committed
            when( metaDataStore.getLastCommittedTransactionId() ).thenReturn( 7L );
            transaction.success();
        }

        // THEN start time and last tx when started should have been taken from when the transaction started
        assertEquals( 5L, commitProcess.transaction.getLatestCommittedTxWhenStarted() );
        assertEquals( startingTime, commitProcess.transaction.getTimeStarted() );
        assertEquals( startingTime+5, commitProcess.transaction.getTimeCommitted() );
    }

    @Test
    public void shouldStillReturnTransactionInstanceWithTerminationMarkToPool() throws Exception
    {
        // GIVEN
        KernelTransactionImplementation transaction = newInitializedTransaction();

        // WHEN
        transaction.markForTermination( Status.General.UnknownFailure );
        transaction.close();

        // THEN
        verify( pool ).release( transaction );
    }

    @Test
    public void shouldBeAbleToReuseTerminatedTransaction() throws Exception
    {
        // GIVEN
        KernelTransactionImplementation transaction = newInitializedTransaction();
        transaction.close();
        transaction.markForTermination( Status.General.UnknownFailure );

        // WHEN
        transaction.initialize( 10L, BASE_TX_COMMIT_TIMESTAMP );
        transaction.txState().nodeDoCreate( 11L );
        transaction.success();
        transaction.close();

        // THEN
        verify( commitProcess ).commit( any( TransactionRepresentation.class ), any( LockGroup.class ),
                any( CommitEvent.class ), any( TransactionApplicationMode.class ) );
    }

    @Test
    public void shouldAcquireNewLocksClientEveryTimeTransactionIsReused() throws Exception
    {
        // GIVEN
        KernelTransactionImplementation transaction = newInitializedTransaction();
        transaction.close();
        verify( locks ).newClient();
        reset( locks );

        // WHEN
        transaction.initialize( 10L, BASE_TX_COMMIT_TIMESTAMP );
        transaction.close();

        // THEN
        verify( locks ).newClient();
    }

    @Test
    public void shouldIncrementReuseCounterOnReuse() throws Exception
    {
        // GIVEN
        KernelTransactionImplementation transaction = newInitializedTransaction();
        int reuseCount = transaction.getReuseCount();

        // WHEN
        transaction.close();
        transaction.initialize( 1, BASE_TX_COMMIT_TIMESTAMP );

        // THEN
        assertEquals( reuseCount + 1, transaction.getReuseCount() );
    }

    @Test
    public void markForTerminationNotInitializedTransaction()
    {
        KernelTransactionImplementation transaction = newTransaction( true, locks );

        transaction.markForTermination( Status.General.UnknownFailure );

        assertEquals( Status.General.UnknownFailure, transaction.getReasonIfTerminated() );
    }

    @Test
    public void markForTerminationInitializedTransaction()
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        KernelTransactionImplementation transaction = newInitializedTransaction( true, locks );

        transaction.markForTermination( Status.General.UnknownFailure );

        assertEquals( Status.General.UnknownFailure, transaction.getReasonIfTerminated() );
        verify( client ).stop();
    }

    @Test
    public void markForTerminationTerminatedTransaction()
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        KernelTransactionImplementation transaction = newInitializedTransaction( true, locks );

        transaction.markForTermination( Status.Transaction.Terminated );
        transaction.markForTermination( Status.Transaction.Outdated );
        transaction.markForTermination( Status.Transaction.LockClientStopped );

        assertEquals( Status.Transaction.Terminated, transaction.getReasonIfTerminated() );
        verify( client ).stop();
        verify( transactionMonitor ).transactionTerminated();
    }

    @Test
    public void terminatedTxMarkedNeitherSuccessNorFailureClosesWithoutThrowing() throws TransactionFailureException
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        KernelTransactionImplementation tx = newInitializedTransaction( true, locks );
        tx.markForTermination( Status.General.UnknownFailure );

        tx.close();

        verify( client ).stop();
        verify( transactionMonitor ).transactionTerminated();
    }

    @Test
    public void terminatedTxMarkedForSuccessThrowsOnClose()
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        KernelTransactionImplementation tx = newInitializedTransaction( true, locks );
        tx.success();
        tx.markForTermination( Status.General.UnknownFailure );

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
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        KernelTransactionImplementation tx = newInitializedTransaction( true, locks );
        tx.failure();
        tx.markForTermination( Status.General.UnknownFailure );

        tx.close();

        verify( client ).stop();
        verify( transactionMonitor ).transactionTerminated();
    }

    @Test
    public void terminatedTxMarkedForBothSuccessAndFailureThrowsOnClose()
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        KernelTransactionImplementation tx = newInitializedTransaction( true, locks );
        tx.success();
        tx.failure();
        tx.markForTermination( Status.General.UnknownFailure );

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
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        KernelTransactionImplementation tx = newInitializedTransaction( true, locks );
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
        KernelTransactionImplementation tx = newInitializedTransaction();
        assertNull( tx.getReasonIfTerminated() );
    }

    @Test
    public void shouldReportCorrectTerminationReason() throws Exception
    {
        Status status = Status.Transaction.Terminated;
        KernelTransactionImplementation tx = newInitializedTransaction();
        tx.markForTermination( status );
        assertSame( status, tx.getReasonIfTerminated() );
    }

    @Test
    public void closedTransactionShouldHaveNoTerminationReason() throws Exception
    {
        KernelTransactionImplementation tx = newInitializedTransaction();
        tx.markForTermination( Status.Transaction.Terminated );
        tx.close();
        assertNull( tx.getReasonIfTerminated() );
    }

    private final NeoStores neoStores = mock( NeoStores.class );
    private final MetaDataStore metaDataStore = mock( MetaDataStore.class );
    private final TransactionHooks hooks = new TransactionHooks();
    private final TransactionRecordState recordState = mock( TransactionRecordState.class );
    private final LegacyIndexTransactionState legacyIndexState = mock( LegacyIndexTransactionState.class );
    private final TransactionMonitor transactionMonitor = mock( TransactionMonitor.class );
    private final CapturingCommitProcess commitProcess = spy( new CapturingCommitProcess() );
    private final TransactionHeaderInformation headerInformation = mock( TransactionHeaderInformation.class );
    private final TransactionHeaderInformationFactory headerInformationFactory = mock( TransactionHeaderInformationFactory.class );
    private final FakeClock clock = new FakeClock();
    private final Pool<KernelTransactionImplementation> pool = mock( Pool.class );
    private final Locks locks = spy( new NoOpLocks() );
    private final StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );

    @Before
    public void before()
    {
        when( headerInformation.getAdditionalHeader() ).thenReturn( new byte[0] );
        when( headerInformationFactory.create() ).thenReturn( headerInformation );
        when( neoStores.getMetaDataStore() ).thenReturn( metaDataStore );
    }

    private KernelTransactionImplementation newInitializedTransaction()
    {
        return newInitializedTransaction( false, locks );
    }

    private KernelTransactionImplementation newInitializedTransaction( boolean txTerminationAware, Locks locks )
    {
        KernelTransactionImplementation transaction = newTransaction( txTerminationAware, locks );
        transaction.initialize( 0, BASE_TX_COMMIT_TIMESTAMP );
        return transaction;
    }

    private KernelTransactionImplementation newTransaction( boolean txTerminationAware, Locks locks )
    {
        when( storeReadLayer.acquireStatement() ).thenReturn( mock( StoreStatement.class ) );

        return new KernelTransactionImplementation( null, null, null, null, null, recordState, null, neoStores,
                hooks, null, headerInformationFactory, commitProcess,
                transactionMonitor, storeReadLayer, legacyIndexState, pool, new StandardConstraintSemantics(), clock,
                TransactionTracer.NULL, new ProcedureCache(), new SimpleStatementLocksFactory( locks ),
                mock( NeoStoreTransactionContext.class ), txTerminationAware );
    }

    public class CapturingCommitProcess implements TransactionCommitProcess
    {
        private long txId = 1;
        private TransactionRepresentation transaction;

        @Override
        public long commit( TransactionRepresentation representation, LockGroup locks, CommitEvent commitEvent,
                            TransactionApplicationMode mode ) throws TransactionFailureException
        {
            assert transaction == null : "Designed to only allow one transaction";
            transaction = representation;
            return txId++;
        }
    }
}
