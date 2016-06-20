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
package org.neo4j.kernel.impl.api;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;

import org.neo4j.collection.pool.Pool;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpLocks;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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
        try ( KernelTransaction transaction = newTransaction() )
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
        try ( KernelTransaction transaction = newTransaction() )
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
        try ( KernelTransaction transaction = newTransaction() )
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
        try ( KernelTransaction transaction = newTransaction() )
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
        boolean exceptionReceived = false;
        try ( KernelTransaction transaction = newTransaction() )
        {
            // WHEN
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
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldRollbackOnClosingSuccessfulButTerminatedTransaction() throws Exception
    {
        try ( KernelTransaction transaction = newTransaction() )
        {
            // WHEN
            transaction.markForTermination();
            assertTrue( transaction.shouldBeTerminated() );
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
        boolean exceptionReceived = false;
        try ( KernelTransaction transaction = newTransaction() )
        {
            // WHEN
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
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldNotDowngradeFailureState() throws Exception
    {
        try ( KernelTransaction transaction = newTransaction() )
        {
            // WHEN
            transaction.markForTermination();
            transaction.failure();
            assertTrue( transaction.shouldBeTerminated() );
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldIgnoreTerminateAfterCommit() throws Exception
    {
        KernelTransaction transaction = newTransaction();
        transaction.success();
        transaction.close();
        transaction.markForTermination();

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( true );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldIgnoreTerminateAfterRollback() throws Exception
    {
        KernelTransaction transaction = newTransaction();
        transaction.close();
        transaction.markForTermination();

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test(expected = TransactionFailureException.class)
    public void shouldThrowOnTerminationInCommit() throws Exception
    {
        KernelTransaction transaction = newTransaction();
        transaction.success();
        transaction.markForTermination();

        transaction.close();
    }

    @Test
    public void shouldIgnoreTerminationDuringRollback() throws Exception
    {
        KernelTransaction transaction = newTransaction();
        transaction.markForTermination();
        transaction.close();

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verify( transactionMonitor, times( 1 ) ).transactionTerminated();
        verifyNoMoreInteractions( transactionMonitor );
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
        final KernelTransaction transaction = newTransaction();
        Thread thread = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
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
        try ( KernelTransactionImplementation transaction = newTransaction() )
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
        KernelTransactionImplementation transaction = newTransaction();

        // WHEN
        transaction.markForTermination();
        transaction.close();

        // THEN
        verify( pool ).release( transaction );
    }

    @Test
    public void shouldBeAbleToReuseTerminatedTransaction() throws Exception
    {
        // GIVEN
        KernelTransactionImplementation transaction = newTransaction();
        transaction.close();
        transaction.markForTermination();

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
        KernelTransactionImplementation transaction = newTransaction();
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
        KernelTransactionImplementation transaction = newTransaction();
        int reuseCount = transaction.getReuseCount();

        // WHEN
        transaction.close();
        transaction.initialize( 1, BASE_TX_COMMIT_TIMESTAMP );

        // THEN
        assertEquals( reuseCount + 1, transaction.getReuseCount() );
    }

    private final NeoStores neoStores = mock( NeoStores.class );
    private final MetaDataStore metaDataStore = mock( MetaDataStore.class );
    private final TransactionHooks hooks = new TransactionHooks();
    private final TransactionRecordState recordState = mock( TransactionRecordState.class );
    private final LegacyIndexTransactionState legacyIndexState = mock( LegacyIndexTransactionState.class );
    private final TransactionMonitor transactionMonitor = mock( TransactionMonitor.class );
    private final CapturingCommitProcess commitProcess = spy( new CapturingCommitProcess() );
    private final TransactionHeaderInformation headerInformation = mock( TransactionHeaderInformation.class );
    private final TransactionHeaderInformationFactory headerInformationFactory =
            mock( TransactionHeaderInformationFactory.class );
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

    private KernelTransactionImplementation newTransaction()
    {
        when(storeReadLayer.acquireStatement()).thenReturn( mock(StoreStatement.class) );

        KernelTransactionImplementation transaction = new KernelTransactionImplementation(
                null, null, null, null, null, recordState, null, neoStores, locks,
                hooks, null, headerInformationFactory, commitProcess, transactionMonitor, storeReadLayer, legacyIndexState,
                pool, new StandardConstraintSemantics(), clock, TransactionTracer.NULL, new ProcedureCache(), mock( NeoStoreTransactionContext
                .class ) );
        transaction.initialize( 0, BASE_TX_COMMIT_TIMESTAMP );
        return transaction;
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
