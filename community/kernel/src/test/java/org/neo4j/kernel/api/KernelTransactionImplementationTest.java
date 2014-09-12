/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.state.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.test.DoubleLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
        when( recordState.isReadOnly() ).thenReturn( false );
        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                List<Command> commands = (List<Command>) invocationOnMock.getArguments()[0];
                commands.add( new Command.NodeCommand() );
                return null;
            }
        } ).when( recordState ).extractCommands( anyListOf( Command.class ) );
        try ( KernelTransactionImplementation transaction = newTransaction() )
        {
            transaction.initialize( headerInformation, 5L );

            // WHEN committing it at a later point
            clock.forward( 5, MILLISECONDS );
            // ...and simulating some other transaction being committed
            when( neoStore.getLastCommittedTransactionId() ).thenReturn( 7L );
            transaction.success();
        }

        // THEN start time and last tx when started should have been taken from when the transaction started
        assertEquals( 5L, commitProcess.transaction.getLatestCommittedTxWhenStarted() );
        assertEquals( startingTime, commitProcess.transaction.getTimeStarted() );
        assertEquals( startingTime+5, commitProcess.transaction.getTimeCommitted() );
    }

    private final NeoStore neoStore = mock( NeoStore.class );
    private final TransactionHooks hooks = new TransactionHooks();
    private final TransactionRecordState recordState = mock( TransactionRecordState.class );
    private final LegacyIndexTransactionState legacyIndexState = mock( LegacyIndexTransactionState.class );
    private final TransactionMonitor transactionMonitor = mock( TransactionMonitor.class );
    private final CapturingCommitProcess commitProcess = new CapturingCommitProcess();
    private final TransactionHeaderInformation headerInformation = mock( TransactionHeaderInformation.class );
    private final FakeClock clock = new FakeClock();

    @Before
    public void before()
    {
        when( recordState.isReadOnly() ).thenReturn( true );
        when( legacyIndexState.isReadOnly() ).thenReturn( true );
        when( headerInformation.getAdditionalHeader() ).thenReturn( new byte[0] );
    }

    private KernelTransactionImplementation newTransaction()
    {
        return new KernelTransactionImplementation( null, false, null, null, null, null, recordState,
                null, neoStore, new NoOpClient(), hooks, null, headerInformation, commitProcess, transactionMonitor,
                null, null, legacyIndexState, clock );
    }

    public class CapturingCommitProcess implements TransactionCommitProcess
    {
        private long txId = 1;
        private TransactionRepresentation transaction;

        @Override
        public long commit( TransactionRepresentation representation, LockGroup locks ) throws TransactionFailureException
        {
            assert transaction == null : "Designed to only allow one transaction";
            transaction = representation;
            return txId++;
        }
    }
}
