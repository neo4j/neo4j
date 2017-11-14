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
package org.neo4j.bolt.v1.runtime;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.util.ValueUtils;
import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.time.FakeClock;
import org.neo4j.values.virtual.MapValue;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.security.auth.AuthenticationResult.AUTH_DISABLED;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class TransactionStateMachineTest
{
    private TransactionStateMachineSPI stateMachineSPI;
    private TransactionStateMachine.MutableTransactionState mutableState;
    private TransactionStateMachine stateMachine;

    @Before
    public void createMocks()
    {
        stateMachineSPI = mock( TransactionStateMachineSPI.class );
        mutableState = mock(TransactionStateMachine.MutableTransactionState.class);
        stateMachine = new TransactionStateMachine( stateMachineSPI, AUTH_DISABLED, new FakeClock() );
    }

    @Test
    public void shouldTransitionToExplicitTransactionOnBegin() throws Exception
    {
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, stateMachineSPI, "begin", EMPTY_MAP ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, stateMachineSPI, "BEGIN", EMPTY_MAP ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, stateMachineSPI, "   begin   ", EMPTY_MAP ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, stateMachineSPI, "   BeGiN ;   ", EMPTY_MAP ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
    }

    @Test
    public void shouldTransitionToAutoCommitOnCommit() throws Exception
    {
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "commit", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "COMMIT", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "   commit   ", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "   CoMmIt ;   ", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
    }

    @Test
    public void shouldTransitionToAutoCommitOnRollback() throws Exception
    {
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "rollback", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "ROLLBACK", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "   rollback   ", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "   RoLlBaCk ;   ", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
    }

    @Test
    public void shouldThrowOnBeginInExplicitTransaction() throws Exception
    {
        try
        {
            TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                    mutableState, stateMachineSPI, "begin", EMPTY_MAP );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("Nested transactions are not supported.", ex.getMessage() );
        }
        try
        {
            TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                    mutableState, stateMachineSPI, " BEGIN ", EMPTY_MAP );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("Nested transactions are not supported.", ex.getMessage() );
        }
    }

    @Test
    public void shouldThrowOnRollbackInAutoCommit() throws Exception
    {
        try
        {
            TransactionStateMachine.State.AUTO_COMMIT.run(
                    mutableState, stateMachineSPI, "rollback", EMPTY_MAP );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to rollback.", ex.getMessage() );
        }
        try
        {
            TransactionStateMachine.State.AUTO_COMMIT.run(
                    mutableState, stateMachineSPI, " ROLLBACK ", EMPTY_MAP );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to rollback.", ex.getMessage() );
        }
    }

    @Test
    public void shouldThrowOnCommitInAutoCommit() throws Exception
    {
        try
        {
            TransactionStateMachine.State.AUTO_COMMIT.run(
                    mutableState, stateMachineSPI, "commit", EMPTY_MAP );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to commit.", ex.getMessage() );
        }
        try
        {
            TransactionStateMachine.State.AUTO_COMMIT.run(
                    mutableState, stateMachineSPI, " COMMIT ", EMPTY_MAP );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to commit.", ex.getMessage() );
        }
    }

    @Test
    public void shouldNotWaitWhenNoBookmarkSupplied() throws Exception
    {
        stateMachine.run( "BEGIN", EMPTY_MAP );
        verify( stateMachineSPI, never() ).awaitUpToDate( anyLong() );
    }

    @Test
    public void shouldAwaitSingleBookmark() throws Exception
    {
        stateMachine.run( "BEGIN", map( "bookmark", "neo4j:bookmark:v1:tx15" ) );
        verify( stateMachineSPI ).awaitUpToDate( 15 );
    }

    @Test
    public void shouldAwaitMultipleBookmarks() throws Exception
    {
        MapValue params = map( "bookmarks", asList(
                "neo4j:bookmark:v1:tx15", "neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx92", "neo4j:bookmark:v1:tx9" )
        );
        stateMachine.run( "BEGIN", params );
        verify( stateMachineSPI ).awaitUpToDate( 92 );
    }

    @Test
    public void shouldAwaitMultipleBookmarksWhenBothSingleAndMultipleSupplied() throws Exception
    {
        MapValue params = map(
                "bookmark", "neo4j:bookmark:v1:tx42",
                "bookmarks", asList( "neo4j:bookmark:v1:tx47", "neo4j:bookmark:v1:tx67", "neo4j:bookmark:v1:tx45" )
        );
        stateMachine.run( "BEGIN", params );
        verify( stateMachineSPI ).awaitUpToDate( 67 );
    }

    @Test
    public void shouldStartWithAutoCommitState() throws Exception
    {
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNull( stateMachine.ctx.currentTransaction );
        assertNull( stateMachine.ctx.currentResultHandle );
        assertNull( stateMachine.ctx.currentResult );
    }

    @Test
    public void shouldDoNothingInAutoCommitTransactionUponInitialisationWhenValidated() throws Exception
    {
        KernelTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        // We're in auto-commit state
        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNull( stateMachine.ctx.currentTransaction );

        // call validate transaction
        stateMachine.validateTransaction();

        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNull( stateMachine.ctx.currentTransaction );

        verify( transaction, never() ).getReasonIfTerminated();
        verify( transaction, never() ).failure();
        verify( transaction, never() ).close();
    }

    @Test
    public void shouldResetInAutoCommitTransactionWhileStatementIsRunningWhenValidated() throws Exception
    {
        KernelTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        // We're in auto-commit state
        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNull( stateMachine.ctx.currentTransaction );

        stateMachine.run( "RETURN 1", null );

        // We're in auto-commit state
        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNotNull( stateMachine.ctx.currentTransaction );

        // call validate transaction
        stateMachine.validateTransaction();

        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNull( stateMachine.ctx.currentTransaction );
        assertNull( stateMachine.ctx.currentResult );
        assertNull( stateMachine.ctx.currentResultHandle );

        verify( transaction, times( 1 ) ).getReasonIfTerminated();
        verify( transaction, times( 1 ) ).failure();
        verify( transaction, times( 1 ) ).close();
    }

    @Test
    public void shouldResetInExplicitTransactionUponTxBeginWhenValidated() throws Exception
    {
        KernelTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        // start an explicit transaction
        stateMachine.run( "BEGIN", map() );
        assertThat( stateMachine.state, is( TransactionStateMachine.State.EXPLICIT_TRANSACTION ) );
        assertNotNull( stateMachine.ctx.currentTransaction );

        // verify transaction, which is timed out
        stateMachine.validateTransaction();

        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNull( stateMachine.ctx.currentTransaction );
        assertNull( stateMachine.ctx.currentResult );
        assertNull( stateMachine.ctx.currentResultHandle );

        verify( transaction, times( 1 ) ).getReasonIfTerminated();
        verify( transaction, times( 1 ) ).failure();
        verify( transaction, times( 1 ) ).close();
    }

    @Test
    public void shouldResetInExplicitTransactionWhileStatementIsRunningWhenValidated() throws Exception
    {
        KernelTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        // start an explicit transaction
        stateMachine.run( "BEGIN", map() );
        assertThat( stateMachine.state, is( TransactionStateMachine.State.EXPLICIT_TRANSACTION ) );
        assertNotNull( stateMachine.ctx.currentTransaction );

        stateMachine.run( "RETURN 1", null );

        // verify transaction, which is timed out
        stateMachine.validateTransaction();

        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNull( stateMachine.ctx.currentTransaction );
        assertNull( stateMachine.ctx.currentResult );
        assertNull( stateMachine.ctx.currentResultHandle );

        verify( transaction, times( 1 ) ).getReasonIfTerminated();
        verify( transaction, times( 1 ) ).failure();
        verify( transaction, times( 1 ) ).close();
    }

    @Test
    public void shouldUnbindTxAfterRun() throws Exception
    {
        KernelTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.run( "SOME STATEMENT", null );

        verify( stateMachineSPI, times( 1 ) ).unbindTransactionFromCurrentThread();
    }

    @Test
    public void shouldUnbindTxAfterStreamResult() throws Exception
    {
        KernelTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.run( "SOME STATEMENT", null );
        stateMachine.streamResult( boltResult ->
        {

        } );

        verify( stateMachineSPI, times( 2 ) ).unbindTransactionFromCurrentThread();
    }

    @Test
    public void shouldThrowDuringRunIfPendingTerminationNoticeExists() throws Exception
    {
        KernelTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.ctx.pendingTerminationNotice = Status.Transaction.TransactionTimedOut;

        try
        {
            stateMachine.run( "SOME STATEMENT", null );
            fail( "exception expected" );
        }
        catch ( TransactionTerminatedException t )
        {
            assertThat( t.status(), is( Status.Transaction.TransactionTimedOut ) );
        }
        catch ( Throwable t )
        {
            fail( "expected TransactionTerminated but got " + t.getMessage() );
        }
    }

    @Test
    public void shouldThrowDuringStreamResultIfPendingTerminationNoticeExists() throws Exception
    {
        KernelTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.run( "SOME STATEMENT", null );
        stateMachine.ctx.pendingTerminationNotice = Status.Transaction.TransactionTimedOut;

        try
        {
            stateMachine.streamResult( boltResult ->
            {

            });

            fail( "exception expected" );
        }
        catch ( TransactionTerminatedException t )
        {
            assertThat( t.status(), is( Status.Transaction.TransactionTimedOut ) );
        }
        catch ( Throwable t )
        {
            fail( "expected TransactionTerminated but got " + t.getMessage() );
        }
    }

    private static KernelTransaction newTransaction()
    {
        KernelTransaction transaction = mock( KernelTransaction.class );

        when( transaction.isOpen() ).thenReturn( true );

        return transaction;
    }

    private static KernelTransaction newTimedOutTransaction()
    {
        KernelTransaction transaction = newTransaction();

        when( transaction.getReasonIfTerminated() ).thenReturn( Optional.of( Status.Transaction.TransactionTimedOut ) );

        return transaction;
    }

    private static TransactionStateMachine newTransactionStateMachine( TransactionStateMachineSPI stateMachineSPI )
    {
        return new TransactionStateMachine( stateMachineSPI, AUTH_DISABLED, new FakeClock() );
    }

    private MapValue map( Object... keyValues )
    {
        return ValueUtils.asMapValue( MapUtil.map( keyValues ) );
    }

    private static TransactionStateMachineSPI newFailingTransactionStateMachineSPI( Status failureStatus ) throws KernelException
    {
        TransactionStateMachine.BoltResultHandle resultHandle = newResultHandle();
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );

        when( stateMachineSPI.beginTransaction( any() ) ).thenReturn( mock( KernelTransaction.class ) );
        when( stateMachineSPI.executeQuery( any(), any(), anyString(), any(), any() ) ).thenReturn( resultHandle );
        when( stateMachineSPI.executeQuery( any(), any(), eq( "FAIL" ), any(), any() ) ).thenThrow( new TransactionTerminatedException( failureStatus ) );

        return stateMachineSPI;
    }

    private static TransactionStateMachineSPI newTransactionStateMachineSPI( KernelTransaction transaction ) throws KernelException
    {
        TransactionStateMachine.BoltResultHandle resultHandle = newResultHandle();
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );

        when( stateMachineSPI.beginTransaction( any() ) ).thenReturn( transaction );
        when( stateMachineSPI.executeQuery( any(), any(), anyString(), any(), any() ) ).thenReturn( resultHandle );

        return stateMachineSPI;
    }

    private static TransactionStateMachine.BoltResultHandle newResultHandle() throws KernelException
    {
        TransactionStateMachine.BoltResultHandle resultHandle = mock( TransactionStateMachine.BoltResultHandle.class );

        when( resultHandle.start() ).thenReturn( BoltResult.EMPTY );

        return resultHandle;
    }
}
