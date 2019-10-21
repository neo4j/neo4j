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
package org.neo4j.bolt.runtime.statemachine.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.dbapi.BoltQueryExecutor;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.impl.TransactionStateMachine.MutableTransactionState;
import org.neo4j.bolt.runtime.statemachine.impl.TransactionStateMachine.StatementOutcome;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.time.FakeClock;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.security.auth.AuthenticationResult.AUTH_DISABLED;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

class TransactionStateMachineTest
{
    private static final String PERIODIC_COMMIT_QUERY =
            "USING PERIODIC COMMIT 1 " +
            "LOAD CSV FROM ''https://neo4j.com/test.csv'' AS line " +
            "CREATE (:Node {id: line[0], name: line[1]})";

    private TransactionStateMachineSPI stateMachineSPI;
    private MutableTransactionState mutableState;
    private TransactionStateMachine stateMachine;
    private static final EmptyResultConsumer EMPTY = new EmptyResultConsumer();
    private static final EmptyResultConsumer ERROR = new EmptyResultConsumer()
    {
        @Override
        public void consume( BoltResult boltResult )
        {
            throw new RuntimeException( "some error" );
        }
    };

    @BeforeEach
    void createMocks()
    {
        FakeClock clock = new FakeClock();
        stateMachineSPI = mock( TransactionStateMachineSPI.class );
        mutableState = new MutableTransactionState( AUTH_DISABLED, clock );
        stateMachine = new TransactionStateMachine( ABSENT_DB_NAME, stateMachineSPI, AUTH_DISABLED, clock );
    }

    @Test
    void shouldTransitionToExplicitTransactionOnBegin() throws Exception
    {
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION,
                TransactionStateMachine.State.AUTO_COMMIT.beginTransaction( mutableState, stateMachineSPI, null, null, AccessMode.WRITE, null ) );
    }

    @Test
    void shouldTransitionToAutoCommitOnCommit() throws Exception
    {
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT,
                TransactionStateMachine.State.EXPLICIT_TRANSACTION.commitTransaction( mutableState, stateMachineSPI ) );
    }

    @Test
    void shouldTransitionToAutoCommitOnRollback() throws Exception
    {
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT,
                TransactionStateMachine.State.EXPLICIT_TRANSACTION.rollbackTransaction( mutableState, stateMachineSPI ) );
    }

    @Test
    void shouldThrowOnBeginInExplicitTransaction() throws Exception
    {
        QueryExecutionKernelException e = assertThrows( QueryExecutionKernelException.class, () ->
                TransactionStateMachine.State.EXPLICIT_TRANSACTION.beginTransaction( mutableState, stateMachineSPI, null, null, AccessMode.WRITE, null ) );

        assertEquals( "Nested transactions are not supported.", e.getMessage() );
    }

    @Test
    void shouldAllowRollbackInAutoCommit() throws Exception
    {
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT,
                TransactionStateMachine.State.AUTO_COMMIT.rollbackTransaction( mutableState, stateMachineSPI ) );
    }

    @Test
    void shouldThrowOnCommitInAutoCommit() throws Exception
    {
        QueryExecutionKernelException e = assertThrows( QueryExecutionKernelException.class, () ->
                TransactionStateMachine.State.AUTO_COMMIT.commitTransaction( mutableState, stateMachineSPI ) );

        assertEquals( "No current transaction to commit.", e.getMessage() );
    }

    @Test
    void shouldStartWithAutoCommitState()
    {
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNull( stateMachine.ctx.currentTransaction );
        assertThat( stateMachine.ctx.statementOutcomes.entrySet(), hasSize( 0 ) );
    }

    @Test
    void shouldDoNothingInAutoCommitTransactionUponInitialisationWhenValidated() throws Exception
    {
        BoltTransaction transaction = newTimedOutTransaction();
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
        verify( transaction, never() ).rollback();
    }

    @Test
    void shouldTryToTerminateAllActiveStatements() throws Exception
    {
        BoltTransaction transaction = newTimedOutTransaction();

        BoltResultHandle resultHandle = newResultHandle();
        doThrow( new RuntimeException( "You shall not pass" ) ).doThrow( new RuntimeException( "Not pass twice" ) ).when( resultHandle ).terminate();
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );

        when( stateMachineSPI.beginTransaction( any(), any(), any(), any(), any() ) ).thenReturn( transaction );
        when( stateMachineSPI.executeQuery( any() , anyString(), any() ) ).thenReturn( resultHandle );
        when( stateMachineSPI.supportsNestedStatementsInTransaction() ).thenReturn( true ); // V4

        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        // We're in explicit-commit state
        beginTx( stateMachine, List.of() );

        assertThat( stateMachine.state, is( TransactionStateMachine.State.EXPLICIT_TRANSACTION ) );
        assertNotNull( stateMachine.ctx.currentTransaction );

        // We run two statements
        stateMachine.run( "RETURN 1", null );
        stateMachine.run( "RETURN 2", null );

        assertThat( stateMachine.state, is( TransactionStateMachine.State.EXPLICIT_TRANSACTION ) );
        assertNotNull( stateMachine.ctx.currentTransaction );
        assertThat( stateMachine.ctx.statementCounter, equalTo( 2 ) );

        RuntimeException error = assertThrows( RuntimeException.class, () -> stateMachine.reset() );

        assertThat( error.getCause().getMessage(), equalTo( "You shall not pass" ) );
        assertThat( error.getSuppressed().length, equalTo( 1 ) );
        assertThat( error.getSuppressed()[0].getMessage(), equalTo( "Not pass twice" ) );
    }

    @Test
    void shouldResetInAutoCommitTransactionWhileStatementIsRunningWhenValidated() throws Exception
    {
        BoltTransaction transaction = newTimedOutTransaction();
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
        assertThat( stateMachine.ctx.statementOutcomes.entrySet(), hasSize( 0 ) );

        verify( transaction ).getReasonIfTerminated();
        verify( transaction ).rollback();
    }

    @Test
    void shouldResetInExplicitTransactionUponTxBeginWhenValidated() throws Exception
    {
        BoltTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        // start an explicit transaction
        beginTx( stateMachine );
        assertThat( stateMachine.state, is( TransactionStateMachine.State.EXPLICIT_TRANSACTION ) );
        assertNotNull( stateMachine.ctx.currentTransaction );

        // verify transaction, which is timed out
        stateMachine.validateTransaction();

        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNull( stateMachine.ctx.currentTransaction );
        assertThat( stateMachine.ctx.statementOutcomes.entrySet(), hasSize( 0 ) );

        verify( transaction ).getReasonIfTerminated();
        verify( transaction ).rollback();
    }

    @Test
    void shouldResetInExplicitTransactionWhileStatementIsRunningWhenValidated() throws Exception
    {
        BoltTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        // start an explicit transaction
        beginTx( stateMachine );
        assertThat( stateMachine.state, is( TransactionStateMachine.State.EXPLICIT_TRANSACTION ) );
        assertNotNull( stateMachine.ctx.currentTransaction );

        stateMachine.run( "RETURN 1", null );

        // verify transaction, which is timed out
        stateMachine.validateTransaction();

        assertThat( stateMachine.state, is( TransactionStateMachine.State.AUTO_COMMIT ) );
        assertNull( stateMachine.ctx.currentTransaction );
        assertThat( stateMachine.ctx.statementOutcomes.entrySet(), hasSize( 0 ) );

        verify( transaction ).getReasonIfTerminated();
        verify( transaction ).rollback();
    }

    @Test
    void shouldUnbindTxAfterRun() throws Exception
    {
        BoltTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.run( "SOME STATEMENT", null );
    }

    @Test
    void shouldUnbindTxAfterStreamResult() throws Throwable
    {
        BoltTransaction transaction = newTimedOutTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.run( "SOME STATEMENT", null );
        stateMachine.streamResult( StatementMetadata.ABSENT_QUERY_ID, EMPTY );
    }

    @Test
    void shouldCloseResultAndTransactionHandlesWhenExecutionFails() throws Exception
    {
        BoltTransaction transaction = newTransaction();
        BoltResultHandle resultHandle = newResultHandle( new RuntimeException( "some error" ) );
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction, resultHandle );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        RuntimeException e = assertThrows( RuntimeException.class, () -> stateMachine.run( "SOME STATEMENT", null ) );
        assertEquals( "some error", e.getMessage() );

        assertThat( stateMachine.ctx.statementOutcomes.entrySet(), hasSize( 0 ) );
        assertNull( stateMachine.ctx.currentTransaction );
    }

    @Test
    void shouldCloseResultAndTransactionHandlesWhenConsumeFails() throws Exception
    {
        BoltTransaction transaction = newTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.run( "SOME STATEMENT", null );

        StatementOutcome outcome = stateMachine.ctx.statementOutcomes.get( StatementMetadata.ABSENT_QUERY_ID );
        assertNotNull( outcome );
        assertNotNull( outcome.resultHandle );
        assertNotNull( outcome.result );

        RuntimeException e = assertThrows( RuntimeException.class, () ->
        {
            stateMachine.streamResult( StatementMetadata.ABSENT_QUERY_ID, ERROR );
        } );
        assertEquals( "some error", e.getMessage() );

        assertThat( stateMachine.ctx.statementOutcomes.entrySet(), hasSize( 0 ) );
        assertNull( stateMachine.ctx.currentTransaction );
    }

    @Test
    void shouldCloseResultHandlesWhenExecutionFailsInExplicitTransaction() throws Exception
    {
        BoltTransaction transaction = newTransaction();
        BoltResultHandle resultHandle = newResultHandle( new RuntimeException( "some error" ) );
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction, resultHandle );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        RuntimeException e = assertThrows( RuntimeException.class, () ->
        {
            beginTx( stateMachine );
            stateMachine.run( "SOME STATEMENT", null );
        } );
        assertEquals( "some error", e.getMessage() );

        assertThat( stateMachine.ctx.statementOutcomes.entrySet(), hasSize( 0 ) );
        assertNotNull( stateMachine.ctx.currentTransaction );
    }

    @Test
    void shouldOpenImplicitTransactionForPeriodicCommitQuery() throws Exception
    {
        BoltTransaction transaction = newTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        when( stateMachineSPI.isPeriodicCommit( PERIODIC_COMMIT_QUERY ) ).thenReturn( true );
        final BoltTransaction periodicTransaction = mock( BoltTransaction.class );
        when( stateMachineSPI.beginPeriodicCommitTransaction( any(), any(), any(), any(), any() )).thenReturn( periodicTransaction );

        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.run( PERIODIC_COMMIT_QUERY, EMPTY_MAP );

        // transaction was created only to stream back result of the periodic commit query
        assertEquals( periodicTransaction, stateMachine.ctx.currentTransaction );

        InOrder inOrder = inOrder( stateMachineSPI );
        inOrder.verify( stateMachineSPI ).isPeriodicCommit( PERIODIC_COMMIT_QUERY );
        // implicit transaction was started for periodic query execution
        inOrder.verify( stateMachineSPI ).beginPeriodicCommitTransaction( any( LoginContext.class ), any(), any(), any(), any() );
        // periodic commit query was executed after specific transaction started
        inOrder.verify( stateMachineSPI ).executeQuery( any( BoltQueryExecutor.class ), eq( PERIODIC_COMMIT_QUERY ), eq( EMPTY_MAP ) );
    }

    @Test
    void shouldCloseResultHandlesWhenConsumeFailsInExplicitTransaction() throws Throwable
    {
        BoltTransaction transaction = newTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        beginTx( stateMachine );
        stateMachine.run( "SOME STATEMENT", null );

        StatementOutcome outcome = stateMachine.ctx.statementOutcomes.get( StatementMetadata.ABSENT_QUERY_ID );
        assertNotNull( outcome );
        assertNotNull( outcome.resultHandle );
        assertNotNull( outcome.result );

        RuntimeException e = assertThrows( RuntimeException.class, () ->
        {
            stateMachine.streamResult( StatementMetadata.ABSENT_QUERY_ID, ERROR );
        } );
        assertEquals( "some error", e.getMessage() );

        assertThat( stateMachine.ctx.statementOutcomes.entrySet(), hasSize( 0 ) );
        assertNotNull( stateMachine.ctx.currentTransaction );
    }

    @Test
    void shouldNotMarkForTerminationWhenNoTransaction() throws Exception
    {
        BoltTransaction transaction = newTransaction();
        TransactionStateMachineSPI stateMachineSPI = newTransactionStateMachineSPI( transaction );

        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.markCurrentTransactionForTermination();
        verify( transaction, never() ).markForTermination( any() );
    }

    private static void beginTx( TransactionStateMachine stateMachine ) throws KernelException
    {
        stateMachine.beginTransaction( null, null, AccessMode.WRITE, Map.of() );
    }

    private static void beginTx( TransactionStateMachine stateMachine, List<Bookmark> bookmarks ) throws KernelException
    {
        stateMachine.beginTransaction( bookmarks, null, AccessMode.WRITE, Map.of() );
    }

    private static BoltTransaction newTransaction()
    {
        return mock( BoltTransaction.class );
    }

    private static BoltTransaction newTimedOutTransaction()
    {
        BoltTransaction transaction = newTransaction();

        when( transaction.getReasonIfTerminated() ).thenReturn( Optional.of( Status.Transaction.TransactionTimedOut ) );

        return transaction;
    }

    private static TransactionStateMachine newTransactionStateMachine( TransactionStateMachineSPI stateMachineSPI )
    {
        return new TransactionStateMachine( ABSENT_DB_NAME, stateMachineSPI, AUTH_DISABLED, new FakeClock() );
    }

    private static MapValue map( Object... keyValues )
    {
        return ValueUtils.asMapValue( MapUtil.map( keyValues ) );
    }

    private static TransactionStateMachineSPI newTransactionStateMachineSPI( BoltTransaction transaction ) throws KernelException
    {
        BoltResultHandle resultHandle = newResultHandle();
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );

        when( stateMachineSPI.beginTransaction( any(), any(), any(), any(), any() ) ).thenReturn( transaction );
        when( stateMachineSPI.executeQuery( any(), anyString(), any() ) ).thenReturn( resultHandle );

        return stateMachineSPI;
    }

    private static TransactionStateMachineSPI newTransactionStateMachineSPI( BoltTransaction transaction,
            BoltResultHandle resultHandle ) throws KernelException
    {
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );

        when( stateMachineSPI.beginTransaction( any(), any(), any(), any(), any() ) ).thenReturn( transaction );
        when( stateMachineSPI.executeQuery( any(), anyString(), any() ) ).thenReturn( resultHandle );

        return stateMachineSPI;
    }

    private static BoltResultHandle newResultHandle() throws KernelException
    {
        BoltResultHandle resultHandle = mock( BoltResultHandle.class );

        when( resultHandle.start() ).thenReturn( BoltResult.EMPTY );

        return resultHandle;
    }

    private static BoltResultHandle newResultHandle( Throwable t ) throws KernelException
    {
        BoltResultHandle resultHandle = mock( BoltResultHandle.class );

        when( resultHandle.start() ).thenThrow( t );

        return resultHandle;
    }

    private static class EmptyResultConsumer implements ResultConsumer
    {
        @Override
        public boolean hasMore()
        {
            return false;
        }

        @Override
        public void consume( BoltResult boltResult )
        {
        }
    }
}
