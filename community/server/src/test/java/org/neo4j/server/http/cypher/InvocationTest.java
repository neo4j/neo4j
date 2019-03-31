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
package org.neo4j.server.http.cypher;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.cypher.SyntaxException;
import org.neo4j.cypher.internal.runtime.QueryStatistics;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.http.cypher.format.api.InputEventStream;
import org.neo4j.server.http.cypher.format.api.InputFormatException;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class InvocationTest
{

    static MapValue NO_PARAMS = VirtualValues.emptyMap();

    private final Log log = mock( Log.class );
    private final Result executionResult = mock( Result.class );
    // cannot be mocked because is final
    private final QueryExecutionType queryExecutionType = null;
    private final QueryStatistics queryStatistics = mock( QueryStatistics.class );
    private final ExecutionPlanDescription executionPlanDescription = mock( ExecutionPlanDescription.class );
    private final Iterable<Notification> notifications = Collections.emptyList();
    private final List<Result.ResultRow> resultRows = new ArrayList<>();
    private final TransitionalTxManagementKernelTransaction transactionContext = mock( TransitionalTxManagementKernelTransaction.class );
    private final TransitionalPeriodTransactionMessContainer kernel = mockKernel();
    private final QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
    private final TransactionRegistry registry = mock( TransactionRegistry.class );
    private final OutputEventStream outputEventStream = mock( OutputEventStream.class );

    @Before
    public void setUp()
    {
        doAnswer( invocation ->
        {
            Object arg0 = invocation.getArgument( 0 );
            Result.ResultVisitor resultVisitor = (Result.ResultVisitor) arg0;

            for ( var resultRow : resultRows )
            {
                resultVisitor.visit( resultRow );
            }
            return null;
        } ).when( executionResult ).accept( any() );
        when( executionResult.getQueryExecutionType() ).thenReturn( queryExecutionType );
        when( executionResult.getQueryStatistics() ).thenReturn( queryStatistics );
        when( executionResult.getExecutionPlanDescription() ).thenReturn( executionPlanDescription );
        when( executionResult.getNotifications() ).thenReturn( notifications );
    }

    @Test
    public void shouldExecuteStatements() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( executionEngine ).executeQuery( "query", NO_PARAMS, transactionalContext, false );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldSuspendTransactionAndReleaseForOtherRequestsAfterExecutingStatements() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, false );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).suspendSinceTransactionsAreStillThreadBound();
        transactionOrder.verify( registry ).release( 1337L, handle );

        verify( executionEngine ).executeQuery( "query", NO_PARAMS, transactionalContext, false );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.OPEN, uriScheme.txCommitUri( 1337L ), 0 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldResumeTransactionWhenExecutingStatementsOnSecondRequest() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, false );

        invocation.execute( outputEventStream );
        reset( transactionContext, registry, executionEngine, outputEventStream );
        when( inputEventStream.read() ).thenReturn( statement, null );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder order = inOrder( transactionContext, registry, executionEngine );
        order.verify( transactionContext ).resumeSinceTransactionsAreStillThreadBound();
        order.verify( executionEngine ).executeQuery( "query", NO_PARAMS, transactionalContext, false );
        order.verify( transactionContext ).suspendSinceTransactionsAreStillThreadBound();
        order.verify( registry ).release( 1337L, handle );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.OPEN, uriScheme.txCommitUri( 1337L ), 0 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldCommitSinglePeriodicCommitStatement() throws Exception
    {
        // given
        String queryText = "USING PERIODIC COMMIT CREATE()";
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.isPeriodicCommit( queryText ) ).thenReturn( true );
        when( executionEngine.executeQuery( queryText, NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( queryText, map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( executionEngine ).executeQuery( queryText, NO_PARAMS, transactionalContext, false );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldCommitTransactionAndTellRegistryToForgetItsHandle() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).commit();
        transactionOrder.verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldRollbackTransactionAndTellRegistryToForgetItsHandle()
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        RollbackInvocation invocation = new RollbackInvocation( log, handle );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).rollback();
        transactionOrder.verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, null, -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldCreateTransactionContextOnlyWhenFirstNeeded() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        // when
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );
        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // then
        verifyZeroInteractions( kernel );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( kernel ).newTransaction( any( Type.class ), any( LoginContext.class ), eq( EMBEDDED_CONNECTION ), anyLong() );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldRollbackTransactionIfExecutionErrorOccurs() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenThrow(
                new IllegalStateException( "Something went wrong" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( executionEngine ).executeQuery( "query", NO_PARAMS, transactionalContext, false );
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );

        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.ExecutionFailed, "Something went wrong" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldHandleCommitError() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );
        doThrow( new IllegalStateException( "Something went wrong" ) ).when( transactionContext ).commit();

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( executionEngine ).executeQuery( "query", NO_PARAMS, transactionalContext, false );
        verify( log ).error( eq( "Failed to commit transaction." ), any( IllegalStateException.class ) );
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Transaction.TransactionCommitFailed, "Something went wrong" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.UNKNOWN, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldHandleErrorWhenStartingTransaction()
    {
        // given
        when( kernel.newTransaction( any(), any(), any(), anyLong() ) ).thenThrow( new IllegalStateException( "Something went wrong" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( log ).error( eq( "Failed to start transaction" ), any( IllegalStateException.class ) );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Transaction.TransactionStartFailed, "Something went wrong" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.NO_TRANSACTION, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldHandleErrorWhenResumingTransaction()
    {
        // given
        doThrow( new IllegalStateException( "Something went wrong" ) ).when( transactionContext ).resumeSinceTransactionsAreStillThreadBound();

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );
        handle.ensureActiveTransaction();

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( log ).error( eq( "Failed to resume transaction" ), any( IllegalStateException.class ) );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Transaction.TransactionNotFound, "Something went wrong" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.NO_TRANSACTION, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldHandleCypherSyntaxError() throws Exception
    {
        // given
        String queryText = "matsch (n) return n";
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( queryText, NO_PARAMS, transactionalContext, false ) ).thenThrow(
                new QueryExecutionKernelException( new SyntaxException( "did you mean MATCH?" ) ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( queryText, map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.SyntaxError, "did you mean MATCH?" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldHandleExecutionEngineThrowingUndeclaredCheckedExceptions() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenThrow( new RuntimeException( "BOO" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.ExecutionFailed, "BOO" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldHandleRollbackError() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenThrow( new RuntimeException( "BOO" ) );
        doThrow( new IllegalStateException( "Something went wrong" ) ).when( transactionContext ).rollback();

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( executionEngine ).executeQuery( "query", NO_PARAMS, transactionalContext, false );
        verify( log ).error( eq( "Failed to roll back transaction." ), any( IllegalStateException.class ) );
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.ExecutionFailed, "BOO" );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Transaction.TransactionRollbackFailed, "Something went wrong" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.UNKNOWN, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldInterruptTransaction() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        invocation.execute( outputEventStream );

        // when
        handle.terminate();

        // then
        verify( transactionContext ).terminate();
    }

    @Test
    public void deadlockExceptionHasCorrectStatus() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenThrow( new DeadlockDetectedException( "deadlock" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Transaction.DeadlockDetected, "deadlock" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void startTransactionWithRequestedTimeout()
    {
        // given
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        TransactionHandle handle =
                new TransactionHandle( kernel, executionEngine, queryService, mock( TransactionRegistry.class ), uriScheme, true, AUTH_DISABLED,
                        EMBEDDED_CONNECTION, 100, NullLogProvider.getInstance() );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenReturn( null );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( kernel ).newTransaction( Type.implicit, AUTH_DISABLED, EMBEDDED_CONNECTION, 100 );
    }

    @Test
    public void shouldHandleInputParsingErrorWhenReadingStatements()
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenThrow( new InputFormatException( "Cannot parse input", new IOException( "JSON ERROR" ) ) );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Request.InvalidFormat, "Cannot parse input" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldHandleConnectionErrorWhenReadingStatementsInImplicitTransaction()
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        try
        {
            invocation.execute( outputEventStream );
            fail();
        }
        catch ( ConnectionException e )
        {
            assertEquals( "Connection error", e.getMessage() );
        }

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337L );

        verifyZeroInteractions( outputEventStream );
    }

    @Test
    public void shouldKeepTransactionOpenIfConnectionErrorWhenReadingStatementsInExplicitTransaction()
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry, false );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        try
        {
            invocation.execute( outputEventStream );
            fail();
        }
        catch ( ConnectionException e )
        {
            assertEquals( "Connection error", e.getMessage() );
        }

        // then
        verify( transactionContext, never() ).rollback();
        verify( transactionContext, never() ).commit();
        verify( registry, never() ).forget( 1337L );

        verifyZeroInteractions( outputEventStream );
    }

    @Test
    public void shouldHandleConnectionErrorWhenWritingOutputInImplicitTransaction() throws QueryExecutionKernelException
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        doThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) ).when( outputEventStream ).writeStatementEnd( any(), any(),
                any(), any() );

        // when
        try
        {
            invocation.execute( outputEventStream );
            fail();
        }
        catch ( ConnectionException e )
        {
            assertEquals( "Connection error", e.getMessage() );
        }

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    public void shouldKeepTransactionOpenIfConnectionErrorWhenWritingOutputInImplicitTransaction() throws QueryExecutionKernelException
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry, false );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, null );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, false );

        doThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) ).when( outputEventStream ).writeStatementEnd( any(), any(),
                any(), any() );

        // when
        try
        {
            invocation.execute( outputEventStream );
            fail();
        }
        catch ( ConnectionException e )
        {
            assertEquals( "Connection error", e.getMessage() );
        }

        // then
        verify( transactionContext, never() ).rollback();
        verify( registry, never() ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        verifyNoMoreInteractions( outputEventStream );
    }

    private void mockDefaultResult()
    {
        when( executionResult.columns() ).thenReturn( List.of( "c1", "c2", "c3" ) );
        mockResultRow( Map.of( "c1", "v1", "c2", "v2", "c3", "v3" ) );
        mockResultRow( Map.of( "c1", "v4", "c2", "v5", "c3", "v6" ) );
    }

    private void verifyDefaultResultRows( InOrder outputOrder )
    {
        outputOrder.verify( outputEventStream ).writeRecord( eq( List.of( "c1", "c2", "c3" ) ),
                argThat( new ValuesMatcher( Map.of( "c1", "v1", "c2", "v2", "c3", "v3" ) ) ) );
        outputOrder.verify( outputEventStream ).writeRecord( eq( List.of( "c1", "c2", "c3" ) ),
                argThat( new ValuesMatcher( Map.of( "c1", "v4", "c2", "v5", "c3", "v6" ) ) ) );
    }

    private TransactionHandle getTransactionHandle( TransitionalPeriodTransactionMessContainer kernel, QueryExecutionEngine executionEngine,
            TransactionRegistry registry )
    {
        return getTransactionHandle( kernel, executionEngine, registry, true );
    }

    private TransactionHandle getTransactionHandle( TransitionalPeriodTransactionMessContainer kernel, QueryExecutionEngine executionEngine,
            TransactionRegistry registry, boolean implicitTransaction )
    {
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        return new TransactionHandle( kernel, executionEngine, queryService, registry, uriScheme, implicitTransaction, AUTH_DISABLED, EMBEDDED_CONNECTION,
                anyLong(), NullLogProvider.getInstance() );
    }

    private TransitionalPeriodTransactionMessContainer mockKernel()
    {
        TransitionalPeriodTransactionMessContainer kernel = mock( TransitionalPeriodTransactionMessContainer.class );
        when( kernel.newTransaction( any( Type.class ), any( LoginContext.class ), any( ClientConnectionInfo.class ), anyLong() ) ).thenReturn(
                transactionContext );
        return kernel;
    }

    private TransactionalContext prepareKernelWithQuerySession( TransitionalPeriodTransactionMessContainer kernel )
    {
        TransactionalContext tc = mock( TransactionalContext.class );
        when( kernel.create( any( GraphDatabaseQueryService.class ), any( Type.class ), any( LoginContext.class ), any( String.class ), any( Map.class ) ) ).
                thenReturn( tc );
        return tc;
    }

    private void mockResultRow( Map<String,Object> row )
    {
        Result.ResultRow resultRow = mock( Result.ResultRow.class );
        row.forEach( ( key, value ) -> when( resultRow.get( key ) ).thenReturn( value ) );
        resultRows.add( resultRow );
    }

    private static class ValuesMatcher implements ArgumentMatcher<Function<String,Object>>
    {

        private final Map<String,Object> values;

        private ValuesMatcher( Map<String,Object> values )
        {
            this.values = values;
        }

        @Override
        public boolean matches( Function<String,Object> valueExtractor )
        {
            return values.entrySet().stream().anyMatch( entry -> entry.getValue().equals( valueExtractor.apply( entry.getKey() ) ) );
        }
    }

    private static final TransactionUriScheme uriScheme = new TransactionUriScheme()
    {
        @Override
        public URI txUri( long id )
        {
            return URI.create( "transaction/" + id );
        }

        @Override
        public URI txCommitUri( long id )
        {
            return URI.create( "transaction/" + id + "/commit" );
        }
    };
}
