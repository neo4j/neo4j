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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.cypher.internal.runtime.QueryStatistics;
import org.neo4j.exceptions.SyntaxException;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.Log;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.http.cypher.format.api.InputEventStream;
import org.neo4j.server.http.cypher.format.api.InputFormatException;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;

class InvocationTest
{
    private static final MapValue NO_PARAMS = VirtualValues.EMPTY_MAP;
    private static final Statement NULL_STATEMENT = null;

    private final Log log = mock( Log.class );
    private final Result executionResult = mock( Result.class );
    // cannot be mocked because is final
    private final QueryExecutionType queryExecutionType = null;
    private final QueryStatistics queryStatistics = mock( QueryStatistics.class );
    private final ExecutionPlanDescription executionPlanDescription = mock( ExecutionPlanDescription.class );
    private final Iterable<Notification> notifications = Collections.emptyList();
    private final List<Result.ResultRow> resultRows = new ArrayList<>();
    private final TransitionalTxManagementKernelTransaction transactionContext = mock( TransitionalTxManagementKernelTransaction.class );
    private final GraphDatabaseFacade databaseFacade = mock( GraphDatabaseFacade.class );
    private final QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
    private final InternalTransaction internalTransaction = mock( InternalTransaction.class );
    private final TransactionRegistry registry = mock( TransactionRegistry.class );
    private final OutputEventStream outputEventStream = mock( OutputEventStream.class );

    @BeforeEach
    void setUp()
    {
        doAnswer( invocation ->
        {
            Result.ResultVisitor<?> resultVisitor = invocation.getArgument( 0 );

            for ( var resultRow : resultRows )
            {
                resultVisitor.visit( resultRow );
            }
            return null;
        } ).when( executionResult ).accept( any() );
        when( databaseFacade.beginTransaction( any(), any(), any() ) ).thenReturn( internalTransaction );
        when( executionResult.getQueryExecutionType() ).thenReturn( queryExecutionType );
        when( executionResult.getQueryStatistics() ).thenReturn( queryStatistics );
        when( executionResult.getExecutionPlanDescription() ).thenReturn( executionPlanDescription );
        when( executionResult.getNotifications() ).thenReturn( notifications );
    }

    @Test
    void shouldExecuteStatements()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( internalTransaction ).execute( "query", emptyMap() );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldSuspendTransactionAndReleaseForOtherRequestsAfterExecutingStatements()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, false );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( registry ).release( 1337L, handle );

        verify( internalTransaction ).execute( "query", emptyMap() );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.OPEN, uriScheme.txCommitUri( 1337L ), 0 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldResumeTransactionWhenExecutingStatementsOnSecondRequest()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, false );

        invocation.execute( outputEventStream );
        reset( transactionContext, registry, internalTransaction, outputEventStream );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );
        when( internalTransaction.execute( "query", emptyMap() ) ).thenReturn( executionResult );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder order = inOrder( transactionContext, registry, internalTransaction );
        order.verify( internalTransaction ).execute( "query", emptyMap() );
        order.verify( registry ).release( 1337L, handle );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.OPEN, uriScheme.txCommitUri( 1337L ), 0 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldCommitSinglePeriodicCommitStatement()
    {
        // given
        String queryText = "USING PERIODIC COMMIT CREATE()";
        var transaction = mock( InternalTransaction.class );
        when( databaseFacade.beginTransaction( eq( IMPLICIT ), any(LoginContext.class), any(ClientConnectionInfo.class), anyLong(), any( TimeUnit.class ) ) )
                .thenReturn( transaction );
        when( transaction.execute( eq( queryText), any() ) ).thenReturn( executionResult );
        when( executionEngine.isPeriodicCommit( queryText ) ).thenReturn( true );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( queryText, map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldCommitTransactionAndTellRegistryToForgetItsHandle()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder transactionOrder = inOrder( internalTransaction, registry );
        transactionOrder.verify( internalTransaction ).commit();
        transactionOrder.verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldRollbackTransactionAndTellRegistryToForgetItsHandle()
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        RollbackInvocation invocation = new RollbackInvocation( log, handle );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder transactionOrder = inOrder( internalTransaction, registry );
        transactionOrder.verify( internalTransaction ).rollback();
        transactionOrder.verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, null, -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldCreateTransactionContextOnlyWhenFirstNeeded()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        // when
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );
        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // then
        verifyNoInteractions( databaseFacade );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( databaseFacade ).beginTransaction( any( KernelTransaction.Type.class ), any( LoginContext.class ), eq( EMBEDDED_CONNECTION ) );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldRollbackTransactionIfExecutionErrorOccurs()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenThrow(
                new IllegalStateException( "Something went wrong" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( internalTransaction ).execute( "query", emptyMap() );
        verify( internalTransaction ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );

        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.ExecutionFailed, "Something went wrong" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleCommitError()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenReturn( executionResult );
        doThrow( new IllegalStateException( "Something went wrong" ) ).when( internalTransaction ).commit();

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( internalTransaction ).execute( "query", emptyMap() );
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
    void shouldHandleErrorWhenStartingTransaction()
    {
        // given
        when( databaseFacade.beginTransaction( any(), any(), any() ) ).thenThrow( new IllegalStateException( "Something went wrong" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

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
    void shouldHandleAuthorizationErrorWhenStartingTransaction()
    {
        // given
        when( databaseFacade.beginTransaction( any(), any(), any() ) ).thenThrow( new AuthorizationViolationException( "Forbidden" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verifyNoMoreInteractions( log );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Security.Forbidden, "Forbidden" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.NO_TRANSACTION, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleCypherSyntaxError()
    {
        // given
        String queryText = "matsch (n) return n";
        when( internalTransaction.execute( queryText, emptyMap() ) ).thenThrow(
                new RuntimeException( new SyntaxException( "did you mean MATCH?" ) ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( queryText, map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( internalTransaction ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.SyntaxError, "did you mean MATCH?" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleExecutionEngineThrowingUndeclaredCheckedExceptions()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenThrow( new RuntimeException( "BOO" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( internalTransaction ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.ExecutionFailed, "BOO" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleRollbackError()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenThrow( new RuntimeException( "BOO" ) );
        doThrow( new IllegalStateException( "Something went wrong" ) ).when( internalTransaction ).rollback();

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( internalTransaction ).execute( "query", emptyMap() );
        verify( log ).error( eq( "Failed to roll back transaction." ), any( IllegalStateException.class ) );
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.ExecutionFailed, "BOO" );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Transaction.TransactionRollbackFailed, "Something went wrong" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.UNKNOWN, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldInterruptTransaction() throws Exception
    {
        // given
        TransactionalContext transactionalContext = prepareKernelWithQuerySession();
        when( executionEngine.executeQuery( "query", NO_PARAMS, transactionalContext, false ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        invocation.execute( outputEventStream );

        // when
        handle.terminate();

        // then
        verify( internalTransaction ).terminate();
    }

    @Test
    void deadlockExceptionHasCorrectStatus()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenThrow( new DeadlockDetectedException( "deadlock" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( internalTransaction ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Transaction.DeadlockDetected, "deadlock" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void startTransactionWithRequestedTimeout()
    {
        // given
        TransactionHandle handle =
                new TransactionHandle( databaseFacade, executionEngine, mock( TransactionRegistry.class ), uriScheme, true, AUTH_DISABLED,
                        EMBEDDED_CONNECTION, 100 );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenReturn( null );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( databaseFacade ).beginTransaction( IMPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, 100, TimeUnit.MILLISECONDS );
    }

    @Test
    void shouldHandleInputParsingErrorWhenReadingStatements()
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenThrow( new InputFormatException( "Cannot parse input", new IOException( "JSON ERROR" ) ) );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( internalTransaction ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Request.InvalidFormat, "Cannot parse input" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 1337L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleConnectionErrorWhenReadingStatementsInImplicitTransaction()
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        var e = assertThrows( ConnectionException.class, () -> invocation.execute( outputEventStream ) );
        assertEquals( "Connection error", e.getMessage() );

        // then
        verify( internalTransaction ).rollback();
        verify( registry ).forget( 1337L );

        verifyNoInteractions( outputEventStream );
    }

    @Test
    void shouldKeepTransactionOpenIfConnectionErrorWhenReadingStatementsInExplicitTransaction()
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry, false );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) );

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        // when
        var e = assertThrows( ConnectionException.class, () -> invocation.execute( outputEventStream ) );
        assertEquals( "Connection error", e.getMessage() );

        // then
        verify( transactionContext, never() ).rollback();
        verify( transactionContext, never() ).commit();
        verify( registry, never() ).forget( 1337L );

        verifyNoInteractions( outputEventStream );
    }

    @Test
    void shouldHandleConnectionErrorWhenWritingOutputInImplicitTransaction()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, true );

        doThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) ).when( outputEventStream ).writeStatementEnd( any(), any(),
                any(), any() );

        // when
        var e = assertThrows( ConnectionException.class, () -> invocation.execute( outputEventStream ) );
        assertEquals( "Connection error", e.getMessage() );

        // then
        verify( internalTransaction ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldKeepTransactionOpenIfConnectionErrorWhenWritingOutputInImplicitTransaction()
    {
        // given
        when( internalTransaction.execute( "query", emptyMap() ) ).thenReturn( executionResult );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry, false );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        mockDefaultResult();

        Invocation invocation = new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), inputEventStream, false );

        doThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) ).when( outputEventStream ).writeStatementEnd( any(), any(),
                any(), any() );

        // when
        var e = assertThrows( ConnectionException.class, () -> invocation.execute( outputEventStream ) );
        assertEquals( "Connection error", e.getMessage() );

        // then
        verify( internalTransaction, never() ).rollback();
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

    private TransactionHandle getTransactionHandle( QueryExecutionEngine executionEngine, TransactionRegistry registry )
    {
        return getTransactionHandle( executionEngine, registry, true );
    }

    private TransactionHandle getTransactionHandle( QueryExecutionEngine executionEngine, TransactionRegistry registry, boolean implicitTransaction )
    {
        return new TransactionHandle( databaseFacade, executionEngine, registry, uriScheme, implicitTransaction, AUTH_DISABLED, EMBEDDED_CONNECTION,
                anyLong() );
    }

    private TransactionalContext prepareKernelWithQuerySession()
    {
        return mock( TransactionalContext.class );
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

        @Override
        public URI dbUri()
        {
            return URI.create( "data/" );
        }
    };
}
