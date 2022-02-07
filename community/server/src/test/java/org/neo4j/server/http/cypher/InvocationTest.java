/*
 * Copyright (c) "Neo4j"
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
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.transaction.CleanUpTransactionContext;
import org.neo4j.bolt.transaction.DefaultProgramResultReference;
import org.neo4j.bolt.transaction.InitializeContext;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.bolt.transaction.TransactionNotFoundException;
import org.neo4j.cypher.internal.runtime.QueryStatistics;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.SyntaxException;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.http.cypher.format.api.InputEventStream;
import org.neo4j.server.http.cypher.format.api.InputFormatException;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.MapValue;

import static java.lang.Long.parseLong;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

class InvocationTest
{
    private static final Statement NULL_STATEMENT = null;
    private static final String TX_ID = "123";

    private final Log log = mock( Log.class );
    // cannot be mocked because is final
    private final QueryExecutionType queryExecutionType = null;
    private final QueryStatistics queryStatistics = mock( QueryStatistics.class );
    private final ExecutionPlanDescription executionPlanDescription = mock( ExecutionPlanDescription.class );
    private final Iterable<Notification> notifications = emptyList();
    private final TransactionManager transactionManager = mock( TransactionManager.class );
    private final LogProvider logProvider = mock( LogProvider.class );
    private final BoltGraphDatabaseManagementServiceSPI boltSPI = mock( BoltGraphDatabaseManagementServiceSPI.class );
    private final QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
    private final InternalTransaction internalTransaction = mock( InternalTransaction.class );
    private final TransactionRegistry registry = mock( TransactionRegistry.class );
    private final OutputEventStream outputEventStream = mock( OutputEventStream.class );
    private final MemoryTracker memoryTracker = mock( MemoryTracker.class );
    private final AuthManager authManager = mock( AuthManager.class );
    private final StatementMetadata metadata = mock( StatementMetadata.class );
    private final BoltResult boltResult = mock( BoltResult.class );
    private final String[] DEFAULT_FIELD_NAMES = new String[]{"c1", "c2", "c3"};

    @BeforeEach
    void setUp() throws Exception
    {
        when( metadata.fieldNames() ).thenReturn( DEFAULT_FIELD_NAMES );
        when( transactionManager.begin( any( LoginContext.class ), anyString(),
                                        anyList(), anyBoolean(), anyMap(), nullable( Duration.class ), anyString() ) ).thenReturn( TX_ID );
        when( transactionManager.runQuery( TX_ID, "query", MapValue.EMPTY ) ).thenReturn( metadata );
        doAnswer( (Answer<Bookmark>) invocationOnMock ->
        {
            ResultConsumer resultConsumer = invocationOnMock.getArgument( 3, ResultConsumer.class );
            resultConsumer.consume( boltResult );
            return Bookmark.EMPTY_BOOKMARK;
        } ).when( transactionManager ).pullData( any( String.class ), any( Integer.class ), any( Long.class ), any( ResultConsumer.class ) );
    }

    @Test
    void shouldExecuteStatements() throws Throwable
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( parseLong( TX_ID ) ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).pullData( any( String.class ), any( Integer.class ), any( Long.class ), any( ResultConsumer.class ) );
        txManagerOrder.verify( transactionManager ).commit( TX_ID );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        verifyNoMoreInteractions( transactionManager );

        // then verify output
        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( any(), any(), any(), any() ); //todo work out why the actual args fails
//        outputOrder.verify( outputEventStream ).writeStatementEnd( query( QueryExecutionType.QueryType.WRITE ), QueryStatistics.EMPTY,
//                                                                   HttpExecutionPlanDescription.EMPTY, emptyList() );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldSuspendTransactionAndReleaseForOtherRequestsAfterExecutingStatements() throws Throwable
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, false );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder transactionOrder = inOrder( registry );
        transactionOrder.verify( registry ).release( 123L, handle );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).pullData( any( String.class ), any( Integer.class ), any( Long.class ), any( ResultConsumer.class ) );
        verifyNoMoreInteractions( transactionManager );

        // then verify output
        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( any(), any(), any(), any() ); //todo work out why the actual args fails
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.OPEN, uriScheme.txCommitUri( 123L ), 0 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldResumeTransactionWhenExecutingStatementsOnSecondRequest() throws Throwable
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, false );

        invocation.execute( outputEventStream );

        reset( registry, outputEventStream );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        // when
        invocation.execute( outputEventStream );

        //then
        InOrder order = inOrder( registry, transactionManager );
        order.verify( registry ).release( 123L, handle );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager ).begin( any( LoginContext.class ), anyString(), anyList(),
                                                           eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).pullData( any( String.class ), any( Integer.class ), any( Long.class ), any( ResultConsumer.class ) );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).pullData( any( String.class ), any( Integer.class ), any( Long.class ), any( ResultConsumer.class ) );
        verifyNoMoreInteractions( transactionManager );

        // verify output
        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( any(), any(), any(), any() ); //todo work out why the actual args fails
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.OPEN, uriScheme.txCommitUri( 123L ), 0 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldCommitSinglePeriodicCommitStatement() throws Throwable
    {
        // given
        String queryText = "USING PERIODIC COMMIT CREATE()";
        when( executionEngine.isPeriodicCommit( queryText ) ).thenReturn( true );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        when( transactionManager
                      .runProgram( any( String.class ), any( LoginContext.class ), eq( "neo4j" ), eq( queryText ), eq( MapValue.EMPTY ),
                                   eq( emptyList() ), eq( true ), eq( emptyMap() ), nullable( Duration.class ), eq( "123" ) ) )
                .thenReturn( new DefaultProgramResultReference( "123", metadata ) );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( queryText, map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).rollback( "123" );
        txManagerOrder.verify( transactionManager )
                      .runProgram( any( String.class ), any( LoginContext.class ), eq( "neo4j" ), eq( queryText ), eq( MapValue.EMPTY ),
                                   eq( emptyList() ), eq( true ), eq( emptyMap() ), nullable( Duration.class ), eq( "123" ) );
        txManagerOrder.verify( transactionManager ).pullData( any( String.class ), any( Integer.class ), any( Long.class ), any( ResultConsumer.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).commit( "123" );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        verifyNoMoreInteractions( transactionManager );

        // then verify output
        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( any(), any(), any(), any() ); //todo work out why the actual args fails
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldCommitTransactionAndTellRegistryToForgetItsHandle() throws Throwable
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder transactionOrder = inOrder( internalTransaction, registry );
        transactionOrder.verify( registry ).forget( 123L );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).commit( "123" );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        txManagerOrder.verifyNoMoreInteractions();

        // then verify output
        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( any(), any(), any(), any() ); //todo work out why the actual args fails
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldRollbackTransactionAndTellRegistryToForgetItsHandle() throws Exception
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        RollbackInvocation invocation = new RollbackInvocation( log, handle );

        // when
        invocation.execute( outputEventStream );

        // then
        InOrder transactionOrder = inOrder( transactionManager, registry );
        transactionOrder.verify( transactionManager ).rollback( "123" );
        transactionOrder.verify( registry ).forget( 123L );
        transactionOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, null, -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldCreateTransactionContextOnlyWhenFirstNeeded() throws Throwable
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        // when
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );
        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // then
        InOrder transactionOrder = inOrder( transactionManager );
        transactionOrder.verify( transactionManager ).initialize( any() );
        transactionOrder.verifyNoMoreInteractions();

        // when
        invocation.execute( outputEventStream );

        // then
        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).pullData( any( String.class ), any( Integer.class ), any( Long.class ), any( ResultConsumer.class ) );
        txManagerOrder.verify( transactionManager ).commit( TX_ID );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        verifyNoMoreInteractions( transactionManager );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( any(), any(), any(), any() ); //todo work out why the actual args fails
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldRollbackTransactionIfExecutionErrorOccurs() throws Exception
    {
        // given
        when( transactionManager.runQuery( TX_ID, "query", MapValue.EMPTY ) ).thenThrow( mock( TransactionNotFoundException.class ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).rollback( TX_ID );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        verifyNoMoreInteractions( transactionManager );

        verify( registry ).forget( 123L );

        InOrder outputOrder = inOrder( outputEventStream );

        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.ExecutionFailed, null );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleCommitError() throws Throwable
    {
        // given
        var commitError = mock( KernelException.class );
        when( commitError.getMessage() ).thenReturn( "Something went wrong!" );
        when( transactionManager.commit( "123" ) ).thenThrow( commitError );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( log ).error( eq( "Failed to commit transaction." ), any( KernelException.class ) );
        verify( registry ).forget( 123L );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).pullData( any( String.class ), any( Integer.class ), any( Long.class ), any( ResultConsumer.class ) );
        txManagerOrder.verify( transactionManager ).commit( TX_ID );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        verifyNoMoreInteractions( transactionManager );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( any(), any(), any(), any() ); //todo work out why the actual args fails
        outputOrder.verify( outputEventStream ).writeFailure( any(), any() ); //todo check error properly here
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.UNKNOWN, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleErrorWhenStartingTransaction() throws Throwable
    {
        // given
        when( transactionManager.begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() ) )
                .thenThrow( mock( KernelException.class ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( log ).error( eq( "Failed to start transaction" ), any( KernelException.class ) );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( any(), any() ); //todo more specific
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.NO_TRANSACTION, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleAuthorizationErrorWhenStartingTransaction() throws Throwable
    {
        // given
        when( transactionManager.begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() ) )
                .thenThrow( new AuthorizationViolationException( "Forbidden" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

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
    void shouldHandleCypherSyntaxError() throws Exception
    {
        // given
        String queryText = "matsch (n) return n";
        when( transactionManager.runQuery( TX_ID, queryText, MapValue.EMPTY ) )
                .thenThrow( new RuntimeException( new SyntaxException( "did you mean MATCH?" ) ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( queryText, map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( registry ).forget( 123L );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, queryText, MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).rollback( TX_ID );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        verifyNoMoreInteractions( transactionManager );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.SyntaxError, "did you mean MATCH?" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleExecutionEngineThrowingUndeclaredCheckedExceptions() throws Exception
    {
        // given
        when( transactionManager.runQuery( TX_ID, "query", MapValue.EMPTY ) ).thenThrow( new RuntimeException( "BOO" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( registry ).forget( 123L );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).rollback( TX_ID );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        verifyNoMoreInteractions( transactionManager );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.ExecutionFailed, "BOO" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleRollbackError() throws Exception
    {
        // given
        when( transactionManager.runQuery( TX_ID, "query", MapValue.EMPTY ) ).thenThrow( new RuntimeException( "BOO" ) );
        doThrow( new IllegalStateException( "Something went wrong" ) ).when( transactionManager ).rollback( TX_ID );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        verify( log ).error( eq( "Failed to roll back transaction." ), any( IllegalStateException.class ) );
        verify( registry ).forget( 123L );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).rollback( TX_ID );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        verifyNoMoreInteractions( transactionManager );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Statement.ExecutionFailed, "BOO" );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Transaction.TransactionRollbackFailed, "Something went wrong" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.UNKNOWN, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldInterruptTransaction() throws Throwable
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        invocation.execute( outputEventStream );

        // when
        handle.terminate();

        // then
        verify( transactionManager ).interrupt( TX_ID );
    }

    @Test
    void deadlockExceptionHasCorrectStatus() throws Throwable
    {
        // given
        when( transactionManager.runQuery( TX_ID, "query", MapValue.EMPTY ) ).thenThrow( new DeadlockDetectedException( "deadlock" ) );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( registry ).forget( 123L );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( true ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).rollback( TX_ID );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        verifyNoMoreInteractions( transactionManager );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Transaction.DeadlockDetected, "deadlock" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void startTransactionWithRequestedTimeout() throws Exception
    {
        // given
        TransactionManager txManager = mock( TransactionManager.class );
        TransactionHandle handle =
                new TransactionHandle( "neo4j", executionEngine, mock( TransactionRegistry.class ), uriScheme, true, AUTH_DISABLED,
                                       mock( ClientConnectionInfo.class ), 100, txManager, mock( LogProvider.class ),
                                       mock( BoltGraphDatabaseManagementServiceSPI.class ), mock( MemoryTracker.class ), mock( AuthManager.class ),
                                       Clocks.nanoClock(), true );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenReturn( null );

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( txManager ).initialize( any() );
        verify( txManager ).begin( AUTH_DISABLED, "neo4j", emptyList(), true, emptyMap(), Duration.ofMillis( 100 ), "0" );
    }

    @Test
    void shouldHandleInputParsingErrorWhenReadingStatements() throws Exception
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenThrow( new InputFormatException( "Cannot parse input", new IOException( "JSON ERROR" ) ) );

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then
        verify( transactionManager ).rollback( TX_ID );
        verify( registry ).forget( 123L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeFailure( Status.Request.InvalidFormat, "Cannot parse input" );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.ROLLED_BACK, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldHandleConnectionErrorWhenReadingStatementsInImplicitTransaction() throws Exception
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) );

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 123L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        var e = assertThrows( ConnectionException.class, () -> invocation.execute( outputEventStream ) );
        assertEquals( "Connection error", e.getMessage() );

        // then
        verify( transactionManager ).rollback( TX_ID );
        verify( registry ).forget( 123L );

        verifyNoInteractions( outputEventStream );
    }

    @Test
    void shouldKeepTransactionOpenIfConnectionErrorWhenReadingStatementsInExplicitTransaction() throws Exception
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry, false, true );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        when( inputEventStream.read() ).thenThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) );

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        var e = assertThrows( ConnectionException.class, () -> invocation.execute( outputEventStream ) );
        assertEquals( "Connection error", e.getMessage() );

        // then
        verify( transactionManager, never() ).rollback( TX_ID );
        verify( transactionManager, never() ).commit( TX_ID );
        verify( registry, never() ).forget( 1337L );

        verifyNoInteractions( outputEventStream );
    }

    @Test
    void shouldHandleConnectionErrorWhenWritingOutputInImplicitTransaction() throws Throwable
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        doThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) ).when( outputEventStream ).writeStatementEnd( any(), any(),
                                                                                                                                                any(), any() );

        // when
        var e = assertThrows( ConnectionException.class, () -> invocation.execute( outputEventStream ) );
        assertEquals( "Connection error", e.getMessage() );

        // then
        verify( transactionManager ).rollback( TX_ID );
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( any(), any(), any(),
                                                                   any() ); //todo work out why the actual args fails
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldKeepTransactionOpenIfConnectionErrorWhenWritingOutputInImplicitTransaction() throws Throwable
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry, false, true );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( 1337L ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, false );

        doThrow( new ConnectionException( "Connection error", new IOException( "Broken pipe" ) ) ).when( outputEventStream ).writeStatementEnd( any(), any(),
                                                                                                                                                any(), any() );

        // when
        var e = assertThrows( ConnectionException.class, () -> invocation.execute( outputEventStream ) );
        assertEquals( "Connection error", e.getMessage() );

        // then
        verify( transactionManager, never() ).rollback( TX_ID );
        verify( registry, never() ).forget( 1337L );

        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( any(), any(), any(), any() ); //todo work out why the actual args fails
        verifyNoMoreInteractions( outputEventStream );
    }

    @Test
    void shouldAllocateAndFreeMemory() throws Throwable
    {
        var handle = getTransactionHandle( executionEngine, registry, false, true );
        var memoryPool = mock( MemoryPool.class );
        var inputEventStream = mock( InputEventStream.class );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        when( inputEventStream.read() ).thenReturn( new Statement( "query", map() ), NULL_STATEMENT );

        setupResultMocks();

        var invocation = new Invocation( mock( Log.class ), handle, uriScheme.txCommitUri( 1337L ), memoryPool, inputEventStream, true );

        invocation.execute( outputEventStream );

        verify( memoryPool, times( 2 ) ).reserveHeap( Statement.SHALLOW_SIZE );
        verify( memoryPool, times( 2 ) ).releaseHeap( Statement.SHALLOW_SIZE );
        verifyNoMoreInteractions( memoryPool );
    }

    @Test
    void shouldFreeMemoryOnException() throws Throwable
    {
        var handle = getTransactionHandle( executionEngine, registry, false, true );
        var memoryPool = mock( MemoryPool.class );
        var inputEventStream = mock( InputEventStream.class );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        when( inputEventStream.read() ).thenReturn( new Statement( "query", map() ), NULL_STATEMENT );

        setupResultMocks();

        doThrow( new ConnectionException( "Something broke", new IOException( "Oh no" ) ) )
                .when( outputEventStream )
                .writeStatementEnd( any(), any(), any(), any() );

        var invocation = new Invocation( mock( Log.class ), handle, uriScheme.txCommitUri( 1337L ), memoryPool, inputEventStream, true );

        assertThrows( ConnectionException.class, () -> invocation.execute( outputEventStream ) );

        verify( memoryPool ).reserveHeap( Statement.SHALLOW_SIZE );
        verify( memoryPool ).releaseHeap( Statement.SHALLOW_SIZE );
        verifyNoMoreInteractions( memoryPool );
    }

    @Test
    void shouldExecuteStatementsWithWriteTransaction() throws Throwable
    {
        // given
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 123L );
        TransactionHandle handle = getTransactionHandle( executionEngine, registry, true, false );

        InputEventStream inputEventStream = mock( InputEventStream.class );
        Statement statement = new Statement( "query", map() );
        when( inputEventStream.read() ).thenReturn( statement, NULL_STATEMENT );

        setupResultMocks();

        Invocation invocation =
                new Invocation( log, handle, uriScheme.txCommitUri( parseLong( TX_ID ) ), mock( MemoryPool.class, RETURNS_MOCKS ), inputEventStream, true );

        // when
        invocation.execute( outputEventStream );

        // then verify transactionManager interaction
        InOrder txManagerOrder = inOrder( transactionManager );
        txManagerOrder.verify( transactionManager ).initialize( any( InitializeContext.class ) );
        txManagerOrder.verify( transactionManager )
                      .begin( any( LoginContext.class ), anyString(), anyList(), eq( false ), anyMap(), nullable( Duration.class ), anyString() );
        txManagerOrder.verify( transactionManager ).runQuery( TX_ID, "query", MapValue.EMPTY );
        txManagerOrder.verify( transactionManager ).pullData( any( String.class ), any( Integer.class ), any( Long.class ), any( ResultConsumer.class ) );
        txManagerOrder.verify( transactionManager ).commit( TX_ID );
        txManagerOrder.verify( transactionManager ).cleanUp( any( CleanUpTransactionContext.class ) );
        verifyNoMoreInteractions( transactionManager );

        // then verify output
        InOrder outputOrder = inOrder( outputEventStream );
        outputOrder.verify( outputEventStream ).writeStatementStart( statement, List.of( "c1", "c2", "c3" ) );
        verifyDefaultResultRows( outputOrder );
        outputOrder.verify( outputEventStream ).writeStatementEnd( any(), any(), any(), any() ); //todo work out why the actual args fails
//        outputOrder.verify( outputEventStream ).writeStatementEnd( query( QueryExecutionType.QueryType.WRITE ), QueryStatistics.EMPTY,
//                                                                   HttpExecutionPlanDescription.EMPTY, emptyList() );
        outputOrder.verify( outputEventStream ).writeTransactionInfo( TransactionNotificationState.COMMITTED, uriScheme.txCommitUri( 123L ), -1 );
        verifyNoMoreInteractions( outputEventStream );
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
        return getTransactionHandle( executionEngine, registry, true, true );
    }

    private TransactionHandle getTransactionHandle( QueryExecutionEngine executionEngine, TransactionRegistry registry, boolean implicitTransaction,
                                                    boolean readOnly )
    {
        return new TransactionHandle( "neo4j", executionEngine, registry, uriScheme, implicitTransaction, AUTH_DISABLED, mock( ClientConnectionInfo.class ),
                                      anyLong(), transactionManager, logProvider, boltSPI, memoryTracker, authManager, Clocks.nanoClock(), readOnly );
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

    private void setupResultMocks() throws Throwable
    {
        var fieldNames = List.of( "c1", "c2", "c3" ).toArray( new String[0] );

        when( boltResult.fieldNames() ).thenReturn( fieldNames );
        when( boltResult.handleRecords( any( BoltResult.RecordConsumer.class ), any( Long.class ) ) ).thenAnswer(
                (Answer<Boolean>) invocation ->
                {
                    var recordConsumer = invocation.getArgument( 0, BoltResult.RecordConsumer.class );
                    recordConsumer.beginRecord( 3 );
                    recordConsumer.consumeField( stringValue( "v1" ) );
                    recordConsumer.consumeField( stringValue( "v2" ) );
                    recordConsumer.consumeField( stringValue( "v3" ) );
                    recordConsumer.endRecord();
                    recordConsumer.beginRecord( 3 );
                    recordConsumer.consumeField( stringValue( "v4" ) );
                    recordConsumer.consumeField( stringValue( "v5" ) );
                    recordConsumer.consumeField( stringValue( "v6" ) );
                    recordConsumer.endRecord();
                    recordConsumer.addMetadata( "type", stringValue( "w" ) );
                    recordConsumer.addMetadata( "db", stringValue( "neo4j" ) );
                    recordConsumer.addMetadata( "t_last", longValue( 0 ) );
                    return false;
                }
        );
    }
}
