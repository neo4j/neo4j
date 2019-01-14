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
package org.neo4j.server.rest.transactional;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.mockito.InOrder;

import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;

import org.neo4j.cypher.SyntaxException;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.web.TransactionUriScheme;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyCollectionOf;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.server.rest.transactional.StubStatementDeserializer.statements;

public class TransactionHandleTest
{
    @Test
    public void shouldExecuteStatements() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();

        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        Result executionResult = mock( Result.class );
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", map(), transactionalContext ) ).thenReturn( executionResult );
        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[]) null ) ), output,
                mock( HttpServletRequest.class ) );

        // then
        verify( executionEngine ).executeQuery( "query", map(), transactionalContext );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[])null );
        outputOrder.verify( output ).notifications( anyCollection() );
        outputOrder.verify( output ).transactionStatus( anyLong() );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldSuspendTransactionAndReleaseForOtherRequestsAfterExecutingStatements() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction( explicit, AUTH_DISABLED, -1 );

        TransactionRegistry registry = mock( TransactionRegistry.class );

        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        Result executionResult = mock( Result.class );
        when( executionEngine.executeQuery( "query", map(), transactionalContext) ).thenReturn( executionResult );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[]) null ) ), output,
                mock( HttpServletRequest.class ) );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).suspendSinceTransactionsAreStillThreadBound();
        transactionOrder.verify( registry ).release( 1337L, handle );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[])null );
        outputOrder.verify( output ).notifications( anyCollection() );
        outputOrder.verify( output ).transactionStatus( anyLong() );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldResumeTransactionWhenExecutingStatementsOnSecondRequest() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction( explicit, AUTH_DISABLED, -1);

        TransactionRegistry registry = mock( TransactionRegistry.class );
        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[]) null ) ), output,
                mock( HttpServletRequest.class ) );
        reset( transactionContext, registry, executionEngine, output );
        Result executionResult = mock( Result.class );
        when( executionEngine.executeQuery( "query", map(), transactionalContext )
        ).thenReturn( executionResult );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[]) null ) ), output,
                mock( HttpServletRequest.class ) );

        // then
        InOrder order = inOrder( transactionContext, registry, executionEngine );
        order.verify( transactionContext ).resumeSinceTransactionsAreStillThreadBound();
        order.verify( executionEngine ).executeQuery( "query", map(), transactionalContext );
        order.verify( transactionContext ).suspendSinceTransactionsAreStillThreadBound();
        order.verify( registry ).release( 1337L, handle );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[]) null );
        outputOrder.verify( output ).notifications( anyCollectionOf( Notification.class ) );
        outputOrder.verify( output ).transactionStatus( anyLong() );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldCommitSinglePeriodicCommitStatement() throws Exception
    {
        // given
        String queryText = "USING PERIODIC COMMIT CREATE()";
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();

        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        Result executionResult = mock( Result.class );
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.isPeriodicCommit( queryText) ).thenReturn( true );
        when( executionEngine.executeQuery( eq( queryText ), eq( map() ), eq( transactionalContext ) ) )
                .thenReturn( executionResult );

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );
        Statement statement = new Statement( queryText, map(), false, (ResultDataContent[]) null );

        // when
        handle.commit( statements( statement ), output, mock( HttpServletRequest.class ) );

        // then
        verify( executionEngine ).executeQuery( queryText, map(), transactionalContext );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[]) null );
        outputOrder.verify( output ).notifications( anyCollectionOf( Notification.class ) );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldCommitTransactionAndTellRegistryToForgetItsHandle() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction( explicit, AUTH_DISABLED, -1 );

        TransactionRegistry registry = mock( TransactionRegistry.class );

        QueryExecutionEngine engine = mock( QueryExecutionEngine.class );
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        Result result = mock( Result.class );
        when( engine.executeQuery( "query", map(), transactionalContext ) ).thenReturn( result );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        TransactionHandle handle = new TransactionHandle( kernel, engine, queryService, registry, uriScheme, false,
                AUTH_DISABLED,

                anyLong(), NullLogProvider.getInstance() );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        Statement statement = new Statement( "query", map(), false, (ResultDataContent[]) null );
        handle.commit( statements( statement ), output, mock( HttpServletRequest.class ) );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).commit();
        transactionOrder.verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).statementResult( result, false, (ResultDataContent[])null );
        outputOrder.verify( output ).notifications( anyCollectionOf( Notification.class ) );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldRollbackTransactionAndTellRegistryToForgetItsHandle()
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction( explicit, AUTH_DISABLED, -1 );

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        TransactionHandle handle = new TransactionHandle( kernel, mock( QueryExecutionEngine.class ), queryService,
                registry, uriScheme, true, AUTH_DISABLED, anyLong(), NullLogProvider.getInstance() );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.rollback( output );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).rollback();
        transactionOrder.verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldCreateTransactionContextOnlyWhenFirstNeeded() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );
        TransactionRegistry registry = mock( TransactionRegistry.class );

        // when
        QueryExecutionEngine engine = mock( QueryExecutionEngine.class );
        Result executionResult = mock( Result.class );
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( engine.executeQuery( "query", map(), transactionalContext ) ).thenReturn( executionResult );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        TransactionHandle handle = new TransactionHandle( kernel, engine, queryService, registry, uriScheme, true,
                AUTH_DISABLED,
                anyLong(), NullLogProvider.getInstance() );

        // then
        verifyZeroInteractions( kernel );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[]) null ) ), output,
                mock( HttpServletRequest.class ) );

        // then
        verify( kernel ).newTransaction( any( Type.class ), any( LoginContext.class ), anyLong() );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[])null );
        outputOrder.verify( output ).notifications( anyCollectionOf( Notification.class ) );
        outputOrder.verify( output ).transactionStatus( anyLong() );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldRollbackTransactionIfExecutionErrorOccurs() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction( explicit, AUTH_DISABLED, -1 );

        TransactionRegistry registry = mock( TransactionRegistry.class );

        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "query", map(), transactionalContext ) ).thenThrow( new NullPointerException() );

        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[]) null ) ), output,
                mock( HttpServletRequest.class ) );

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).errors( argThat( hasErrors( Status.Statement.ExecutionFailed ) ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldLogMessageIfCommitErrorOccurs() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction( explicit, AUTH_DISABLED, -1 );
        doThrow( new NullPointerException() ).when( transactionContext ).commit();

        LogProvider logProvider = mock( LogProvider.class );
        Log log = mock( Log.class );
        when( logProvider.getLog( TransactionHandle.class ) ).thenReturn( log );

        TransactionRegistry registry = mock( TransactionRegistry.class );

        QueryExecutionEngine engine = mock( QueryExecutionEngine.class );
        Result executionResult = mock( Result.class );
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( engine.executeQuery( "query", map(), transactionalContext ) ).thenReturn( executionResult );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        TransactionHandle handle = new TransactionHandle( kernel, engine, queryService, registry, uriScheme, false,
                AUTH_DISABLED, anyLong(), logProvider );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        Statement statement = new Statement( "query", map(), false, (ResultDataContent[]) null );
        handle.commit( statements( statement ), output, mock( HttpServletRequest.class ) );

        // then
        verify( log ).error( eq( "Failed to commit transaction." ), any( NullPointerException.class ) );
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[])null );
        outputOrder.verify( output ).notifications( anyCollectionOf( Notification.class ) );
        outputOrder.verify( output ).errors( argThat( hasErrors( Status.Transaction
                .TransactionCommitFailed ) ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldLogMessageIfCypherSyntaxErrorOccurs() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();

        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        TransactionalContext transactionalContext = prepareKernelWithQuerySession( kernel );
        when( executionEngine.executeQuery( "matsch (n) return n", map(), transactionalContext ) )
                .thenThrow( new QueryExecutionKernelException( new SyntaxException( "did you mean MATCH?" ) ) );

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        TransactionHandle handle = new TransactionHandle( kernel, executionEngine, queryService, registry, uriScheme, false,
                AUTH_DISABLED, anyLong(), NullLogProvider.getInstance() );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        Statement statement = new Statement( "matsch (n) return n", map(), false, (ResultDataContent[]) null );
        handle.commit( statements( statement ), output, mock( HttpServletRequest.class ) );

        // then
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).errors( argThat( hasErrors( Status.Statement.SyntaxError ) ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldHandleExecutionEngineThrowingUndeclaredCheckedExceptions() throws Exception
    {
        // given
        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        when( executionEngine.executeQuery( eq( "match (n) return n" ), eq( map() ), any( TransactionalContext.class ) ) ).thenAnswer(
                invocationOnMock ->
                {
                    throw new Exception( "BOO" );
                } );

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        TransactionHandle handle = new TransactionHandle( mockKernel(), executionEngine, queryService, registry,
                uriScheme, false, AUTH_DISABLED, anyLong(), NullLogProvider.getInstance() );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        Statement statement = new Statement( "match (n) return n", map(), false, (ResultDataContent[]) null );
        handle.commit( statements( statement ), output, mock( HttpServletRequest.class ) );

        // then
        verify( registry ).forget( 1337L );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).statementResult( isNull(), eq( false ), isNull() );
        outputOrder.verify( output ).errors( argThat( hasErrors( Status.Statement.ExecutionFailed ) ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldInterruptTransaction()
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        TransitionalTxManagementKernelTransaction tx = mock( TransitionalTxManagementKernelTransaction.class );
        when( kernel.newTransaction( any( Type.class ), any( LoginContext.class ), anyLong() ) ).thenReturn( tx );
        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin( any( TransactionHandle.class ) ) ).thenReturn( 1337L );
        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        TransactionHandle handle = getTransactionHandle( kernel, executionEngine, registry );

        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );
        Statement statement = new Statement( "MATCH (n) RETURN n", map(), false, (ResultDataContent[]) null );
        handle.execute( statements( statement ), output, mock( HttpServletRequest.class ) );

        // when
        handle.terminate();

        // then
        verify( tx, times( 1 ) ).terminate();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void deadlockExceptionHasCorrectStatus() throws Exception
    {
        // given
        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        when( executionEngine.executeQuery( anyString(), anyMap(), isNull() ) )
                .thenThrow( new DeadlockDetectedException( "deadlock" ) );

        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        TransactionHandle handle = new TransactionHandle( mockKernel(), executionEngine,
                queryService, mock( TransactionRegistry.class ), uriScheme, true, AUTH_DISABLED, anyLong(),
                NullLogProvider.getInstance() );

        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[]) null ) ), output,
                mock( HttpServletRequest.class ) );

        // then
        verify( output ).errors( argThat( hasErrors( Status.Transaction.DeadlockDetected ) ) );
    }

    @Test
    public void startTransactionWithRequestedTimeout()
    {
        QueryExecutionEngine executionEngine = mock( QueryExecutionEngine.class );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        TransitionalPeriodTransactionMessContainer txManagerFacade = mockKernel();
        TransactionHandle handle = new TransactionHandle( txManagerFacade, executionEngine,
                queryService, mock( TransactionRegistry.class ), uriScheme, true, AUTH_DISABLED, 100,
                NullLogProvider.getInstance() );

        handle.commit( statements( new Statement( "query", map(), false, (ResultDataContent[]) null ) ), output,
                mock( HttpServletRequest.class ) );

        verify( txManagerFacade ).newTransaction( Type.implicit, AUTH_DISABLED, 100 );
    }

    private TransactionHandle getTransactionHandle( TransitionalPeriodTransactionMessContainer kernel,
            QueryExecutionEngine executionEngine, TransactionRegistry registry )
    {
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        return new TransactionHandle( kernel, executionEngine, queryService, registry, uriScheme, true, AUTH_DISABLED,
                anyLong(), NullLogProvider.getInstance() );
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

    private TransitionalPeriodTransactionMessContainer mockKernel()
    {
        TransitionalTxManagementKernelTransaction context = mock( TransitionalTxManagementKernelTransaction.class );
        TransitionalPeriodTransactionMessContainer kernel = mock( TransitionalPeriodTransactionMessContainer.class );
        when( kernel.newTransaction( any( Type.class ), any( LoginContext.class ), anyLong() ) ).thenReturn( context );
        return kernel;
    }

    private static Matcher<Iterable<Neo4jError>> hasNoErrors()
    {
        return hasErrors();
    }

    private static Matcher<Iterable<Neo4jError>> hasErrors( Status... codes )
    {
        final Set<Status> expectedErrorsCodes = new HashSet<>( asList( codes ) );

        return new TypeSafeMatcher<Iterable<Neo4jError>>()
        {
            @Override
            protected boolean matchesSafely( Iterable<Neo4jError> item )
            {
                Set<Status> actualErrorCodes = new HashSet<>();
                for ( Neo4jError neo4jError : item )
                {
                    actualErrorCodes.add( neo4jError.status() );
                }
                return expectedErrorsCodes.equals( actualErrorCodes );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Errors with set of codes" ).appendValue( expectedErrorsCodes );
            }
        };
    }

    private TransactionalContext prepareKernelWithQuerySession( TransitionalPeriodTransactionMessContainer kernel )
    {
        TransactionalContext tc = mock( TransactionalContext.class );
        when(
                kernel.create(
                        any( HttpServletRequest.class ),
                        any( GraphDatabaseQueryService.class ),
                        any( Type.class ),
                        any( LoginContext.class ),
                        any( String.class ),
                        any( Map.class ) ) ).
                thenReturn( tc );
        return tc;
    }
}
