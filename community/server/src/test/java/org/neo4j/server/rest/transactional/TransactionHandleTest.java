/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.rest.transactional;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.cypher.SyntaxException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.web.TransactionUriScheme;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.transactional.StubStatementDeserializer.statements;

public class TransactionHandleTest
{
    @Test
    public void shouldExecuteStatements() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );
        ExecutionResult executionResult = mock( ExecutionResult.class );
        when( executionEngine.execute( "query", map() ) ).thenReturn( executionResult );
        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );
        TransactionHandle handle = new TransactionHandle( kernel, executionEngine,
                registry, uriScheme, StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[])null ) ), output );

        // then
        verify( executionEngine ).execute( "query", map() );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[])null );
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
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        ExecutionEngine engine = mock( ExecutionEngine.class );
        ExecutionResult executionResult = mock( ExecutionResult.class );
        when( engine.execute( "query", map() ) ).thenReturn( executionResult );
        TransactionHandle handle = new TransactionHandle( kernel, engine,
                registry, uriScheme, StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[]) null ) ), output );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).suspendSinceTransactionsAreStillThreadBound();
        transactionOrder.verify( registry ).release( 1337l, handle );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[])null );
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
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );

        TransactionHandle handle = new TransactionHandle( kernel, executionEngine, registry, uriScheme,
                StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[])null ) ), output );
        reset( transactionContext, registry, executionEngine, output );
        ExecutionResult executionResult = mock( ExecutionResult.class );
        when( executionEngine.execute( "query", map() ) ).thenReturn( executionResult );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[])null ) ), output );

        // then
        InOrder order = inOrder( transactionContext, registry, executionEngine );
        order.verify( transactionContext ).resumeSinceTransactionsAreStillThreadBound();
        order.verify( executionEngine ).execute( "query", map() );
        order.verify( transactionContext ).suspendSinceTransactionsAreStillThreadBound();
        order.verify( registry ).release( 1337l, handle );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[])null );
        outputOrder.verify( output ).transactionStatus( anyLong() );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldCommitTransactionAndTellRegistryToForgetItsHandle() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        ExecutionEngine engine = mock( ExecutionEngine.class );
        ExecutionResult result = mock( ExecutionResult.class );
        when( engine.execute( "query", map() ) ).thenReturn( result );
        TransactionHandle handle = new TransactionHandle( kernel, engine,
                                                          registry, uriScheme, StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.commit( statements( new Statement( "query", map(), false, (ResultDataContent[]) null ) ), output );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).commit();
        transactionOrder.verify( registry ).forget( 1337l );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).statementResult( result, false, (ResultDataContent[])null );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldRollbackTransactionAndTellRegistryToForgetItsHandle() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ),
                registry, uriScheme, StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.rollback( output );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).rollback();
        transactionOrder.verify( registry ).forget( 1337l );

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
        when( registry.begin() ).thenReturn( 1337l );

        // when
        ExecutionEngine engine = mock( ExecutionEngine.class );
        ExecutionResult executionResult = mock( ExecutionResult.class );
        when( engine.execute( "query", map() ) ).thenReturn( executionResult );
        TransactionHandle handle = new TransactionHandle( kernel, engine,
                registry, uriScheme, StringLogger.DEV_NULL );

        // then
        verifyZeroInteractions( kernel );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[])null ) ), output );

        // then
        verify( kernel ).newTransaction();

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[])null );
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
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );
        when( executionEngine.execute( "query", map() ) ).thenThrow( new NullPointerException() );

        TransactionHandle handle = new TransactionHandle( kernel, executionEngine, registry, uriScheme,
                StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( statements( new Statement( "query", map(), false, (ResultDataContent[])null ) ), output );

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337l );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).errors( argThat( hasErrors( Status.Statement.ExecutionFailure ) ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldLogMessageIfCommitErrorOccurs() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();
        TransitionalTxManagementKernelTransaction transactionContext = kernel.newTransaction();
        doThrow( new NullPointerException() ).when( transactionContext ).commit();

        StringLogger log = mock( StringLogger.class );

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        ExecutionEngine engine = mock( ExecutionEngine.class );
        ExecutionResult executionResult = mock( ExecutionResult.class );
        when( engine.execute( "query", map() ) ).thenReturn( executionResult );
        TransactionHandle handle = new TransactionHandle( kernel, engine, registry, uriScheme, log );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.commit( statements( new Statement( "query", map(), false, (ResultDataContent[])null ) ), output );

        // then
        verify( log ).error( eq( "Failed to commit transaction." ), any( NullPointerException.class ) );
        verify( registry ).forget( 1337l );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).statementResult( executionResult, false, (ResultDataContent[])null );
        outputOrder.verify( output ).errors( argThat( hasErrors( Status.Transaction.CouldNotCommit ) ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldLogMessageIfCypherSyntaxErrorOccurs() throws Exception
    {
        // given
        TransitionalPeriodTransactionMessContainer kernel = mockKernel();

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );
        when( executionEngine.execute( "matsch (n) return n", map() ) ).thenThrow( new SyntaxException( "did you mean MATCH?" ) );

        StringLogger log = mock( StringLogger.class );

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, executionEngine, registry, uriScheme, log );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.commit( statements( new Statement( "matsch (n) return n", map(), false, (ResultDataContent[])null ) ), output );

        // then
        verify( registry ).forget( 1337l );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).errors( argThat( hasErrors( Status.Statement.InvalidSyntax ) ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldHandleExecutionEngineThrowingUndeclaredCheckedExceptions() throws Exception
    {
        // given

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );
        when( executionEngine.execute( "match (n) return n", map() ) ).thenAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocationOnMock ) throws Throwable { throw new Exception("BOO"); }
        });

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( mockKernel(), executionEngine, registry, uriScheme,
                mock( StringLogger.class ) );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.commit( statements( new Statement( "match (n) return n", map(), false, (ResultDataContent[])null ) ),
                output );

        // then
        verify( registry ).forget( 1337l );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).errors( argThat( hasErrors( Status.Statement.ExecutionFailure ) ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
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
        when( kernel.newTransaction() ).thenReturn( context );
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
}
