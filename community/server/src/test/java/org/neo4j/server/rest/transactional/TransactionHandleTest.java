/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.transactional.StubStatementDeserializer.deserilizationErrors;
import static org.neo4j.server.rest.transactional.StubStatementDeserializer.statements;

import java.net.URI;
import java.util.Iterator;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.mockito.InOrder;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.rest.transactional.error.InvalidRequestError;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.web.TransactionUriScheme;

public class TransactionHandleTest
{
    @Test
    public void shouldExecuteStatements() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );
        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );
        TransactionHandle handle = new TransactionHandle( kernel, executionEngine,
                registry, uriScheme, StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( statements( new Statement( "query", map() ) ), output );

        // then
        verify( executionEngine ).execute( "query", map() );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( any( ExecutionResult.class ) );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldSuspendTransactionAndReleaseForOtherRequestsAfterExecutingStatements() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ),
                registry, uriScheme, StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( statements( new Statement( "query", map() ) ), output );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).suspendSinceTransactionsAreStillThreadBound();
        transactionOrder.verify( registry ).release( 1337l, handle );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( any( ExecutionResult.class ) );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldResumeTransactionWhenExecutingStatementsOnSecondRequest() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );

        TransactionHandle handle = new TransactionHandle( kernel, executionEngine, registry, uriScheme, StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        handle.execute( statements( new Statement( "query", map() ) ), output );
        reset( transactionContext, registry, executionEngine, output );

        // when
        handle.execute( statements( new Statement( "query", map() ) ), output );

        // then
        InOrder order = inOrder( transactionContext, registry, executionEngine );
        order.verify( transactionContext ).resumeSinceTransactionsAreStillThreadBound();
        order.verify( executionEngine ).execute( "query", map() );
        order.verify( transactionContext ).suspendSinceTransactionsAreStillThreadBound();
        order.verify( registry ).release( 1337l, handle );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( any( ExecutionResult.class ) );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldCommitTransactionAndTellRegistryToForgetItsHandle() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ),
                registry, uriScheme, StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.commit( statements( new Statement( "query", map() ) ), output );

        // then
        InOrder transactionOrder = inOrder( transactionContext, registry );
        transactionOrder.verify( transactionContext ).commit();
        transactionOrder.verify( registry ).forget( 1337l );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).statementResult( any( ExecutionResult.class ) );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldRollbackTransactionAndTellRegistryToForgetItsHandle() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

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
        KernelAPI kernel = mockKernel();
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );
        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        // when
        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ),
                registry, uriScheme, StringLogger.DEV_NULL );

        // then
        verifyZeroInteractions( kernel );

        // when
        handle.execute( statements( new Statement( "query", map() ) ), output );

        // then
        verify( kernel ).newTransactionContext();

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).statementResult( any( ExecutionResult.class ) );
        outputOrder.verify( output ).errors( argThat( hasNoErrors() ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldRollbackTransactionIfDeserializationErrorOccurs() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ), registry,
                uriScheme, StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( deserilizationErrors( new InvalidRequestError( "invalid request" ) ), output );

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337l );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).errors( argThat( hasErrors( 1 ) ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldRollbackTransactionIfExecutionErrorOccurs() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );
        when( executionEngine.execute( "query", map() ) ).thenThrow( new NullPointerException() );

        TransactionHandle handle = new TransactionHandle( kernel, executionEngine, registry, uriScheme, StringLogger.DEV_NULL );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.execute( statements( new Statement( "query", map() ) ), output );

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337l );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).transactionCommitUri( uriScheme.txCommitUri( 1337 ) );
        outputOrder.verify( output ).errors( argThat( hasErrors( 1 ) ) );
        outputOrder.verify( output ).finish();
        verifyNoMoreInteractions( output );
    }

    @Test
    public void shouldLogMessageIfCommitErrorOccurs() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();
        doThrow( new NullPointerException() ).when( transactionContext ).commit();

        StringLogger log = mock( StringLogger.class );

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ), registry, uriScheme, log );
        ExecutionResultSerializer output = mock( ExecutionResultSerializer.class );

        // when
        handle.commit( statements( new Statement( "query", map() ) ), output );

        // then
        verify( log ).error( eq( "Failed to commit transaction." ), any( NullPointerException.class ) );
        verify( registry ).forget( 1337l );

        InOrder outputOrder = inOrder( output );
        outputOrder.verify( output ).statementResult( any( ExecutionResult.class ) );
        outputOrder.verify( output ).errors( argThat( hasErrors( 1 ) ) );
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

    private KernelAPI mockKernel()
    {
        TransitionalTxManagementTransactionContext context = mock( TransitionalTxManagementTransactionContext.class );
        KernelAPI kernel = mock( KernelAPI.class );
        when( kernel.newTransactionContext() ).thenReturn( context );
        return kernel;
    }

    private static Matcher<Iterable<Neo4jError>> hasNoErrors()
    {
        return hasErrors( 0 );
    }

    private static Matcher<Iterable<Neo4jError>> hasErrors( final int errorCount )
    {
        return new TypeSafeMatcher<Iterable<Neo4jError>>()
        {
            @Override
            protected boolean matchesSafely( Iterable<Neo4jError> item )
            {
                int errors = 0;
                for ( Iterator<Neo4jError> iterator = item.iterator(); iterator.hasNext(); iterator.next() )
                {
                    errors++;
                }
                return errors == errorCount;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( errorCount ).appendText( "errors" );
            }
        };
    }
}
