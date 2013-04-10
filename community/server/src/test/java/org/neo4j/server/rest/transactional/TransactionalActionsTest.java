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

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.anyMapOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.error;
import static org.neo4j.server.rest.transactional.TransactionalActionsTest.TestStatementDeserializer.failingRequest;
import static org.neo4j.server.rest.transactional.TransactionalActionsTest.TestStatementDeserializer.request;
import static org.neo4j.server.rest.transactional.error.Neo4jError.Code.INVALID_REQUEST;
import static org.neo4j.server.rest.transactional.error.Neo4jError.Code.UNKNOWN_COMMIT_ERROR;
import static org.neo4j.server.rest.transactional.error.Neo4jError.Code.UNKNOWN_ROLLBACK_ERROR;
import static org.neo4j.server.rest.transactional.error.Neo4jError.Code.UNKNOWN_STATEMENT_ERROR;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.server.rest.transactional.error.InvalidRequestError;
import org.neo4j.server.rest.transactional.error.InvalidTransactionIdError;
import org.neo4j.server.rest.transactional.error.Neo4jError;

public class TransactionalActionsTest
{
    private TransactionRegistry registry;
    private TransitionalPeriodTransactionMessContainer mess;
    private TransactionContext ctx;
    private ExecutionEngine engine;
    private ExecutionResult result;
    private long txId = 1337l;
    private TestLogger logger;

    static class TestResultHandler implements TransactionalActions.ResultHandler
    {
        // Used to validate that the contract of this interface is never violated.
        private enum State
        {
            BEFORE_PROLOGUE,
            AFTER_PROLOGUE,
            AFTER_EPILOGUE
        }

        List<ExecutionResult> results = new ArrayList<ExecutionResult>();
        long txId;
        List<Neo4jError> errors = new ArrayList<Neo4jError>();

        private State state = State.BEFORE_PROLOGUE;

        @Override
        public void prologue( long txId )
        {
            prologue();
            this.txId = txId;
        }

        @Override
        public void prologue()
        {
            assertEquals( state, State.BEFORE_PROLOGUE );
            this.state = State.AFTER_PROLOGUE;
        }

        @Override
        public void visitStatementResult( ExecutionResult result ) throws Neo4jError
        {
            assertEquals( state, State.AFTER_PROLOGUE );
            results.add( result );
        }

        @Override
        public void epilogue( Iterator<Neo4jError> errors )
        {
            assertEquals( state, State.AFTER_PROLOGUE );
            state = State.AFTER_EPILOGUE;

            while ( errors.hasNext() )
            {
                this.errors.add( errors.next() );
            }
        }
    }

    static class TestStatementDeserializer extends StatementDeserializer
    {

        private final Iterator<Statement> statements;
        private final Iterator<Neo4jError> errors;

        static TestStatementDeserializer request( Statement... statements )
        {
            return new TestStatementDeserializer( IteratorUtil.<Neo4jError>emptyIterator(), iterator( statements ) );
        }

        static TestStatementDeserializer failingRequest( Neo4jError... errors )
        {
            return new TestStatementDeserializer( iterator( errors ), IteratorUtil.<Statement>emptyIterator() );
        }

        public TestStatementDeserializer( Iterator<Neo4jError> errors, Iterator<Statement> statements )
        {
            super( new ByteArrayInputStream( new byte[]{} ) );
            this.statements = statements;
            this.errors = errors;
        }

        @Override
        public boolean hasNext()
        {
            return statements.hasNext();
        }

        @Override
        public Statement next()
        {
            return statements.next();
        }

        @Override
        public Iterator<Neo4jError> errors()
        {
            return errors;
        }
    }

    @Before
    public void mockDependencies() throws Neo4jError
    {
        logger = new TestLogger();
        result = mock( ExecutionResult.class );
        engine = mock( ExecutionEngine.class );
        when( engine.execute( anyString(), anyMapOf( String.class, Object.class ) ) ).thenReturn( result );

        ctx = mock( TransactionContext.class );

        mess = mock( TransitionalPeriodTransactionMessContainer.class );
        when( mess.newTransactionContext() ).thenReturn( ctx );

        registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( txId );
        when( registry.resume( txId ) ).thenReturn( ctx );
    }

    @Test
    public void shouldStartExecuteAndCommitSingleStatementTransaction() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.newTransaction().commit( request( new Statement( "My Cypher Query", parameters ) ), results );

        // Then
        assertThat( results.results.get( 0 ), equalTo( result ) );

        verify( mess ).newTransactionContext();
        verify( engine ).execute( "My Cypher Query", parameters );
        verify( registry ).begin();
        verify( registry ).finish( txId );
        verify( ctx ).commit();
        verifyNoMoreInteractions( mess, registry, engine, ctx );
    }

    @Test
    public void shouldStartExecuteAndLeaveOpen() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.newTransaction().execute( request( new Statement( "My Cypher Query", parameters ) ), results );

        // Then
        assertThat( results.results.get( 0 ), equalTo( result ) );
        assertThat( results.txId, equalTo( txId ) );

        verify( mess ).newTransactionContext();
        verify( engine ).execute( "My Cypher Query", parameters );
        verify( registry ).begin();
        verify( registry ).suspend( txId, ctx );
        verifyNoMoreInteractions( mess, registry, engine, ctx );
    }

    @Test
    public void shouldResumeAndLeaveOpen() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.findTransaction( txId ).execute( request( new Statement( "My Cypher Query", parameters ) ), results );

        // Then
        assertThat( results.results.get( 0 ), equalTo( result ) );
        assertThat( results.txId, equalTo( txId ) );

        verify( engine ).execute( "My Cypher Query", parameters );
        verify( registry ).resume( txId );
        verify( registry ).suspend( txId, ctx );
        verifyNoMoreInteractions( mess, registry, engine, ctx );
    }

    @Test
    public void shouldCommitRunningTx() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.findTransaction( txId ).commit( request( new Statement( "My Cypher Query", parameters ) ), results );

        // Then
        assertThat( results.results.get( 0 ), equalTo( result ) );

        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).commit();
        verify( registry ).resume( txId );
        verify( registry ).finish( txId );
        verifyNoMoreInteractions( mess, registry, engine, ctx );
    }

    @Test
    public void shouldRollback() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );

        // When
        TestResultHandler results = new TestResultHandler();
        actions.findTransaction( txId ).rollback( results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );

        verify( registry ).resume( txId );
        verify( registry ).finish( txId );
        verify( ctx ).rollback();
        verifyNoMoreInteractions( mess, registry, engine, ctx );
    }

    @Test
    public void shouldRollbackIfStatementFailsDuringCommit() throws Exception
    {
        // Given
        Throwable exception = new RuntimeException( "HA!" );
        when( engine.execute( anyString(), anyMapOf( String.class, Object.class ) ) ).thenThrow( exception );

        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.findTransaction( txId ).commit( request( new Statement( "My Cypher Query", parameters ) ), results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( UNKNOWN_STATEMENT_ERROR ) );

        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).rollback();
        verify( registry ).resume( txId );
        verify( registry ).finish( txId );
        verifyNoMoreInteractions( mess, registry, engine, ctx );
    }

    @Test
    public void shouldRollbackIfStatementFails() throws Exception
    {
        // Given
        Throwable exception = new RuntimeException( "HA!" );
        when( engine.execute( anyString(), anyMapOf( String.class, Object.class ) ) ).thenThrow( exception );

        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.findTransaction( txId ).execute( request( new Statement( "My Cypher Query", parameters ) ), results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );
        assertThat( results.txId, equalTo( txId ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( UNKNOWN_STATEMENT_ERROR ) );

        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).rollback();
        verify( registry ).resume( txId );
        verify( registry ).finish( txId );
        verifyNoMoreInteractions( mess, registry, engine, ctx );
    }

    @Test
    public void shouldReportErrorIfCommitFails() throws Exception
    {
        // Given
        Throwable exc = new RuntimeException( "HA" );
        doThrow( exc ).when( ctx ).commit();

        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.findTransaction( txId ).commit( request( new Statement( "My Cypher Query", parameters ) ), results );

        // Then
        assertThat( results.results.get( 0 ), equalTo( result ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( UNKNOWN_COMMIT_ERROR ) );

        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).commit();
        verify( registry ).resume( txId );
        verify( registry ).finish( txId );
        verifyNoMoreInteractions( mess, registry, engine, ctx );
        logger.assertExactly( error( "Failed to commit transaction.", exc ) );
    }

    @Test
    public void shouldReportErrorIfRollbackFails() throws Exception
    {
        // Given
        Throwable exc = new RuntimeException( "HA" );
        doThrow( exc ).when( ctx ).rollback();

        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );

        // When
        TestResultHandler results = new TestResultHandler();
        actions.findTransaction( txId ).rollback( results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( UNKNOWN_ROLLBACK_ERROR ) );

        verify( ctx ).rollback();
        verify( registry ).resume( txId );
        verify( registry ).finish( txId );
        verifyNoMoreInteractions( mess, registry, engine, ctx );
        logger.assertExactly( error( "Failed to rollback transaction.", exc ) );
    }

    @Test
    public void shouldReportErrorIfCreatingTransactionFails() throws Exception
    {
        // Given
        Throwable exc = new RuntimeException( "HA" );
        when( mess.newTransactionContext() ).thenThrow( exc );

        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );

        // When
        try
        {
            actions.newTransaction();
            fail( "should have thrown exception" );
        }
        catch ( Neo4jError neo4jError )
        {
            // Then
            assertEquals( Neo4jError.Code.UNABLE_TO_START_TRANSACTION, neo4jError.getErrorCode() );
        }
    }

    @Test
    public void shouldReportErrorIfTransactionHasBeenEvicted() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        long txIdThatDoesNotExist = 7331l;
        when( registry.resume( txIdThatDoesNotExist ) ).thenThrow( new InvalidTransactionIdError( "" ) );

        // When
        try
        {
            actions.findTransaction( txIdThatDoesNotExist );
            fail( "should have thrown exception" );
        }
        catch ( Neo4jError neo4jError )
        {
            // Then
            assertEquals( Neo4jError.Code.INVALID_TRANSACTION_ID, neo4jError.getErrorCode() );
        }
    }

    @Test
    public void shouldReportErrorIfParsingRequestFails() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );

        // When
        TestResultHandler results = new TestResultHandler();
        actions.findTransaction( txId ).commit( failingRequest( new InvalidRequestError( "Lolwut" ) ), results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( INVALID_REQUEST ) );

        verify( registry ).resume( txId );
        verify( registry ).finish( txId );
        verify( ctx ).rollback();

        verifyNoMoreInteractions( mess, registry, engine, ctx );
        logger.assertNoLoggingOccurred();
    }
}
