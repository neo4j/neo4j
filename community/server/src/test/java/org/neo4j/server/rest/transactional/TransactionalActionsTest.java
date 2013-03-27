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
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.error;
import static org.neo4j.server.rest.transactional.TransactionalActionsTest.TestStatementDeserializer.failingRequest;
import static org.neo4j.server.rest.transactional.TransactionalActionsTest.TestStatementDeserializer.request;
import static org.neo4j.server.rest.transactional.error.Neo4jError.Code.INVALID_REQUEST;
import static org.neo4j.server.rest.transactional.error.Neo4jError.Code.INVALID_TRANSACTION_ID;
import static org.neo4j.server.rest.transactional.error.Neo4jError.Code.UNKNOWN_COMMIT_ERROR;
import static org.neo4j.server.rest.transactional.error.Neo4jError.Code.UNKNOWN_ROLLBACK_ERROR;
import static org.neo4j.server.rest.transactional.error.Neo4jError.Code.UNKNOWN_STATEMENT_ERROR;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.StatementContext;
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
    private StatementContext stmtCtx;
    private ExecutionEngine engine;
    private ExecutionResult result;
    private long txId = 1337l;
    private TestLogger logger;

    static class TestResultHandler implements TransactionalActions.ResultHandler
    {

        // Used to validate that the contract of this interface is never violated.
        private enum State
        {
            BEFORE_BEGIN,
            RUNNING,
            FINISHED;
        }

        List<ExecutionResult> results = new ArrayList<ExecutionResult>();
        long txId;
        List<Neo4jError> errors = new ArrayList<Neo4jError>();

        private State state = State.BEFORE_BEGIN;

        @Override
        public void begin( long txId )
        {
            assertEquals( state, State.BEFORE_BEGIN );
            this.state = State.RUNNING;
            this.txId = txId;
        }

        @Override
        public void visitStatementResult( ExecutionResult result ) throws Neo4jError
        {
            assertEquals( state, State.RUNNING );
            results.add( result );
        }

        @Override
        public void finish( Iterator<Neo4jError> errors )
        {
            assertEquals( state, State.RUNNING );
            state = State.FINISHED;

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

        static TestStatementDeserializer request( Statement... statements ) throws UnsupportedEncodingException
        {
            return new TestStatementDeserializer( IteratorUtil.<Neo4jError>emptyIterator(), iterator( statements ) );
        }

        static TestStatementDeserializer failingRequest( Neo4jError... errors ) throws UnsupportedEncodingException
        {
            return new TestStatementDeserializer( iterator( errors ), IteratorUtil.<Statement>emptyIterator() );
        }

        public TestStatementDeserializer( Iterator<Neo4jError> errors, Iterator<Statement> statements ) throws
                UnsupportedEncodingException
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
        when( engine.execute( anyString(), anyMap() ) ).thenReturn( result );

        stmtCtx = mock( StatementContext.class );

        ctx = mock( TransactionContext.class );
        when( ctx.newStatementContext() ).thenReturn( stmtCtx );

        mess = mock( TransitionalPeriodTransactionMessContainer.class );
        when( mess.newTransactionContext() ).thenReturn( ctx );

        registry = mock( TransactionRegistry.class );
        when( registry.newId() ).thenReturn( txId );
        when( registry.pop( txId ) ).thenReturn( ctx );
    }

    @Test
    public void shouldStartExecuteAndCommitSingleStatementTransaction() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.commit( request( new Statement( "My Cypher Query", parameters ) ), results );

        // Then
        assertThat( results.results.get( 0 ), equalTo( result ) );
        assertThat( results.txId, equalTo( -1l ) );

        verify( mess ).newTransactionContext();
        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).newStatementContext();
        verify( stmtCtx ).close();
        verify( ctx ).commit();
        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
    }

    @Test
    public void shouldStartExecuteAndLeaveOpen() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.executeInNewTransaction( request( new Statement( "My Cypher Query", parameters ) ), txId, results );

        // Then
        assertThat( results.results.get( 0 ), equalTo( result ) );
        assertThat( results.txId, equalTo( txId ) );

        verify( mess ).newTransactionContext();
        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).newStatementContext();
        verify( stmtCtx ).close();
        verify( registry ).put( txId, ctx );
        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
    }

    @Test
    public void shouldResumeAndLeaveOpen() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.executeInExistingTransaction( request( new Statement( "My Cypher Query", parameters ) ), txId,
                results );

        // Then
        assertThat( results.results.get( 0 ), equalTo( result ) );
        assertThat( results.txId, equalTo( txId ) );

        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).newStatementContext();
        verify( stmtCtx ).close();
        verify( registry ).pop( txId );
        verify( registry ).put( txId, ctx );
        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
    }

    @Test
    public void shouldCommitRunningTx() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.commit( request( new Statement( "My Cypher Query", parameters ) ), txId, results );

        // Then
        assertThat( results.results.get( 0 ), equalTo( result ) );
        assertThat( results.txId, equalTo( txId ) );

        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).newStatementContext();
        verify( ctx ).commit();
        verify( stmtCtx ).close();
        verify( registry ).pop( txId );
        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
    }

    @Test
    public void shouldRollback() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );

        // When
        TestResultHandler results = new TestResultHandler();
        actions.rollback( txId, results );

        // Then
        assertThat( results.txId, equalTo( txId ) );
        assertThat( results.results.size(), equalTo( 0 ) );

        verify( registry ).pop( txId );
        verify( ctx ).rollback();
        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
    }

    //
    // Test Failure Scenarios
    //

    @Test
    public void shouldRollbackIfStatementFailsDuringCommit() throws Exception
    {
        // Given
        Throwable exception = new RuntimeException( "HA!" );
        when( engine.execute( anyString(), anyMap() ) ).thenThrow( exception );

        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.commit( request( new Statement( "My Cypher Query", parameters ) ), txId, results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );
        assertThat( results.txId, equalTo( txId ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( UNKNOWN_STATEMENT_ERROR ) );

        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).newStatementContext();
        verify( ctx ).rollback();
        verify( stmtCtx ).close();
        verify( registry ).pop( txId );
        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
    }

    @Test
    public void shouldRollbackIfStatementFails() throws Exception
    {
        // Given
        Throwable exception = new RuntimeException( "HA!" );
        when( engine.execute( anyString(), anyMap() ) ).thenThrow( exception );

        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.executeInExistingTransaction( request( new Statement( "My Cypher Query", parameters ) ), txId,
                results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );
        assertThat( results.txId, equalTo( txId ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( UNKNOWN_STATEMENT_ERROR ) );

        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).newStatementContext();
        verify( ctx ).rollback();
        verify( stmtCtx ).close();
        verify( registry ).pop( txId );
        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
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
        actions.commit( request( new Statement( "My Cypher Query", parameters ) ), txId, results );

        // Then
        assertThat( results.results.get( 0 ), equalTo( result ) );
        assertThat( results.txId, equalTo( txId ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( UNKNOWN_COMMIT_ERROR ) );

        verify( engine ).execute( "My Cypher Query", parameters );
        verify( ctx ).newStatementContext();
        verify( ctx ).commit();
        verify( stmtCtx ).close();
        verify( registry ).pop( txId );
        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
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
        actions.rollback( txId, results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );
        assertThat( results.txId, equalTo( txId ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( UNKNOWN_ROLLBACK_ERROR ) );

        verify( ctx ).rollback();
        verify( registry ).pop( txId );
        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
        logger.assertExactly( error( "Failed to rollback transaction.", exc ) );
    }

    @Test
    public void shouldReportErrorIfCreatingTransactionFails() throws Exception
    {
        // Given
        Throwable exc = new RuntimeException( "HA" );
        when( mess.newTransactionContext() ).thenThrow( exc );

        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();

        // When
        TestResultHandler results = new TestResultHandler();
        actions.commit( request( new Statement( "My Cypher Query", parameters ) ), results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );
        assertThat( results.txId, equalTo( -1l ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( Neo4jError.Code.UNABLE_TO_START_TRANSACTION ) );

        verify( mess ).newTransactionContext();
        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
        logger.assertExactly( error( "Failed to start transaction.", exc ) );
    }

    @Test
    public void shouldReportErrorIfTransactionHasBeenEvicted() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );
        Map<String, Object> parameters = map();
        long txIdThatDoesNotExist = 7331l;
        when( registry.pop( txIdThatDoesNotExist ) ).thenThrow( new InvalidTransactionIdError( "", null ) );

        // When
        TestResultHandler results = new TestResultHandler();
        actions.commit( request( new Statement( "My Cypher Query", parameters ) ), txIdThatDoesNotExist, results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );
        assertThat( results.txId, equalTo( txIdThatDoesNotExist ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( INVALID_TRANSACTION_ID ) );

        verify( registry ).pop( txIdThatDoesNotExist );

        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
        logger.assertNoLoggingOccurred();
    }

    @Test
    public void shouldReportErrorIfParsingRequestFails() throws Exception
    {
        // Given
        TransactionalActions actions = new TransactionalActions( mess, engine, registry, logger );

        // When
        TestResultHandler results = new TestResultHandler();
        actions.commit( failingRequest( new InvalidRequestError( "Lolwut" ) ), txId, results );

        // Then
        assertThat( results.results.size(), equalTo( 0 ) );
        assertThat( results.txId, equalTo( txId ) );
        assertThat( results.errors.get( 0 ).getErrorCode(), equalTo( INVALID_REQUEST ) );

        verify( registry ).pop( txId );
        verify( ctx ).rollback();

        verifyNoMoreInteractions( mess, registry, engine, ctx, stmtCtx );
        logger.assertNoLoggingOccurred();
    }
}
