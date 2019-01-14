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
package org.neo4j.bolt.v1.runtime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.MutableConnectionState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.v1.messaging.request.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.InterruptSignal;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.graphdb.security.AuthorizationExpiredException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

class StreamingStateTest
{
    private final StreamingState state = new StreamingState();

    private final BoltStateMachineState readyState = mock( BoltStateMachineState.class );
    private final BoltStateMachineState interruptedState = mock( BoltStateMachineState.class );
    private final BoltStateMachineState failedState = mock( BoltStateMachineState.class );

    private final StateMachineContext context = mock( StateMachineContext.class );
    private final MutableConnectionState connectionState = new MutableConnectionState();

    @BeforeEach
    void setUp()
    {
        state.setReadyState( readyState );
        state.setInterruptedState( interruptedState );
        state.setFailedState( failedState );

        when( context.connectionState() ).thenReturn( connectionState );
    }

    @Test
    void shouldThrowWhenNotInitialized() throws Exception
    {
        StreamingState state = new StreamingState();

        assertThrows( IllegalStateException.class, () -> state.process( PullAllMessage.INSTANCE, context ) );

        state.setReadyState( readyState );
        assertThrows( IllegalStateException.class, () -> state.process( PullAllMessage.INSTANCE, context ) );

        state.setReadyState( null );
        state.setInterruptedState( interruptedState );
        assertThrows( IllegalStateException.class, () -> state.process( PullAllMessage.INSTANCE, context ) );

        state.setInterruptedState( null );
        state.setFailedState( failedState );
        assertThrows( IllegalStateException.class, () -> state.process( PullAllMessage.INSTANCE, context ) );
    }

    @Test
    void shouldProcessPullAllMessage() throws Exception
    {
        StatementProcessor statementProcessor = mock( StatementProcessor.class );
        connectionState.setStatementProcessor( statementProcessor );

        BoltStateMachineState nextState = state.process( PullAllMessage.INSTANCE, context );

        assertEquals( readyState, nextState );
        verify( statementProcessor ).streamResult( any() );
    }

    @Test
    void shouldHandleAuthErrorWhenProcessingPullAllMessage() throws Exception
    {
        AuthorizationExpiredException error = new AuthorizationExpiredException( "Hello" );

        StatementProcessor statementProcessor = mock( StatementProcessor.class );
        doThrow( error ).when( statementProcessor ).streamResult( any() );
        connectionState.setStatementProcessor( statementProcessor );

        BoltStateMachineState nextState = state.process( PullAllMessage.INSTANCE, context );

        assertEquals( failedState, nextState );
        verify( context ).handleFailure( error, true );
    }

    @Test
    void shouldHandleErrorWhenProcessingPullAllMessage() throws Exception
    {
        RuntimeException error = new RuntimeException( "Hello" );

        StatementProcessor statementProcessor = mock( StatementProcessor.class );
        doThrow( error ).when( statementProcessor ).streamResult( any() );
        connectionState.setStatementProcessor( statementProcessor );

        BoltStateMachineState nextState = state.process( PullAllMessage.INSTANCE, context );

        assertEquals( failedState, nextState );
        verify( context ).handleFailure( error, false );
    }

    @Test
    void shouldProcessDiscardAllMessage() throws Exception
    {
        StatementProcessor statementProcessor = mock( StatementProcessor.class );
        connectionState.setStatementProcessor( statementProcessor );

        BoltStateMachineState nextState = state.process( DiscardAllMessage.INSTANCE, context );

        assertEquals( readyState, nextState );
        verify( statementProcessor ).streamResult( any() );
    }

    @Test
    void shouldHandleAuthErrorWhenProcessingDiscardAllMessage() throws Exception
    {
        AuthorizationExpiredException error = new AuthorizationExpiredException( "Hello" );

        StatementProcessor statementProcessor = mock( StatementProcessor.class );
        doThrow( error ).when( statementProcessor ).streamResult( any() );
        connectionState.setStatementProcessor( statementProcessor );

        BoltStateMachineState nextState = state.process( DiscardAllMessage.INSTANCE, context );

        assertEquals( failedState, nextState );
        verify( context ).handleFailure( error, true );
    }

    @Test
    void shouldHandleErrorWhenProcessingDiscardAllMessage() throws Exception
    {
        RuntimeException error = new RuntimeException( "Hello" );

        StatementProcessor statementProcessor = mock( StatementProcessor.class );
        doThrow( error ).when( statementProcessor ).streamResult( any() );
        connectionState.setStatementProcessor( statementProcessor );

        BoltStateMachineState nextState = state.process( DiscardAllMessage.INSTANCE, context );

        assertEquals( failedState, nextState );
        verify( context ).handleFailure( error, false );
    }

    @Test
    void shouldProcessResetMessage() throws Exception
    {
        when( context.resetMachine() ).thenReturn( true ); // reset successful

        BoltStateMachineState newState = state.process( ResetMessage.INSTANCE, context );

        assertEquals( readyState, newState );
    }

    @Test
    void shouldHandleResetMessageFailure() throws Exception
    {
        when( context.resetMachine() ).thenReturn( false ); // reset failed

        BoltStateMachineState newState = state.process( ResetMessage.INSTANCE, context );

        assertEquals( failedState, newState );
    }

    @Test
    void shouldProcessInterruptMessage() throws Exception
    {
        BoltStateMachineState newState = state.process( InterruptSignal.INSTANCE, context );

        assertEquals( interruptedState, newState );
    }

    @Test
    void shouldNotProcessUnsupportedMessages() throws Exception
    {
        List<RequestMessage> unsupportedMessages =
                asList( AckFailureMessage.INSTANCE, new RunMessage( "RETURN 1", EMPTY_MAP ), new InitMessage( "Driver 2.5", emptyMap() ) );

        for ( RequestMessage message : unsupportedMessages )
        {
            assertNull( state.process( message, context ) );
        }
    }
}
