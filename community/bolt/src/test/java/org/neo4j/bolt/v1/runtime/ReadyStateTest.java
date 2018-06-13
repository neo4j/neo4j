/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.time.Clock;
import java.util.List;

import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.MutableConnectionState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.runtime.StateMachineMessage;
import org.neo4j.bolt.runtime.StatementMetadata;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.v1.messaging.AckFailure;
import org.neo4j.bolt.v1.messaging.DiscardAll;
import org.neo4j.bolt.v1.messaging.Interrupt;
import org.neo4j.bolt.v1.messaging.PullAll;
import org.neo4j.bolt.v1.messaging.Reset;
import org.neo4j.bolt.v1.messaging.Run;
import org.neo4j.graphdb.security.AuthorizationExpiredException;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

class ReadyStateTest
{
    private final ReadyState state = new ReadyState();

    private final BoltStateMachineState streamingState = mock( BoltStateMachineState.class );
    private final BoltStateMachineState interruptedState = mock( BoltStateMachineState.class );
    private final BoltStateMachineState failedState = mock( BoltStateMachineState.class );

    private final StateMachineContext context = mock( StateMachineContext.class );
    private final MutableConnectionState connectionState = new MutableConnectionState();

    @BeforeEach
    void setUp()
    {
        state.setStreamingState( streamingState );
        state.setInterruptedState( interruptedState );
        state.setFailedState( failedState );

        when( context.connectionState() ).thenReturn( connectionState );
        when( context.clock() ).thenReturn( Clock.systemUTC() );
    }

    @Test
    void shouldThrowWhenNotInitialized() throws Exception
    {
        ReadyState state = new ReadyState();

        assertThrows( IllegalStateException.class, () -> state.process( PullAll.INSTANCE, context ) );

        state.setStreamingState( streamingState );
        assertThrows( IllegalStateException.class, () -> state.process( PullAll.INSTANCE, context ) );

        state.setStreamingState( null );
        state.setInterruptedState( interruptedState );
        assertThrows( IllegalStateException.class, () -> state.process( PullAll.INSTANCE, context ) );

        state.setInterruptedState( null );
        state.setFailedState( failedState );
        assertThrows( IllegalStateException.class, () -> state.process( PullAll.INSTANCE, context ) );
    }

    @Test
    void shouldProcessRunMessage() throws Exception
    {
        StatementMetadata statementMetadata = mock( StatementMetadata.class );
        when( statementMetadata.fieldNames() ).thenReturn( new String[]{"foo", "bar", "baz"} );
        StatementProcessor statementProcessor = mock( StatementProcessor.class );
        when( statementProcessor.run( any(), any() ) ).thenReturn( statementMetadata );
        connectionState.setStatementProcessor( statementProcessor );

        BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
        connectionState.setResponseHandler( responseHandler );

        BoltStateMachineState nextState = state.process( new Run( "RETURN 1", EMPTY_MAP ), context );

        assertEquals( streamingState, nextState );
        verify( statementProcessor ).run( "RETURN 1", EMPTY_MAP );
        verify( responseHandler ).onMetadata( "fields", stringArray( "foo", "bar", "baz" ) );
        verify( responseHandler ).onMetadata( eq( "result_available_after" ), any() );
    }

    @Test
    void shouldHandleAuthFailureDuringRunMessageProcessing() throws Exception
    {
        AuthorizationExpiredException error = new AuthorizationExpiredException( "Hello" );

        StatementProcessor statementProcessor = mock( StatementProcessor.class );
        when( statementProcessor.run( any(), any() ) ).thenThrow( error );
        connectionState.setStatementProcessor( statementProcessor );

        BoltStateMachineState nextState = state.process( new Run( "RETURN 1", EMPTY_MAP ), context );

        assertEquals( failedState, nextState );
        verify( context ).handleFailure( error, true );
    }

    @Test
    void shouldHandleFailureDuringRunMessageProcessing() throws Exception
    {
        RuntimeException error = new RuntimeException( "Hello" );

        StatementProcessor statementProcessor = mock( StatementProcessor.class );
        when( statementProcessor.run( any(), any() ) ).thenThrow( error );
        connectionState.setStatementProcessor( statementProcessor );

        BoltStateMachineState nextState = state.process( new Run( "RETURN 1", EMPTY_MAP ), context );

        assertEquals( failedState, nextState );
        verify( context ).handleFailure( error, false );
    }

    @Test
    void shouldProcessResetMessage() throws Exception
    {
        when( context.resetMachine() ).thenReturn( true ); // reset successful

        BoltStateMachineState newState = state.process( Reset.INSTANCE, context );

        assertEquals( state, newState );
    }

    @Test
    void shouldHandleFailureDuringResetMessageProcessing() throws Exception
    {
        when( context.resetMachine() ).thenReturn( false ); // reset failed

        BoltStateMachineState newState = state.process( Reset.INSTANCE, context );

        assertEquals( failedState, newState );
    }

    @Test
    void shouldProcessInterruptMessage() throws Exception
    {
        BoltStateMachineState newState = state.process( Interrupt.INSTANCE, context );

        assertEquals( interruptedState, newState );
    }

    @Test
    void shouldNotProcessUnsupportedMessages() throws Exception
    {
        List<StateMachineMessage> unsupportedMessages = asList( PullAll.INSTANCE, DiscardAll.INSTANCE, AckFailure.INSTANCE );

        for ( StateMachineMessage message : unsupportedMessages )
        {
            assertNull( state.process( message, context ) );
        }
    }
}
