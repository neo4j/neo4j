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

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.MutableConnectionState;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.v1.messaging.request.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InterruptSignal;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

class FailedStateTest
{
    private final FailedState state = new FailedState();

    private final BoltStateMachineState readyState = mock( BoltStateMachineState.class );
    private final BoltStateMachineState interruptedState = mock( BoltStateMachineState.class );

    private final StateMachineContext context = mock( StateMachineContext.class );
    private final MutableConnectionState connectionState = new MutableConnectionState();

    @BeforeEach
    void setUp()
    {
        state.setReadyState( readyState );
        state.setInterruptedState( interruptedState );

        when( context.connectionState() ).thenReturn( connectionState );
    }

    @Test
    void shouldThrowWhenNotInitialized() throws Exception
    {
        FailedState state = new FailedState();

        assertThrows( IllegalStateException.class, () -> state.process( AckFailureMessage.INSTANCE, context ) );

        state.setReadyState( readyState );
        assertThrows( IllegalStateException.class, () -> state.process( AckFailureMessage.INSTANCE, context ) );

        state.setReadyState( null );
        state.setInterruptedState( interruptedState );
        assertThrows( IllegalStateException.class, () -> state.process( AckFailureMessage.INSTANCE, context ) );
    }

    @Test
    void shouldProcessRunMessage() throws Exception
    {
        BoltStateMachineState newState = state.process( new RunMessage( "RETURN 1", EMPTY_MAP ), context );

        assertEquals( state, newState ); // remains in failed state
        assertTrue( connectionState.hasPendingIgnore() );
    }

    @Test
    void shouldProcessPullAllMessage() throws Exception
    {
        BoltStateMachineState newState = state.process( PullAllMessage.INSTANCE, context );

        assertEquals( state, newState ); // remains in failed state
        assertTrue( connectionState.hasPendingIgnore() );
    }

    @Test
    void shouldProcessDiscardAllMessage() throws Exception
    {
        BoltStateMachineState newState = state.process( DiscardAllMessage.INSTANCE, context );

        assertEquals( state, newState ); // remains in failed state
        assertTrue( connectionState.hasPendingIgnore() );
    }

    @Test
    void shouldProcessAckFailureMessageWithPendingIgnore() throws Exception
    {
        connectionState.markIgnored();
        assertTrue( connectionState.hasPendingIgnore() );

        BoltStateMachineState newState = state.process( AckFailureMessage.INSTANCE, context );

        assertEquals( readyState, newState );
        assertFalse( connectionState.hasPendingIgnore() );
    }

    @Test
    void shouldProcessAckFailureMessageWithPendingError() throws Exception
    {
        Neo4jError error = Neo4jError.from( new RuntimeException() );
        connectionState.markFailed( error );
        assertEquals( error, connectionState.getPendingError() );

        BoltStateMachineState newState = state.process( AckFailureMessage.INSTANCE, context );

        assertEquals( readyState, newState );
        assertNull( connectionState.getPendingError() );
    }

    @Test
    void shouldProcessResetMessageWithPerndingIgnore() throws Exception
    {
        when( context.resetMachine() ).thenReturn( true ); // reset successful
        connectionState.markIgnored();
        assertTrue( connectionState.hasPendingIgnore() );

        BoltStateMachineState newState = state.process( ResetMessage.INSTANCE, context );

        assertEquals( readyState, newState );
        assertFalse( connectionState.hasPendingIgnore() );
    }

    @Test
    void shouldProcessResetMessageWithPerndingError() throws Exception
    {
        when( context.resetMachine() ).thenReturn( true ); // reset successful
        Neo4jError error = Neo4jError.from( new RuntimeException() );
        connectionState.markFailed( error );
        assertEquals( error, connectionState.getPendingError() );

        BoltStateMachineState newState = state.process( ResetMessage.INSTANCE, context );

        assertEquals( readyState, newState );
        assertNull( connectionState.getPendingError() );
    }

    @Test
    void shouldHandleResetMessageFailure() throws Exception
    {
        when( context.resetMachine() ).thenReturn( false ); // reset failed

        BoltStateMachineState newState = state.process( ResetMessage.INSTANCE, context );

        assertEquals( state, newState ); // remains in failed state
    }

    @Test
    void shouldProcessInterruptMessage() throws Exception
    {
        BoltStateMachineState newState = state.process( InterruptSignal.INSTANCE, context );

        assertEquals( interruptedState, newState );
    }

    @Test
    void shouldNotProcessUnsupportedMessage() throws Exception
    {
        RequestMessage unsupportedMessage = mock( RequestMessage.class );

        BoltStateMachineState newState = state.process( unsupportedMessage, context );

        assertNull( newState );
    }
}
