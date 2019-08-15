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
package org.neo4j.bolt.v3.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v3.messaging.BoltV3Messages;
import org.neo4j.bolt.v3.messaging.request.InterruptSignal;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;

class InterruptedStateIT extends BoltStateMachineV3StateTestBase
{
    @Test
    void shouldMoveReadyOnReset_succ() throws Throwable
    {
        // Given
        BoltStateMachineV3 machine = getBoltStateMachineInInterruptedState();

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( ResetMessage.INSTANCE, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
    }

    @Test
    void shouldStayInInterruptedOnMoreReset() throws Throwable
    {
        // Given
        BoltStateMachineV3 machine = getBoltStateMachineInInterruptedState();
        machine.interrupt();
        machine.interrupt(); // need two reset to recover

        // When & Then
        machine.process( ResetMessage.INSTANCE, nullResponseHandler() );
        assertThat( machine.state(), instanceOf( InterruptedState.class ) );

        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( ResetMessage.INSTANCE, recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
    }

    @Test
    void shouldStayInInterruptedOnInterruptedSignal() throws Throwable
    {
        // Given
        BoltStateMachineV3 machine = getBoltStateMachineInInterruptedState();

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( InterruptSignal.INSTANCE, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( InterruptedState.class ) );
    }

    @ParameterizedTest
    @MethodSource( "illegalV3Messages" )
    void shouldCloseConnectionOnIllegalV3Messages( RequestMessage message ) throws Throwable
    {
        shouldCloseConnectionOnIllegalMessages( message );
    }

    private void shouldCloseConnectionOnIllegalMessages( RequestMessage message ) throws InterruptedException, BoltConnectionFatality
    {
        // Given
        BoltStateMachineV3 machine = getBoltStateMachineInInterruptedState();

        // when
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( message, recorder );

        // then
        assertThat( recorder.nextResponse(), wasIgnored() );
        assertThat( machine.state(), instanceOf( InterruptedState.class ) );
    }

    private BoltStateMachineV3 getBoltStateMachineInInterruptedState() throws BoltConnectionFatality
    {
        BoltStateMachineV3 machine = newStateMachine();
        machine.process( newHelloMessage(), nullResponseHandler() );
        machine.process( InterruptSignal.INSTANCE, nullResponseHandler() );
        assertThat( machine.state(), instanceOf( InterruptedState.class ) );
        return machine;
    }

    private static Stream<RequestMessage> illegalV3Messages() throws BoltIOException
    {
        // All messages except RESET
        return BoltV3Messages.supported().filter( it -> !it.equals( BoltV3Messages.reset() ) );
    }
}
