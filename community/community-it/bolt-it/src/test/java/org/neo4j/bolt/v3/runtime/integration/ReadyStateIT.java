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
package org.neo4j.bolt.v3.runtime.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.InterruptSignal;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.bolt.v3.runtime.FailedState;
import org.neo4j.bolt.v3.runtime.InterruptedState;
import org.neo4j.bolt.v3.runtime.StreamingState;
import org.neo4j.bolt.v3.runtime.TransactionReadyState;
import org.neo4j.kernel.api.exceptions.Status;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.verifyKillsConnection;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.GoodbyeMessage.GOODBYE_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.RollbackMessage.ROLLBACK_MESSAGE;

class ReadyStateIT extends BoltStateMachineStateTestBase
{
    @Test
    void shouldMoveToStreamingOnRun_succ() throws Throwable
    {
        // Given
        BoltStateMachineV3 machine = newStateMachine();
        machine.process( newHelloMessage(), nullResponseHandler() );

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( new RunMessage( "CREATE (n {k:'k'}) RETURN n.k", EMPTY_PARAMS ), recorder );

        // Then

        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertTrue( response.hasMetadata( "fields" ) );
        assertTrue( response.hasMetadata( "t_first") );
        assertThat( machine.state(), instanceOf( StreamingState.class ) );
    }

    @Test
    void shouldMoveToStreamingOnBegin_succ() throws Throwable
    {
        // Given
        BoltStateMachineV3 machine = newStateMachine();
        machine.process( newHelloMessage(), nullResponseHandler() );

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( new BeginMessage(), recorder );

        // Then
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertThat( machine.state(), instanceOf( TransactionReadyState.class ) );
    }

    @Test
    void shouldMoveToInterruptedOnInterrupt() throws Throwable
    {
        // Given
        BoltStateMachineV3 machine = newStateMachine();
        machine.process( newHelloMessage(), nullResponseHandler() );

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( InterruptSignal.INSTANCE, recorder );

        // Then
        assertThat( machine.state(), instanceOf( InterruptedState.class ) );
    }

    @Test
    void shouldMoveToFailedStateOnRun_fail() throws Throwable
    {
        // Given
        BoltStateMachineV3 machine = newStateMachine();
        machine.process( newHelloMessage(), nullResponseHandler() );

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        RunMessage runMessage = mock( RunMessage.class );
        when( runMessage.statement() ).thenThrow( new RuntimeException( "Fail" ) );
        machine.process( runMessage, recorder );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.General.UnknownError ) );
        assertThat( machine.state(), instanceOf( FailedState.class ) );
    }

    @Test
    void shouldMoveToFailedStateOnBegin_fail() throws Throwable
    {
        // Given
        BoltStateMachineV3 machine = newStateMachine();
        machine.process( newHelloMessage(), nullResponseHandler() );

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        BeginMessage beginMessage = mock( BeginMessage.class );
        when( beginMessage.bookmark() ).thenThrow( new RuntimeException( "Fail" ) );
        machine.process( beginMessage, recorder );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.General.UnknownError ) );
        assertThat( machine.state(), instanceOf( FailedState.class ) );
    }

    @ParameterizedTest
    @MethodSource( "illegalV3Messages" )
    void shouldCloseConnectionOnIllegalV3Messages( RequestMessage message ) throws Throwable
    {
        shouldCloseConnectionOnIllegalMessages( message );
    }

    @ParameterizedTest
    @MethodSource( "illegalV2Messages" )
    void shouldCloseConnectionOnIllegalV2Messages( RequestMessage message ) throws Throwable
    {
        shouldCloseConnectionOnIllegalMessages( message );
    }

    private void shouldCloseConnectionOnIllegalMessages( RequestMessage message ) throws InterruptedException, BoltConnectionFatality
    {
        // Given
        BoltStateMachineV3 machine = newStateMachine();
        machine.process( newHelloMessage(), nullResponseHandler() );

        // when
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.process( message, recorder ) );

        // then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
        assertNull( machine.state() );
    }

    private static Stream<RequestMessage> illegalV3Messages()
    {
        return Stream.of( newHelloMessage(), DiscardAllMessage.INSTANCE, PullAllMessage.INSTANCE, COMMIT_MESSAGE, ROLLBACK_MESSAGE, GOODBYE_MESSAGE );
    }

    private static Stream<RequestMessage> illegalV2Messages()
    {
        return Stream.of( new InitMessage( USER_AGENT, emptyMap() ), new org.neo4j.bolt.v1.messaging.request.RunMessage( "RETURN 1", EMPTY_PARAMS ) );
    }
}
