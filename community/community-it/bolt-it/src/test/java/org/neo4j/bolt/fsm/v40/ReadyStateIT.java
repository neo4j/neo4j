/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.fsm.v40;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40.ABSENT_DB_NAME;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.testing.assertions.MapValueAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.hello;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.Signal;
import org.neo4j.bolt.protocol.v40.fsm.AutoCommitState;
import org.neo4j.bolt.protocol.v40.fsm.FailedState;
import org.neo4j.bolt.protocol.v40.fsm.InTransactionState;
import org.neo4j.bolt.protocol.v40.fsm.InterruptedState;
import org.neo4j.bolt.protocol.v40.messaging.request.BeginMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.RunMessage;
import org.neo4j.bolt.testing.messages.BoltV40Messages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;

class ReadyStateIT extends BoltStateMachineV4StateTestBase {
    @Test
    void shouldMoveToAutoCommitOnRun_succ() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachine();
        machine.process(hello(), nullResponseHandler());

        // When
        machine.process(new RunMessage("CREATE (n {k:'k'}) RETURN n.k", EMPTY_PARAMS), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse(
                        meta -> assertThat(meta).containsKey("fields").containsKey("t_first"));

        assertThat(machine).isInState(AutoCommitState.class);
    }

    @Test
    void shouldMoveToInTransactionOnBegin_succ() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachine();
        machine.process(hello(), nullResponseHandler());

        // When
        machine.process(new BeginMessage(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse();

        assertThat(machine).isInState(InTransactionState.class);
    }

    @Test
    void shouldMoveToInterruptedOnInterrupt() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachine();
        machine.process(hello(), nullResponseHandler());

        // When
        machine.process(Signal.INTERRUPT, recorder);

        // Then
        assertThat(machine).isInState(InterruptedState.class);
    }

    @Test
    void shouldMoveToFailedStateOnRun_fail() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachine();

        var runMessage = mock(RunMessage.class);
        when(runMessage.databaseName()).thenReturn(ABSENT_DB_NAME);
        when(runMessage.statement()).thenThrow(new RuntimeException("Fail"));

        machine.process(hello(), nullResponseHandler());

        // When
        machine.process(runMessage, recorder);

        // Then
        assertThat(recorder).hasFailureResponse(Status.General.UnknownError);
        assertThat(machine).isInState(FailedState.class);
    }

    @Test
    void shouldMoveToFailedStateOnBegin_fail() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachine();

        var beginMessage = mock(BeginMessage.class);
        when(beginMessage.databaseName()).thenReturn(ABSENT_DB_NAME);
        when(beginMessage.bookmarks()).thenThrow(new RuntimeException("Fail"));

        machine.process(hello(), nullResponseHandler());

        // When
        machine.process(beginMessage, recorder);

        // Then
        assertThat(recorder).hasFailureResponse(Status.General.UnknownError);

        assertThat(machine).isInState(FailedState.class);
    }

    @ParameterizedTest
    @MethodSource("illegalV4Messages")
    void shouldCloseConnectionOnIllegalV3Messages(RequestMessage message) throws Throwable {
        var recorder = new ResponseRecorder();
        var machine = newStateMachine();

        machine.process(hello(), nullResponseHandler());

        assertThat(machine)
                .shouldKillConnection(fsm -> fsm.process(message, recorder))
                .isInInvalidState();

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
    }

    private static Stream<RequestMessage> illegalV4Messages() throws BoltIOException {
        return Stream.of(
                hello(),
                BoltV40Messages.pull(),
                BoltV40Messages.discard(),
                BoltV40Messages.commit(),
                BoltV40Messages.rollback(),
                BoltV40Messages.goodbye());
    }
}
